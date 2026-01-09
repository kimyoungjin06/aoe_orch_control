//! tmux session management

use anyhow::{bail, Result};
use std::process::Command;

use super::{session_exists_from_cache, SESSION_PREFIX};
use crate::process::{self, ProcessInputState};
use crate::session::Status;

const SPINNER_CHARS: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

fn strip_ansi(content: &str) -> String {
    let mut result = content.to_string();

    // Remove CSI sequences: ESC [ ... letter
    while let Some(start) = result.find("\x1b[") {
        let rest = &result[start + 2..];
        let end_offset = rest
            .find(|c: char| c.is_ascii_alphabetic())
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        result = format!("{}{}", &result[..start], &result[start + 2 + end_offset..]);
    }

    // Remove OSC sequences: ESC ] ... BEL
    while let Some(start) = result.find("\x1b]") {
        if let Some(end) = result[start..].find('\x07') {
            result = format!("{}{}", &result[..start], &result[start + end + 1..]);
        } else {
            break;
        }
    }

    result
}

pub struct Session {
    name: String,
}

impl Session {
    pub fn new(id: &str, title: &str) -> Result<Self> {
        Ok(Self {
            name: Self::generate_name(id, title),
        })
    }

    pub fn generate_name(id: &str, title: &str) -> String {
        let safe_title = sanitize_session_name(title);
        let short_id = if id.len() > 8 { &id[..8] } else { id };
        format!("{}{}_{}", SESSION_PREFIX, safe_title, short_id)
    }

    pub fn exists(&self) -> bool {
        // Try cache first
        if let Some(exists) = session_exists_from_cache(&self.name) {
            return exists;
        }

        // Fallback to direct check
        Command::new("tmux")
            .args(["has-session", "-t", &self.name])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    pub fn create(&self, working_dir: &str, command: Option<&str>) -> Result<()> {
        if self.exists() {
            return Ok(());
        }

        let mut args = vec![
            "new-session".to_string(),
            "-d".to_string(),
            "-s".to_string(),
            self.name.clone(),
            "-c".to_string(),
            working_dir.to_string(),
        ];

        if let Some(cmd) = command {
            args.push(cmd.to_string());
        }

        let output = Command::new("tmux").args(&args).output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("Failed to create tmux session: {}", stderr);
        }

        // Register in cache
        super::refresh_session_cache();

        Ok(())
    }

    pub fn kill(&self) -> Result<()> {
        if !self.exists() {
            return Ok(());
        }

        let output = Command::new("tmux")
            .args(["kill-session", "-t", &self.name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("Failed to kill tmux session: {}", stderr);
        }

        Ok(())
    }

    pub fn attach(&self) -> Result<()> {
        if !self.exists() {
            bail!("Session does not exist: {}", self.name);
        }

        // Check if we're already in tmux
        if std::env::var("TMUX").is_ok() {
            // Switch to session
            let status = Command::new("tmux")
                .args(["switch-client", "-t", &self.name])
                .status()?;

            if !status.success() {
                bail!("Failed to switch to tmux session");
            }
        } else {
            // Attach to session
            let status = Command::new("tmux")
                .args(["attach-session", "-t", &self.name])
                .status()?;

            if !status.success() {
                bail!("Failed to attach to tmux session");
            }
        }

        Ok(())
    }

    pub fn capture_pane(&self, lines: usize) -> Result<String> {
        if !self.exists() {
            return Ok(String::new());
        }

        let output = Command::new("tmux")
            .args([
                "capture-pane",
                "-t",
                &self.name,
                "-p",
                "-S",
                &format!("-{}", lines),
            ])
            .output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Ok(String::new())
        }
    }

    pub fn get_pane_pid(&self) -> Option<u32> {
        process::get_pane_pid(&self.name)
    }

    pub fn get_foreground_pid(&self) -> Option<u32> {
        let pane_pid = self.get_pane_pid()?;
        process::get_foreground_pid(pane_pid).or(Some(pane_pid))
    }

    pub fn detect_status(&self, tool: &str) -> Result<Status> {
        let content = self.capture_pane(50)?;
        let fg_pid = self.get_foreground_pid();
        Ok(detect_status_from_content(&content, tool, fg_pid))
    }
}

fn sanitize_session_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(20)
        .collect()
}

fn detect_status_from_content(content: &str, tool: &str, fg_pid: Option<u32>) -> Status {
    // #region agent log
    use std::io::Write;
    let log_path = "/Users/nbrake/scm/agent-deck/.cursor/debug.log";
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let _ = writeln!(
            f,
            r#"{{"hypothesisId":"F","location":"session.rs:detect_status_from_content","message":"tool value check","data":{{"tool":"{}","fg_pid":{:?}}}}}"#,
            tool, fg_pid
        );
    }
    // #endregion

    // Layer 1: Process state detection (most reliable)
    if let Some(pid) = fg_pid {
        let proc_state = process::is_waiting_for_input(pid);

        // #region agent log
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
        {
            let _ = writeln!(
                f,
                r#"{{"hypothesisId":"B","location":"session.rs:detect_status_from_content","message":"process state","data":{{"tool":"{}","pid":{},"proc_state":"{:?}"}}}}"#,
                tool, pid, proc_state
            );
        }
        // #endregion

        match proc_state {
            ProcessInputState::WaitingForInput => {
                return Status::Waiting;
            }
            ProcessInputState::Running => {
                return Status::Running;
            }
            _ => {
                // Unknown or sleeping on other I/O - fall through to pattern matching
            }
        }
    }

    // Layer 2: Pattern matching (fallback for when process detection is unavailable)
    let lines: Vec<&str> = content.lines().collect();
    let last_lines = if lines.len() > 10 {
        &lines[lines.len() - 10..]
    } else {
        &lines
    };
    let last_content = last_lines.join("\n");
    let last_content_lower = last_content.to_lowercase();

    // #region agent log
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let escaped = last_content_lower
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        let _ = writeln!(
            f,
            r#"{{"hypothesisId":"C","location":"session.rs:detect_status_from_content","message":"content for pattern matching","data":{{"tool":"{}","line_count":{},"last_content":"{}"}}}}"#,
            tool,
            lines.len(),
            escaped
        );
    }
    // #endregion

    // #region agent log
    let effective_tool = if tool == "shell" && is_opencode_content(&last_content_lower) {
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
        {
            let _ = writeln!(
                f,
                r#"{{"hypothesisId":"FIX","location":"session.rs:detect_status_from_content","message":"auto-detected opencode from content","data":{{}}}}"#
            );
        }
        "opencode"
    } else {
        tool
    };
    // #endregion

    match effective_tool {
        "claude" => detect_claude_status(&last_content),
        "opencode" => detect_opencode_status(&last_content_lower),
        _ => detect_shell_status(&last_content_lower),
    }
}

fn is_opencode_content(content: &str) -> bool {
    let opencode_indicators = ["tab switch agent", "ctrl+p commands", "/compact", "/status"];
    opencode_indicators.iter().any(|ind| content.contains(ind))
}

fn detect_claude_status(content: &str) -> Status {
    let lines: Vec<&str> = content.lines().collect();
    let last_15: Vec<&str> = lines
        .iter()
        .rev()
        .take(15)
        .filter(|l| !l.trim().is_empty())
        .copied()
        .collect();
    let recent_content = last_15.iter().rev().cloned().collect::<Vec<_>>().join("\n");
    let recent_lower = recent_content.to_lowercase();

    // RUNNING: "esc to interrupt" is shown when Claude is busy
    if recent_lower.contains("esc to interrupt") {
        return Status::Running;
    }

    // Also check for spinner characters in last 5 lines (backup)
    let last_5: Vec<&str> = lines.iter().rev().take(5).copied().collect();
    for line in &last_5 {
        for spinner in SPINNER_CHARS {
            if line.contains(spinner) {
                return Status::Running;
            }
        }
    }

    // WAITING: Selection menus (shows "Enter to select" or "Esc to cancel")
    // These indicate interactive prompts waiting for user choice
    if recent_lower.contains("enter to select") || recent_lower.contains("esc to cancel") {
        return Status::Waiting;
    }

    // WAITING: Permission prompts (Claude-specific UI elements)
    let permission_prompts = [
        "Yes, allow once",
        "Yes, allow always",
        "Allow once",
        "Allow always",
        "❯ Yes",
        "❯ No",
        "Do you trust the files in this folder?",
    ];
    for prompt in &permission_prompts {
        if content.contains(prompt) {
            return Status::Waiting;
        }
    }

    // WAITING: Selection cursor with numbered options (e.g., "❯ 1.", "❯ 2.")
    if recent_content.contains("❯")
        && (recent_content.contains("1.") || recent_content.contains("2."))
    {
        return Status::Waiting;
    }

    // WAITING: Check if last non-empty line is ">" input prompt
    if let Some(last_line) = lines.iter().rev().find(|l| !l.trim().is_empty()) {
        let clean_line = strip_ansi(last_line).trim().to_string();
        if clean_line == ">" || clean_line == "> " {
            return Status::Waiting;
        }
        if clean_line.starts_with("> ")
            && !clean_line.to_lowercase().contains("esc")
            && clean_line.len() < 100
        {
            return Status::Waiting;
        }
    }

    // WAITING: Y/N confirmation prompts
    let question_prompts = ["(Y/n)", "(y/N)", "[Y/n]", "[y/N]"];
    for prompt in &question_prompts {
        if recent_content.contains(prompt) {
            return Status::Waiting;
        }
    }

    Status::Idle
}

fn detect_opencode_status(content: &str) -> Status {
    // #region agent log
    use std::io::Write;
    let log_path = "/Users/nbrake/scm/agent-deck/.cursor/debug.log";
    // #endregion

    let lines: Vec<&str> = content.lines().collect();
    let last_15: Vec<&str> = lines
        .iter()
        .rev()
        .take(15)
        .filter(|l| !l.trim().is_empty())
        .copied()
        .collect();
    let recent_content = last_15.iter().rev().cloned().collect::<Vec<_>>().join("\n");
    let recent_lower = recent_content.to_lowercase();

    // RUNNING: OpenCode shows "esc to interrupt" at the bottom when busy (same as Claude Code)
    let has_esc_interrupt =
        recent_lower.contains("esc to interrupt") || recent_lower.contains("esc interrupt");

    // #region agent log
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let _ = writeln!(
            f,
            r#"{{"hypothesisId":"D","location":"session.rs:detect_opencode_status","message":"checking esc interrupt","data":{{"has_esc_interrupt":{}}}}}"#,
            has_esc_interrupt
        );
    }
    // #endregion

    if has_esc_interrupt {
        return Status::Running;
    }

    // Also check for spinner characters in last 5 lines
    let last_5: Vec<&str> = lines.iter().rev().take(5).copied().collect();
    let mut has_spinner = false;
    for line in &last_5 {
        for spinner in SPINNER_CHARS {
            if line.contains(spinner) {
                has_spinner = true;
                break;
            }
        }
    }

    // #region agent log
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let last5_escaped: Vec<String> = last_5
            .iter()
            .map(|s| s.replace('\\', "\\\\").replace('"', "\\\""))
            .collect();
        let _ = writeln!(
            f,
            r#"{{"hypothesisId":"D","location":"session.rs:detect_opencode_status","message":"checking spinners","data":{{"has_spinner":{},"last_5":{:?}}}}}"#,
            has_spinner, last5_escaped
        );
    }
    // #endregion

    if has_spinner {
        return Status::Running;
    }

    // WAITING: Selection menus (shows "Enter to select" or "Esc to cancel")
    if recent_lower.contains("enter to select") || recent_lower.contains("esc to cancel") {
        return Status::Waiting;
    }

    // WAITING: Permission/confirmation prompts
    let permission_prompts = [
        "(y/n)",
        "[y/n]",
        "continue?",
        "proceed?",
        "approve",
        "allow",
    ];
    for prompt in &permission_prompts {
        if recent_lower.contains(prompt) {
            return Status::Waiting;
        }
    }

    // WAITING: Selection cursor with numbered options
    if recent_content.contains("❯")
        && (recent_content.contains("1.") || recent_content.contains("2."))
    {
        return Status::Waiting;
    }

    // WAITING: Check if last non-empty line is input prompt
    let last_line_result = lines.iter().rev().find(|l| !l.trim().is_empty());

    // #region agent log
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let line_info = last_line_result
            .map(|l| {
                let clean = strip_ansi(l).trim().to_string();
                format!(
                    "raw='{}', clean='{}'",
                    l.replace('\\', "\\\\").replace('"', "\\\""),
                    clean.replace('\\', "\\\\").replace('"', "\\\"")
                )
            })
            .unwrap_or_else(|| "none".to_string());
        let _ = writeln!(
            f,
            r#"{{"hypothesisId":"E","location":"session.rs:detect_opencode_status","message":"last line check","data":{{"last_line":"{}"}}}}"#,
            line_info
        );
    }
    // #endregion

    if let Some(last_line) = last_line_result {
        let clean_line = strip_ansi(last_line).trim().to_string();

        // OpenCode input prompts
        if clean_line == ">" || clean_line == "> " || clean_line == ">>" {
            return Status::Waiting;
        }
        if clean_line.starts_with("> ")
            && !clean_line.to_lowercase().contains("esc")
            && clean_line.len() < 100
        {
            return Status::Waiting;
        }
    }

    // WAITING - Completion indicators + input prompt nearby
    let completion_indicators = [
        "complete",
        "done",
        "finished",
        "ready",
        "what would you like",
        "what else",
        "anything else",
        "how can i help",
        "let me know",
    ];
    let has_completion = completion_indicators
        .iter()
        .any(|ind| recent_lower.contains(ind));
    if has_completion {
        for line in &last_5 {
            let clean = strip_ansi(line).trim().to_string();
            if clean == ">" || clean == "> " || clean == ">>" {
                return Status::Waiting;
            }
        }
    }

    // #region agent log
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let _ = writeln!(
            f,
            r#"{{"hypothesisId":"D","location":"session.rs:detect_opencode_status","message":"returning Idle - no patterns matched","data":{{}}}}"#
        );
    }
    // #endregion

    Status::Idle
}

fn detect_shell_status(content: &str) -> Status {
    // Shell prompts
    if content.ends_with("$ ")
        || content.ends_with("> ")
        || content.ends_with("# ")
        || content.ends_with("% ")
    {
        return Status::Waiting;
    }

    // Running if we see a spinner or progress indicator
    let running_indicators = [
        "⠋",
        "⠙",
        "⠹",
        "⠸",
        "⠼",
        "⠴",
        "⠦",
        "⠧",
        "⠇",
        "⠏",
        "...",
        "───",
    ];
    for indicator in &running_indicators {
        if content.contains(indicator) {
            return Status::Running;
        }
    }

    Status::Idle
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_session_name() {
        assert_eq!(sanitize_session_name("my-project"), "my-project");
        assert_eq!(sanitize_session_name("my project"), "my_project");
        assert_eq!(sanitize_session_name("a".repeat(30).as_str()).len(), 20);
    }

    #[test]
    fn test_generate_name() {
        let name = Session::generate_name("abc123def456", "My Project");
        assert!(name.starts_with(SESSION_PREFIX));
        assert!(name.contains("My_Project"));
        assert!(name.contains("abc123de"));
    }

    #[test]
    fn test_detect_claude_status_running() {
        // "esc to interrupt" indicates Claude is actively working
        assert_eq!(
            detect_claude_status("Working on your request (esc to interrupt)"),
            Status::Running
        );
        assert_eq!(
            detect_claude_status("Thinking... · esc to interrupt"),
            Status::Running
        );

        // Spinner characters indicate active processing
        assert_eq!(detect_claude_status("Processing ⠋"), Status::Running);
        assert_eq!(detect_claude_status("Loading ⠹"), Status::Running);
    }

    #[test]
    fn test_detect_claude_status_waiting() {
        // Permission prompts
        assert_eq!(detect_claude_status("Yes, allow once"), Status::Waiting);
        assert_eq!(
            detect_claude_status("Do you trust the files in this folder?"),
            Status::Waiting
        );

        // Input prompt
        assert_eq!(detect_claude_status("Task complete.\n>"), Status::Waiting);
        assert_eq!(detect_claude_status("Done!\n> "), Status::Waiting);

        // Question prompts
        assert_eq!(detect_claude_status("Continue? (Y/n)"), Status::Waiting);

        // Selection menus
        assert_eq!(
            detect_claude_status("Enter to select · Tab/Arrow keys to navigate · Esc to cancel"),
            Status::Waiting
        );
        assert_eq!(
            detect_claude_status("❯ 1. Planned activities\n  2. Spontaneous"),
            Status::Waiting
        );
    }

    #[test]
    fn test_detect_claude_status_idle() {
        // No indicators = idle
        assert_eq!(detect_claude_status("completed the task"), Status::Idle);
        assert_eq!(detect_claude_status("some random output"), Status::Idle);
    }

    #[test]
    fn test_strip_ansi() {
        assert_eq!(strip_ansi("\x1b[32mgreen\x1b[0m"), "green");
        assert_eq!(strip_ansi("no codes here"), "no codes here");
        assert_eq!(strip_ansi("\x1b[1;34mbold blue\x1b[0m"), "bold blue");
    }

    #[test]
    fn test_detect_opencode_status_running() {
        // "esc to interrupt" at bottom = running (same pattern as Claude Code)
        assert_eq!(
            detect_opencode_status("Processing your request\nesc to interrupt"),
            Status::Running
        );
        assert_eq!(
            detect_opencode_status("Working... esc interrupt"),
            Status::Running
        );

        // Spinner characters indicate active processing
        assert_eq!(detect_opencode_status("Generating ⠋"), Status::Running);
        assert_eq!(detect_opencode_status("Loading ⠹"), Status::Running);
    }

    #[test]
    fn test_detect_opencode_status_waiting() {
        // Permission prompts
        assert_eq!(
            detect_opencode_status("Allow this action? [y/n]"),
            Status::Waiting
        );
        assert_eq!(detect_opencode_status("Continue? (y/n)"), Status::Waiting);
        assert_eq!(detect_opencode_status("Approve changes"), Status::Waiting);

        // Input prompt
        assert_eq!(detect_opencode_status("Task complete.\n>"), Status::Waiting);
        assert_eq!(
            detect_opencode_status("Ready for input\n> "),
            Status::Waiting
        );

        // Completion + prompt
        assert_eq!(
            detect_opencode_status("Done! What else can I help with?\n>"),
            Status::Waiting
        );
    }

    #[test]
    fn test_detect_opencode_status_idle() {
        // No indicators = idle
        assert_eq!(detect_opencode_status("completed the task"), Status::Idle);
        assert_eq!(detect_opencode_status("some random output"), Status::Idle);
        assert_eq!(
            detect_opencode_status("file saved successfully"),
            Status::Idle
        );
    }
}
