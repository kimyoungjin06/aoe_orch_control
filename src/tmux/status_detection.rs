//! Status detection for agent sessions

use crate::session::Status;

use super::utils::strip_ansi;

const SPINNER_CHARS: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// How far up from the bottom of the pane an interactive prompt can start.
///
/// Agent tools draw their prompt, options and footer at the bottom; anything
/// further up is scrolled-back tool output. Scanning the whole capture made a
/// session that merely printed the word "approved" read as waiting for input.
const PROMPT_SCAN_LINES: usize = 8;

/// Longest line still plausible as interactive UI. Prompts and menu rows are
/// short; diff hunks, grep hits and JSON rows are not.
const PROMPT_MAX_CHARS: usize = 80;

/// Diff, tree-drawing and table framing that only ever appears in captured
/// tool output, never at the start of a prompt line.
const OUTPUT_LINE_MARKERS: &[char] = &['+', '|', '│', '└', '├', '╭', '╰'];

/// True when `needle` occurs in `haystack` on identifier boundaries.
///
/// Without this, `approve` matches `reviewing_not_approved` and `allow`
/// matches `ALLOWED_POSTS`, so any agent working on review or permission code
/// reports itself as waiting for approval.
fn contains_word(haystack: &str, needle: &str) -> bool {
    let is_word_char = |c: char| c.is_alphanumeric() || c == '_';
    let mut from = 0;
    while let Some(offset) = haystack[from..].find(needle) {
        let start = from + offset;
        let end = start + needle.len();
        let boundary_before = haystack[..start].chars().next_back().is_none_or(|c| {
            // Only guard the boundary when the needle itself starts with a
            // word character; `(y/n)` may legitimately follow one.
            !needle.starts_with(is_word_char) || !is_word_char(c)
        });
        let boundary_after = haystack[end..]
            .chars()
            .next()
            .is_none_or(|c| !needle.ends_with(is_word_char) || !is_word_char(c));
        if boundary_before && boundary_after {
            return true;
        }
        from = end;
    }
    false
}

/// True when a line looks like interactive UI rather than captured output.
fn is_prompt_like(trimmed: &str) -> bool {
    if trimmed.is_empty() || trimmed.chars().count() > PROMPT_MAX_CHARS {
        return false;
    }
    if trimmed.starts_with(OUTPUT_LINE_MARKERS) {
        return false;
    }
    // grep and diff hunks render as "149:  code" or "25 +  code"; menu rows
    // render as "1. Yes", so only reject a leading number followed by a
    // non-menu separator.
    let after_digits = trimmed.trim_start_matches(|c: char| c.is_ascii_digit());
    if after_digits.len() < trimmed.len() && after_digits.trim_start().starts_with([':', '+', '-'])
    {
        return false;
    }
    true
}

/// True when the bottom of the pane contains a Codex activity status line.
///
/// Ordinary output frequently contains words such as "working" and
/// "thinking". Codex's activity line has a much narrower shape, with the
/// activity word followed by parenthesized timing/status detail.
fn has_codex_activity_status(non_empty_lines: &[&str]) -> bool {
    non_empty_lines
        .iter()
        .rev()
        .take(PROMPT_SCAN_LINES)
        .any(|line| {
            let clean = strip_ansi(line);
            let trimmed = clean.trim().trim_start_matches('•').trim_start();
            ["working (", "thinking ("]
                .iter()
                .any(|prefix| trimmed.starts_with(prefix))
        })
}

/// Scan the bottom of the pane for one of `prompts`, ignoring matches that sit
/// inside tool output rather than in an actual prompt.
fn has_approval_prompt(non_empty_lines: &[&str], prompts: &[&str]) -> bool {
    non_empty_lines
        .iter()
        .rev()
        .take(PROMPT_SCAN_LINES)
        .any(|line| {
            let clean = strip_ansi(line);
            let trimmed = clean.trim();
            if !is_prompt_like(trimmed) || !prompts.iter().any(|p| contains_word(trimmed, p)) {
                return false;
            }
            // Bare approval words also appear in ordinary agent output. A
            // standalone prompt is a question; non-question selector hints
            // carry their own explicit keyboard marker.
            trimmed.contains('?')
                || ["(y/n)", "[y/n]", "enter to select", "esc to cancel"]
                    .iter()
                    .any(|marker| contains_word(trimmed, marker))
        })
}

/// True when the bottom of the pane shows a numbered selection menu.
fn has_numbered_selection(non_empty_lines: &[&str]) -> bool {
    non_empty_lines
        .iter()
        .rev()
        .take(PROMPT_SCAN_LINES)
        .any(|line| {
            let clean = strip_ansi(line);
            let trimmed = clean.trim();
            let Some(after_cursor) = trimmed.strip_prefix('❯') else {
                return false;
            };
            let after_cursor = after_cursor.trim_start();
            ["1.", "2.", "3."]
                .iter()
                .any(|n| after_cursor.starts_with(n))
        })
}

pub fn detect_status_from_content(content: &str, tool: &str, _fg_pid: Option<u32>) -> Status {
    crate::agents::get_agent(tool)
        .map(|a| (a.detect_status)(content))
        .unwrap_or_else(|| detect_claude_status(content))
}

pub fn detect_claude_status(content: &str) -> Status {
    let lines: Vec<&str> = content.lines().collect();
    let non_empty_lines: Vec<&str> = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .copied()
        .collect();

    let last_lines: String = non_empty_lines
        .iter()
        .rev()
        .take(30)
        .rev()
        .copied()
        .collect::<Vec<&str>>()
        .join("\n");
    let last_lines_lower = last_lines.to_lowercase();

    if last_lines_lower.contains("esc to interrupt")
        || last_lines_lower.contains("ctrl+c to interrupt")
    {
        return Status::Running;
    }

    for line in &lines {
        for spinner in SPINNER_CHARS {
            if line.contains(spinner) {
                return Status::Running;
            }
        }
    }

    if last_lines_lower.contains("enter to select") || last_lines_lower.contains("esc to cancel") {
        return Status::Waiting;
    }

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
        if last_lines.contains(prompt) {
            return Status::Waiting;
        }
    }

    for line in &lines {
        let trimmed = line.trim();
        if trimmed.starts_with("❯") && trimmed.len() > 2 {
            let rest = &trimmed[3..].trim_start();
            if rest.starts_with("1.") || rest.starts_with("2.") || rest.starts_with("3.") {
                return Status::Waiting;
            }
        }
    }

    for line in non_empty_lines.iter().rev().take(10) {
        let clean_line = strip_ansi(line).trim().to_string();
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
    // Only check in last lines
    let question_prompts = ["(Y/n)", "(y/N)", "[Y/n]", "[y/N]"];
    for prompt in &question_prompts {
        if last_lines.contains(prompt) {
            return Status::Waiting;
        }
    }

    Status::Idle
}

pub fn detect_opencode_status(raw_content: &str) -> Status {
    let content = raw_content.to_lowercase();
    let lines: Vec<&str> = content.lines().collect();
    let non_empty_lines: Vec<&str> = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .copied()
        .collect();

    let last_lines: String = non_empty_lines
        .iter()
        .rev()
        .take(30)
        .rev()
        .copied()
        .collect::<Vec<&str>>()
        .join("\n");
    let last_lines_lower = last_lines.to_lowercase();

    // RUNNING: OpenCode shows "esc to interrupt" when busy (same as Claude Code)
    // Only check in last lines to avoid matching comments/code in terminal output
    if last_lines_lower.contains("esc to interrupt") || last_lines_lower.contains("esc interrupt") {
        return Status::Running;
    }

    for line in &lines {
        for spinner in SPINNER_CHARS {
            if line.contains(spinner) {
                return Status::Running;
            }
        }
    }

    // WAITING: Selection menus (shows "Enter to select" or "Esc to cancel")
    // Only check in last lines to avoid matching comments/code
    if last_lines_lower.contains("enter to select") || last_lines_lower.contains("esc to cancel") {
        return Status::Waiting;
    }

    // WAITING: Permission/confirmation prompts
    // Only check in last lines
    let permission_prompts = [
        "(y/n)",
        "[y/n]",
        "continue?",
        "proceed?",
        "approve",
        "allow",
    ];
    for prompt in &permission_prompts {
        if last_lines_lower.contains(prompt) {
            return Status::Waiting;
        }
    }

    for line in &lines {
        let trimmed = line.trim();
        if trimmed.starts_with("❯") && trimmed.len() > 2 {
            let after_cursor = trimmed.get(3..).unwrap_or("").trim_start();
            if after_cursor.starts_with("1.")
                || after_cursor.starts_with("2.")
                || after_cursor.starts_with("3.")
            {
                return Status::Waiting;
            }
        }
    }
    if lines.iter().any(|line| {
        line.contains("❯") && (line.contains(" 1.") || line.contains(" 2.") || line.contains(" 3."))
    }) {
        return Status::Waiting;
    }

    for line in non_empty_lines.iter().rev().take(10) {
        let clean_line = strip_ansi(line).trim().to_string();

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
    // Only check in last lines
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
        .any(|ind| last_lines_lower.contains(ind));
    if has_completion {
        for line in non_empty_lines.iter().rev().take(10) {
            let clean = strip_ansi(line).trim().to_string();
            if clean == ">" || clean == "> " || clean == ">>" {
                return Status::Waiting;
            }
        }
    }

    Status::Idle
}

pub fn detect_vibe_status(raw_content: &str) -> Status {
    let content = raw_content.to_lowercase();
    let lines: Vec<&str> = content.lines().collect();
    let non_empty_lines: Vec<&str> = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .copied()
        .collect();

    let last_lines: String = non_empty_lines
        .iter()
        .rev()
        .take(30)
        .rev()
        .copied()
        .collect::<Vec<&str>>()
        .join("\n");
    let last_lines_lower = last_lines.to_lowercase();

    // Vibe uses Textual TUI which can render text vertically (one char per line).
    // Join recent single-char lines to reconstruct words for detection.
    let recent_text: String = non_empty_lines
        .iter()
        .rev()
        .take(50)
        .rev()
        .map(|l| l.trim())
        .collect::<Vec<&str>>()
        .join("");
    let recent_text_lower = recent_text.to_lowercase();

    // WAITING checks come first - they're more specific than Running indicators

    // WAITING: Vibe's approval prompts show navigation hints
    // Pattern: "↑↓ navigate  Enter select  ESC reject"
    if last_lines_lower.contains("↑↓ navigate")
        || last_lines_lower.contains("enter select")
        || last_lines_lower.contains("esc reject")
    {
        return Status::Waiting;
    }

    // WAITING: Tool approval warning (shows "⚠ {tool_name} command")
    if last_lines.contains("⚠") && last_lines_lower.contains("command") {
        return Status::Waiting;
    }

    // WAITING: Approval options shown by Vibe
    let approval_options = [
        "yes and always allow",
        "no and tell the agent",
        "› 1.", // Selected numbered option
        "› 2.",
        "› 3.",
    ];
    for option in &approval_options {
        if last_lines_lower.contains(option) {
            return Status::Waiting;
        }
    }

    // WAITING: Generic selection cursor (› followed by text)
    for line in &lines {
        let trimmed = line.trim();
        if trimmed.starts_with("›") && trimmed.len() > 2 {
            return Status::Waiting;
        }
    }

    // RUNNING: Check for braille spinners anywhere in recent content
    // Vibe renders vertically so spinner may be on its own line
    for spinner in SPINNER_CHARS {
        if recent_text.contains(spinner) {
            return Status::Running;
        }
    }

    // RUNNING: Activity indicators (may be rendered vertically)
    let activity_indicators = [
        "running",
        "reading",
        "writing",
        "executing",
        "processing",
        "generating",
        "thinking",
    ];
    for indicator in &activity_indicators {
        if recent_text_lower.contains(indicator) {
            return Status::Running;
        }
    }

    // RUNNING: Ellipsis at end often indicates ongoing activity
    if recent_text.ends_with("…") || recent_text.ends_with("...") {
        return Status::Running;
    }

    Status::Idle
}

pub fn detect_codex_status(raw_content: &str) -> Status {
    let content = raw_content.to_lowercase();
    let lines: Vec<&str> = content.lines().collect();
    let non_empty_lines: Vec<&str> = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .copied()
        .collect();

    let last_lines: String = non_empty_lines
        .iter()
        .rev()
        .take(30)
        .rev()
        .copied()
        .collect::<Vec<&str>>()
        .join("\n");
    let last_lines_lower = last_lines.to_lowercase();

    // RUNNING: Codex shows "esc to interrupt" or similar when processing. The
    // interrupt hints are specific enough to trust anywhere in the tail;
    // "working"/"thinking" are ordinary words that also occur in tool output,
    // so they only count on the status line at the bottom.
    if last_lines_lower.contains("esc to interrupt")
        || last_lines_lower.contains("ctrl+c to interrupt")
        || has_codex_activity_status(&non_empty_lines)
    {
        return Status::Running;
    }

    for line in &lines {
        for spinner in SPINNER_CHARS {
            if line.contains(spinner) {
                return Status::Running;
            }
        }
    }

    // WAITING: Approval prompts (Codex uses ask-for-approval modes) and
    // selection menus, both drawn at the bottom of the pane.
    const CODEX_APPROVAL_PROMPTS: &[&str] = &[
        "approve",
        "approval",
        "allow",
        "(y/n)",
        "[y/n]",
        "continue?",
        "proceed?",
        "execute?",
        "run command?",
        "enter to select",
        "esc to cancel",
    ];
    if has_approval_prompt(&non_empty_lines, CODEX_APPROVAL_PROMPTS)
        || has_numbered_selection(&non_empty_lines)
    {
        return Status::Waiting;
    }

    // WAITING: Input prompt ready
    for line in non_empty_lines.iter().rev().take(10) {
        let clean_line = strip_ansi(line).trim().to_string();
        if clean_line == ">" || clean_line == "> " || clean_line == "codex>" {
            return Status::Waiting;
        }
        if clean_line.starts_with("> ")
            && !clean_line.to_lowercase().contains("esc")
            && clean_line.len() < 100
        {
            return Status::Waiting;
        }
    }

    Status::Idle
}

pub fn detect_gemini_status(raw_content: &str) -> Status {
    let content = raw_content.to_lowercase();
    let lines: Vec<&str> = content.lines().collect();
    let non_empty_lines: Vec<&str> = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .copied()
        .collect();

    let last_lines: String = non_empty_lines
        .iter()
        .rev()
        .take(30)
        .rev()
        .copied()
        .collect::<Vec<&str>>()
        .join("\n");
    let last_lines_lower = last_lines.to_lowercase();

    // RUNNING: Gemini shows activity indicators
    if last_lines_lower.contains("esc to interrupt")
        || last_lines_lower.contains("ctrl+c to interrupt")
    {
        return Status::Running;
    }

    for line in &lines {
        for spinner in SPINNER_CHARS {
            if line.contains(spinner) {
                return Status::Running;
            }
        }
    }

    // WAITING: Approval prompts, drawn at the bottom of the pane
    const GEMINI_APPROVAL_PROMPTS: &[&str] = &[
        "(y/n)",
        "[y/n]",
        "allow",
        "approve",
        "approval",
        "execute?",
        "enter to select",
        "esc to cancel",
    ];
    if has_approval_prompt(&non_empty_lines, GEMINI_APPROVAL_PROMPTS) {
        return Status::Waiting;
    }

    // WAITING: Input prompt
    for line in non_empty_lines.iter().rev().take(10) {
        let clean_line = strip_ansi(line).trim().to_string();
        if clean_line == ">" || clean_line == "> " {
            return Status::Waiting;
        }
    }

    Status::Idle
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_claude_status_running() {
        assert_eq!(
            detect_claude_status("Working on your request (esc to interrupt)"),
            Status::Running
        );
        assert_eq!(
            detect_claude_status("Thinking... · esc to interrupt"),
            Status::Running
        );
        assert_eq!(
            detect_claude_status("✶ Hashing… (ctrl+c to interrupt)"),
            Status::Running
        );
        assert_eq!(detect_claude_status("Processing ⠋"), Status::Running);
        assert_eq!(detect_claude_status("Loading ⠹"), Status::Running);
    }

    #[test]
    fn test_detect_claude_status_waiting() {
        assert_eq!(detect_claude_status("Yes, allow once"), Status::Waiting);
        assert_eq!(
            detect_claude_status("Do you trust the files in this folder?"),
            Status::Waiting
        );
        assert_eq!(detect_claude_status("Task complete.\n>"), Status::Waiting);
        assert_eq!(detect_claude_status("Done!\n> "), Status::Waiting);
        assert_eq!(detect_claude_status("Continue? (Y/n)"), Status::Waiting);
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
        assert_eq!(detect_claude_status("completed the task"), Status::Idle);
        assert_eq!(detect_claude_status("some random output"), Status::Idle);
    }

    #[test]
    fn test_detect_opencode_status_running() {
        assert_eq!(
            detect_opencode_status("Processing your request\nesc to interrupt"),
            Status::Running
        );
        assert_eq!(
            detect_opencode_status("Working... esc interrupt"),
            Status::Running
        );
        assert_eq!(detect_opencode_status("Generating ⠋"), Status::Running);
        assert_eq!(detect_opencode_status("Loading ⠹"), Status::Running);
    }

    #[test]
    fn test_detect_opencode_status_waiting() {
        assert_eq!(
            detect_opencode_status("allow this action? [y/n]"),
            Status::Waiting
        );
        assert_eq!(detect_opencode_status("continue? (y/n)"), Status::Waiting);
        assert_eq!(detect_opencode_status("approve changes"), Status::Waiting);
        assert_eq!(detect_opencode_status("task complete.\n>"), Status::Waiting);
        assert_eq!(
            detect_opencode_status("ready for input\n> "),
            Status::Waiting
        );
        assert_eq!(
            detect_opencode_status("done! what else can i help with?\n>"),
            Status::Waiting
        );
    }

    #[test]
    fn test_detect_opencode_status_idle() {
        assert_eq!(detect_opencode_status("some random output"), Status::Idle);
        assert_eq!(
            detect_opencode_status("file saved successfully"),
            Status::Idle
        );
    }

    #[test]
    fn test_detect_status_from_content_falls_back_to_claude() {
        let content = "Processing ⠋";
        let status = detect_status_from_content(content, "unknown_tool", None);
        assert_eq!(status, Status::Running);
    }

    #[test]
    fn test_detect_claude_status_numbered_list_selection() {
        let content = "Choose an option:\n❯ 1. First option\n  2. Second option\n  3. Third option";
        assert_eq!(detect_claude_status(content), Status::Waiting);
    }

    #[test]
    fn test_detect_claude_status_all_spinner_chars() {
        for spinner in SPINNER_CHARS {
            let content = format!("Working... {}", spinner);
            assert_eq!(
                detect_claude_status(&content),
                Status::Running,
                "Failed for spinner: {}",
                spinner
            );
        }
    }

    #[test]
    fn test_detect_claude_status_prompt_with_text() {
        assert_eq!(detect_claude_status("> hello"), Status::Waiting);
    }

    #[test]
    fn test_detect_claude_status_yn_variations() {
        assert_eq!(detect_claude_status("Continue? [Y/n]"), Status::Waiting);
        assert_eq!(detect_claude_status("Proceed? [y/N]"), Status::Waiting);
        assert_eq!(detect_claude_status("Confirm (Y/n)"), Status::Waiting);
        assert_eq!(detect_claude_status("Delete? (y/N)"), Status::Waiting);
    }

    #[test]
    fn test_detect_claude_status_allow_prompts() {
        assert_eq!(detect_claude_status("❯ Yes"), Status::Waiting);
        assert_eq!(detect_claude_status("❯ No"), Status::Waiting);
        assert_eq!(detect_claude_status("Allow once"), Status::Waiting);
        assert_eq!(detect_claude_status("Allow always"), Status::Waiting);
    }

    #[test]
    fn test_detect_opencode_status_numbered_selection() {
        let content = "Select:\n❯ 1. Option A\n  2. Option B";
        assert_eq!(detect_opencode_status(content), Status::Waiting);
    }

    #[test]
    fn test_detect_opencode_status_completion_with_prompt() {
        let content = "Task complete! What else can I help with?\n>";
        assert_eq!(detect_opencode_status(content), Status::Waiting);
    }

    #[test]
    fn test_detect_opencode_status_double_prompt() {
        assert_eq!(detect_opencode_status("Ready\n>>"), Status::Waiting);
    }

    #[test]
    fn test_detect_vibe_status_running() {
        // Braille spinners
        assert_eq!(detect_vibe_status("processing ⠋"), Status::Running);
        assert_eq!(detect_vibe_status("⠹"), Status::Running);

        // Activity indicators
        assert_eq!(detect_vibe_status("Running bash"), Status::Running);
        assert_eq!(detect_vibe_status("Reading file"), Status::Running);
        assert_eq!(detect_vibe_status("Writing changes"), Status::Running);
        assert_eq!(detect_vibe_status("Generating code"), Status::Running);

        // Vertical text (Vibe's Textual TUI renders one char per line)
        assert_eq!(
            detect_vibe_status("⠋\nR\nu\nn\nn\ni\nn\ng\nb\na\ns\nh\n…"),
            Status::Running
        );

        // Ellipsis indicates ongoing activity
        assert_eq!(detect_vibe_status("Working…"), Status::Running);
        assert_eq!(detect_vibe_status("Loading..."), Status::Running);
    }

    #[test]
    fn test_detect_vibe_status_waiting() {
        // Vibe's approval prompt navigation hints
        assert_eq!(
            detect_vibe_status("↑↓ navigate  Enter select  ESC reject"),
            Status::Waiting
        );
        // Tool approval warning
        assert_eq!(
            detect_vibe_status("⚠ bash command\nExecute this?"),
            Status::Waiting
        );
        // Approval options
        assert_eq!(
            detect_vibe_status(
                "› Yes\n  Yes and always allow bash for this session\n  No and tell the agent"
            ),
            Status::Waiting
        );
    }

    #[test]
    fn test_detect_vibe_status_idle() {
        assert_eq!(detect_vibe_status("some random output"), Status::Idle);
        assert_eq!(detect_vibe_status("file saved successfully"), Status::Idle);
        assert_eq!(detect_vibe_status("Done!"), Status::Idle);
    }

    #[test]
    fn test_detect_codex_status_running() {
        assert_eq!(
            detect_codex_status("processing request\nesc to interrupt"),
            Status::Running
        );
        assert_eq!(
            detect_codex_status("• Thinking (4s • esc to interrupt)"),
            Status::Running
        );
        assert_eq!(
            detect_codex_status("• Working (12s • esc to interrupt)"),
            Status::Running
        );
        assert_eq!(detect_codex_status("generating ⠋"), Status::Running);
    }

    #[test]
    fn test_detect_codex_status_waiting() {
        assert_eq!(
            detect_codex_status("run this command? (y/n)"),
            Status::Waiting
        );
        assert_eq!(detect_codex_status("approve changes?"), Status::Waiting);
        assert_eq!(
            detect_codex_status("execute this action? [y/n]"),
            Status::Waiting
        );
        assert_eq!(detect_codex_status("ready\ncodex>"), Status::Waiting);
        assert_eq!(detect_codex_status("done\n>"), Status::Waiting);
    }

    #[test]
    fn test_detect_codex_status_idle() {
        assert_eq!(detect_codex_status("file saved"), Status::Idle);
        assert_eq!(detect_codex_status("random output text"), Status::Idle);
    }

    #[test]
    fn test_detect_codex_status_ignores_approval_words_in_output() {
        // Captured from a session auditing a review workflow: the agent's own
        // diff and grep output mention approval, but nothing waits on the
        // operator. This shape pushed 27 false "waiting" cards in one night.
        let content = concat!(
            "• Ran jq '{pairs:[.pairs[]|select(.pair_id==\"a-1\")]}' audit.json\n",
            " 25 +    def test_review_is_explicitly_not_an_approved_registry(self):\n",
            " 27 +        self.assertEqual('reviewing_not_approved', self.review)\n",
            "149:        if path not in ALLOWED_POSTS:\n",
            "• Explored\n",
            "  └ Read MEMORY.md",
        );
        assert_eq!(detect_codex_status(content), Status::Idle);
    }

    #[test]
    fn test_detect_codex_status_waiting_on_real_prompt() {
        let content = concat!(
            "• Ran cargo test\n",
            "  └ 42 passed\n",
            "Allow Codex to run `rm -rf target`?\n",
            "❯ 1. Yes, run it\n",
            "  2. No, and tell Codex what to do differently\n",
            "  Press Enter to select · Esc to cancel",
        );
        assert_eq!(detect_codex_status(content), Status::Waiting);
    }

    #[test]
    fn test_detect_codex_status_running_from_status_line() {
        let content = concat!(
            "• Explored\n",
            "  └ Read MEMORY.md\n",
            "• Working (2m 27s • esc to interrupt) · 11 background terminals running\n",
            "› Write tests for @filename\n",
            "  gpt-5.6-sol xhigh · ~/Desktop/Workspace/97.scoreboard",
        );
        assert_eq!(detect_codex_status(content), Status::Running);
    }

    #[test]
    fn test_detect_codex_status_prompt_beats_stale_activity_word() {
        // "working" scrolled up in tool output must not mask an open prompt.
        let content = concat!(
            "• Ran ls\n",
            "  └ changed working directory to /tmp\n",
            "  └ ok\n  └ ok\n  └ ok\n  └ ok\n  └ ok\n  └ ok\n",
            "Approve this change?\n",
            "❯ 1. Yes\n",
            "  2. No",
        );
        assert_eq!(detect_codex_status(content), Status::Waiting);
    }

    #[test]
    fn test_detect_codex_status_prompt_beats_adjacent_activity_word_in_output() {
        let content = concat!(
            "changed working directory to /tmp\n",
            "Approve this change?\n",
            "❯ 1. Yes\n",
            "  2. No",
        );
        assert_eq!(detect_codex_status(content), Status::Waiting);
    }

    #[test]
    fn test_detect_codex_status_ignores_bare_approval_sentence() {
        assert_eq!(
            detect_codex_status("This change still needs approval"),
            Status::Idle
        );
    }

    #[test]
    fn test_detect_gemini_status_ignores_approval_words_in_output() {
        let content = concat!(
            "read src/policy.py\n",
            "149:        if path not in ALLOWED_POSTS:\n",
            "  └ wrote 3 files",
        );
        assert_eq!(detect_gemini_status(content), Status::Idle);
    }

    #[test]
    fn test_contains_word_respects_identifier_boundaries() {
        assert!(contains_word("approve this change?", "approve"));
        assert!(contains_word("run it? (y/n)", "(y/n)"));
        assert!(!contains_word("reviewing_not_approved", "approve"));
        assert!(!contains_word("allowed_posts", "allow"));
        assert!(!contains_word("disallow", "allow"));
    }

    #[test]
    fn test_detect_gemini_status_running() {
        assert_eq!(
            detect_gemini_status("processing request\nesc to interrupt"),
            Status::Running
        );
        assert_eq!(detect_gemini_status("generating ⠋"), Status::Running);
        assert_eq!(detect_gemini_status("working ⠹"), Status::Running);
    }

    #[test]
    fn test_detect_gemini_status_waiting() {
        assert_eq!(
            detect_gemini_status("run this command? (y/n)"),
            Status::Waiting
        );
        assert_eq!(detect_gemini_status("approve changes?"), Status::Waiting);
        assert_eq!(
            detect_gemini_status("execute this action? [y/n]"),
            Status::Waiting
        );
        assert_eq!(detect_gemini_status("ready\n>"), Status::Waiting);
    }

    #[test]
    fn test_detect_gemini_status_idle() {
        assert_eq!(detect_gemini_status("file saved"), Status::Idle);
        assert_eq!(detect_gemini_status("random output text"), Status::Idle);
    }
}
