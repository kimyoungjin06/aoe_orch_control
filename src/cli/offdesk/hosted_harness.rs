//! Hosted harness profile and compact prompt-packet adapter.
//!
//! The command handler owns CLI parsing and output side effects. This module
//! owns profile lookup, first-read inspection, budget assessment, and Markdown
//! assembly for the hosted harness start contract.

use anyhow::Result;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Serialize)]
pub(super) struct HostedHarnessProfileView {
    pub(super) id: &'static str,
    pub(super) display_name: &'static str,
    pub(super) support_status: &'static str,
    pub(super) launch_command: Option<&'static str>,
    pub(super) runner: &'static str,
    pub(super) mutation_scope: &'static str,
    pub(super) prompt_contract: HostedHarnessPromptContract,
    evidence_sources: &'static [&'static str],
    pub(super) result_artifact: &'static str,
    pub(super) failure_signal: &'static str,
    closeout_package: &'static str,
    retention_policy: &'static str,
    pub(super) notes: &'static str,
}

#[derive(Serialize)]
pub(super) struct HostedHarnessPromptContract {
    pub(super) strategy: &'static str,
    pub(super) inline_context_budget_bytes: usize,
    pub(super) first_read_file_budget_bytes: u64,
    pub(super) first_read_total_budget_bytes: u64,
    first_read_required: bool,
    pub(super) preferred_first_reads: &'static [&'static str],
    discouraged_inline_context: &'static [&'static str],
    invocation_hint: &'static str,
}

const COMPACT_FIRST_READ_PROMPT_CONTRACT: HostedHarnessPromptContract = HostedHarnessPromptContract {
    strategy: "compact_prompt_with_first_read_artifacts",
    inline_context_budget_bytes: 4096,
    first_read_file_budget_bytes: 65_536,
    first_read_total_budget_bytes: 262_144,
    first_read_required: true,
    preferred_first_reads: &[
        "RETURN_PACKAGE.md",
        "closeout_plan.json",
        "result.json",
        "focused task brief",
        "operator-selected source files",
    ],
    discouraged_inline_context: &[
        "full git diff",
        "large logs",
        "raw scrollback",
        "entire repository inventory",
    ],
    invocation_hint: "Pass a short task prompt, keep large context in artifacts, and make the harness read only the listed first-read paths.",
};

const PLANNED_PROMPT_CONTRACT: HostedHarnessPromptContract = HostedHarnessPromptContract {
    strategy: "unvalidated",
    inline_context_budget_bytes: 0,
    first_read_file_budget_bytes: 65_536,
    first_read_total_budget_bytes: 262_144,
    first_read_required: true,
    preferred_first_reads: &["to be defined by integration smoke"],
    discouraged_inline_context: &["full git diff", "large logs", "raw scrollback"],
    invocation_hint:
        "Do not promote this harness until a compact prompt and first-read artifact smoke passes.",
};

const HOSTED_HARNESS_PROFILES: &[HostedHarnessProfileView] = &[
    HostedHarnessProfileView {
        id: "codex",
        display_name: "Codex CLI",
        support_status: "supported",
        launch_command: Some("codex"),
        runner: "local-tmux",
        mutation_scope: "operator-selected repository or disposable worktree",
        prompt_contract: COMPACT_FIRST_READ_PROMPT_CONTRACT,
        evidence_sources: &[
            "tmux pane capture",
            "Forager background run record",
            "runner log artifact when launched through offdesk",
            "result artifact declared on the task",
            "offdesk closeout package",
        ],
        result_artifact: "task-declared result sidecar or closeout RETURN_PACKAGE.md",
        failure_signal: "missing tmux runtime, nonzero runner exit, stale heartbeat/progress, or missing result artifact",
        closeout_package: "offdesk closeout plan plus Ondesk return package",
        retention_policy: "preserve command summary, logs, result sidecar, closeout package, and review verdict",
        notes: "Primary supported harness for current Forager golden-loop work.",
    },
    HostedHarnessProfileView {
        id: "claude",
        display_name: "Claude Code",
        support_status: "supported",
        launch_command: Some("claude"),
        runner: "local-tmux",
        mutation_scope: "operator-selected repository or disposable worktree",
        prompt_contract: COMPACT_FIRST_READ_PROMPT_CONTRACT,
        evidence_sources: &[
            "tmux pane capture",
            "Forager background run record",
            "runner log artifact when launched through offdesk",
            "result artifact declared on the task",
            "offdesk closeout package",
        ],
        result_artifact: "task-declared result sidecar or closeout RETURN_PACKAGE.md",
        failure_signal: "missing tmux runtime, nonzero runner exit, stale heartbeat/progress, or missing result artifact",
        closeout_package: "offdesk closeout plan plus Ondesk return package",
        retention_policy: "preserve command summary, logs, result sidecar, closeout package, and review verdict",
        notes: "Primary supported harness alongside Codex for current Forager golden-loop work.",
    },
    HostedHarnessProfileView {
        id: "gemini",
        display_name: "Gemini CLI",
        support_status: "planned",
        launch_command: Some("gemini"),
        runner: "local-tmux",
        mutation_scope: "not yet part of the supported golden loop",
        prompt_contract: PLANNED_PROMPT_CONTRACT,
        evidence_sources: &["to be validated with a disposable smoke task"],
        result_artifact: "planned",
        failure_signal: "planned",
        closeout_package: "planned",
        retention_policy: "planned",
        notes: "Registry entry exists, but the hosted harness evidence contract is not yet validated.",
    },
    HostedHarnessProfileView {
        id: "openhands",
        display_name: "OpenHands",
        support_status: "planned",
        launch_command: None,
        runner: "external-or-local",
        mutation_scope: "not yet part of the supported golden loop",
        prompt_contract: PLANNED_PROMPT_CONTRACT,
        evidence_sources: &["to be defined after integration smoke"],
        result_artifact: "planned",
        failure_signal: "planned",
        closeout_package: "planned",
        retention_policy: "planned",
        notes: "Future integration candidate; not a current support target.",
    },
    HostedHarnessProfileView {
        id: "aider",
        display_name: "Aider",
        support_status: "planned",
        launch_command: None,
        runner: "local-tmux",
        mutation_scope: "not yet part of the supported golden loop",
        prompt_contract: PLANNED_PROMPT_CONTRACT,
        evidence_sources: &["to be defined after integration smoke"],
        result_artifact: "planned",
        failure_signal: "planned",
        closeout_package: "planned",
        retention_policy: "planned",
        notes: "Future integration candidate; not a current support target.",
    },
];

pub(super) struct HarnessPromptRequest {
    pub(super) task: String,
    pub(super) first_reads: Vec<PathBuf>,
    pub(super) result_artifact: Option<PathBuf>,
    pub(super) workdir: Option<PathBuf>,
    pub(super) output: Option<PathBuf>,
    pub(super) max_first_read_total_bytes: Option<u64>,
}

#[derive(Serialize)]
pub(super) struct HostedHarnessPromptPacket {
    harness_id: String,
    display_name: String,
    support_status: String,
    prompt_strategy: String,
    inline_context_budget_bytes: usize,
    first_read_file_budget_bytes: u64,
    first_read_total_budget_bytes: u64,
    first_read_required: bool,
    first_read_total_bytes: u64,
    pub(super) first_read_budget_status: String,
    task: String,
    workdir: Option<String>,
    first_reads: Vec<HostedHarnessFirstRead>,
    result_artifact: Option<String>,
    pub(super) output_path: Option<String>,
    pub(super) warnings: Vec<String>,
    pub(super) prompt_markdown: String,
}

#[derive(Serialize)]
struct HostedHarnessFirstRead {
    path: String,
    present: bool,
    size_bytes: Option<u64>,
    over_file_budget: bool,
}

pub(super) fn hosted_harness_profiles() -> &'static [HostedHarnessProfileView] {
    HOSTED_HARNESS_PROFILES
}

pub(super) fn hosted_harness_profile(id: &str) -> Option<&'static HostedHarnessProfileView> {
    HOSTED_HARNESS_PROFILES
        .iter()
        .find(|profile| profile.id.eq_ignore_ascii_case(id))
}

pub(super) fn build_harness_prompt_packet(
    profile: &HostedHarnessProfileView,
    request: HarnessPromptRequest,
) -> Result<HostedHarnessPromptPacket> {
    let first_read_total_budget_bytes = request
        .max_first_read_total_bytes
        .unwrap_or(profile.prompt_contract.first_read_total_budget_bytes);
    let first_read_file_budget_bytes = profile.prompt_contract.first_read_file_budget_bytes;
    let first_reads = request
        .first_reads
        .iter()
        .map(|path| {
            let size_bytes = path
                .metadata()
                .ok()
                .filter(|meta| meta.is_file())
                .map(|meta| meta.len());
            HostedHarnessFirstRead {
                path: path.display().to_string(),
                present: path.exists(),
                size_bytes,
                over_file_budget: size_bytes
                    .is_some_and(|size| size > first_read_file_budget_bytes),
            }
        })
        .collect::<Vec<_>>();
    let first_read_total_bytes = first_reads
        .iter()
        .filter_map(|read| read.size_bytes)
        .sum::<u64>();
    let workdir = request.workdir.map(|path| path.display().to_string());
    let result_artifact = request
        .result_artifact
        .map(|path| path.display().to_string());
    let output_path = request.output.map(|path| path.display().to_string());
    let mut warnings = Vec::new();
    let mut first_read_budget_warning = false;
    if profile.prompt_contract.first_read_required && first_reads.is_empty() {
        first_read_budget_warning = true;
        warnings.push("no first-read artifacts were provided".to_string());
    }
    let missing_first_reads = first_reads.iter().filter(|read| !read.present).count();
    if missing_first_reads > 0 {
        first_read_budget_warning = true;
        warnings.push(format!(
            "{missing_first_reads} first-read artifact(s) are missing"
        ));
    }
    for read in first_reads.iter().filter(|read| read.over_file_budget) {
        if let Some(size) = read.size_bytes {
            first_read_budget_warning = true;
            warnings.push(format!(
                "first-read artifact {} is {} bytes; profile file budget is {} bytes",
                read.path, size, first_read_file_budget_bytes
            ));
        }
    }
    if first_read_total_bytes > first_read_total_budget_bytes {
        first_read_budget_warning = true;
        warnings.push(format!(
            "first-read artifacts total {} bytes; budget is {} bytes",
            first_read_total_bytes, first_read_total_budget_bytes
        ));
    }
    if request.task.len() > profile.prompt_contract.inline_context_budget_bytes {
        warnings.push(format!(
            "task text is {} bytes; profile inline budget is {} bytes",
            request.task.len(),
            profile.prompt_contract.inline_context_budget_bytes
        ));
    }
    let prompt_markdown = render_harness_prompt_markdown(
        profile,
        &request.task,
        workdir.as_deref(),
        &first_reads,
        result_artifact.as_deref(),
    );

    Ok(HostedHarnessPromptPacket {
        harness_id: profile.id.to_string(),
        display_name: profile.display_name.to_string(),
        support_status: profile.support_status.to_string(),
        prompt_strategy: profile.prompt_contract.strategy.to_string(),
        inline_context_budget_bytes: profile.prompt_contract.inline_context_budget_bytes,
        first_read_file_budget_bytes,
        first_read_total_budget_bytes,
        first_read_required: profile.prompt_contract.first_read_required,
        first_read_total_bytes,
        first_read_budget_status: if first_read_budget_warning {
            "warning"
        } else {
            "ok"
        }
        .to_string(),
        task: request.task,
        workdir,
        first_reads,
        result_artifact,
        output_path,
        warnings,
        prompt_markdown,
    })
}

fn render_harness_prompt_markdown(
    profile: &HostedHarnessProfileView,
    task: &str,
    workdir: Option<&str>,
    first_reads: &[HostedHarnessFirstRead],
    result_artifact: Option<&str>,
) -> String {
    let mut output = String::new();
    output.push_str("# Hosted Harness Start Packet\n\n");
    output.push_str(&format!(
        "- harness: {} (`{}`)\n",
        profile.display_name, profile.id
    ));
    output.push_str(&format!(
        "- strategy: `{}`\n",
        profile.prompt_contract.strategy
    ));
    output.push_str(&format!(
        "- inline_context_budget_bytes: `{}`\n",
        profile.prompt_contract.inline_context_budget_bytes
    ));
    output.push_str(&format!(
        "- first_read_file_budget_bytes: `{}`\n",
        profile.prompt_contract.first_read_file_budget_bytes
    ));
    output.push_str(&format!(
        "- first_read_total_budget_bytes: `{}`\n",
        profile.prompt_contract.first_read_total_budget_bytes
    ));
    if let Some(workdir) = workdir {
        output.push_str(&format!("- workdir: `{workdir}`\n"));
    }
    if let Some(result_artifact) = result_artifact {
        output.push_str(&format!("- result_artifact: `{result_artifact}`\n"));
    }
    output.push_str("\n## Task\n\n");
    output.push_str(task.trim());
    output.push_str("\n\n## Operating Contract\n\n");
    output.push_str("- Use this compact prompt as the instruction surface.\n");
    output.push_str("- Read the first-read artifacts before making a decision.\n");
    output.push_str(
        "- Do not ask the operator to paste full git diffs, raw logs, or scrollback inline.\n",
    );
    output.push_str(
        "- Summarize missing context as explicit missing evidence instead of guessing.\n",
    );
    output
        .push_str("- Write or inspect the declared result artifact before reporting completion.\n");
    output.push_str("\n## First-Read Artifacts\n\n");
    if first_reads.is_empty() {
        output.push_str(
            "- None provided. Ask for a first-read artifact before using large inline context.\n",
        );
    } else {
        for read in first_reads {
            let present = if read.present { "present" } else { "missing" };
            let size = read
                .size_bytes
                .map(|bytes| format!(", {bytes} bytes"))
                .unwrap_or_default();
            let budget = if read.over_file_budget {
                ", over file budget"
            } else {
                ""
            };
            output.push_str(&format!("- `{}` ({present}{size}{budget})\n", read.path));
        }
    }
    output.push_str("\n## Response Contract\n\n");
    output.push_str("- verdict: pass, caution, or fail\n");
    output.push_str("- evidence_read: paths actually read\n");
    output.push_str("- strongest_positive_signal\n");
    output.push_str("- strongest_risk\n");
    output.push_str("- one_next_action\n");
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn prompt_request(first_reads: Vec<PathBuf>) -> HarnessPromptRequest {
        HarnessPromptRequest {
            task: "Review the declared evidence.".to_string(),
            first_reads,
            result_artifact: Some(PathBuf::from("result.json")),
            workdir: Some(PathBuf::from("/tmp/worktree")),
            output: None,
            max_first_read_total_bytes: None,
        }
    }

    #[test]
    fn profile_lookup_is_case_insensitive() {
        assert_eq!(
            hosted_harness_profile("CoDeX").map(|profile| profile.id),
            Some("codex")
        );
        assert!(hosted_harness_profile("unknown").is_none());
    }

    #[test]
    fn prompt_packet_reports_missing_first_read() -> Result<()> {
        let temp = tempdir()?;
        let missing = temp.path().join("missing.md");
        let packet = build_harness_prompt_packet(
            hosted_harness_profile("claude").expect("claude profile"),
            prompt_request(vec![missing.clone()]),
        )?;

        assert_eq!(packet.first_read_budget_status, "warning");
        assert!(packet
            .warnings
            .iter()
            .any(|warning| warning.contains("1 first-read artifact(s) are missing")));
        assert!(packet
            .prompt_markdown
            .contains(&format!("`{}` (missing)", missing.display())));
        Ok(())
    }

    #[test]
    fn prompt_packet_applies_file_and_total_budgets() -> Result<()> {
        let temp = tempdir()?;
        let exact = temp.path().join("exact.md");
        let oversized = temp.path().join("oversized.md");
        std::fs::write(&exact, vec![b'x'; 65_536])?;
        std::fs::write(&oversized, vec![b'y'; 65_537])?;
        let mut request = prompt_request(vec![exact, oversized.clone()]);
        request.max_first_read_total_bytes = Some(100_000);

        let packet = build_harness_prompt_packet(
            hosted_harness_profile("codex").expect("codex profile"),
            request,
        )?;

        assert_eq!(packet.first_read_budget_status, "warning");
        assert!(packet.warnings.iter().any(|warning| {
            warning.contains(&format!(
                "first-read artifact {} is 65537 bytes",
                oversized.display()
            ))
        }));
        assert!(packet
            .warnings
            .iter()
            .any(|warning| warning.contains("first-read artifacts total 131073 bytes")));
        Ok(())
    }
}
