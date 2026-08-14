//! Offdesk deck assembly, rendering, and presentation adapter.
//!
//! The source JSON remains authoritative. This module may write only the
//! selected Markdown deck and optional Marp render output.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, ValueEnum};
use serde::Serialize;
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::offdesk::{operator_safe_report, operator_safe_text};

#[derive(Args)]
pub struct DeckArgs {
    /// Source Offdesk JSON artifact to summarize into a Marp deck
    #[arg(long = "from")]
    source: PathBuf,

    /// Artifact shape. Use auto unless the source is ambiguous.
    #[arg(long, value_enum, default_value = "auto")]
    kind: OffdeskDeckKind,

    /// Markdown deck output path. Defaults to `<source-stem>.marp.md`.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Overwrite the Markdown deck or rendered artifact if it already exists
    #[arg(long)]
    force: bool,

    /// Optional deck title
    #[arg(long)]
    title: Option<String>,

    /// Render the deck with Marp CLI after writing Markdown
    #[arg(long, value_enum)]
    render: Option<OffdeskDeckRenderFormat>,

    /// Marp CLI binary to use with --render
    #[arg(long, default_value = "marp")]
    marp_bin: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum OffdeskDeckKind {
    Auto,
    Closeout,
    Plan,
    Status,
}

impl OffdeskDeckKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Closeout => "closeout",
            Self::Plan => "plan",
            Self::Status => "status",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum OffdeskDeckRenderFormat {
    Html,
    Pdf,
    Pptx,
}

impl OffdeskDeckRenderFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Html => "html",
            Self::Pdf => "pdf",
            Self::Pptx => "pptx",
        }
    }

    fn extension(self) -> &'static str {
        self.as_str()
    }
}

#[derive(Serialize)]
struct OffdeskDeckReport {
    schema: &'static str,
    generated_at: DateTime<Utc>,
    source_path: String,
    source_kind: String,
    marp_markdown_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    rendered_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    render_format: Option<String>,
    render_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    render_error: Option<String>,
    source_of_truth: &'static str,
}

pub(super) fn run_deck(args: DeckArgs) -> Result<()> {
    let json = args.json;
    let report = build_deck_report(&args)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_deck_report(&report);
    Ok(())
}

fn build_deck_report(args: &DeckArgs) -> Result<OffdeskDeckReport> {
    let generated_at = Utc::now();
    let source = read_json_file(&args.source)?;
    let source_kind = detect_offdesk_deck_kind(&source, args.kind);
    let title = args
        .title
        .as_deref()
        .map(operator_safe_text)
        .unwrap_or_else(|| default_offdesk_deck_title(&source, source_kind));
    let markdown_path = args
        .out
        .clone()
        .unwrap_or_else(|| default_offdesk_deck_path(&args.source));
    let markdown =
        render_offdesk_deck_markdown(&source, source_kind, &title, &args.source, generated_at);
    write_deck_artifact(&markdown_path, markdown.as_bytes(), args.force)
        .with_context(|| format!("write Marp deck {}", markdown_path.display()))?;

    let mut rendered_path = None;
    let mut render_format = None;
    let render_status = if let Some(format) = args.render {
        let output_path = offdesk_deck_render_path(&markdown_path, format);
        render_offdesk_deck_with_marp(
            &args.marp_bin,
            &markdown_path,
            &output_path,
            format,
            args.force,
        )?;
        rendered_path = Some(output_path.display().to_string());
        render_format = Some(format.as_str().to_string());
        "rendered".to_string()
    } else {
        "not_requested".to_string()
    };

    Ok(OffdeskDeckReport {
        schema: "offdesk_marp_deck.v1",
        generated_at,
        source_path: args.source.display().to_string(),
        source_kind: source_kind.as_str().to_string(),
        marp_markdown_path: markdown_path.display().to_string(),
        rendered_path,
        render_format,
        render_status,
        render_error: None,
        source_of_truth: "source JSON remains authoritative",
    })
}

fn detect_offdesk_deck_kind(value: &Value, explicit: OffdeskDeckKind) -> OffdeskDeckKind {
    if explicit != OffdeskDeckKind::Auto {
        return explicit;
    }
    if value.get("closeout_id").is_some()
        || value.get("closeout_receipt").is_some()
        || value.pointer("/artifacts/closeout_plan_json").is_some()
    {
        return OffdeskDeckKind::Closeout;
    }
    if value.get("plan_id").is_some()
        || value.get("ready_for_operator_review").is_some()
        || value.get("does_not_authorize").is_some()
        || value.get("launch_prep_id").is_some()
    {
        return OffdeskDeckKind::Plan;
    }
    OffdeskDeckKind::Status
}

fn default_offdesk_deck_title(value: &Value, kind: OffdeskDeckKind) -> String {
    match kind {
        OffdeskDeckKind::Closeout => deck_text_at(value, "/closeout_id")
            .map(|id| format!("Offdesk Closeout {id}"))
            .unwrap_or_else(|| "Offdesk Closeout Review".to_string()),
        OffdeskDeckKind::Plan => deck_text_at(value, "/plan_id")
            .or_else(|| deck_text_at(value, "/launch_prep_id"))
            .map(|id| format!("Offdesk Plan {id}"))
            .unwrap_or_else(|| "Offdesk Plan Review".to_string()),
        OffdeskDeckKind::Status | OffdeskDeckKind::Auto => "Offdesk Status Review".to_string(),
    }
}

fn default_offdesk_deck_path(source_path: &Path) -> PathBuf {
    let parent = source_path.parent().unwrap_or_else(|| Path::new(""));
    let stem = source_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("offdesk_artifact");
    parent.join(format!("{stem}.marp.md"))
}

fn write_deck_artifact(path: &Path, bytes: &[u8], force: bool) -> io::Result<usize> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    if force {
        fs::write(path, bytes)?;
        Ok(bytes.len())
    } else {
        write_new_file(path, bytes)
    }
}

fn offdesk_deck_render_path(markdown_path: &Path, format: OffdeskDeckRenderFormat) -> PathBuf {
    let extension = format.extension();
    if let Some(file_name) = markdown_path.file_name().and_then(|value| value.to_str()) {
        if let Some(stem) = file_name.strip_suffix(".marp.md") {
            return markdown_path.with_file_name(format!("{stem}.{extension}"));
        }
    }
    markdown_path.with_extension(extension)
}

fn render_offdesk_deck_with_marp(
    marp_bin: &str,
    markdown_path: &Path,
    output_path: &Path,
    format: OffdeskDeckRenderFormat,
    force: bool,
) -> Result<()> {
    if output_path.exists() && !force {
        bail!(
            "render output already exists: {} (use --force to overwrite)",
            output_path.display()
        );
    }
    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create render output directory {}", parent.display()))?;
    }
    let output = Command::new(marp_bin)
        .arg(markdown_path)
        .arg("--output")
        .arg(output_path)
        .output()
        .with_context(|| {
            format!(
                "run Marp CLI `{}` for {} output",
                operator_safe_text(marp_bin),
                format.as_str()
            )
        })?;
    if !output.status.success() {
        let stdout = operator_safe_text(String::from_utf8_lossy(&output.stdout).trim());
        let stderr = operator_safe_text(String::from_utf8_lossy(&output.stderr).trim());
        bail!(
            "Marp CLI failed for {} output (status: {}, stdout: {}, stderr: {})",
            format.as_str(),
            output.status,
            stdout,
            stderr
        );
    }
    Ok(())
}

fn render_offdesk_deck_markdown(
    value: &Value,
    kind: OffdeskDeckKind,
    title: &str,
    source_path: &Path,
    generated_at: DateTime<Utc>,
) -> String {
    let mut output = String::new();
    output.push_str("---\n");
    output.push_str("marp: true\n");
    output.push_str("theme: default\n");
    output.push_str("paginate: true\n");
    output.push_str("---\n\n");
    output.push_str(&format!("# {}\n\n", deck_escape_markdown(title)));
    output.push_str(&format!(
        "- source_kind: `{}`\n",
        deck_escape_markdown(kind.as_str())
    ));
    output.push_str(&format!(
        "- source_file: `{}`\n",
        deck_escape_markdown(&deck_file_name(source_path))
    ));
    output.push_str(&format!("- generated_at: `{generated_at}`\n"));
    output.push_str("- source_of_truth: source JSON remains authoritative\n");
    output.push_str("- boundary: review surface only; does not approve or execute work\n");

    match kind {
        OffdeskDeckKind::Closeout => render_closeout_deck_slides(&mut output, value),
        OffdeskDeckKind::Plan => render_plan_deck_slides(&mut output, value),
        OffdeskDeckKind::Status | OffdeskDeckKind::Auto => {
            render_status_deck_slides(&mut output, value)
        }
    }

    output
}

fn render_closeout_deck_slides(output: &mut String, value: &Value) {
    output.push_str("\n---\n\n## Closeout State\n\n");
    deck_push_optional(output, "closeout_id", deck_text_at(value, "/closeout_id"));
    deck_push_optional(output, "profile", deck_text_at(value, "/profile"));
    deck_push_optional(
        output,
        "completed_tasks",
        deck_text_at(value, "/summary/completed_tasks"),
    );
    deck_push_optional(
        output,
        "active_or_blocked_tasks",
        deck_text_at(value, "/summary/active_or_blocked_tasks"),
    );
    deck_push_optional(
        output,
        "missing_artifacts",
        deck_text_at(value, "/summary/missing_artifacts"),
    );
    deck_push_optional(
        output,
        "return_package_required",
        deck_text_at(value, "/summary/return_package_required"),
    );
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Review And Decisions\n\n");
    deck_push_count(
        output,
        "open_decisions",
        deck_array_len_at(value, "/open_decisions"),
    );
    deck_push_count(
        output,
        "verification_commands",
        deck_array_len_at(value, "/verification_commands"),
    );
    deck_push_count(
        output,
        "required_first_reads",
        deck_array_len_at(value, "/required_first_reads"),
    );
    deck_push_limited_items(
        output,
        "open decision",
        deck_items_at(value, "/open_decisions", 5),
    );
    deck_push_limited_items(
        output,
        "verification",
        deck_items_at(value, "/verification_commands", 4),
    );
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Evidence Surface\n\n");
    deck_push_optional(
        output,
        "artifact_dir",
        deck_text_at(value, "/artifact_dir").map(deck_file_name_from_text),
    );
    deck_push_limited_items(output, "artifact", deck_artifact_items(value, 8));
    deck_push_limited_items(
        output,
        "first read",
        deck_items_at(value, "/required_first_reads", 5),
    );
    deck_push_empty_if_needed(output);
}

fn render_plan_deck_slides(output: &mut String, value: &Value) {
    output.push_str("\n---\n\n## Plan Identity\n\n");
    for pointer in [
        ("/plan_id", "plan_id"),
        ("/launch_prep_id", "launch_prep_id"),
        ("/project_key", "project_key"),
        ("/request_id", "request_id"),
        ("/task_id", "task_id"),
        ("/review_status", "review_status"),
        ("/ready_for_operator_review", "ready_for_operator_review"),
    ] {
        deck_push_optional(output, pointer.1, deck_text_at(value, pointer.0));
    }
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Operator Boundary\n\n");
    deck_push_optional(
        output,
        "next_safe_action",
        deck_text_at(value, "/next_safe_action"),
    );
    deck_push_limited_items(
        output,
        "does not authorize",
        deck_items_at(value, "/does_not_authorize", 6),
    );
    deck_push_limited_items(
        output,
        "validation failure",
        deck_items_at(value, "/validation_failures", 6),
    );
    deck_push_limited_items(output, "blocker", deck_items_at(value, "/blockers", 6));
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Follow-Up\n\n");
    deck_push_limited_items(output, "follow-up", deck_items_at(value, "/followups", 6));
    deck_push_limited_items(output, "approval", deck_items_at(value, "/approvals", 6));
    deck_push_limited_items(output, "artifact", deck_artifact_items(value, 8));
    deck_push_empty_if_needed(output);
}

fn render_status_deck_slides(output: &mut String, value: &Value) {
    output.push_str("\n---\n\n## Runtime Status\n\n");
    for pointer in [
        ("/status", "status"),
        ("/state", "state"),
        ("/remote_status", "remote_status"),
        ("/agent_runtime_status", "agent_runtime_status"),
        ("/listener_status", "listener_status"),
        ("/model", "model"),
        ("/endpoint", "endpoint"),
    ] {
        deck_push_optional(output, pointer.1, deck_text_at(value, pointer.0));
    }
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Queue And Attention\n\n");
    for pointer in [
        ("/pending_approvals", "pending_approvals"),
        ("/queued_offdesk_tasks", "queued_offdesk_tasks"),
        ("/active_offdesk_tasks", "active_offdesk_tasks"),
        ("/failed_offdesk_tasks", "failed_offdesk_tasks"),
        (
            "/closeout_required_offdesk_tasks",
            "closeout_required_offdesk_tasks",
        ),
    ] {
        deck_push_count(output, pointer.1, deck_array_len_at(value, pointer.0));
    }
    deck_push_limited_items(
        output,
        "pending approval",
        deck_items_at(value, "/pending_approvals", 5),
    );
    deck_push_limited_items(output, "task", deck_items_at(value, "/tasks", 5));
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Source Keys\n\n");
    deck_push_limited_items(output, "top-level key", deck_top_level_keys(value, 10));
    deck_push_empty_if_needed(output);
}

fn deck_push_optional(output: &mut String, label: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        output.push_str(&format!(
            "- {}: {}\n",
            deck_escape_markdown(label),
            deck_escape_markdown(&value)
        ));
    }
}

fn deck_push_count(output: &mut String, label: &str, count: Option<usize>) {
    if let Some(count) = count {
        output.push_str(&format!("- {}: {}\n", deck_escape_markdown(label), count));
    }
}

fn deck_push_limited_items(output: &mut String, label: &str, items: Vec<String>) {
    for item in items {
        output.push_str(&format!(
            "- {}: {}\n",
            deck_escape_markdown(label),
            deck_escape_markdown(&item)
        ));
    }
}

fn deck_push_empty_if_needed(output: &mut String) {
    let slide = output.rsplit("\n---\n\n").next().unwrap_or(output.as_str());
    if !slide.lines().any(|line| line.starts_with("- ")) {
        output.push_str("- No matching fields found in this artifact.\n");
    }
}

fn deck_text_at(value: &Value, pointer: &str) -> Option<String> {
    let value = value.pointer(pointer)?;
    deck_value_text(value)
}

fn deck_value_text(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null | Value::Array(_) | Value::Object(_) => return None,
    };
    Some(operator_safe_text(text.trim()))
}

fn deck_array_len_at(value: &Value, pointer: &str) -> Option<usize> {
    value.pointer(pointer)?.as_array().map(Vec::len)
}

fn deck_items_at(value: &Value, pointer: &str, limit: usize) -> Vec<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(deck_item_summary)
                .take(limit)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn deck_item_summary(value: &Value) -> Option<String> {
    if let Some(text) = deck_value_text(value) {
        return Some(text);
    }
    let object = value.as_object()?;
    for key in [
        "kind",
        "detail",
        "summary",
        "title",
        "task_id",
        "request_id",
        "project_key",
        "path",
        "command",
        "reason",
        "status",
        "action",
    ] {
        if let Some(text) = object.get(key).and_then(deck_value_text) {
            return Some(format!("{key}: {text}"));
        }
    }
    Some(format!("object with {} field(s)", object.len()))
}

fn deck_artifact_items(value: &Value, limit: usize) -> Vec<String> {
    let mut items = Vec::new();
    if let Some(artifacts) = value.get("artifacts").and_then(Value::as_object) {
        for (key, value) in artifacts.iter().take(limit) {
            if let Some(text) = deck_value_text(value) {
                items.push(format!("{key}: {}", deck_file_name_from_text(text)));
            }
        }
    }
    items
}

fn deck_top_level_keys(value: &Value, limit: usize) -> Vec<String> {
    value
        .as_object()
        .map(|object| {
            object
                .keys()
                .take(limit)
                .map(|key| operator_safe_text(key))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn deck_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(operator_safe_text)
        .unwrap_or_else(|| operator_safe_text(path.to_string_lossy().as_ref()))
}

fn deck_file_name_from_text(path: String) -> String {
    Path::new(&path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(operator_safe_text)
        .unwrap_or(path)
}

fn deck_escape_markdown(text: &str) -> String {
    operator_safe_text(text)
        .replace(['\n', '\r'], " ")
        .replace('|', "\\|")
}

fn read_json_file(path: &Path) -> Result<Value> {
    serde_json::from_str(
        &fs::read_to_string(path).with_context(|| format!("read JSON {}", path.display()))?,
    )
    .with_context(|| format!("parse JSON {}", path.display()))
}

fn write_new_file(path: &Path, bytes: &[u8]) -> io::Result<usize> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    Ok(bytes.len())
}

fn print_deck_report(report: &OffdeskDeckReport) {
    println!("Offdesk Marp deck");
    println!("  generated_at: {}", report.generated_at);
    println!("  source_kind:  {}", report.source_kind);
    println!(
        "  source:       {}",
        operator_safe_report(&report.source_path).text
    );
    println!(
        "  markdown:     {}",
        operator_safe_report(&report.marp_markdown_path).text
    );
    println!("  render:       {}", report.render_status);
    if let Some(path) = report.rendered_path.as_deref() {
        println!("  rendered:     {}", operator_safe_report(path).text);
    }
    println!("  authority:    {}", report.source_of_truth);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn auto_kind_detection_and_explicit_override_are_stable() {
        assert_eq!(
            detect_offdesk_deck_kind(&json!({"closeout_id": "closeout-1"}), OffdeskDeckKind::Auto),
            OffdeskDeckKind::Closeout
        );
        assert_eq!(
            detect_offdesk_deck_kind(&json!({"plan_id": "plan-1"}), OffdeskDeckKind::Auto),
            OffdeskDeckKind::Plan
        );
        assert_eq!(
            detect_offdesk_deck_kind(&json!({"status": "healthy"}), OffdeskDeckKind::Auto),
            OffdeskDeckKind::Status
        );
        assert_eq!(
            detect_offdesk_deck_kind(
                &json!({"closeout_id": "closeout-1"}),
                OffdeskDeckKind::Status,
            ),
            OffdeskDeckKind::Status
        );
    }

    #[test]
    fn deck_paths_preserve_the_marp_stem_contract() {
        assert_eq!(
            default_offdesk_deck_path(Path::new("/tmp/closeout.json")),
            PathBuf::from("/tmp/closeout.marp.md")
        );
        assert_eq!(
            offdesk_deck_render_path(
                Path::new("/tmp/closeout.marp.md"),
                OffdeskDeckRenderFormat::Pdf,
            ),
            PathBuf::from("/tmp/closeout.pdf")
        );
        assert_eq!(
            offdesk_deck_render_path(Path::new("/tmp/review.md"), OffdeskDeckRenderFormat::Html,),
            PathBuf::from("/tmp/review.html")
        );
    }

    #[test]
    fn markdown_keeps_the_review_only_authority_boundary() -> Result<()> {
        let generated_at =
            DateTime::parse_from_rfc3339("2026-08-14T00:00:00Z")?.with_timezone(&Utc);
        let markdown = render_offdesk_deck_markdown(
            &json!({
                "plan_id": "plan_alpha",
                "does_not_authorize": ["runtime dispatch"],
                "next_safe_action": "Review the plan."
            }),
            OffdeskDeckKind::Plan,
            "Plan | Review",
            Path::new("/tmp/plan.json"),
            generated_at,
        );

        assert!(markdown.contains("# Plan \\| Review"));
        assert!(markdown.contains("source JSON remains authoritative"));
        assert!(markdown.contains("review surface only; does not approve or execute work"));
        assert!(markdown.contains("does not authorize: runtime dispatch"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn deck_report_runs_marp_and_records_the_rendered_artifact() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir()?;
        let marp_bin = temp.path().join("fake-marp");
        let source_path = temp.path().join("status.json");
        let markdown_path = temp.path().join("review.marp.md");
        let output_path = temp.path().join("review.pdf");
        fs::write(
            &marp_bin,
            "#!/bin/sh\n[ \"$2\" = \"--output\" ] || exit 2\ncp \"$1\" \"$3\"\n",
        )?;
        let mut permissions = fs::metadata(&marp_bin)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&marp_bin, permissions)?;
        fs::write(&source_path, r#"{"status":"healthy"}"#)?;

        let report = build_deck_report(&DeckArgs {
            source: source_path,
            kind: OffdeskDeckKind::Auto,
            out: Some(markdown_path.clone()),
            force: false,
            title: Some("Runtime Review".to_string()),
            render: Some(OffdeskDeckRenderFormat::Pdf),
            marp_bin: marp_bin.display().to_string(),
            json: true,
        })?;

        assert_eq!(report.source_kind, "status");
        assert_eq!(report.render_status, "rendered");
        assert_eq!(report.render_format.as_deref(), Some("pdf"));
        assert_eq!(report.rendered_path.as_deref(), output_path.to_str());
        assert_eq!(
            fs::read_to_string(output_path)?,
            fs::read_to_string(markdown_path)?
        );
        Ok(())
    }
}
