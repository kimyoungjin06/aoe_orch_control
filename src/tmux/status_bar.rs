//! tmux status bar configuration for Forager sessions

use anyhow::Result;
use std::process::Command;

const TITLE_OPTION: &str = "@forager_title";
const PROJECT_OPTION: &str = "@forager_project";
const BRANCH_OPTION: &str = "@forager_branch";
const SANDBOX_OPTION: &str = "@forager_sandbox";
const LEGACY_TITLE_OPTION: &str = "@aoe_title";
const LEGACY_BRANCH_OPTION: &str = "@aoe_branch";
const LEGACY_SANDBOX_OPTION: &str = "@aoe_sandbox";

/// Legacy sandbox metadata for status bar display.
pub struct SandboxDisplay {
    pub container_name: String,
}

/// Set the @forager_* metadata options on a session without touching the
/// status bar styling. Applied unconditionally so a user's own tmux config
/// can reference #{@forager_project}/#{@forager_title} even when Forager's
/// styling is suppressed by `tmux.status_bar = "auto"`.
pub fn apply_session_metadata(
    session_name: &str,
    title: &str,
    project: Option<&str>,
    branch: Option<&str>,
    sandbox: Option<&SandboxDisplay>,
) -> Result<()> {
    set_session_option(session_name, TITLE_OPTION, title)?;
    set_session_option(session_name, LEGACY_TITLE_OPTION, title)?;

    // Set the project/group so identically-titled sessions (Claude, Codex,
    // Orchestrator across many projects) stay distinguishable in the bar.
    if let Some(project_name) = project.filter(|p| !p.trim().is_empty()) {
        set_session_option(session_name, PROJECT_OPTION, project_name)?;
    }

    // Set branch if provided (for worktree sessions)
    if let Some(branch_name) = branch {
        set_session_option(session_name, BRANCH_OPTION, branch_name)?;
        set_session_option(session_name, LEGACY_BRANCH_OPTION, branch_name)?;
    }

    // Preserve legacy sandbox metadata if the stored session has it.
    if let Some(sandbox_info) = sandbox {
        set_session_option(session_name, SANDBOX_OPTION, &sandbox_info.container_name)?;
        set_session_option(
            session_name,
            LEGACY_SANDBOX_OPTION,
            &sandbox_info.container_name,
        )?;
    }

    Ok(())
}

/// Apply Forager-styled status bar configuration to a tmux session.
///
/// Sets the metadata options and configures status-left/right styling.
pub fn apply_status_bar(
    session_name: &str,
    title: &str,
    project: Option<&str>,
    branch: Option<&str>,
    sandbox: Option<&SandboxDisplay>,
) -> Result<()> {
    apply_session_metadata(session_name, title, project, branch, sandbox)?;

    // Configure the status bar format using Forager's KISTI-aligned theme
    // colour32 = KISTI blue approximation, colour39 = cyan, colour160 = KISTI red approximation
    // colour235 = dark background
    //
    // Format: "forager: Project:Title | branch | [legacy container] | 14:30"
    // - #{?#{@forager_project},#{@forager_project}:,}: conditional project display
    // - #{@forager_title}: session title
    // - #{?#{@forager_branch}, | #{@forager_branch},}: conditional branch display
    // - #{?#{@forager_sandbox}, [#{@forager_sandbox}],}: conditional sandbox display
    let status_format = concat!(
        " #[fg=colour32,bold]forager#[fg=colour252,nobold]: ",
        "#{?#{@forager_project},#[fg=colour39]#{@forager_project}#[fg=colour252]:,}",
        "#{@forager_title}",
        "#{?#{@forager_branch}, #[fg=colour39]| #{@forager_branch}#[fg=colour252],}",
        "#{?#{@forager_sandbox}, #[fg=colour160]⬡ #{@forager_sandbox}#[fg=colour252],}",
        " | %H:%M "
    );

    set_session_option(session_name, "status-right", status_format)?;
    set_session_option(session_name, "status-right-length", "80")?;

    // Dark background with light text - matches the Forager website.
    // status-left leads with project · title (the operator-facing identity);
    // the raw #S session name stays available via tmux itself.
    set_session_option(session_name, "status-style", "bg=colour235,fg=colour252")?;
    set_session_option(
        session_name,
        "status-left",
        " #[fg=colour32,bold]#{?#{@forager_project},#{@forager_project}:,}#{@forager_title}#[fg=colour252,nobold] │ #[fg=colour245]Ctrl+b d#[fg=colour240] to detach ",
    )?;
    set_session_option(session_name, "status-left-length", "60")?;

    // Report "Group:Title" as the outer terminal/tab title (PowerShell,
    // Windows Terminal, ...) instead of the raw forager_* session name.
    set_session_option(session_name, "set-titles", "on")?;
    set_session_option(
        session_name,
        "set-titles-string",
        "#{?#{@forager_title},#{?#{@forager_project},#{@forager_project}:,}#{@forager_title},#S}",
    )?;

    Ok(())
}

/// Set a tmux option for a specific session.
fn set_session_option(session_name: &str, option: &str, value: &str) -> Result<()> {
    let output = Command::new("tmux")
        .args(["set-option", "-t", session_name, option, value])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Don't fail on option errors - status bar is non-critical
        tracing::debug!("Failed to set tmux option {}: {}", option, stderr);
    }

    Ok(())
}

/// Apply mouse support option to a tmux session.
/// When enabled, scrolling with the mouse wheel enters copy mode.
pub fn apply_mouse_option(session_name: &str, enabled: bool) -> Result<()> {
    let value = if enabled { "on" } else { "off" };
    set_session_option(session_name, "mouse", value)
}

/// Apply all configured tmux options to a session.
/// This is a unified entry point that applies status bar styling and mouse settings.
pub fn apply_all_tmux_options(
    session_name: &str,
    title: &str,
    project: Option<&str>,
    branch: Option<&str>,
    sandbox: Option<&SandboxDisplay>,
) {
    use crate::session::config::{should_apply_tmux_mouse, should_apply_tmux_status_bar};

    if should_apply_tmux_status_bar() {
        if let Err(e) = apply_status_bar(session_name, title, project, branch, sandbox) {
            tracing::debug!("Failed to apply tmux status bar: {}", e);
        }
    } else if let Err(e) = apply_session_metadata(session_name, title, project, branch, sandbox) {
        // Styling is suppressed (user tmux config or disabled), but the
        // @forager_* metadata stays available for the user's own bar.
        tracing::debug!("Failed to apply tmux session metadata: {}", e);
    }

    if let Some(mouse_enabled) = should_apply_tmux_mouse() {
        if let Err(e) = apply_mouse_option(session_name, mouse_enabled) {
            tracing::debug!("Failed to apply tmux mouse option: {}", e);
        }
    }
}

/// Session info retrieved from tmux user options.
pub struct SessionInfo {
    pub title: String,
    pub project: Option<String>,
    pub branch: Option<String>,
    pub sandbox: Option<String>,
}

/// Get session info for the current tmux session (used by `forager tmux status`).
/// Returns structured session info for use in user's custom tmux status bar.
pub fn get_session_info_for_current() -> Option<SessionInfo> {
    let session_name = crate::tmux::get_current_session_name()?;

    // Check if this is a Forager session
    if !crate::tmux::is_forager_session_name(&session_name) {
        return None;
    }

    // Try to get the Forager title from tmux user option
    let title = get_session_option(&session_name, TITLE_OPTION)
        .or_else(|| get_session_option(&session_name, LEGACY_TITLE_OPTION))
        .unwrap_or_else(|| {
            // Fallback: extract title from session name
            // Session names are: forager_<title>_<id> or legacy aoe_<title>_<id>
            let name_without_prefix = crate::tmux::strip_forager_session_prefix(&session_name);
            if let Some(last_underscore) = name_without_prefix.rfind('_') {
                name_without_prefix[..last_underscore].to_string()
            } else {
                name_without_prefix.to_string()
            }
        });

    let project = get_session_option(&session_name, PROJECT_OPTION);
    let branch = get_session_option(&session_name, BRANCH_OPTION)
        .or_else(|| get_session_option(&session_name, LEGACY_BRANCH_OPTION));
    let sandbox = get_session_option(&session_name, SANDBOX_OPTION)
        .or_else(|| get_session_option(&session_name, LEGACY_SANDBOX_OPTION));

    Some(SessionInfo {
        title,
        project,
        branch,
        sandbox,
    })
}

/// Get formatted status string for the current tmux session.
/// Returns a plain text string like "forager: Title | branch | [legacy container]"
pub fn get_status_for_current_session() -> Option<String> {
    let info = get_session_info_for_current()?;

    let mut result = match &info.project {
        Some(project) => format!("forager: {}:{}", project, info.title),
        None => format!("forager: {}", info.title),
    };

    if let Some(b) = &info.branch {
        result.push_str(" | ");
        result.push_str(b);
    }

    if let Some(s) = &info.sandbox {
        result.push_str(" [");
        result.push_str(s);
        result.push(']');
    }

    Some(result)
}

/// Get a tmux option value for a session.
fn get_session_option(session_name: &str, option: &str) -> Option<String> {
    let output = Command::new("tmux")
        .args(["show-options", "-t", session_name, "-v", option])
        .output()
        .ok()?;

    if output.status.success() {
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !value.is_empty() {
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_status_returns_none_for_non_tmux() {
        // When not in tmux, get_current_session_name returns None
        // so get_status_for_current_session should also return None
        // This test just verifies the function doesn't panic
        let _ = get_status_for_current_session();
    }
}
