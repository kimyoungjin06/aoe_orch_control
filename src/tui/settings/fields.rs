//! Setting field definitions and config mapping

use crate::session::{
    validate_check_interval, validate_path_exists, Config, ProfileConfig, TmuxMouseMode,
    TmuxStatusBarMode,
};
use crate::sound::{validate_sound_exists, SoundMode};

use super::SettingsScope;

/// Categories of settings
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsCategory {
    General,
    Updates,
    Worktree,
    Cleanup,
    Tmux,
    Session,
    Claude,
    Diff,
    Sound,
    Hooks,
}

impl SettingsCategory {
    pub fn label(&self) -> &'static str {
        match self {
            Self::General => "General",
            Self::Updates => "Updates",
            Self::Worktree => "Worktree",
            Self::Cleanup => "Cleanup",
            Self::Tmux => "Tmux",
            Self::Session => "Session",
            Self::Claude => "Claude",
            Self::Diff => "Diff",
            Self::Sound => "Sound",
            Self::Hooks => "Hooks",
        }
    }
}

/// Type-safe field identifiers (prevents typos in string matching)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKey {
    // General
    DefaultProfile,
    // Updates
    CheckEnabled,
    CheckIntervalHours,
    NotifyInCli,
    // Worktree
    PathTemplate,
    BareRepoPathTemplate,
    WorktreeAutoCleanup,
    ShowBranchInTui,
    DeleteBranchOnCleanup,
    // Legacy cleanup
    SandboxAutoCleanup,
    // Session
    DefaultTool,
    YoloModeDefault,
    AutoOrchestrator,
    OrchestratorTitle,
    OrchestratorCommand,
    // Claude
    ClaudeConfigDir,
    // Diff
    DiffDefaultBranch,
    DiffContextLines,
    // Tmux
    StatusBar,
    Mouse,
    // Sound
    SoundEnabled,
    SoundMode,
    SoundOnStart,
    SoundOnRunning,
    SoundOnWaiting,
    SoundOnIdle,
    SoundOnError,
    // Hooks
    HookOnCreate,
    HookOnLaunch,
}

/// Resolve a field value from global config and optional profile override.
/// Returns (value, has_override).
fn resolve_value<T: Clone>(scope: SettingsScope, global: T, profile: Option<T>) -> (T, bool) {
    match scope {
        SettingsScope::Global => (global, false),
        SettingsScope::Profile | SettingsScope::Repo => {
            let has_override = profile.is_some();
            let value = profile.unwrap_or(global);
            (value, has_override)
        }
    }
}

/// Resolve an optional field (Option<T>) where both global and profile values are Option<T>.
/// The `has_explicit_override` flag indicates if the profile explicitly set this field.
fn resolve_optional<T: Clone>(
    scope: SettingsScope,
    global: Option<T>,
    profile: Option<T>,
    has_explicit_override: bool,
) -> (Option<T>, bool) {
    match scope {
        SettingsScope::Global => (global, false),
        SettingsScope::Profile | SettingsScope::Repo => {
            let value = profile.or(global);
            (value, has_explicit_override)
        }
    }
}

/// Helper to set or clear a profile override based on whether value matches global.
fn set_or_clear_override<T, S, F>(
    new_value: T,
    global_value: &T,
    section: &mut Option<S>,
    set_field: F,
) where
    T: Clone + PartialEq,
    S: Default,
    F: FnOnce(&mut S, Option<T>),
{
    if new_value == *global_value {
        if let Some(ref mut s) = section {
            set_field(s, None);
        }
    } else {
        let s = section.get_or_insert_with(S::default);
        set_field(s, Some(new_value));
    }
}

fn set_optional_string_override<S, F>(
    new_value: &Option<String>,
    global_value: &Option<String>,
    section: &mut Option<S>,
    set_field: F,
) where
    S: Default,
    F: FnOnce(&mut S, Option<String>),
{
    let override_value = if new_value == global_value {
        None
    } else {
        Some(new_value.clone().unwrap_or_default())
    };

    if override_value.is_some() {
        let value = section.get_or_insert_with(S::default);
        set_field(value, override_value);
    } else if let Some(value) = section {
        set_field(value, None);
    }
}

/// Value types for settings fields
#[derive(Debug, Clone)]
pub enum FieldValue {
    Bool(bool),
    Text(String),
    Number(u64),
    Select {
        selected: usize,
        options: Vec<String>,
    },
    List(Vec<String>),
    OptionalText(Option<String>),
}

/// A setting field with metadata
#[derive(Debug, Clone)]
pub struct SettingField {
    pub key: FieldKey,
    pub label: &'static str,
    pub description: &'static str,
    pub value: FieldValue,
    pub category: SettingsCategory,
    /// Whether this field has a profile override (only relevant in profile scope)
    pub has_override: bool,
}

impl SettingField {
    pub fn validate(&self) -> Result<(), String> {
        match (&self.key, &self.value) {
            (FieldKey::CheckIntervalHours, FieldValue::Number(n)) => {
                validate_check_interval(*n)?;
                Ok(())
            }
            (FieldKey::DiffContextLines, FieldValue::Number(n)) => usize::try_from(*n)
                .map(|_| ())
                .map_err(|_| "Diff context lines exceed this platform's limit".to_string()),
            (FieldKey::ClaudeConfigDir, FieldValue::OptionalText(Some(path))) => {
                validate_path_exists(path)
            }
            // Sound field validation - check if sound file exists
            (
                FieldKey::SoundOnStart
                | FieldKey::SoundOnRunning
                | FieldKey::SoundOnWaiting
                | FieldKey::SoundOnIdle
                | FieldKey::SoundOnError,
                FieldValue::OptionalText(Some(name)),
            ) => {
                if !name.is_empty() {
                    validate_sound_exists(name)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Build fields for a category based on scope and current config values.
///
/// For Repo scope, `global` should be the resolved (global+profile merged) config,
/// and `profile` should be the repo config converted to ProfileConfig via `repo_config_to_profile`.
pub fn build_fields_for_category(
    category: SettingsCategory,
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    match category {
        SettingsCategory::General => build_general_fields(global),
        SettingsCategory::Updates => build_updates_fields(scope, global, profile),
        SettingsCategory::Worktree => build_worktree_fields(scope, global, profile),
        SettingsCategory::Cleanup => build_cleanup_fields(scope, global, profile),
        SettingsCategory::Tmux => build_tmux_fields(scope, global, profile),
        SettingsCategory::Session => build_session_fields(scope, global, profile),
        SettingsCategory::Claude => build_claude_fields(scope, global, profile),
        SettingsCategory::Diff => build_diff_fields(scope, global, profile),
        SettingsCategory::Sound => build_sound_fields(scope, global, profile),
        SettingsCategory::Hooks => build_hooks_fields(scope, global, profile),
    }
}

fn build_general_fields(global: &Config) -> Vec<SettingField> {
    let mut options = crate::session::list_profiles().unwrap_or_default();
    if !options
        .iter()
        .any(|profile| profile == &global.default_profile)
    {
        options.push(global.default_profile.clone());
        options.sort();
    }
    let selected = options
        .iter()
        .position(|profile| profile == &global.default_profile)
        .unwrap_or(0);

    vec![SettingField {
        key: FieldKey::DefaultProfile,
        label: "Default Profile",
        description: "Profile used when forager starts without -p",
        value: FieldValue::Select { selected, options },
        category: SettingsCategory::General,
        has_override: false,
    }]
}

fn build_updates_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let updates = profile.updates.as_ref();

    let (check_enabled, o1) = resolve_value(
        scope,
        global.updates.check_enabled,
        updates.and_then(|u| u.check_enabled),
    );
    let (check_interval, o2) = resolve_value(
        scope,
        global.updates.check_interval_hours,
        updates.and_then(|u| u.check_interval_hours),
    );
    let (notify_in_cli, o3) = resolve_value(
        scope,
        global.updates.notify_in_cli,
        updates.and_then(|u| u.notify_in_cli),
    );

    vec![
        SettingField {
            key: FieldKey::CheckEnabled,
            label: "Check for Updates",
            description: "Automatically check for updates on startup",
            value: FieldValue::Bool(check_enabled),
            category: SettingsCategory::Updates,
            has_override: o1,
        },
        SettingField {
            key: FieldKey::CheckIntervalHours,
            label: "Check Interval (hours)",
            description: "How often to check for updates",
            value: FieldValue::Number(check_interval),
            category: SettingsCategory::Updates,
            has_override: o2,
        },
        SettingField {
            key: FieldKey::NotifyInCli,
            label: "Notify in CLI",
            description: "Show update notifications in CLI output",
            value: FieldValue::Bool(notify_in_cli),
            category: SettingsCategory::Updates,
            has_override: o3,
        },
    ]
}

fn build_worktree_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let wt = profile.worktree.as_ref();

    let (path_template, o1) = resolve_value(
        scope,
        global.worktree.path_template.clone(),
        wt.and_then(|w| w.path_template.clone()),
    );
    let (bare_repo_template, o2) = resolve_value(
        scope,
        global.worktree.bare_repo_path_template.clone(),
        wt.and_then(|w| w.bare_repo_path_template.clone()),
    );
    let (auto_cleanup, o3) = resolve_value(
        scope,
        global.worktree.auto_cleanup,
        wt.and_then(|w| w.auto_cleanup),
    );
    let (delete_branch_on_cleanup, o4) = resolve_value(
        scope,
        global.worktree.delete_branch_on_cleanup,
        wt.and_then(|w| w.delete_branch_on_cleanup),
    );
    let (show_branch_in_tui, o5) = resolve_value(
        scope,
        global.worktree.show_branch_in_tui,
        wt.and_then(|w| w.show_branch_in_tui),
    );

    vec![
        SettingField {
            key: FieldKey::PathTemplate,
            label: "Path Template",
            description: "Template for worktree paths ({repo-name}, {branch})",
            value: FieldValue::Text(path_template),
            category: SettingsCategory::Worktree,
            has_override: o1,
        },
        SettingField {
            key: FieldKey::BareRepoPathTemplate,
            label: "Bare Repo Template",
            description: "Template for bare repo worktree paths",
            value: FieldValue::Text(bare_repo_template),
            category: SettingsCategory::Worktree,
            has_override: o2,
        },
        SettingField {
            key: FieldKey::WorktreeAutoCleanup,
            label: "Auto Cleanup",
            description: "Automatically clean up worktrees on session delete",
            value: FieldValue::Bool(auto_cleanup),
            category: SettingsCategory::Worktree,
            has_override: o3,
        },
        SettingField {
            key: FieldKey::ShowBranchInTui,
            label: "Show Branch in TUI",
            description: "Display worktree branch names in the session list",
            value: FieldValue::Bool(show_branch_in_tui),
            category: SettingsCategory::Worktree,
            has_override: o5,
        },
        SettingField {
            key: FieldKey::DeleteBranchOnCleanup,
            label: "Delete Branch on Cleanup",
            description: "Also delete the git branch when deleting a worktree",
            value: FieldValue::Bool(delete_branch_on_cleanup),
            category: SettingsCategory::Worktree,
            has_override: o4,
        },
    ]
}

fn build_cleanup_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let sandbox = profile.sandbox.as_ref();
    let (auto_cleanup, has_override) = resolve_value(
        scope,
        global.sandbox.auto_cleanup,
        sandbox.and_then(|s| s.auto_cleanup),
    );

    vec![SettingField {
        key: FieldKey::SandboxAutoCleanup,
        label: "Legacy Sandbox Cleanup",
        description: "Select stored legacy sandbox containers for deletion by default",
        value: FieldValue::Bool(auto_cleanup),
        category: SettingsCategory::Cleanup,
        has_override,
    }]
}

fn build_tmux_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let tmux = profile.tmux.as_ref();

    let (status_bar, status_bar_override) = resolve_value(
        scope,
        global.tmux.status_bar,
        tmux.and_then(|t| t.status_bar),
    );

    let (mouse, mouse_override) =
        resolve_value(scope, global.tmux.mouse, tmux.and_then(|t| t.mouse));

    let status_bar_selected = match status_bar {
        TmuxStatusBarMode::Auto => 0,
        TmuxStatusBarMode::Enabled => 1,
        TmuxStatusBarMode::Disabled => 2,
    };

    let mouse_selected = match mouse {
        TmuxMouseMode::Auto => 0,
        TmuxMouseMode::Enabled => 1,
        TmuxMouseMode::Disabled => 2,
    };

    vec![
        SettingField {
            key: FieldKey::StatusBar,
            label: "Status Bar",
            description: "Control tmux status bar styling (Auto respects your tmux config)",
            value: FieldValue::Select {
                selected: status_bar_selected,
                options: vec!["Auto".into(), "Enabled".into(), "Disabled".into()],
            },
            category: SettingsCategory::Tmux,
            has_override: status_bar_override,
        },
        SettingField {
            key: FieldKey::Mouse,
            label: "Mouse Support",
            description: "Control mouse scrolling (Auto respects your tmux config)",
            value: FieldValue::Select {
                selected: mouse_selected,
                options: vec!["Auto".into(), "Enabled".into(), "Disabled".into()],
            },
            category: SettingsCategory::Tmux,
            has_override: mouse_override,
        },
    ]
}

fn build_session_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let session = profile.session.as_ref();

    let (default_tool, has_override) = resolve_optional(
        scope,
        global.session.default_tool.clone(),
        session.and_then(|s| s.default_tool.clone()),
        session.map(|s| s.default_tool.is_some()).unwrap_or(false),
    );

    let selected = crate::agents::settings_index_from_name(default_tool.as_deref());

    let mut options = vec!["Auto (first available)".to_string()];
    options.extend(crate::agents::agent_names().iter().map(|n| n.to_string()));

    let (yolo_mode_default, yolo_override) = resolve_value(
        scope,
        global.session.yolo_mode_default,
        session.and_then(|s| s.yolo_mode_default),
    );
    let (auto_orchestrator, auto_orchestrator_override) = resolve_value(
        scope,
        global.session.auto_orchestrator,
        session.and_then(|s| s.auto_orchestrator),
    );
    let (orchestrator_title, orchestrator_title_override) = resolve_optional(
        scope,
        global.session.orchestrator_title.clone(),
        session.and_then(|s| s.orchestrator_title.clone()),
        session
            .map(|s| s.orchestrator_title.is_some())
            .unwrap_or(false),
    );
    let (orchestrator_command, orchestrator_command_override) = resolve_optional(
        scope,
        global.session.orchestrator_command.clone(),
        session.and_then(|s| s.orchestrator_command.clone()),
        session
            .map(|s| s.orchestrator_command.is_some())
            .unwrap_or(false),
    );

    let mut fields = vec![
        SettingField {
            key: FieldKey::DefaultTool,
            label: "Default Tool",
            description: "Default coding tool for new sessions",
            value: FieldValue::Select { selected, options },
            category: SettingsCategory::Session,
            has_override,
        },
        SettingField {
            key: FieldKey::YoloModeDefault,
            label: "YOLO Mode Default",
            description: "Enable YOLO mode by default for new sessions",
            value: FieldValue::Bool(yolo_mode_default),
            category: SettingsCategory::Session,
            has_override: yolo_override,
        },
        SettingField {
            key: FieldKey::AutoOrchestrator,
            label: "Auto Orchestrator",
            description: "Create an orchestrator session with each project session",
            value: FieldValue::Bool(auto_orchestrator),
            category: SettingsCategory::Session,
            has_override: auto_orchestrator_override,
        },
        SettingField {
            key: FieldKey::OrchestratorTitle,
            label: "Orchestrator Title",
            description: "Optional title for automatically created orchestrator sessions",
            value: FieldValue::OptionalText(orchestrator_title),
            category: SettingsCategory::Session,
            has_override: orchestrator_title_override,
        },
        SettingField {
            key: FieldKey::OrchestratorCommand,
            label: "Orchestrator Command",
            description: "Optional command for automatically created orchestrator sessions",
            value: FieldValue::OptionalText(orchestrator_command),
            category: SettingsCategory::Session,
            has_override: orchestrator_command_override,
        },
    ];
    if scope == SettingsScope::Repo {
        fields.retain(|field| field.key != FieldKey::OrchestratorCommand);
    }
    fields
}

fn build_diff_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let diff = profile.diff.as_ref();
    let (default_branch, default_branch_override) = resolve_optional(
        scope,
        global.diff.default_branch.clone(),
        diff.and_then(|d| d.default_branch.clone()),
        diff.map(|d| d.default_branch.is_some()).unwrap_or(false),
    );
    let (context_lines, context_lines_override) = resolve_value(
        scope,
        global.diff.context_lines,
        diff.and_then(|d| d.context_lines),
    );

    vec![
        SettingField {
            key: FieldKey::DiffDefaultBranch,
            label: "Default Branch",
            description: "Base branch for diffs; empty uses repository auto-detection",
            value: FieldValue::OptionalText(default_branch),
            category: SettingsCategory::Diff,
            has_override: default_branch_override,
        },
        SettingField {
            key: FieldKey::DiffContextLines,
            label: "Context Lines",
            description: "Number of unchanged lines around each diff hunk",
            value: FieldValue::Number(u64::try_from(context_lines).unwrap_or(u64::MAX)),
            category: SettingsCategory::Diff,
            has_override: context_lines_override,
        },
    ]
}

fn build_claude_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let claude = profile.claude.as_ref();
    let (config_dir, has_override) = resolve_optional(
        scope,
        global.claude.config_dir.clone(),
        claude.and_then(|c| c.config_dir.clone()),
        claude.map(|c| c.config_dir.is_some()).unwrap_or(false),
    );

    vec![SettingField {
        key: FieldKey::ClaudeConfigDir,
        label: "Config Directory",
        description: "CLAUDE_CONFIG_DIR for newly started Claude sessions",
        value: FieldValue::OptionalText(config_dir),
        category: SettingsCategory::Claude,
        has_override,
    }]
}

fn build_sound_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let snd = profile.sound.as_ref();

    let (enabled, o1) = resolve_value(scope, global.sound.enabled, snd.and_then(|s| s.enabled));

    let (mode, o2) = resolve_value(
        scope,
        global.sound.mode.clone(),
        snd.and_then(|s| s.mode.clone()),
    );

    let mode_selected = match &mode {
        SoundMode::Random => 0,
        SoundMode::Specific(_) => 1,
    };

    let (on_start, o3) = resolve_optional(
        scope,
        global.sound.on_start.clone(),
        snd.and_then(|s| s.on_start.clone()),
        snd.map(|s| s.on_start.is_some()).unwrap_or(false),
    );
    let (on_running, o4) = resolve_optional(
        scope,
        global.sound.on_running.clone(),
        snd.and_then(|s| s.on_running.clone()),
        snd.map(|s| s.on_running.is_some()).unwrap_or(false),
    );
    let (on_waiting, o5) = resolve_optional(
        scope,
        global.sound.on_waiting.clone(),
        snd.and_then(|s| s.on_waiting.clone()),
        snd.map(|s| s.on_waiting.is_some()).unwrap_or(false),
    );
    let (on_idle, o6) = resolve_optional(
        scope,
        global.sound.on_idle.clone(),
        snd.and_then(|s| s.on_idle.clone()),
        snd.map(|s| s.on_idle.is_some()).unwrap_or(false),
    );
    let (on_error, o7) = resolve_optional(
        scope,
        global.sound.on_error.clone(),
        snd.and_then(|s| s.on_error.clone()),
        snd.map(|s| s.on_error.is_some()).unwrap_or(false),
    );

    vec![
        SettingField {
            key: FieldKey::SoundEnabled,
            label: "Enabled",
            description: "Play sounds on agent state transitions",
            value: FieldValue::Bool(enabled),
            category: SettingsCategory::Sound,
            has_override: o1,
        },
        SettingField {
            key: FieldKey::SoundMode,
            label: "Mode",
            description: "How to select sounds (Random or Specific file name)",
            value: FieldValue::Select {
                selected: mode_selected,
                options: vec!["Random".into(), "Specific".into()],
            },
            category: SettingsCategory::Sound,
            has_override: o2,
        },
        SettingField {
            key: FieldKey::SoundOnStart,
            label: "On Start",
            description: "Specify file name with extension",
            value: FieldValue::OptionalText(on_start),
            category: SettingsCategory::Sound,
            has_override: o3,
        },
        SettingField {
            key: FieldKey::SoundOnRunning,
            label: "On Running",
            description: "Specify file name with extension",
            value: FieldValue::OptionalText(on_running),
            category: SettingsCategory::Sound,
            has_override: o4,
        },
        SettingField {
            key: FieldKey::SoundOnWaiting,
            label: "On Waiting",
            description: "Specify file name with extension",
            value: FieldValue::OptionalText(on_waiting),
            category: SettingsCategory::Sound,
            has_override: o5,
        },
        SettingField {
            key: FieldKey::SoundOnIdle,
            label: "On Idle",
            description: "Specify file name with extension",
            value: FieldValue::OptionalText(on_idle),
            category: SettingsCategory::Sound,
            has_override: o6,
        },
        SettingField {
            key: FieldKey::SoundOnError,
            label: "On Error",
            description: "Specify file name with extension",
            value: FieldValue::OptionalText(on_error),
            category: SettingsCategory::Sound,
            has_override: o7,
        },
    ]
}

fn build_hooks_fields(
    scope: SettingsScope,
    global: &Config,
    profile: &ProfileConfig,
) -> Vec<SettingField> {
    let hooks = profile.hooks.as_ref();

    let (on_create, o1) = resolve_value(
        scope,
        global.hooks.on_create.clone(),
        hooks.and_then(|h| h.on_create.clone()),
    );
    let (on_launch, o2) = resolve_value(
        scope,
        global.hooks.on_launch.clone(),
        hooks.and_then(|h| h.on_launch.clone()),
    );

    vec![
        SettingField {
            key: FieldKey::HookOnCreate,
            label: "On Create",
            description: "Commands run once on the host when a session is first created.",
            value: FieldValue::List(on_create),
            category: SettingsCategory::Hooks,
            has_override: o1,
        },
        SettingField {
            key: FieldKey::HookOnLaunch,
            label: "On Launch",
            description: "Commands run on the host every time a session starts.",
            value: FieldValue::List(on_launch),
            category: SettingsCategory::Hooks,
            has_override: o2,
        },
    ]
}

/// Apply a field's value back to the appropriate config.
/// For profile scope, if the value matches global, the override is removed.
pub fn apply_field_to_config(
    field: &SettingField,
    scope: SettingsScope,
    global: &mut Config,
    profile: &mut ProfileConfig,
) {
    match scope {
        SettingsScope::Global => apply_field_to_global(field, global),
        SettingsScope::Profile | SettingsScope::Repo => {
            apply_field_to_profile(field, global, profile)
        }
    }
}

fn apply_field_to_global(field: &SettingField, config: &mut Config) {
    match (&field.key, &field.value) {
        // General
        (FieldKey::DefaultProfile, FieldValue::Select { selected, options }) => {
            if let Some(profile) = options.get(*selected) {
                config.default_profile = profile.clone();
            }
        }
        // Updates
        (FieldKey::CheckEnabled, FieldValue::Bool(v)) => config.updates.check_enabled = *v,
        (FieldKey::CheckIntervalHours, FieldValue::Number(v)) => {
            config.updates.check_interval_hours = *v
        }
        (FieldKey::NotifyInCli, FieldValue::Bool(v)) => config.updates.notify_in_cli = *v,
        // Worktree
        (FieldKey::PathTemplate, FieldValue::Text(v)) => config.worktree.path_template = v.clone(),
        (FieldKey::BareRepoPathTemplate, FieldValue::Text(v)) => {
            config.worktree.bare_repo_path_template = v.clone()
        }
        (FieldKey::WorktreeAutoCleanup, FieldValue::Bool(v)) => config.worktree.auto_cleanup = *v,
        (FieldKey::ShowBranchInTui, FieldValue::Bool(v)) => config.worktree.show_branch_in_tui = *v,
        (FieldKey::DeleteBranchOnCleanup, FieldValue::Bool(v)) => {
            config.worktree.delete_branch_on_cleanup = *v
        }
        // Legacy cleanup
        (FieldKey::SandboxAutoCleanup, FieldValue::Bool(v)) => config.sandbox.auto_cleanup = *v,
        // Tmux
        (FieldKey::StatusBar, FieldValue::Select { selected, .. }) => {
            config.tmux.status_bar = match selected {
                0 => TmuxStatusBarMode::Auto,
                1 => TmuxStatusBarMode::Enabled,
                _ => TmuxStatusBarMode::Disabled,
            };
        }
        (FieldKey::Mouse, FieldValue::Select { selected, .. }) => {
            config.tmux.mouse = match selected {
                0 => TmuxMouseMode::Auto,
                1 => TmuxMouseMode::Enabled,
                _ => TmuxMouseMode::Disabled,
            };
        }
        // Session
        (FieldKey::DefaultTool, FieldValue::Select { selected, .. }) => {
            config.session.default_tool =
                crate::agents::name_from_settings_index(*selected).map(|s| s.to_string());
        }
        (FieldKey::YoloModeDefault, FieldValue::Bool(v)) => config.session.yolo_mode_default = *v,
        (FieldKey::AutoOrchestrator, FieldValue::Bool(v)) => config.session.auto_orchestrator = *v,
        (FieldKey::OrchestratorTitle, FieldValue::OptionalText(v)) => {
            config.session.orchestrator_title = v.clone()
        }
        (FieldKey::OrchestratorCommand, FieldValue::OptionalText(v)) => {
            config.session.orchestrator_command = v.clone()
        }
        // Claude
        (FieldKey::ClaudeConfigDir, FieldValue::OptionalText(v)) => {
            config.claude.config_dir = v.clone()
        }
        // Diff
        (FieldKey::DiffDefaultBranch, FieldValue::OptionalText(v)) => {
            config.diff.default_branch = v.clone()
        }
        (FieldKey::DiffContextLines, FieldValue::Number(v)) => {
            if let Ok(value) = usize::try_from(*v) {
                config.diff.context_lines = value;
            }
        }
        // Sound
        (FieldKey::SoundEnabled, FieldValue::Bool(v)) => config.sound.enabled = *v,
        (FieldKey::SoundMode, FieldValue::Select { selected, .. }) => {
            config.sound.mode = match selected {
                1 => SoundMode::Specific(String::new()),
                _ => SoundMode::Random,
            };
        }
        (FieldKey::SoundOnStart, FieldValue::OptionalText(v)) => {
            config.sound.on_start = v.clone();
        }
        (FieldKey::SoundOnRunning, FieldValue::OptionalText(v)) => {
            config.sound.on_running = v.clone();
        }
        (FieldKey::SoundOnWaiting, FieldValue::OptionalText(v)) => {
            config.sound.on_waiting = v.clone();
        }
        (FieldKey::SoundOnIdle, FieldValue::OptionalText(v)) => {
            config.sound.on_idle = v.clone();
        }
        (FieldKey::SoundOnError, FieldValue::OptionalText(v)) => {
            config.sound.on_error = v.clone();
        }
        // Hooks
        (FieldKey::HookOnCreate, FieldValue::List(v)) => config.hooks.on_create = v.clone(),
        (FieldKey::HookOnLaunch, FieldValue::List(v)) => config.hooks.on_launch = v.clone(),
        _ => {}
    }
}

/// Apply a field to the profile config.
/// If the value matches the global config, the override is cleared instead of set.
fn apply_field_to_profile(field: &SettingField, global: &Config, config: &mut ProfileConfig) {
    match (&field.key, &field.value) {
        // Updates
        (FieldKey::CheckEnabled, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.updates.check_enabled,
                &mut config.updates,
                |s, val| s.check_enabled = val,
            );
        }
        (FieldKey::CheckIntervalHours, FieldValue::Number(v)) => {
            set_or_clear_override(
                *v,
                &global.updates.check_interval_hours,
                &mut config.updates,
                |s, val| s.check_interval_hours = val,
            );
        }
        (FieldKey::NotifyInCli, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.updates.notify_in_cli,
                &mut config.updates,
                |s, val| s.notify_in_cli = val,
            );
        }
        // Worktree
        (FieldKey::PathTemplate, FieldValue::Text(v)) => {
            set_or_clear_override(
                v.clone(),
                &global.worktree.path_template,
                &mut config.worktree,
                |s, val| s.path_template = val,
            );
        }
        (FieldKey::BareRepoPathTemplate, FieldValue::Text(v)) => {
            set_or_clear_override(
                v.clone(),
                &global.worktree.bare_repo_path_template,
                &mut config.worktree,
                |s, val| s.bare_repo_path_template = val,
            );
        }
        (FieldKey::WorktreeAutoCleanup, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.worktree.auto_cleanup,
                &mut config.worktree,
                |s, val| s.auto_cleanup = val,
            );
        }
        (FieldKey::ShowBranchInTui, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.worktree.show_branch_in_tui,
                &mut config.worktree,
                |s, val| s.show_branch_in_tui = val,
            );
        }
        (FieldKey::DeleteBranchOnCleanup, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.worktree.delete_branch_on_cleanup,
                &mut config.worktree,
                |s, val| s.delete_branch_on_cleanup = val,
            );
        }
        // Legacy cleanup
        (FieldKey::SandboxAutoCleanup, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.sandbox.auto_cleanup,
                &mut config.sandbox,
                |s, val| s.auto_cleanup = val,
            );
        }
        // Tmux
        (FieldKey::StatusBar, FieldValue::Select { selected, .. }) => {
            let mode = match selected {
                0 => TmuxStatusBarMode::Auto,
                1 => TmuxStatusBarMode::Enabled,
                _ => TmuxStatusBarMode::Disabled,
            };
            set_or_clear_override(mode, &global.tmux.status_bar, &mut config.tmux, |s, val| {
                s.status_bar = val
            });
        }
        (FieldKey::Mouse, FieldValue::Select { selected, .. }) => {
            let mode = match selected {
                0 => TmuxMouseMode::Auto,
                1 => TmuxMouseMode::Enabled,
                _ => TmuxMouseMode::Disabled,
            };
            set_or_clear_override(mode, &global.tmux.mouse, &mut config.tmux, |s, val| {
                s.mouse = val
            });
        }
        // Session
        (FieldKey::DefaultTool, FieldValue::Select { selected, .. }) => {
            let tool = crate::agents::name_from_settings_index(*selected).map(|s| s.to_string());
            set_optional_string_override(
                &tool,
                &global.session.default_tool,
                &mut config.session,
                |session, value| session.default_tool = value,
            );
        }
        (FieldKey::YoloModeDefault, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.session.yolo_mode_default,
                &mut config.session,
                |s, val| s.yolo_mode_default = val,
            );
        }
        (FieldKey::AutoOrchestrator, FieldValue::Bool(v)) => {
            set_or_clear_override(
                *v,
                &global.session.auto_orchestrator,
                &mut config.session,
                |s, val| s.auto_orchestrator = val,
            );
        }
        (FieldKey::OrchestratorTitle, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.session.orchestrator_title,
                &mut config.session,
                |session, value| session.orchestrator_title = value,
            );
        }
        (FieldKey::OrchestratorCommand, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.session.orchestrator_command,
                &mut config.session,
                |session, value| session.orchestrator_command = value,
            );
        }
        // Claude
        (FieldKey::ClaudeConfigDir, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.claude.config_dir,
                &mut config.claude,
                |claude, value| claude.config_dir = value,
            );
        }
        // Diff
        (FieldKey::DiffDefaultBranch, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.diff.default_branch,
                &mut config.diff,
                |diff, value| diff.default_branch = value,
            );
        }
        (FieldKey::DiffContextLines, FieldValue::Number(v)) => {
            if let Ok(value) = usize::try_from(*v) {
                set_or_clear_override(
                    value,
                    &global.diff.context_lines,
                    &mut config.diff,
                    |s, val| s.context_lines = val,
                );
            }
        }
        // Sound
        (FieldKey::SoundEnabled, FieldValue::Bool(v)) => {
            set_or_clear_override(*v, &global.sound.enabled, &mut config.sound, |s, val| {
                s.enabled = val
            });
        }
        (FieldKey::SoundMode, FieldValue::Select { selected, .. }) => {
            let mode = match selected {
                1 => SoundMode::Specific(String::new()),
                _ => SoundMode::Random,
            };
            set_or_clear_override(mode, &global.sound.mode, &mut config.sound, |s, val| {
                s.mode = val
            });
        }
        (FieldKey::SoundOnStart, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.sound.on_start,
                &mut config.sound,
                |sound, value| sound.on_start = value,
            );
        }
        (FieldKey::SoundOnRunning, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.sound.on_running,
                &mut config.sound,
                |sound, value| sound.on_running = value,
            );
        }
        (FieldKey::SoundOnWaiting, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.sound.on_waiting,
                &mut config.sound,
                |sound, value| sound.on_waiting = value,
            );
        }
        (FieldKey::SoundOnIdle, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.sound.on_idle,
                &mut config.sound,
                |sound, value| sound.on_idle = value,
            );
        }
        (FieldKey::SoundOnError, FieldValue::OptionalText(v)) => {
            set_optional_string_override(
                v,
                &global.sound.on_error,
                &mut config.sound,
                |sound, value| sound.on_error = value,
            );
        }
        // Hooks
        (FieldKey::HookOnCreate, FieldValue::List(v)) => {
            set_or_clear_override(
                v.clone(),
                &global.hooks.on_create,
                &mut config.hooks,
                |s, val| s.on_create = val,
            );
        }
        (FieldKey::HookOnLaunch, FieldValue::List(v)) => {
            set_or_clear_override(
                v.clone(),
                &global.hooks.on_launch,
                &mut config.hooks,
                |s, val| s.on_launch = val,
            );
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Config, ProfileConfig};

    #[test]
    fn test_profile_field_has_no_override_after_global_change() {
        // Start with default configs
        let mut global = Config::default();
        let profile = ProfileConfig::default();

        // Verify initial state - profile shows no override
        let fields = build_fields_for_category(
            SettingsCategory::Updates,
            SettingsScope::Profile,
            &global,
            &profile,
        );

        let check_enabled_field = fields
            .iter()
            .find(|f| f.key == FieldKey::CheckEnabled)
            .unwrap();
        assert!(
            !check_enabled_field.has_override,
            "Profile should not show override initially"
        );

        // Change global setting
        global.updates.check_enabled = !global.updates.check_enabled;

        // Rebuild profile fields - should still show no override
        let fields = build_fields_for_category(
            SettingsCategory::Updates,
            SettingsScope::Profile,
            &global,
            &profile,
        );

        let check_enabled_field = fields
            .iter()
            .find(|f| f.key == FieldKey::CheckEnabled)
            .unwrap();
        assert!(
            !check_enabled_field.has_override,
            "Profile should NOT show override after global change - it should inherit"
        );
    }

    #[test]
    fn test_profile_field_shows_override_after_profile_change() {
        let global = Config::default();
        let mut profile = ProfileConfig::default();

        // Initially no override
        let fields = build_fields_for_category(
            SettingsCategory::Updates,
            SettingsScope::Profile,
            &global,
            &profile,
        );
        let check_enabled_field = fields
            .iter()
            .find(|f| f.key == FieldKey::CheckEnabled)
            .unwrap();
        assert!(!check_enabled_field.has_override);

        // Set a profile override
        profile.updates = Some(crate::session::UpdatesConfigOverride {
            check_enabled: Some(false),
            ..Default::default()
        });

        // Rebuild - should now show override
        let fields = build_fields_for_category(
            SettingsCategory::Updates,
            SettingsScope::Profile,
            &global,
            &profile,
        );
        let check_enabled_field = fields
            .iter()
            .find(|f| f.key == FieldKey::CheckEnabled)
            .unwrap();
        assert!(
            check_enabled_field.has_override,
            "Profile SHOULD show override after explicit profile change"
        );
    }

    #[test]
    fn test_default_tool_options_include_all_registered_agents() {
        let global = Config::default();
        let profile = ProfileConfig::default();

        let fields = build_fields_for_category(
            SettingsCategory::Session,
            SettingsScope::Global,
            &global,
            &profile,
        );

        let tool_field = fields
            .iter()
            .find(|f| f.key == FieldKey::DefaultTool)
            .expect("DefaultTool field should exist");

        let options = match &tool_field.value {
            FieldValue::Select { options, .. } => options,
            _ => panic!("DefaultTool should be a Select field"),
        };

        let tool_options: Vec<&str> = options.iter().skip(1).map(|s| s.as_str()).collect();
        let agent_names = crate::agents::agent_names();

        for name in &agent_names {
            assert!(
                tool_options.contains(name),
                "Settings UI missing agent '{}'. UI options: {:?}",
                name,
                tool_options
            );
        }

        for option in &tool_options {
            assert!(
                agent_names.contains(option),
                "Settings UI has unknown agent '{}' not in registry.",
                option
            );
        }
    }

    #[test]
    fn active_runtime_fields_are_present_in_settings() {
        let global = Config::default();
        let profile = ProfileConfig::default();

        let cases = [
            (
                SettingsCategory::Session,
                vec![
                    FieldKey::DefaultTool,
                    FieldKey::YoloModeDefault,
                    FieldKey::AutoOrchestrator,
                    FieldKey::OrchestratorTitle,
                    FieldKey::OrchestratorCommand,
                ],
            ),
            (
                SettingsCategory::Diff,
                vec![FieldKey::DiffDefaultBranch, FieldKey::DiffContextLines],
            ),
            (SettingsCategory::Claude, vec![FieldKey::ClaudeConfigDir]),
            (
                SettingsCategory::Worktree,
                vec![
                    FieldKey::PathTemplate,
                    FieldKey::BareRepoPathTemplate,
                    FieldKey::WorktreeAutoCleanup,
                    FieldKey::ShowBranchInTui,
                    FieldKey::DeleteBranchOnCleanup,
                ],
            ),
            (
                SettingsCategory::Cleanup,
                vec![FieldKey::SandboxAutoCleanup],
            ),
        ];

        for (category, expected) in cases {
            let actual: Vec<_> =
                build_fields_for_category(category, SettingsScope::Global, &global, &profile)
                    .into_iter()
                    .map(|field| field.key)
                    .collect();
            assert_eq!(actual, expected, "missing settings fields in {category:?}");
        }
    }

    #[test]
    fn active_runtime_fields_apply_to_global_and_profile_configs() {
        let mut global = Config::default();
        let mut profile = ProfileConfig::default();

        let fields = [
            SettingField {
                key: FieldKey::ShowBranchInTui,
                label: "",
                description: "",
                value: FieldValue::Bool(false),
                category: SettingsCategory::Worktree,
                has_override: false,
            },
            SettingField {
                key: FieldKey::AutoOrchestrator,
                label: "",
                description: "",
                value: FieldValue::Bool(true),
                category: SettingsCategory::Session,
                has_override: false,
            },
            SettingField {
                key: FieldKey::OrchestratorCommand,
                label: "",
                description: "",
                value: FieldValue::OptionalText(Some("forager-orch start".to_string())),
                category: SettingsCategory::Session,
                has_override: false,
            },
            SettingField {
                key: FieldKey::DiffContextLines,
                label: "",
                description: "",
                value: FieldValue::Number(7),
                category: SettingsCategory::Diff,
                has_override: false,
            },
            SettingField {
                key: FieldKey::ClaudeConfigDir,
                label: "",
                description: "",
                value: FieldValue::OptionalText(Some("/tmp/claude-profile".to_string())),
                category: SettingsCategory::Claude,
                has_override: false,
            },
            SettingField {
                key: FieldKey::SandboxAutoCleanup,
                label: "",
                description: "",
                value: FieldValue::Bool(false),
                category: SettingsCategory::Cleanup,
                has_override: false,
            },
        ];

        for field in &fields {
            apply_field_to_config(field, SettingsScope::Profile, &mut global, &mut profile);
        }

        assert_eq!(
            profile.worktree.as_ref().and_then(|w| w.show_branch_in_tui),
            Some(false)
        );
        assert_eq!(
            profile.session.as_ref().and_then(|s| s.auto_orchestrator),
            Some(true)
        );
        assert_eq!(
            profile
                .session
                .as_ref()
                .and_then(|s| s.orchestrator_command.as_deref()),
            Some("forager-orch start")
        );
        assert_eq!(profile.diff.as_ref().and_then(|d| d.context_lines), Some(7));
        assert_eq!(
            profile
                .claude
                .as_ref()
                .and_then(|c| c.config_dir.as_deref()),
            Some("/tmp/claude-profile")
        );
        assert_eq!(
            profile.sandbox.as_ref().and_then(|s| s.auto_cleanup),
            Some(false)
        );

        for field in &fields {
            apply_field_to_config(field, SettingsScope::Global, &mut global, &mut profile);
        }

        assert!(!global.worktree.show_branch_in_tui);
        assert!(global.session.auto_orchestrator);
        assert_eq!(
            global.session.orchestrator_command.as_deref(),
            Some("forager-orch start")
        );
        assert_eq!(global.diff.context_lines, 7);
        assert_eq!(
            global.claude.config_dir.as_deref(),
            Some("/tmp/claude-profile")
        );
        assert!(!global.sandbox.auto_cleanup);
    }

    #[test]
    fn profile_can_explicitly_clear_inherited_optional_settings() {
        let mut global = Config::default();
        global.session.default_tool = Some("claude".to_string());
        global.diff.default_branch = Some("main".to_string());
        global.claude.config_dir = Some("/tmp/global-claude".to_string());
        let mut profile = ProfileConfig::default();

        let fields = [
            SettingField {
                key: FieldKey::DefaultTool,
                label: "",
                description: "",
                value: FieldValue::Select {
                    selected: 0,
                    options: vec!["Auto".to_string()],
                },
                category: SettingsCategory::Session,
                has_override: false,
            },
            SettingField {
                key: FieldKey::DiffDefaultBranch,
                label: "",
                description: "",
                value: FieldValue::OptionalText(None),
                category: SettingsCategory::Diff,
                has_override: false,
            },
            SettingField {
                key: FieldKey::ClaudeConfigDir,
                label: "",
                description: "",
                value: FieldValue::OptionalText(None),
                category: SettingsCategory::Claude,
                has_override: false,
            },
        ];

        for field in &fields {
            apply_field_to_config(field, SettingsScope::Profile, &mut global, &mut profile);
        }

        assert_eq!(
            profile
                .session
                .as_ref()
                .and_then(|session| session.default_tool.as_deref()),
            Some("")
        );
        assert_eq!(
            profile
                .diff
                .as_ref()
                .and_then(|diff| diff.default_branch.as_deref()),
            Some("")
        );
        assert_eq!(
            profile
                .claude
                .as_ref()
                .and_then(|claude| claude.config_dir.as_deref()),
            Some("")
        );

        let effective = crate::session::merge_configs(global, &profile);
        assert!(effective.session.default_tool.is_none());
        assert!(effective.diff.default_branch.is_none());
        assert!(effective.claude.config_dir.is_none());
    }
}
