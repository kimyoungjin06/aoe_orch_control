//! Profile-specific configuration with override support
//!
//! Profile configs allow per-profile overrides of global settings.
//! Fields set to None inherit from the global config.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;

use super::config::{Config, TmuxMouseMode, TmuxStatusBarMode};
use super::get_profile_dir;

/// Profile-specific settings. All fields are Option<T> - None means "inherit from global"
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfileConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub theme: Option<ThemeConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude: Option<ClaudeConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updates: Option<UpdatesConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<WorktreeConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmux: Option<TmuxConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<DiffConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<HooksConfigOverride>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sound: Option<crate::sound::SoundConfigOverride>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThemeConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClaudeConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_dir: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdatesConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_enabled: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_update: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_interval_hours: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notify_in_cli: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorktreeConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_template: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bare_repo_path_template: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_cleanup: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub show_branch_in_tui: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_branch_on_cleanup: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SandboxConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_cleanup: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TmuxConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_bar: Option<TmuxStatusBarMode>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mouse: Option<TmuxMouseMode>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_tool: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yolo_mode_default: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_orchestrator: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestrator_title: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestrator_command: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiffConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_lines: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HooksConfigOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_create: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_launch: Option<Vec<String>>,
}

/// Load profile-specific config. Returns empty config if file doesn't exist.
pub fn load_profile_config(profile: &str) -> Result<ProfileConfig> {
    let path = get_profile_config_path(profile)?;
    if !path.exists() {
        return Ok(ProfileConfig::default());
    }
    let content = fs::read_to_string(&path)?;
    if content.trim().is_empty() {
        return Ok(ProfileConfig::default());
    }
    let config: ProfileConfig = toml::from_str(&content)?;
    Ok(config)
}

/// Save profile-specific config
pub fn save_profile_config(profile: &str, config: &ProfileConfig) -> Result<()> {
    let path = get_profile_config_path(profile)?;
    let content = toml::to_string_pretty(config)?;
    fs::write(&path, content)?;
    Ok(())
}

/// Get the path to a profile's config file
pub fn get_profile_config_path(profile: &str) -> Result<std::path::PathBuf> {
    Ok(get_profile_dir(profile)?.join("config.toml"))
}

/// Check if a profile has any overrides set
pub fn profile_has_overrides(config: &ProfileConfig) -> bool {
    config.theme.is_some()
        || config.claude.is_some()
        || config.updates.is_some()
        || config.worktree.is_some()
        || config.sandbox.is_some()
        || config.tmux.is_some()
        || config.session.is_some()
        || config.diff.is_some()
        || config.hooks.is_some()
        || config.sound.is_some()
}

/// Load effective config for a profile (global + profile overrides merged)
pub fn resolve_config(profile: &str) -> Result<Config> {
    let global = Config::load()?;
    let profile_config = load_profile_config(profile)?;
    Ok(merge_configs(global, &profile_config))
}

/// Apply sandbox config overrides to a target config.
pub fn apply_sandbox_overrides(
    target: &mut super::config::SandboxConfig,
    source: &SandboxConfigOverride,
) {
    if let Some(auto_cleanup) = source.auto_cleanup {
        target.auto_cleanup = auto_cleanup;
    }
}

/// Apply worktree config overrides to a target config.
pub fn apply_worktree_overrides(
    target: &mut super::config::WorktreeConfig,
    source: &WorktreeConfigOverride,
) {
    if let Some(enabled) = source.enabled {
        target.enabled = enabled;
    }
    if let Some(ref path_template) = source.path_template {
        target.path_template = path_template.clone();
    }
    if let Some(ref bare_repo_path_template) = source.bare_repo_path_template {
        target.bare_repo_path_template = bare_repo_path_template.clone();
    }
    if let Some(auto_cleanup) = source.auto_cleanup {
        target.auto_cleanup = auto_cleanup;
    }
    if let Some(show_branch_in_tui) = source.show_branch_in_tui {
        target.show_branch_in_tui = show_branch_in_tui;
    }
    if let Some(delete_branch_on_cleanup) = source.delete_branch_on_cleanup {
        target.delete_branch_on_cleanup = delete_branch_on_cleanup;
    }
}

/// Apply hooks config overrides to a target config.
pub fn apply_hooks_overrides(
    target: &mut crate::session::repo_config::HooksConfig,
    source: &HooksConfigOverride,
) {
    if let Some(ref on_create) = source.on_create {
        target.on_create = on_create.clone();
    }
    if let Some(ref on_launch) = source.on_launch {
        target.on_launch = on_launch.clone();
    }
}

/// Apply session config overrides to a target config.
pub fn apply_session_overrides(
    target: &mut super::config::SessionConfig,
    source: &SessionConfigOverride,
) {
    apply_optional_string_override(&mut target.default_tool, &source.default_tool);
    if let Some(yolo_mode_default) = source.yolo_mode_default {
        target.yolo_mode_default = yolo_mode_default;
    }
    if let Some(auto_orchestrator) = source.auto_orchestrator {
        target.auto_orchestrator = auto_orchestrator;
    }
    apply_optional_string_override(&mut target.orchestrator_title, &source.orchestrator_title);
    apply_optional_string_override(
        &mut target.orchestrator_command,
        &source.orchestrator_command,
    );
}

pub fn apply_diff_overrides(target: &mut super::config::DiffConfig, source: &DiffConfigOverride) {
    apply_optional_string_override(&mut target.default_branch, &source.default_branch);
    if let Some(context_lines) = source.context_lines {
        target.context_lines = context_lines;
    }
}

fn apply_optional_string_override(target: &mut Option<String>, source: &Option<String>) {
    if let Some(value) = source {
        *target = if value.trim().is_empty() {
            None
        } else {
            Some(value.clone())
        };
    }
}

/// Apply tmux config overrides to a target config.
pub fn apply_tmux_overrides(target: &mut super::config::TmuxConfig, source: &TmuxConfigOverride) {
    if let Some(status_bar) = source.status_bar {
        target.status_bar = status_bar;
    }
    if let Some(mouse) = source.mouse {
        target.mouse = mouse;
    }
}

/// Merge profile overrides into global config
pub fn merge_configs(mut global: Config, profile: &ProfileConfig) -> Config {
    if let Some(ref theme_override) = profile.theme {
        if let Some(ref name) = theme_override.name {
            global.theme.name = name.clone();
        }
    }

    if let Some(ref claude_override) = profile.claude {
        apply_optional_string_override(&mut global.claude.config_dir, &claude_override.config_dir);
    }

    if let Some(ref updates_override) = profile.updates {
        if let Some(check_enabled) = updates_override.check_enabled {
            global.updates.check_enabled = check_enabled;
        }
        if let Some(auto_update) = updates_override.auto_update {
            global.updates.auto_update = auto_update;
        }
        if let Some(check_interval_hours) = updates_override.check_interval_hours {
            global.updates.check_interval_hours = check_interval_hours;
        }
        if let Some(notify_in_cli) = updates_override.notify_in_cli {
            global.updates.notify_in_cli = notify_in_cli;
        }
    }

    if let Some(ref worktree_override) = profile.worktree {
        apply_worktree_overrides(&mut global.worktree, worktree_override);
    }

    if let Some(ref sandbox_override) = profile.sandbox {
        apply_sandbox_overrides(&mut global.sandbox, sandbox_override);
    }

    if let Some(ref tmux_override) = profile.tmux {
        apply_tmux_overrides(&mut global.tmux, tmux_override);
    }

    if let Some(ref session_override) = profile.session {
        apply_session_overrides(&mut global.session, session_override);
    }

    if let Some(ref diff_override) = profile.diff {
        apply_diff_overrides(&mut global.diff, diff_override);
    }

    if let Some(ref hooks_override) = profile.hooks {
        apply_hooks_overrides(&mut global.hooks, hooks_override);
    }

    if let Some(ref sound_override) = profile.sound {
        crate::sound::apply_sound_overrides(&mut global.sound, sound_override);
    }

    global
}

/// Validate a path exists (for config_dir validation)
pub fn validate_path_exists(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Ok(());
    }

    let expanded = if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            home.join(stripped)
        } else {
            return Err("Cannot expand home directory".to_string());
        }
    } else {
        std::path::PathBuf::from(path)
    };

    if expanded.exists() {
        Ok(())
    } else {
        Err(format!("Path does not exist: {}", path))
    }
}

/// Validate Docker volume format (host:container[:options])
pub fn validate_volume_format(volume: &str) -> Result<(), String> {
    if volume.is_empty() {
        return Err("Volume cannot be empty".to_string());
    }

    let parts: Vec<&str> = volume.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err("Volume must be in format host:container[:options]".to_string());
    }

    if parts[0].is_empty() || parts[1].is_empty() {
        return Err("Host and container paths cannot be empty".to_string());
    }

    Ok(())
}

/// Validate check interval is positive
pub fn validate_check_interval(hours: u64) -> Result<(), String> {
    if hours == 0 {
        Err("Check interval must be greater than 0".to_string())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_config_default() {
        let config = ProfileConfig::default();
        assert!(config.theme.is_none());
        assert!(config.claude.is_none());
        assert!(config.updates.is_none());
        assert!(config.worktree.is_none());
        assert!(config.sandbox.is_none());
        assert!(config.tmux.is_none());
        assert!(config.diff.is_none());
    }

    #[test]
    fn diff_overrides_merge_into_effective_profile_config() {
        let mut global = Config::default();
        global.diff.default_branch = Some("main".to_string());
        global.diff.context_lines = 3;
        let profile = ProfileConfig {
            diff: Some(DiffConfigOverride {
                default_branch: Some("develop".to_string()),
                context_lines: Some(9),
            }),
            ..Default::default()
        };

        let merged = merge_configs(global, &profile);

        assert_eq!(merged.diff.default_branch.as_deref(), Some("develop"));
        assert_eq!(merged.diff.context_lines, 9);
    }

    #[test]
    fn test_profile_config_serialization_empty() {
        let config = ProfileConfig::default();
        let serialized = toml::to_string(&config).unwrap();
        // Empty config should serialize to empty (skip_serializing_if)
        assert!(serialized.trim().is_empty());
    }

    #[test]
    fn test_profile_config_serialization_partial() {
        let config = ProfileConfig {
            updates: Some(UpdatesConfigOverride {
                check_enabled: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };

        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("[updates]"));
        assert!(serialized.contains("check_enabled = false"));
    }

    #[test]
    fn test_profile_config_deserialization() {
        let toml = r#"
            [updates]
            check_enabled = false
            check_interval_hours = 48

            [sandbox]
            enabled_by_default = true
            auto_cleanup = false
        "#;

        let config: ProfileConfig = toml::from_str(toml).unwrap();
        assert!(config.updates.is_some());
        let updates = config.updates.unwrap();
        assert_eq!(updates.check_enabled, Some(false));
        assert_eq!(updates.check_interval_hours, Some(48));
        assert!(updates.auto_update.is_none());

        assert!(config.sandbox.is_some());
        let sandbox = config.sandbox.unwrap();
        assert_eq!(sandbox.auto_cleanup, Some(false));
    }

    #[test]
    fn test_merge_configs_no_overrides() {
        let global = Config::default();
        let profile = ProfileConfig::default();
        let merged = merge_configs(global.clone(), &profile);

        assert_eq!(merged.updates.check_enabled, global.updates.check_enabled);
        assert_eq!(merged.worktree.enabled, global.worktree.enabled);
    }

    #[test]
    fn test_merge_configs_with_overrides() {
        let global = Config::default();
        let profile = ProfileConfig {
            updates: Some(UpdatesConfigOverride {
                check_enabled: Some(false),
                check_interval_hours: Some(48),
                ..Default::default()
            }),
            worktree: Some(WorktreeConfigOverride {
                enabled: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };

        let merged = merge_configs(global, &profile);

        assert!(!merged.updates.check_enabled);
        assert_eq!(merged.updates.check_interval_hours, 48);
        // notify_in_cli should retain global default since not overridden
        assert!(merged.updates.notify_in_cli);
        assert!(merged.worktree.enabled);
    }

    #[test]
    fn test_profile_has_overrides() {
        let empty = ProfileConfig::default();
        assert!(!profile_has_overrides(&empty));

        let with_override = ProfileConfig {
            theme: Some(ThemeConfigOverride {
                name: Some("dark".to_string()),
            }),
            ..Default::default()
        };
        assert!(profile_has_overrides(&with_override));
    }

    #[test]
    fn test_validate_volume_format() {
        assert!(validate_volume_format("/host:/container").is_ok());
        assert!(validate_volume_format("/host:/container:ro").is_ok());
        assert!(validate_volume_format("").is_err());
        assert!(validate_volume_format("/only-one").is_err());
        assert!(validate_volume_format(":/container").is_err());
        assert!(validate_volume_format("/host:").is_err());
    }

    #[test]
    fn test_validate_check_interval() {
        assert!(validate_check_interval(1).is_ok());
        assert!(validate_check_interval(24).is_ok());
        assert!(validate_check_interval(0).is_err());
    }

    #[test]
    fn test_merge_configs_with_tmux_mouse_override() {
        let global = Config::default();
        assert_eq!(global.tmux.mouse, TmuxMouseMode::Auto);

        let profile = ProfileConfig {
            tmux: Some(TmuxConfigOverride {
                mouse: Some(TmuxMouseMode::Enabled),
                ..Default::default()
            }),
            ..Default::default()
        };

        let merged = merge_configs(global, &profile);
        assert_eq!(merged.tmux.mouse, TmuxMouseMode::Enabled);
    }

    #[test]
    fn test_merge_configs_tmux_mouse_inherits_when_not_overridden() {
        let mut global = Config::default();
        global.tmux.mouse = TmuxMouseMode::Enabled;

        let profile = ProfileConfig {
            tmux: Some(TmuxConfigOverride {
                status_bar: Some(TmuxStatusBarMode::Enabled),
                mouse: None,
            }),
            ..Default::default()
        };

        let merged = merge_configs(global, &profile);
        assert_eq!(merged.tmux.mouse, TmuxMouseMode::Enabled); // Should inherit from global
        assert_eq!(merged.tmux.status_bar, TmuxStatusBarMode::Enabled);
    }

    #[test]
    fn test_merge_configs_tmux_mouse_disabled_override() {
        let mut global = Config::default();
        global.tmux.mouse = TmuxMouseMode::Enabled;

        let profile = ProfileConfig {
            tmux: Some(TmuxConfigOverride {
                mouse: Some(TmuxMouseMode::Disabled),
                ..Default::default()
            }),
            ..Default::default()
        };

        let merged = merge_configs(global, &profile);
        assert_eq!(merged.tmux.mouse, TmuxMouseMode::Disabled);
    }

    #[test]
    fn test_merge_configs_with_sandbox_auto_cleanup_override() {
        let global = Config::default();
        assert!(global.sandbox.auto_cleanup);

        let profile = ProfileConfig {
            sandbox: Some(SandboxConfigOverride {
                auto_cleanup: Some(false),
            }),
            ..Default::default()
        };

        let merged = merge_configs(global, &profile);
        assert!(!merged.sandbox.auto_cleanup);
    }

    #[test]
    fn test_merge_configs_sandbox_auto_cleanup_inherits_when_not_overridden() {
        let mut global = Config::default();
        global.sandbox.auto_cleanup = false;

        let profile = ProfileConfig {
            sandbox: Some(SandboxConfigOverride::default()),
            ..Default::default()
        };

        let merged = merge_configs(global, &profile);
        assert!(!merged.sandbox.auto_cleanup);
    }

    #[test]
    fn test_sandbox_cleanup_override_serialization() {
        let config = ProfileConfig {
            sandbox: Some(SandboxConfigOverride {
                auto_cleanup: Some(false),
            }),
            ..Default::default()
        };

        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("auto_cleanup"));

        let deserialized: ProfileConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(deserialized.sandbox.unwrap().auto_cleanup, Some(false));
    }

    #[test]
    fn test_tmux_config_override_serialization() {
        let config = ProfileConfig {
            tmux: Some(TmuxConfigOverride {
                status_bar: Some(TmuxStatusBarMode::Enabled),
                mouse: Some(TmuxMouseMode::Enabled),
            }),
            ..Default::default()
        };

        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("[tmux]"));
        assert!(serialized.contains(r#"mouse = "enabled""#));

        let deserialized: ProfileConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(
            deserialized.tmux.as_ref().unwrap().mouse,
            Some(TmuxMouseMode::Enabled)
        );
    }
}
