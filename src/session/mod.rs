//! Session management module

pub mod auto_orchestrator;
pub mod builder;
pub mod civilizations;
pub mod config;
mod groups;
mod instance;
pub mod profile_config;
pub mod project_registry;
pub mod remote_identity;
pub mod repo_config;
mod storage;

pub use crate::sound::{SoundConfig, SoundConfigOverride};
pub use config::{
    load_config, load_config_read_only, save_config, ClaudeConfig, Config, DiffConfig,
    SandboxConfig, SessionConfig, ThemeConfig, TmuxMouseMode, TmuxStatusBarMode, UpdatesConfig,
    WorktreeConfig,
};
pub use groups::{flatten_tree, Group, GroupTree, Item};
pub use instance::{Instance, SandboxInfo, Status, TerminalInfo, WorktreeInfo};
pub use profile_config::{
    load_profile_config, merge_configs, resolve_config, save_profile_config,
    validate_check_interval, validate_path_exists, validate_volume_format, ClaudeConfigOverride,
    DiffConfigOverride, HooksConfigOverride, ProfileConfig, SandboxConfigOverride,
    SessionConfigOverride, ThemeConfigOverride, TmuxConfigOverride, UpdatesConfigOverride,
    WorktreeConfigOverride,
};
pub use remote_identity::{RemoteSessionIdentityV1, RemoteSessionWorktreeKindV1};
pub use repo_config::{
    check_hook_trust, execute_hooks, load_repo_config, merge_repo_config_unchecked,
    profile_to_repo_config, repo_config_to_profile, resolve_config_with_repo, save_repo_config,
    trust_repo, HookTrustStatus, HooksConfig, RepoConfig,
};
pub use storage::Storage;

use anyhow::Result;
use std::fs;
use std::path::PathBuf;

pub const DEFAULT_PROFILE: &str = "default";
#[cfg(target_os = "linux")]
const APP_DIR_NAME: &str = "forager";
#[cfg(target_os = "linux")]
const LEGACY_APP_DIR_NAME: &str = "agent-of-empires";
#[cfg(not(target_os = "linux"))]
const DOT_APP_DIR_NAME: &str = ".forager";
const LEGACY_DOT_APP_DIR_NAME: &str = ".agent-of-empires";

#[derive(Debug, Clone)]
pub struct AppDirResolution {
    pub active_path: PathBuf,
    pub active_source: &'static str,
    pub primary_path: PathBuf,
    pub primary_exists: bool,
    pub legacy_paths: Vec<PathBuf>,
}

pub fn get_app_dir() -> Result<PathBuf> {
    let dir = app_dir_resolution()?.active_path;
    if !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    Ok(dir)
}

pub(crate) fn resolved_app_dir_path() -> Result<PathBuf> {
    Ok(app_dir_resolution()?.active_path)
}

pub fn app_dir_resolution() -> Result<AppDirResolution> {
    let primary = primary_app_dir_path()?;
    let primary_exists = primary.exists();
    let legacy_paths = legacy_app_dir_paths();

    if primary.exists() {
        return Ok(AppDirResolution {
            active_path: primary.clone(),
            active_source: "primary",
            primary_path: primary,
            primary_exists,
            legacy_paths,
        });
    }

    if let Some(legacy) = legacy_paths.iter().find(|path| path.exists()).cloned() {
        return Ok(AppDirResolution {
            active_path: legacy,
            active_source: "legacy",
            primary_path: primary,
            primary_exists,
            legacy_paths,
        });
    }

    Ok(AppDirResolution {
        active_path: primary.clone(),
        active_source: "new_primary",
        primary_path: primary,
        primary_exists,
        legacy_paths,
    })
}

pub(crate) fn primary_app_dir_path() -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    let dir = dirs::config_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot find config directory"))?
        .join(APP_DIR_NAME);

    #[cfg(not(target_os = "linux"))]
    let dir = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot find home directory"))?
        .join(DOT_APP_DIR_NAME);

    Ok(dir)
}

pub(crate) fn legacy_app_dir_paths() -> Vec<PathBuf> {
    let mut dirs = Vec::new();

    #[cfg(target_os = "linux")]
    {
        if let Some(config_dir) = dirs::config_dir() {
            dirs.push(config_dir.join(LEGACY_APP_DIR_NAME));
        }
        if let Some(home) = dirs::home_dir() {
            dirs.push(home.join(LEGACY_DOT_APP_DIR_NAME));
        }
    }

    #[cfg(not(target_os = "linux"))]
    if let Some(home) = dirs::home_dir() {
        dirs.push(home.join(LEGACY_DOT_APP_DIR_NAME));
    }

    dirs
}

pub(crate) fn app_dir_candidates() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(primary) = primary_app_dir_path() {
        dirs.push(primary);
    }
    dirs.extend(legacy_app_dir_paths());
    dirs
}

pub fn normalize_profile_name(profile: &str) -> Result<String> {
    let profile = profile.trim();
    if profile.is_empty() {
        return Ok(DEFAULT_PROFILE.to_string());
    }

    if profile == "." || profile == ".." {
        anyhow::bail!("Profile name cannot be '.' or '..'");
    }

    if profile.contains('/') || profile.contains('\\') {
        anyhow::bail!("Profile name cannot contain path separators");
    }

    if profile.contains('\0') {
        anyhow::bail!("Profile name cannot contain NUL bytes");
    }

    Ok(profile.to_string())
}

pub fn get_profile_dir(profile: &str) -> Result<PathBuf> {
    let base = get_app_dir()?;
    let profile_name = normalize_profile_name(profile)?;
    let dir = base.join("profiles").join(profile_name);
    if !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    Ok(dir)
}

pub fn list_profiles() -> Result<Vec<String>> {
    let base = get_app_dir()?;
    let profiles_dir = base.join("profiles");

    if !profiles_dir.exists() {
        return Ok(vec![]);
    }

    let mut profiles = Vec::new();
    for entry in fs::read_dir(&profiles_dir)? {
        let entry = entry?;
        if entry.path().is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                profiles.push(name.to_string());
            }
        }
    }
    profiles.sort();
    Ok(profiles)
}

pub fn create_profile(name: &str) -> Result<()> {
    let name = normalize_profile_name(name)?;

    let profiles = list_profiles()?;
    if profiles.contains(&name) {
        anyhow::bail!("Profile '{}' already exists", name);
    }

    get_profile_dir(&name)?;
    Ok(())
}

pub fn delete_profile(name: &str) -> Result<()> {
    let name = normalize_profile_name(name)?;
    if name == DEFAULT_PROFILE {
        anyhow::bail!("Cannot delete the default profile");
    }

    let base = get_app_dir()?;
    let profile_dir = base.join("profiles").join(&name);

    if !profile_dir.exists() {
        anyhow::bail!("Profile '{}' does not exist", name);
    }

    fs::remove_dir_all(&profile_dir)?;
    Ok(())
}

pub fn set_default_profile(name: &str) -> Result<()> {
    let name = normalize_profile_name(name)?;
    let mut config = load_config()?.unwrap_or_default();
    config.default_profile = name;
    save_config(&config)?;
    Ok(())
}
