//! Session instance definition and operations

use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tmux;

use super::Config;

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalInfo {
    #[serde(default)]
    pub created: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Running,
    Waiting,
    #[default]
    Idle,
    Stopped,
    Error,
    Starting,
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeInfo {
    pub branch: String,
    pub main_repo_path: String,
    #[serde(alias = "managed_by_aoe")]
    pub managed_by_forager: bool,
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_true")]
    pub cleanup_on_delete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxInfo {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    pub image: String,
    pub container_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// Additional environment variable keys to pass from host (session-specific)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_env_keys: Option<Vec<String>>,
    /// Additional KEY=VALUE environment variables (session-specific overrides)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_env_values: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    pub title: String,
    pub project_path: String,
    #[serde(default)]
    pub group_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub command: String,
    #[serde(default)]
    pub tool: String,
    #[serde(default)]
    pub yolo_mode: bool,
    #[serde(default)]
    pub status: Status,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<DateTime<Utc>>,

    // Git worktree integration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_info: Option<WorktreeInfo>,

    // Legacy Docker sandbox metadata. New sandbox sessions are no longer created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_info: Option<SandboxInfo>,

    // Paired terminal session
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_info: Option<TerminalInfo>,

    // Runtime state (not serialized)
    #[serde(skip)]
    pub last_error_check: Option<std::time::Instant>,
    #[serde(skip)]
    pub last_start_time: Option<std::time::Instant>,
    #[serde(skip)]
    pub last_error: Option<String>,

    // Search optimization: pre-computed lowercase strings (not serialized)
    #[serde(skip)]
    pub title_lower: String,
    #[serde(skip)]
    pub project_path_lower: String,
}

impl Instance {
    pub fn new(title: &str, project_path: &str) -> Self {
        Self {
            id: generate_id(),
            title: title.to_string(),
            project_path: project_path.to_string(),
            group_path: String::new(),
            parent_session_id: None,
            command: String::new(),
            tool: "claude".to_string(),
            yolo_mode: false,
            status: Status::Idle,
            created_at: Utc::now(),
            last_accessed_at: None,
            worktree_info: None,
            sandbox_info: None,
            terminal_info: None,
            last_error_check: None,
            last_start_time: None,
            last_error: None,
            title_lower: title.to_lowercase(),
            project_path_lower: project_path.to_lowercase(),
        }
    }

    /// Update the pre-computed lowercase fields for search optimization.
    /// Call this after loading instances from disk or modifying title/path.
    pub fn update_search_cache(&mut self) {
        self.title_lower = self.title.to_lowercase();
        self.project_path_lower = self.project_path.to_lowercase();
    }

    pub fn is_sub_session(&self) -> bool {
        self.parent_session_id.is_some()
    }

    pub fn is_sandboxed(&self) -> bool {
        self.sandbox_info.as_ref().is_some_and(|s| s.enabled)
    }

    pub fn is_yolo_mode(&self) -> bool {
        self.yolo_mode
    }

    pub fn get_tool_command(&self) -> &str {
        if self.command.is_empty() {
            crate::agents::get_agent(&self.tool)
                .map(|a| a.binary)
                .unwrap_or("bash")
        } else {
            &self.command
        }
    }

    pub fn tmux_session(&self) -> Result<tmux::Session> {
        tmux::Session::new(&self.id, &self.title)
    }

    fn resolve_on_launch_hooks(&self, config: &Config) -> Vec<String> {
        let mut hooks = config.hooks.on_launch.clone();
        if let Ok(super::repo_config::HookTrustStatus::Trusted(repo_hooks)) =
            super::repo_config::check_hook_trust(Path::new(&self.project_path))
        {
            if !repo_hooks.on_launch.is_empty() {
                hooks = repo_hooks.on_launch;
            }
        }
        hooks
    }

    fn agent_environment(&self, config: &Config) -> Vec<(String, String)> {
        if self.tool != "claude" {
            return Vec::new();
        }

        let mut environment = vec![(
            "CLAUDE_CODE_DISABLE_ALTERNATE_SCREEN".to_string(),
            "1".to_string(),
        )];

        if let Some(path) = config
            .claude
            .config_dir
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .map(expand_home_path)
        {
            environment.push(("CLAUDE_CONFIG_DIR".to_string(), path));
        }

        environment
    }

    pub fn terminal_tmux_session(&self) -> Result<tmux::TerminalSession> {
        tmux::TerminalSession::new(&self.id, &self.title)
    }

    pub fn has_terminal(&self) -> bool {
        self.terminal_info
            .as_ref()
            .map(|t| t.created)
            .unwrap_or(false)
    }

    pub fn start_terminal(&mut self, config: &Config) -> Result<()> {
        self.start_terminal_with_size(None, config)
    }

    pub fn start_terminal_with_size(
        &mut self,
        size: Option<(u16, u16)>,
        config: &Config,
    ) -> Result<()> {
        let session = self.terminal_tmux_session()?;

        let is_new = !session.exists();
        if is_new {
            session.create_with_size(&self.project_path, None, size)?;
        }

        // Apply all configured tmux options to terminal sessions too
        if is_new {
            self.apply_terminal_tmux_options(config);
        }

        self.terminal_info = Some(TerminalInfo {
            created: true,
            created_at: Some(Utc::now()),
        });

        Ok(())
    }

    pub fn kill_terminal(&self) -> Result<()> {
        let session = self.terminal_tmux_session()?;
        if session.exists() {
            session.kill()?;
        }
        Ok(())
    }

    fn sandbox_display(&self) -> Option<crate::tmux::status_bar::SandboxDisplay> {
        self.sandbox_info.as_ref().and_then(|s| {
            if s.enabled {
                Some(crate::tmux::status_bar::SandboxDisplay {
                    container_name: s.container_name.clone(),
                })
            } else {
                None
            }
        })
    }

    /// Apply all configured tmux options to a session with the given name and title.
    fn apply_session_tmux_options(&self, session_name: &str, display_title: &str, config: &Config) {
        let branch = self.worktree_info.as_ref().map(|w| w.branch.as_str());
        let sandbox = self.sandbox_display();
        let project = Some(self.group_path.as_str()).filter(|g| !g.trim().is_empty());
        crate::tmux::status_bar::apply_all_tmux_options(
            session_name,
            display_title,
            project,
            branch,
            sandbox.as_ref(),
            &config.tmux,
        );
    }

    pub fn start(&mut self, config: &Config) -> Result<()> {
        self.start_with_size(None, config)
    }

    pub fn start_with_size(&mut self, size: Option<(u16, u16)>, config: &Config) -> Result<()> {
        self.start_with_size_opts(size, false, config)
    }

    /// Start the session, optionally skipping on_launch hooks (e.g. when they
    /// already ran in the background creation poller).
    pub fn start_with_size_opts(
        &mut self,
        size: Option<(u16, u16)>,
        skip_on_launch: bool,
        config: &Config,
    ) -> Result<()> {
        if self.is_sandboxed() {
            anyhow::bail!(
                "Legacy Docker sandbox sessions cannot be started. Docker sandbox support is deferred in Forager; create a new host session or delete the legacy session."
            );
        }

        let session = self.tmux_session()?;

        if session.exists() {
            return Ok(());
        }

        // Resolve on_launch hooks from the full config chain (global > profile > repo).
        // Repo hooks go through trust verification; global/profile hooks are implicitly trusted.
        let on_launch_hooks = if skip_on_launch {
            None
        } else {
            // The caller supplies the already resolved active-profile config.
            // Never reload the global default profile from this low-level path.
            let resolved_on_launch = self.resolve_on_launch_hooks(config);

            if resolved_on_launch.is_empty() {
                None
            } else {
                Some(resolved_on_launch)
            }
        };

        let cmd = {
            // Run on_launch hooks on host.
            if let Some(ref hook_cmds) = on_launch_hooks {
                if let Err(e) =
                    super::repo_config::execute_hooks(hook_cmds, Path::new(&self.project_path))
                {
                    tracing::warn!("on_launch hook failed: {}", e);
                }
            }

            if self.command.is_empty() {
                crate::agents::get_agent(&self.tool)
                    .filter(|a| a.supports_host_launch)
                    .map(|a| {
                        let mut cmd = a.binary.to_string();
                        if self.is_yolo_mode() {
                            if let Some(ref yolo) = a.yolo {
                                match yolo {
                                    crate::agents::YoloMode::CliFlag(flag) => {
                                        cmd = format!("{} {}", cmd, flag);
                                    }
                                    crate::agents::YoloMode::EnvVar(key, value) => {
                                        cmd = format!("{}={} {}", key, value, cmd);
                                    }
                                }
                            }
                        }
                        wrap_command_ignore_suspend(&cmd)
                    })
            } else {
                let mut cmd = self.command.clone();
                if self.is_yolo_mode() {
                    let agent = crate::agents::get_agent(&self.tool);
                    if let Some(ref yolo) = agent.and_then(|a| a.yolo.as_ref()) {
                        match yolo {
                            crate::agents::YoloMode::CliFlag(flag) => {
                                cmd = format!("{} {}", cmd, flag);
                            }
                            crate::agents::YoloMode::EnvVar(key, value) => {
                                cmd = format!("{}={} {}", key, value, cmd);
                            }
                        }
                    }
                }
                Some(wrap_command_ignore_suspend(&cmd))
            }
        };

        tracing::debug!("session cmd: {}", cmd.as_ref().map_or("none", |v| v));
        let environment = self.agent_environment(config);
        session.create_with_size_env(&self.project_path, cmd.as_deref(), size, &environment)?;

        // Apply all configured tmux options (status bar, mouse, etc.)
        self.apply_tmux_options(config);

        self.status = Status::Starting;
        self.last_start_time = Some(std::time::Instant::now());

        Ok(())
    }

    fn apply_tmux_options(&self, config: &Config) {
        let name = tmux::Session::generate_name(&self.id, &self.title);
        self.apply_session_tmux_options(&name, &self.title, config);
    }

    /// Reapply non-destructive tmux metadata and interaction policy to an
    /// existing session before attaching. This also upgrades sessions created
    /// by an older Forager process without restarting the agent.
    pub fn refresh_tmux_options(&self, config: &Config) {
        self.apply_tmux_options(config);
    }

    fn apply_terminal_tmux_options(&self, config: &Config) {
        let name = tmux::TerminalSession::generate_name(&self.id, &self.title);
        self.apply_session_tmux_options(&name, &format!("{} (terminal)", self.title), config);
    }

    pub fn restart(&mut self, config: &Config) -> Result<()> {
        self.restart_with_size(None, config)
    }

    pub fn restart_with_size(&mut self, size: Option<(u16, u16)>, config: &Config) -> Result<()> {
        let session = self.tmux_session()?;

        if session.exists() {
            session.kill()?;
        }

        // Small delay to ensure tmux cleanup
        std::thread::sleep(std::time::Duration::from_millis(100));

        self.start_with_size(size, config)
    }

    pub fn kill(&self) -> Result<()> {
        let session = self.tmux_session()?;
        if session.exists() {
            session.kill()?;
        }
        Ok(())
    }

    pub fn update_status(&mut self) {
        // Skip expensive checks for recently errored sessions
        if self.status == Status::Error {
            if let Some(last_check) = self.last_error_check {
                if last_check.elapsed().as_secs() < 30 {
                    return;
                }
            }
        }

        // Grace period for starting sessions
        if let Some(start_time) = self.last_start_time {
            if start_time.elapsed().as_secs() < 3 {
                self.status = Status::Starting;
                return;
            }
        }

        let session = match self.tmux_session() {
            Ok(s) => s,
            Err(_) => {
                self.status = Status::Error;
                self.last_error_check = Some(std::time::Instant::now());
                return;
            }
        };

        if !crate::tmux::is_tmux_available() {
            self.status = Status::Error;
            self.last_error = Some("tmux is not available".to_string());
            self.last_error_check = Some(std::time::Instant::now());
            return;
        }

        if !session.exists() {
            self.status = Status::Stopped;
            self.last_error = None;
            self.last_error_check = Some(std::time::Instant::now());
            return;
        }

        // Detect status from pane content
        self.status = match session.detect_status(&self.tool) {
            Ok(status) => status,
            Err(_) => Status::Idle,
        };
    }

    pub fn capture_output_with_size(
        &self,
        lines: usize,
        width: u16,
        height: u16,
    ) -> Result<String> {
        let session = self.tmux_session()?;
        session.capture_pane_with_size(lines, Some(width), Some(height))
    }
}

fn generate_id() -> String {
    Uuid::new_v4().to_string().replace("-", "")[..16].to_string()
}

fn expand_home_path(path: &str) -> String {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(stripped).to_string_lossy().into_owned();
        }
    }
    path.to_string()
}

/// Wrap a command to disable Ctrl-Z (SIGTSTP) suspension.
///
/// When running agents directly as tmux session commands (without a parent shell),
/// pressing Ctrl-Z suspends the process with no way to recover via job control.
/// This wrapper disables the suspend character at the terminal level before exec'ing
/// the actual command.
///
/// Uses POSIX-standard `stty susp undef` which works on both Linux and macOS.
fn wrap_command_ignore_suspend(cmd: &str) -> String {
    format!("bash -c 'stty susp undef; exec {}'", cmd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_instance() {
        let inst = Instance::new("test", "/tmp/test");
        assert_eq!(inst.title, "test");
        assert_eq!(inst.project_path, "/tmp/test");
        assert_eq!(inst.status, Status::Idle);
        assert_eq!(inst.id.len(), 16);
    }

    #[test]
    fn claude_environment_comes_from_the_supplied_effective_config() {
        let mut config = Config::default();
        let mut instance = Instance::new("test", "/tmp/test");
        instance.tool = "claude".to_string();

        assert_eq!(
            instance.agent_environment(&config),
            vec![(
                "CLAUDE_CODE_DISABLE_ALTERNATE_SCREEN".to_string(),
                "1".to_string()
            )]
        );

        config.claude.config_dir = Some("/tmp/profile-claude".to_string());
        assert_eq!(
            instance.agent_environment(&config),
            vec![
                (
                    "CLAUDE_CODE_DISABLE_ALTERNATE_SCREEN".to_string(),
                    "1".to_string()
                ),
                (
                    "CLAUDE_CONFIG_DIR".to_string(),
                    "/tmp/profile-claude".to_string()
                )
            ]
        );

        instance.tool = "codex".to_string();
        assert!(instance.agent_environment(&config).is_empty());
    }

    #[test]
    fn test_is_sub_session() {
        let mut inst = Instance::new("test", "/tmp/test");
        assert!(!inst.is_sub_session());

        inst.parent_session_id = Some("parent123".to_string());
        assert!(inst.is_sub_session());
    }

    #[test]
    fn test_all_agents_have_yolo_support() {
        for agent in crate::agents::AGENTS {
            assert!(
                agent.yolo.is_some(),
                "Agent '{}' should have YOLO mode configured",
                agent.name
            );
        }
    }

    #[test]
    fn test_yolo_mode_helper() {
        let mut inst = Instance::new("test", "/tmp/test");
        assert!(!inst.is_yolo_mode());

        inst.yolo_mode = true;
        assert!(inst.is_yolo_mode());

        inst.yolo_mode = false;
        assert!(!inst.is_yolo_mode());
    }

    #[test]
    fn test_yolo_mode_without_sandbox() {
        let mut inst = Instance::new("test", "/tmp/test");
        assert!(!inst.is_sandboxed());

        inst.yolo_mode = true;
        assert!(inst.is_yolo_mode());
        assert!(!inst.is_sandboxed());
    }

    // Additional tests for is_sandboxed
    #[test]
    fn test_is_sandboxed_without_sandbox_info() {
        let inst = Instance::new("test", "/tmp/test");
        assert!(!inst.is_sandboxed());
    }

    #[test]
    fn test_is_sandboxed_with_disabled_sandbox() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.sandbox_info = Some(SandboxInfo {
            enabled: false,
            container_id: None,
            image: "test-image".to_string(),
            container_name: "test".to_string(),
            created_at: None,
            extra_env_keys: None,
            extra_env_values: None,
        });
        assert!(!inst.is_sandboxed());
    }

    #[test]
    fn test_is_sandboxed_with_enabled_sandbox() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.sandbox_info = Some(SandboxInfo {
            enabled: true,
            container_id: None,
            image: "test-image".to_string(),
            container_name: "test".to_string(),
            created_at: None,
            extra_env_keys: None,
            extra_env_values: None,
        });
        assert!(inst.is_sandboxed());
    }

    #[test]
    fn test_start_refuses_legacy_sandbox_session() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.sandbox_info = Some(SandboxInfo {
            enabled: true,
            container_id: None,
            image: "test-image".to_string(),
            container_name: "test".to_string(),
            created_at: None,
            extra_env_keys: None,
            extra_env_values: None,
        });

        let err = inst
            .start_with_size_opts(None, false, &Config::default())
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Legacy Docker sandbox sessions cannot be started"));
    }

    #[test]
    fn launch_hooks_come_from_the_supplied_active_profile_config() {
        let project = tempfile::tempdir().unwrap();
        let mut global = Config {
            default_profile: "wrong-default".to_string(),
            ..Config::default()
        };
        global.hooks.on_launch = vec!["wrong-global-hook".to_string()];
        let active_profile = super::super::profile_config::ProfileConfig {
            hooks: Some(super::super::profile_config::HooksConfigOverride {
                on_create: None,
                on_launch: Some(vec!["active-profile-hook".to_string()]),
            }),
            ..Default::default()
        };
        let active_config = super::super::profile_config::merge_configs(global, &active_profile);
        let instance = Instance::new("test", project.path().to_str().unwrap());

        assert_eq!(
            instance.resolve_on_launch_hooks(&active_config),
            vec!["active-profile-hook"]
        );
    }

    // Tests for get_tool_command
    #[test]
    fn test_get_tool_command_default_claude() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.tool = "claude".to_string();
        assert_eq!(inst.get_tool_command(), "claude");
    }

    #[test]
    fn test_get_tool_command_opencode() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.tool = "opencode".to_string();
        assert_eq!(inst.get_tool_command(), "opencode");
    }

    #[test]
    fn test_get_tool_command_codex() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.tool = "codex".to_string();
        assert_eq!(inst.get_tool_command(), "codex");
    }

    #[test]
    fn test_get_tool_command_gemini() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.tool = "gemini".to_string();
        assert_eq!(inst.get_tool_command(), "gemini");
    }

    #[test]
    fn test_get_tool_command_unknown_tool() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.tool = "unknown".to_string();
        assert_eq!(inst.get_tool_command(), "bash");
    }

    #[test]
    fn test_get_tool_command_custom_command() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.tool = "claude".to_string();
        inst.command = "claude --resume abc123".to_string();
        assert_eq!(inst.get_tool_command(), "claude --resume abc123");
    }

    // Tests for update_search_cache
    #[test]
    fn test_update_search_cache() {
        let mut inst = Instance::new("Test Title", "/Path/To/Project");
        // Manually modify title
        inst.title = "New Title".to_string();
        inst.project_path = "/New/Path".to_string();

        // Cache is stale
        assert_ne!(inst.title_lower, "new title");
        assert_ne!(inst.project_path_lower, "/new/path");

        // Update cache
        inst.update_search_cache();

        assert_eq!(inst.title_lower, "new title");
        assert_eq!(inst.project_path_lower, "/new/path");
    }

    // Tests for Status enum
    #[test]
    fn test_status_default() {
        let status = Status::default();
        assert_eq!(status, Status::Idle);
    }

    #[test]
    fn test_status_serialization() {
        let statuses = vec![
            Status::Running,
            Status::Waiting,
            Status::Idle,
            Status::Stopped,
            Status::Error,
            Status::Starting,
            Status::Deleting,
        ];

        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: Status = serde_json::from_str(&json).unwrap();
            assert_eq!(status, deserialized);
        }
    }

    // Tests for WorktreeInfo
    #[test]
    fn test_worktree_info_serialization() {
        let info = WorktreeInfo {
            branch: "feature/test".to_string(),
            main_repo_path: "/home/user/repo".to_string(),
            managed_by_forager: true,
            created_at: Utc::now(),
            cleanup_on_delete: true,
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: WorktreeInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(info.branch, deserialized.branch);
        assert_eq!(info.main_repo_path, deserialized.main_repo_path);
        assert_eq!(info.managed_by_forager, deserialized.managed_by_forager);
        assert_eq!(info.cleanup_on_delete, deserialized.cleanup_on_delete);
    }

    #[test]
    fn test_worktree_info_default_cleanup_on_delete() {
        // Deserialize without cleanup_on_delete field - should default to true
        let json = r#"{"branch":"test","main_repo_path":"/path","managed_by_aoe":true,"created_at":"2024-01-01T00:00:00Z"}"#;
        let info: WorktreeInfo = serde_json::from_str(json).unwrap();
        assert!(info.cleanup_on_delete);
    }

    // Tests for SandboxInfo
    #[test]
    fn test_sandbox_info_serialization() {
        let info = SandboxInfo {
            enabled: true,
            container_id: Some("abc123".to_string()),
            image: "myimage:latest".to_string(),
            container_name: "test_container".to_string(),
            created_at: Some(Utc::now()),
            extra_env_keys: Some(vec!["MY_VAR".to_string(), "OTHER_VAR".to_string()]),
            extra_env_values: None,
        };

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: SandboxInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(info.enabled, deserialized.enabled);
        assert_eq!(info.container_id, deserialized.container_id);
        assert_eq!(info.image, deserialized.image);
        assert_eq!(info.container_name, deserialized.container_name);
        assert_eq!(info.extra_env_keys, deserialized.extra_env_keys);
    }

    #[test]
    fn test_sandbox_info_minimal_serialization() {
        // Required fields: enabled, image, container_name
        let json = r#"{"enabled":false,"image":"test-image","container_name":"test"}"#;
        let info: SandboxInfo = serde_json::from_str(json).unwrap();

        assert!(!info.enabled);
        assert_eq!(info.image, "test-image");
        assert_eq!(info.container_name, "test");
        assert!(info.container_id.is_none());
        assert!(info.created_at.is_none());
    }

    // Tests for Instance serialization
    #[test]
    fn test_instance_serialization_roundtrip() {
        let mut inst = Instance::new("Test Project", "/home/user/project");
        inst.tool = "claude".to_string();
        inst.group_path = "work/clients".to_string();
        inst.command = "claude --resume xyz".to_string();

        let json = serde_json::to_string(&inst).unwrap();
        let deserialized: Instance = serde_json::from_str(&json).unwrap();

        assert_eq!(inst.id, deserialized.id);
        assert_eq!(inst.title, deserialized.title);
        assert_eq!(inst.project_path, deserialized.project_path);
        assert_eq!(inst.group_path, deserialized.group_path);
        assert_eq!(inst.tool, deserialized.tool);
        assert_eq!(inst.command, deserialized.command);
    }

    #[test]
    fn test_instance_serialization_skips_runtime_fields() {
        let mut inst = Instance::new("Test", "/tmp/test");
        inst.last_error_check = Some(std::time::Instant::now());
        inst.last_start_time = Some(std::time::Instant::now());
        inst.last_error = Some("test error".to_string());

        let json = serde_json::to_string(&inst).unwrap();

        // Runtime fields should not appear in JSON
        assert!(!json.contains("last_error_check"));
        assert!(!json.contains("last_start_time"));
        assert!(!json.contains("last_error"));
    }

    #[test]
    fn test_instance_with_worktree_info() {
        let mut inst = Instance::new("Test", "/tmp/worktree");
        inst.worktree_info = Some(WorktreeInfo {
            branch: "feature/abc".to_string(),
            main_repo_path: "/tmp/main".to_string(),
            managed_by_forager: true,
            created_at: Utc::now(),
            cleanup_on_delete: true,
        });

        let json = serde_json::to_string(&inst).unwrap();
        let deserialized: Instance = serde_json::from_str(&json).unwrap();

        assert!(deserialized.worktree_info.is_some());
        let wt = deserialized.worktree_info.unwrap();
        assert_eq!(wt.branch, "feature/abc");
        assert!(wt.managed_by_forager);
    }

    // Test generate_id function properties
    #[test]
    fn test_generate_id_uniqueness() {
        let ids: Vec<String> = (0..100).map(|_| Instance::new("t", "/t").id).collect();
        let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(ids.len(), unique_ids.len());
    }

    #[test]
    fn test_generate_id_format() {
        let inst = Instance::new("test", "/tmp/test");
        // ID should be 16 hex characters
        assert_eq!(inst.id.len(), 16);
        assert!(inst.id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_has_terminal_false_by_default() {
        let inst = Instance::new("test", "/tmp/test");
        assert!(!inst.has_terminal());
    }

    #[test]
    fn test_has_terminal_true_when_created() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.terminal_info = Some(TerminalInfo {
            created: true,
            created_at: Some(Utc::now()),
        });
        assert!(inst.has_terminal());
    }

    #[test]
    fn test_terminal_info_none_means_no_terminal() {
        let inst = Instance::new("test", "/tmp/test");
        assert!(inst.terminal_info.is_none());
        assert!(!inst.has_terminal());
    }

    #[test]
    fn test_terminal_info_created_false_means_no_terminal() {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.terminal_info = Some(TerminalInfo {
            created: false,
            created_at: None,
        });
        assert!(!inst.has_terminal());
    }
}
