//! Session instance definition and operations

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

use crate::tmux;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Running,
    Waiting,
    #[default]
    Idle,
    Error,
    Starting,
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
    pub status: Status,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<DateTime<Utc>>,

    // Claude Code integration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_detected_at: Option<DateTime<Utc>>,

    // MCP tracking
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub loaded_mcp_names: Vec<String>,

    // Runtime state (not serialized)
    #[serde(skip)]
    pub last_error_check: Option<std::time::Instant>,
    #[serde(skip)]
    pub last_start_time: Option<std::time::Instant>,
    #[serde(skip)]
    pub skip_mcp_regenerate: bool,
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
            status: Status::Idle,
            created_at: Utc::now(),
            last_accessed_at: None,
            claude_session_id: None,
            claude_detected_at: None,
            loaded_mcp_names: Vec::new(),
            last_error_check: None,
            last_start_time: None,
            skip_mcp_regenerate: false,
        }
    }

    pub fn is_sub_session(&self) -> bool {
        self.parent_session_id.is_some()
    }

    pub fn tmux_session(&self) -> Result<tmux::Session> {
        tmux::Session::new(&self.id, &self.title)
    }

    pub fn start(&mut self) -> Result<()> {
        let session = self.tmux_session()?;

        if session.exists() {
            return Ok(());
        }

        let cmd = if self.command.is_empty() {
            None
        } else {
            Some(self.command.as_str())
        };

        session.create(&self.project_path, cmd)?;
        self.status = Status::Starting;
        self.last_start_time = Some(std::time::Instant::now());

        Ok(())
    }

    pub fn restart(&mut self) -> Result<()> {
        let session = self.tmux_session()?;

        if session.exists() {
            session.kill()?;
        }

        // Small delay to ensure tmux cleanup
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Regenerate MCP config if needed
        if !self.skip_mcp_regenerate && self.tool == "claude" {
            let path = Path::new(&self.project_path);
            if let Ok(mcps) = super::mcp::get_attached_mcps(&self.project_path) {
                let _ = super::mcp::write_mcp_json(path, &mcps);
                self.loaded_mcp_names = mcps;
            }
        }
        self.skip_mcp_regenerate = false;

        self.start()
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

        if !session.exists() {
            self.status = Status::Error;
            self.last_error_check = Some(std::time::Instant::now());
            return;
        }

        // Detect status from pane content
        self.status = match session.detect_status(&self.tool) {
            Ok(status) => status,
            Err(_) => Status::Idle,
        };

        // Detect Claude session ID if applicable
        if self.tool == "claude" && self.claude_session_id.is_none() {
            if let Ok(Some(id)) = super::claude::detect_session_id(&self.project_path) {
                self.claude_session_id = Some(id);
                self.claude_detected_at = Some(Utc::now());
            }
        }
    }

    pub fn fork(&self, new_title: &str, new_group: &str) -> Result<Instance> {
        if self.tool != "claude" {
            anyhow::bail!("Fork is only supported for Claude sessions");
        }

        let claude_id = self
            .claude_session_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No Claude session ID to fork"))?;

        let mut forked = Self::new(new_title, &self.project_path);
        forked.group_path = new_group.to_string();
        forked.command = format!("claude --resume {}", claude_id);
        forked.tool = "claude".to_string();
        forked.parent_session_id = Some(self.id.clone());

        Ok(forked)
    }

    pub fn capture_output(&self, lines: usize) -> Result<String> {
        let session = self.tmux_session()?;
        session.capture_pane(lines)
    }
}

fn generate_id() -> String {
    Uuid::new_v4().to_string().replace("-", "")[..16].to_string()
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
    fn test_is_sub_session() {
        let mut inst = Instance::new("test", "/tmp/test");
        assert!(!inst.is_sub_session());

        inst.parent_session_id = Some("parent123".to_string());
        assert!(inst.is_sub_session());
    }
}
