//! tmux integration module

mod session;
pub mod status_bar;
pub(crate) mod status_detection;
mod terminal_session;
mod utils;

pub use session::Session;
pub use status_bar::{get_session_info_for_current, get_status_for_current_session};
pub use status_detection::detect_status_from_content;
pub use terminal_session::TerminalSession;

use std::collections::HashMap;
use std::process::Command;
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub const SESSION_PREFIX: &str = "forager_";
pub const LEGACY_SESSION_PREFIX: &str = "aoe_";
pub const TERMINAL_PREFIX: &str = "forager_term_";
pub const LEGACY_TERMINAL_PREFIX: &str = "aoe_term_";

static SESSION_CACHE: RwLock<SessionCache> = RwLock::new(SessionCache {
    data: None,
    time: None,
});

struct SessionCache {
    data: Option<HashMap<String, i64>>,
    time: Option<Instant>,
}

pub fn refresh_session_cache() {
    // Space separator, not tab: without a UTF-8 locale (systemd services,
    // cron) tmux sanitizes control characters in format output to '_',
    // which silently broke the tab split and made every session look
    // stopped. rsplit keeps names containing spaces safe since the
    // activity field is always the trailing token.
    let output = Command::new("tmux")
        .args(["list-sessions", "-F", "#{session_name} #{session_activity}"])
        .output();

    let new_data = match output {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let mut map = HashMap::new();
            for line in stdout.lines() {
                if let Some((name, activity)) = line.rsplit_once(' ') {
                    let activity: i64 = activity.parse().unwrap_or(0);
                    map.insert(name.to_string(), activity);
                }
            }
            Some(map)
        }
        _ => None,
    };

    if let Ok(mut cache) = SESSION_CACHE.write() {
        cache.data = new_data;
        cache.time = Some(Instant::now());
    }
}

pub fn session_exists_from_cache(name: &str) -> Option<bool> {
    let cache = SESSION_CACHE.read().ok()?;

    // Cache valid for 2 seconds
    if cache
        .time
        .map(|t| t.elapsed() > Duration::from_secs(2))
        .unwrap_or(true)
    {
        return None;
    }

    cache.data.as_ref().map(|m| m.contains_key(name))
}

pub(crate) fn tmux_session_exists(name: &str) -> bool {
    if let Some(exists) = session_exists_from_cache(name) {
        return exists;
    }

    Command::new("tmux")
        .args(["has-session", "-t", name])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

pub fn is_forager_session_name(name: &str) -> bool {
    name.starts_with(SESSION_PREFIX) || name.starts_with(LEGACY_SESSION_PREFIX)
}

pub fn strip_forager_session_prefix(name: &str) -> &str {
    name.strip_prefix(SESSION_PREFIX)
        .or_else(|| name.strip_prefix(LEGACY_SESSION_PREFIX))
        .unwrap_or(name)
}

pub fn get_current_session_name() -> Option<String> {
    let output = Command::new("tmux")
        .args(["display-message", "-p", "#{session_name}"])
        .output()
        .ok()?;

    if output.status.success() {
        let name = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

pub fn is_tmux_available() -> bool {
    Command::new("tmux").arg("-V").output().is_ok()
}

fn is_agent_available(agent: &crate::agents::AgentDef) -> bool {
    use crate::agents::DetectionMethod;
    match &agent.detection {
        DetectionMethod::Which(binary) => Command::new("which")
            .arg(binary)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false),
        DetectionMethod::RunWithArg(binary, arg) => Command::new(binary).arg(arg).output().is_ok(),
    }
}

#[derive(Debug, Clone)]
pub struct AvailableTools {
    available: Vec<&'static str>,
}

impl AvailableTools {
    pub fn detect() -> Self {
        let available = crate::agents::AGENTS
            .iter()
            .filter(|a| is_agent_available(a))
            .map(|a| a.name)
            .collect();
        Self { available }
    }

    pub fn any_available(&self) -> bool {
        !self.available.is_empty()
    }

    pub fn available_list(&self) -> Vec<&'static str> {
        self.available.clone()
    }

    #[cfg(test)]
    pub fn with_tools(tools: &[&'static str]) -> Self {
        Self {
            available: tools.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forager_session_name_recognizes_primary_and_legacy_prefixes() {
        assert!(is_forager_session_name("forager_demo_abc12345"));
        assert!(is_forager_session_name("aoe_demo_abc12345"));
        assert!(!is_forager_session_name("other_demo_abc12345"));
    }

    #[test]
    fn strip_forager_session_prefix_handles_primary_and_legacy_prefixes() {
        assert_eq!(
            strip_forager_session_prefix("forager_demo_abc12345"),
            "demo_abc12345"
        );
        assert_eq!(
            strip_forager_session_prefix("aoe_demo_abc12345"),
            "demo_abc12345"
        );
        assert_eq!(strip_forager_session_prefix("other_demo"), "other_demo");
    }
}
