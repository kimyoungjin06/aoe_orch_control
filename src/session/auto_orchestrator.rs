//! Auto-orchestrator provisioning for newly created sessions.

use std::path::PathBuf;

use super::{Config, Instance};

#[derive(Debug, Clone)]
pub struct AutoOrchestratorOutcome {
    pub session_id: String,
    pub title: String,
    pub launched: bool,
}

fn parse_bool_env(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn normalize_project_path(path: &str) -> String {
    let canonical = PathBuf::from(path)
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(path));
    canonical
        .to_string_lossy()
        .trim_end_matches('/')
        .to_string()
}

fn auto_orchestrator_enabled(config: &Config) -> bool {
    if let Ok(value) = std::env::var("AOE_AUTO_ORCHESTRATOR") {
        if let Some(parsed) = parse_bool_env(&value) {
            return parsed;
        }
    }
    config.session.auto_orchestrator
}

fn orchestrator_title(config: &Config) -> String {
    if let Ok(from_env) = std::env::var("AOE_ORCHESTRATOR_TITLE") {
        let trimmed = from_env.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    config
        .session
        .orchestrator_title
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Orchestrator".to_string())
}

/// Ensure an orchestrator session exists for the same project path as `trigger_instance`.
///
/// This is best-effort and never fails hard: any launch error is logged and we still
/// keep the orchestrator session in storage so users can start it later manually.
pub fn maybe_create_for_instance(
    instances: &mut Vec<Instance>,
    trigger_instance: &Instance,
    config: &Config,
) -> Option<AutoOrchestratorOutcome> {
    if !auto_orchestrator_enabled(config) {
        return None;
    }

    let title = orchestrator_title(config);
    if trigger_instance.title.eq_ignore_ascii_case(&title) {
        return None;
    }

    let trigger_path = normalize_project_path(&trigger_instance.project_path);
    let already_exists = instances.iter().any(|inst| {
        inst.title.eq_ignore_ascii_case(&title)
            && normalize_project_path(&inst.project_path) == trigger_path
    });

    if already_exists {
        return None;
    }

    let mut orchestrator = Instance::new(&title, &trigger_instance.project_path);
    orchestrator.group_path = trigger_instance.group_path.clone();
    orchestrator.tool = config
        .session
        .default_tool
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if trigger_instance.tool.trim().is_empty() {
                "codex".to_string()
            } else {
                trigger_instance.tool.clone()
            }
        });
    orchestrator.yolo_mode = trigger_instance.yolo_mode;

    if let Some(command) = config
        .session
        .orchestrator_command
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        orchestrator.command = command.to_string();
    }

    let session_id = orchestrator.id.clone();
    let mut launched = false;
    if let Err(e) = orchestrator.start_with_size(None, config) {
        tracing::warn!(
            "Failed to auto-start orchestrator session '{}' for '{}': {}",
            title,
            trigger_instance.project_path,
            e
        );
    } else if let Ok(session) = orchestrator.tmux_session() {
        launched = session.exists();
    }

    instances.push(orchestrator);

    Some(AutoOrchestratorOutcome {
        session_id,
        title,
        launched,
    })
}

/// Ensure orchestrator sessions exist for all existing project sessions.
/// Returns the number of newly created orchestrator sessions.
pub fn ensure_for_existing_sessions(instances: &mut Vec<Instance>, config: &Config) -> usize {
    let snapshot = instances.clone();
    let mut created = 0usize;

    for inst in &snapshot {
        if maybe_create_for_instance(instances, inst, config).is_some() {
            created += 1;
        }
    }

    created
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_env_handles_common_values() {
        assert_eq!(parse_bool_env("1"), Some(true));
        assert_eq!(parse_bool_env("true"), Some(true));
        assert_eq!(parse_bool_env("0"), Some(false));
        assert_eq!(parse_bool_env("off"), Some(false));
        assert_eq!(parse_bool_env("maybe"), None);
    }

    #[test]
    fn orchestrator_title_comes_from_the_supplied_effective_config() {
        let mut config = Config::default();
        config.session.orchestrator_title = Some("Active Profile Orchestrator".to_string());

        assert_eq!(orchestrator_title(&config), "Active Profile Orchestrator");
    }
}
