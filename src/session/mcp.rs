//! MCP (Model Context Protocol) server management

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::config::{load_config, McpConfig};

pub fn get_available_mcps() -> Result<HashMap<String, McpConfig>> {
    let config = load_config()?.unwrap_or_default();
    Ok(config.mcps)
}

pub fn get_attached_mcps(project_path: &str) -> Result<Vec<String>> {
    let mcp_json_path = Path::new(project_path).join(".mcp.json");

    if !mcp_json_path.exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(&mcp_json_path)?;
    let mcp_json: McpJsonFile = serde_json::from_str(&content)?;

    Ok(mcp_json.mcpServers.keys().cloned().collect())
}

pub fn write_mcp_json(project_path: &Path, mcp_names: &[String]) -> Result<()> {
    let available = get_available_mcps()?;

    let mut servers = HashMap::new();
    for name in mcp_names {
        if let Some(config) = available.get(name) {
            let server = mcp_config_to_server(config);
            servers.insert(name.clone(), server);
        }
    }

    let mcp_json = McpJsonFile {
        mcpServers: servers,
    };

    let content = serde_json::to_string_pretty(&mcp_json)?;
    let mcp_json_path = project_path.join(".mcp.json");
    fs::write(&mcp_json_path, content)?;

    Ok(())
}

pub fn attach_local_mcp(project_path: &str, mcp_name: &str) -> Result<()> {
    let mut attached = get_attached_mcps(project_path)?;

    if !attached.contains(&mcp_name.to_string()) {
        attached.push(mcp_name.to_string());
        write_mcp_json(Path::new(project_path), &attached)?;
    }

    Ok(())
}

pub fn detach_local_mcp(project_path: &str, mcp_name: &str) -> Result<()> {
    let mut attached = get_attached_mcps(project_path)?;
    attached.retain(|n| n != mcp_name);
    write_mcp_json(Path::new(project_path), &attached)?;
    Ok(())
}

pub fn attach_global_mcp(mcp_name: &str) -> Result<()> {
    let claude_config_path = get_claude_global_config_path()?;
    let mut claude_config = load_claude_config(&claude_config_path)?;

    let available = get_available_mcps()?;
    if let Some(config) = available.get(mcp_name) {
        let server = mcp_config_to_server(config);
        claude_config
            .mcpServers
            .insert(mcp_name.to_string(), server);
        save_claude_config(&claude_config_path, &claude_config)?;
    }

    Ok(())
}

pub fn detach_global_mcp(mcp_name: &str) -> Result<()> {
    let claude_config_path = get_claude_global_config_path()?;
    let mut claude_config = load_claude_config(&claude_config_path)?;
    claude_config.mcpServers.remove(mcp_name);
    save_claude_config(&claude_config_path, &claude_config)?;
    Ok(())
}

fn get_claude_global_config_path() -> Result<std::path::PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot find home directory"))?;

    // Check custom config dir
    if let Some(custom_dir) = super::get_claude_config_dir() {
        return Ok(custom_dir.join("claude_desktop_config.json"));
    }

    // Default path
    #[cfg(target_os = "macos")]
    let config_path = home.join("Library/Application Support/Claude/claude_desktop_config.json");

    #[cfg(target_os = "linux")]
    let config_path = home.join(".config/Claude/claude_desktop_config.json");

    #[cfg(target_os = "windows")]
    let config_path = home.join("AppData/Roaming/Claude/claude_desktop_config.json");

    Ok(config_path)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct McpJsonFile {
    #[serde(default)]
    mcpServers: HashMap<String, McpServer>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct McpServer {
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    args: Vec<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    env: HashMap<String, String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    transport: Option<String>,
}

fn mcp_config_to_server(config: &McpConfig) -> McpServer {
    McpServer {
        command: config.command.clone(),
        args: config.args.clone(),
        env: config.env.clone(),
        url: config.url.clone(),
        transport: config.transport.clone(),
    }
}

fn load_claude_config(path: &std::path::Path) -> Result<McpJsonFile> {
    if !path.exists() {
        return Ok(McpJsonFile::default());
    }
    let content = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content)?)
}

fn save_claude_config(path: &std::path::Path, config: &McpJsonFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(config)?;
    fs::write(path, content)?;
    Ok(())
}
