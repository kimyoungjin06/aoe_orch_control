//! `agent-of-empires mcp` subcommands implementation

use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::session::{mcp, Storage};

#[derive(Subcommand)]
pub enum McpCommands {
    /// List available MCPs from config.toml
    List(McpListArgs),

    /// Show MCPs attached to a session
    Attached(McpAttachedArgs),

    /// Attach MCP to session
    Attach(McpAttachArgs),

    /// Detach MCP from session
    Detach(McpDetachArgs),
}

#[derive(Args)]
pub struct McpListArgs {
    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct McpAttachedArgs {
    /// Session ID or title (optional, auto-detects in tmux)
    identifier: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct McpAttachArgs {
    /// Session ID or title
    identifier: String,

    /// MCP name to attach
    mcp_name: String,

    /// Apply to global Claude config (all projects)
    #[arg(long)]
    global: bool,

    /// Restart session after change
    #[arg(long)]
    restart: bool,
}

#[derive(Args)]
pub struct McpDetachArgs {
    /// Session ID or title
    identifier: String,

    /// MCP name to detach
    mcp_name: String,

    /// Apply to global Claude config (all projects)
    #[arg(long)]
    global: bool,

    /// Restart session after change
    #[arg(long)]
    restart: bool,
}

#[derive(Serialize)]
struct McpInfo {
    name: String,
    description: String,
    command: Option<String>,
    url: Option<String>,
    transport: String,
}

pub async fn run(profile: &str, command: McpCommands) -> Result<()> {
    match command {
        McpCommands::List(args) => list_mcps(args).await,
        McpCommands::Attached(args) => attached_mcps(profile, args).await,
        McpCommands::Attach(args) => attach_mcp(profile, args).await,
        McpCommands::Detach(args) => detach_mcp(profile, args).await,
    }
}

async fn list_mcps(args: McpListArgs) -> Result<()> {
    let mcps = mcp::get_available_mcps()?;

    if mcps.is_empty() {
        if !args.json {
            println!("No MCPs configured in ~/.agent-of-empires/config.toml");
            println!();
            println!("Example config:");
            println!("[mcps.exa]");
            println!(r#"command = "npx""#);
            println!(r#"args = ["-y", "exa-mcp-server"]"#);
            println!(r#"env = {{ EXA_API_KEY = "your-key" }}"#);
            println!(r#"description = "Web search via Exa AI""#);
        } else {
            println!("[]");
        }
        return Ok(());
    }

    if args.json {
        let mcp_list: Vec<McpInfo> = mcps
            .iter()
            .map(|(name, config)| McpInfo {
                name: name.clone(),
                description: config.description.clone().unwrap_or_default(),
                command: config.command.clone(),
                url: config.url.clone(),
                transport: config
                    .transport
                    .clone()
                    .unwrap_or_else(|| "stdio".to_string()),
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&mcp_list)?);
    } else {
        println!("Available MCPs:\n");
        for (name, config) in &mcps {
            let desc = config.description.as_deref().unwrap_or("No description");
            let transport = config.transport.as_deref().unwrap_or("stdio");
            println!("  {} [{}]", name, transport);
            println!("    {}", desc);
        }
        println!("\nTotal: {} MCPs", mcps.len());
    }

    Ok(())
}

async fn attached_mcps(profile: &str, args: McpAttachedArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (instances, _) = storage.load_with_groups()?;

    let inst = if let Some(id) = &args.identifier {
        super::resolve_session(id, &instances)?
    } else {
        // Auto-detect from tmux
        let current_session = std::env::var("TMUX_PANE")
            .ok()
            .and_then(|_| crate::tmux::get_current_session_name());

        if let Some(session_name) = current_session {
            instances
                .iter()
                .find(|i| {
                    let tmux_name = crate::tmux::Session::generate_name(&i.id, &i.title);
                    tmux_name == session_name
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("Current tmux session is not an Agent of Empires session")
                })?
        } else {
            bail!("Not in a tmux session. Specify a session ID or run inside tmux.");
        }
    };

    let attached = mcp::get_attached_mcps(&inst.project_path)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&attached)?);
    } else {
        if attached.is_empty() {
            println!("No MCPs attached to session: {}", inst.title);
        } else {
            println!("MCPs attached to '{}':\n", inst.title);
            for name in &attached {
                println!("  • {}", name);
            }
        }
    }

    Ok(())
}

async fn attach_mcp(profile: &str, args: McpAttachArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (mut instances, groups) = storage.load_with_groups()?;

    let inst = instances
        .iter_mut()
        .find(|i| {
            i.id == args.identifier
                || i.id.starts_with(&args.identifier)
                || i.title == args.identifier
        })
        .ok_or_else(|| anyhow::anyhow!("Session not found: {}", args.identifier))?;

    let available = mcp::get_available_mcps()?;
    if !available.contains_key(&args.mcp_name) {
        bail!(
            "MCP '{}' not found in config.toml. Available: {}",
            args.mcp_name,
            available.keys().cloned().collect::<Vec<_>>().join(", ")
        );
    }

    if args.global {
        mcp::attach_global_mcp(&args.mcp_name)?;
        println!("✓ Attached MCP '{}' globally", args.mcp_name);
    } else {
        mcp::attach_local_mcp(&inst.project_path, &args.mcp_name)?;
        println!(
            "✓ Attached MCP '{}' to session '{}'",
            args.mcp_name, inst.title
        );
    }

    if args.restart {
        inst.restart()?;
        let group_tree = crate::session::GroupTree::new_with_groups(&instances, &groups);
        storage.save_with_groups(&instances, &group_tree)?;
        println!("  Session restarted to load new MCP");
    }

    Ok(())
}

async fn detach_mcp(profile: &str, args: McpDetachArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (mut instances, groups) = storage.load_with_groups()?;

    let inst = instances
        .iter_mut()
        .find(|i| {
            i.id == args.identifier
                || i.id.starts_with(&args.identifier)
                || i.title == args.identifier
        })
        .ok_or_else(|| anyhow::anyhow!("Session not found: {}", args.identifier))?;

    if args.global {
        mcp::detach_global_mcp(&args.mcp_name)?;
        println!("✓ Detached MCP '{}' globally", args.mcp_name);
    } else {
        mcp::detach_local_mcp(&inst.project_path, &args.mcp_name)?;
        println!(
            "✓ Detached MCP '{}' from session '{}'",
            args.mcp_name, inst.title
        );
    }

    if args.restart {
        inst.restart()?;
        let group_tree = crate::session::GroupTree::new_with_groups(&instances, &groups);
        storage.save_with_groups(&instances, &group_tree)?;
        println!("  Session restarted to unload MCP");
    }

    Ok(())
}
