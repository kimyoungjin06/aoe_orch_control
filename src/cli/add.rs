//! `agent-of-empires add` command implementation

use anyhow::{bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::session::{GroupTree, Instance, Storage};

#[derive(Args)]
pub struct AddArgs {
    /// Project directory (defaults to current directory)
    #[arg(default_value = ".")]
    path: PathBuf,

    /// Session title (defaults to folder name)
    #[arg(short = 't', long)]
    title: Option<String>,

    /// Group path (defaults to parent folder)
    #[arg(short = 'g', long)]
    group: Option<String>,

    /// Command to run (e.g., 'claude', 'opencode')
    #[arg(short = 'c', long = "cmd")]
    command: Option<String>,

    /// Parent session (creates sub-session, inherits group)
    #[arg(short = 'P', long)]
    parent: Option<String>,

    /// MCPs to attach (can specify multiple times)
    #[arg(long = "mcp")]
    mcps: Vec<String>,

    /// Launch the session immediately after creating
    #[arg(short = 'l', long)]
    launch: bool,
}

pub async fn run(profile: &str, args: AddArgs) -> Result<()> {
    let path = if args.path.as_os_str() == "." {
        std::env::current_dir()?
    } else {
        args.path.canonicalize()?
    };

    if !path.is_dir() {
        bail!("Path is not a directory: {}", path.display());
    }

    let user_provided_title = args.title.is_some();
    let title = args.title.unwrap_or_else(|| {
        path.file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "untitled".to_string())
    });

    let storage = Storage::new(profile)?;
    let (mut instances, groups) = storage.load_with_groups()?;

    // Resolve parent session if specified
    let mut group_path = args.group.clone();
    let parent_id = if let Some(parent_ref) = &args.parent {
        let parent = super::resolve_session(parent_ref, &instances)?;
        if parent.is_sub_session() {
            bail!("Cannot create sub-session of a sub-session (single level only)");
        }
        group_path = Some(parent.group_path.clone());
        Some(parent.id.clone())
    } else {
        None
    };

    // Generate unique title if needed
    let final_title = if !user_provided_title {
        generate_unique_title(&instances, &title, path.to_str().unwrap_or(""))
    } else {
        // Check for exact duplicate
        if is_duplicate_session(&instances, &title, path.to_str().unwrap_or("")) {
            println!("Session already exists with same title and path: {}", title);
            return Ok(());
        }
        title
    };

    let mut instance = Instance::new(&final_title, path.to_str().unwrap_or(""));

    if let Some(group) = &group_path {
        instance.group_path = group.clone();
    }

    if let Some(parent) = parent_id {
        instance.parent_session_id = Some(parent);
    }

    if let Some(cmd) = &args.command {
        instance.command = cmd.clone();
        instance.tool = detect_tool(cmd);
    }

    instances.push(instance.clone());

    // Rebuild group tree
    let mut group_tree = GroupTree::new_with_groups(&instances, &groups);
    if !instance.group_path.is_empty() {
        group_tree.create_group(&instance.group_path);
    }

    storage.save_with_groups(&instances, &group_tree)?;

    // Handle MCP attachment
    if !args.mcps.is_empty() {
        let available_mcps = crate::session::mcp::get_available_mcps()?;
        for mcp_name in &args.mcps {
            if !available_mcps.contains_key(mcp_name) {
                bail!("MCP '{}' not found in config.toml", mcp_name);
            }
        }
        crate::session::mcp::write_mcp_json(&path, &args.mcps)?;
    }

    println!("✓ Added session: {}", final_title);
    println!("  Profile: {}", storage.profile());
    println!("  Path:    {}", path.display());
    println!("  Group:   {}", instance.group_path);
    println!("  ID:      {}", instance.id);
    if let Some(cmd) = &args.command {
        println!("  Cmd:     {}", cmd);
    }
    if !args.mcps.is_empty() {
        println!("  MCPs:    {}", args.mcps.join(", "));
    }
    if let Some(parent) = &args.parent {
        println!("  Parent:  {}", parent);
    }

    if args.launch {
        let idx = instances
            .iter()
            .position(|i| i.id == instance.id)
            .expect("just added instance");
        instances[idx].start()?;
        storage.save_with_groups(&instances, &group_tree)?;

        let tmux_session = crate::tmux::Session::new(&instance.id, &instance.title)?;
        tmux_session.attach()?;
    } else {
        println!();
        println!("Next steps:");
        println!(
            "  agent-of-empires session start {}   # Start the session",
            final_title
        );
        println!("  agent-of-empires                         # Open TUI and press Enter to attach");
    }

    Ok(())
}

pub fn is_duplicate_session(instances: &[Instance], title: &str, path: &str) -> bool {
    let normalized_path = path.trim_end_matches('/');
    instances.iter().any(|inst| {
        let existing_path = inst.project_path.trim_end_matches('/');
        existing_path == normalized_path && inst.title == title
    })
}

pub fn generate_unique_title(instances: &[Instance], base_title: &str, path: &str) -> String {
    let title_exists = |title: &str| -> bool {
        instances
            .iter()
            .any(|inst| inst.project_path == path && inst.title == title)
    };

    if !title_exists(base_title) {
        return base_title.to_string();
    }

    for i in 2..=100 {
        let candidate = format!("{} ({})", base_title, i);
        if !title_exists(&candidate) {
            return candidate;
        }
    }

    format!("{} ({})", base_title, chrono::Utc::now().timestamp())
}

fn detect_tool(cmd: &str) -> String {
    let cmd_lower = cmd.to_lowercase();
    if cmd_lower.is_empty() || cmd_lower.contains("claude") {
        "claude".to_string()
    } else if cmd_lower.contains("opencode") || cmd_lower.contains("open-code") {
        "opencode".to_string()
    } else if cmd_lower.contains("cursor") {
        "cursor".to_string()
    } else {
        "shell".to_string()
    }
}
