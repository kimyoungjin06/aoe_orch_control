//! `agent-of-empires group` subcommands implementation

use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::session::{GroupTree, Storage};

#[derive(Subcommand)]
pub enum GroupCommands {
    /// List all groups
    List(GroupListArgs),

    /// Create a new group
    Create(GroupCreateArgs),

    /// Delete a group
    Delete(GroupDeleteArgs),

    /// Move session to group
    Move(GroupMoveArgs),
}

#[derive(Args)]
pub struct GroupListArgs {
    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct GroupCreateArgs {
    /// Group name
    name: String,

    /// Parent group for creating subgroups
    #[arg(long)]
    parent: Option<String>,
}

#[derive(Args)]
pub struct GroupDeleteArgs {
    /// Group name
    name: String,

    /// Force delete by moving sessions to default group
    #[arg(long)]
    force: bool,
}

#[derive(Args)]
pub struct GroupMoveArgs {
    /// Session ID or title
    identifier: String,

    /// Target group
    group: String,
}

#[derive(Serialize)]
struct GroupInfo {
    name: String,
    path: String,
    session_count: usize,
    children: Vec<String>,
}

pub async fn run(profile: &str, command: GroupCommands) -> Result<()> {
    match command {
        GroupCommands::List(args) => list_groups(profile, args).await,
        GroupCommands::Create(args) => create_group(profile, args).await,
        GroupCommands::Delete(args) => delete_group(profile, args).await,
        GroupCommands::Move(args) => move_session(profile, args).await,
    }
}

async fn list_groups(profile: &str, args: GroupListArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (instances, groups) = storage.load_with_groups()?;

    let group_tree = GroupTree::new_with_groups(&instances, &groups);

    if args.json {
        let group_list: Vec<GroupInfo> = group_tree
            .get_all_groups()
            .iter()
            .map(|g| {
                let session_count = instances.iter().filter(|i| i.group_path == g.path).count();
                GroupInfo {
                    name: g.name.clone(),
                    path: g.path.clone(),
                    session_count,
                    children: g.children.iter().map(|c| c.name.clone()).collect(),
                }
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&group_list)?);
    } else {
        let all_groups = group_tree.get_all_groups();
        if all_groups.is_empty() {
            println!("No groups found.");
            println!("Create one with: agent-of-empires group create <name>");
            return Ok(());
        }

        println!("Groups:\n");
        for group in &all_groups {
            let session_count = instances
                .iter()
                .filter(|i| i.group_path == group.path)
                .count();
            let indent = group.path.matches('/').count();
            println!(
                "{}• {} ({} sessions)",
                "  ".repeat(indent),
                group.name,
                session_count
            );
        }
        println!("\nTotal: {} groups", all_groups.len());
    }

    Ok(())
}

async fn create_group(profile: &str, args: GroupCreateArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (instances, groups) = storage.load_with_groups()?;

    let group_path = if let Some(parent) = &args.parent {
        format!("{}/{}", parent, args.name)
    } else {
        args.name.clone()
    };

    let mut group_tree = GroupTree::new_with_groups(&instances, &groups);

    if group_tree.group_exists(&group_path) {
        bail!("Group already exists: {}", group_path);
    }

    group_tree.create_group(&group_path);
    storage.save_with_groups(&instances, &group_tree)?;

    println!("✓ Created group: {}", group_path);

    Ok(())
}

async fn delete_group(profile: &str, args: GroupDeleteArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (mut instances, groups) = storage.load_with_groups()?;

    let mut group_tree = GroupTree::new_with_groups(&instances, &groups);

    if !group_tree.group_exists(&args.name) {
        bail!("Group not found: {}", args.name);
    }

    // Check for sessions in this group
    let session_count = instances
        .iter()
        .filter(|i| {
            i.group_path == args.name || i.group_path.starts_with(&format!("{}/", args.name))
        })
        .count();

    if session_count > 0 {
        if !args.force {
            bail!(
                "Group '{}' contains {} sessions. Use --force to move them to default group.",
                args.name,
                session_count
            );
        }

        // Move sessions to default group
        for inst in &mut instances {
            if inst.group_path == args.name
                || inst.group_path.starts_with(&format!("{}/", args.name))
            {
                inst.group_path = String::new();
            }
        }
    }

    group_tree.delete_group(&args.name);
    storage.save_with_groups(&instances, &group_tree)?;

    println!("✓ Deleted group: {}", args.name);
    if args.force && session_count > 0 {
        println!("  Moved {} sessions to default group", session_count);
    }

    Ok(())
}

async fn move_session(profile: &str, args: GroupMoveArgs) -> Result<()> {
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

    let old_group = inst.group_path.clone();
    inst.group_path = args.group.clone();

    let mut group_tree = GroupTree::new_with_groups(&instances, &groups);
    if !args.group.is_empty() {
        group_tree.create_group(&args.group);
    }

    storage.save_with_groups(&instances, &group_tree)?;

    if old_group.is_empty() {
        println!("✓ Moved session to group: {}", args.group);
    } else {
        println!("✓ Moved session from '{}' to '{}'", old_group, args.group);
    }

    Ok(())
}
