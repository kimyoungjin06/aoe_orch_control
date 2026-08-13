//! `forager remove` command implementation

use anyhow::Result;
use clap::Args;

use crate::containers;
use crate::session::{GroupTree, Instance, Storage};

#[derive(Args)]
pub struct RemoveArgs {
    /// Session ID or title to remove
    identifier: String,

    /// Delete worktree directory (default: keep worktree)
    #[arg(long = "delete-worktree")]
    delete_worktree: bool,

    /// Force worktree removal even with untracked/modified files
    #[arg(long)]
    force: bool,

    /// Keep legacy sandbox container instead of deleting it (default: delete per config)
    #[arg(long = "keep-container")]
    keep_container: bool,
}

fn needs_worktree_cleanup(inst: &Instance, args: &RemoveArgs) -> bool {
    inst.worktree_info
        .as_ref()
        .is_some_and(|wt| wt.managed_by_forager && args.delete_worktree)
}

pub async fn run(profile: &str, args: RemoveArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (mut instances, groups) = storage.load_with_groups()?;

    let index = crate::cli::resolve_session_index(&args.identifier, &instances).map_err(|e| {
        anyhow::anyhow!(
            "{} in profile '{}'",
            e.to_string().trim_end_matches('.'),
            storage.profile()
        )
    })?;
    let inst = instances.remove(index);
    let removed_title = inst.title.clone();
    let effective_config = crate::session::resolve_config_with_repo(
        profile,
        std::path::Path::new(&inst.project_path),
    )?;

    let will_cleanup_worktree = needs_worktree_cleanup(&inst, &args);

    // Show warning and get confirmation for worktree deletion
    let user_confirmed = if will_cleanup_worktree {
        use std::io::{self, Write};

        let wt_info = inst
            .worktree_info
            .as_ref()
            .expect("worktree cleanup checked");
        println!("\nThis will delete:");
        println!(
            "  - Worktree: {} (branch: {})",
            inst.project_path, wt_info.branch
        );
        print!("\nProceed? (Y/n): ");
        io::stdout().flush()?;

        let mut response = String::new();
        io::stdin().read_line(&mut response)?;
        let response = response.trim().to_lowercase();

        response.is_empty() || response == "y" || response == "yes"
    } else {
        true
    };

    // Handle worktree cleanup
    if will_cleanup_worktree {
        if user_confirmed {
            use crate::git::GitWorktree;
            use std::path::PathBuf;

            let wt_info = inst
                .worktree_info
                .as_ref()
                .expect("worktree cleanup checked");
            let worktree_path = PathBuf::from(&inst.project_path);
            let main_repo = PathBuf::from(&wt_info.main_repo_path);

            match GitWorktree::new(main_repo) {
                Ok(git_wt) => {
                    if let Err(e) = git_wt.remove_worktree(&worktree_path, args.force) {
                        eprintln!("Warning: failed to remove worktree: {}", e);
                        eprintln!(
                            "You may need to remove it manually with: git worktree remove {}",
                            inst.project_path
                        );
                    } else {
                        println!("✓ Worktree removed");
                    }
                }
                Err(e) => {
                    eprintln!("Warning: failed to access git repository: {}", e);
                }
            }
        } else {
            println!("Worktree preserved at: {}", inst.project_path);
        }
    } else if let Some(wt_info) = &inst.worktree_info {
        // Worktree exists but not scheduled for deletion (user didn't use --delete-worktree)
        if wt_info.managed_by_forager {
            println!(
                "Worktree preserved at: {} (use --delete-worktree to remove)",
                inst.project_path
            );
        }
    }

    // Kill tmux session if it exists
    if let Ok(tmux_session) = crate::tmux::Session::new(&inst.id, &inst.title) {
        if tmux_session.exists() {
            if let Err(e) = tmux_session.kill() {
                eprintln!("Warning: failed to kill tmux session: {}", e);
                eprintln!("Session removed from Forager but may still be running in tmux");
            }
        }
    }

    // Legacy sandbox container cleanup (if config allows and user didn't request --keep-container)
    if let Some(sandbox) = &inst.sandbox_info {
        if sandbox.enabled && !args.keep_container {
            if effective_config.sandbox.auto_cleanup {
                let container = containers::DockerContainer::from_stored_name(
                    &inst.id,
                    &sandbox.image,
                    &sandbox.container_name,
                );
                if container.exists().unwrap_or(false) {
                    if let Err(e) = container.remove(true) {
                        eprintln!("Warning: failed to remove legacy sandbox container: {}", e);
                    } else {
                        println!("✓ Legacy sandbox container removed");
                    }
                }
            } else {
                println!(
                    "Legacy sandbox container preserved: {} (auto_cleanup disabled in config)",
                    sandbox.container_name
                );
            }
        } else if args.keep_container {
            println!(
                "Legacy sandbox container preserved: {}",
                sandbox.container_name
            );
        }
    }

    // Rebuild group tree and save
    let group_tree = GroupTree::new_with_groups(&instances, &groups);
    storage.save_with_groups(&instances, &group_tree)?;

    println!(
        "✓ Removed session: {} (from profile '{}')",
        removed_title,
        storage.profile()
    );

    Ok(())
}
