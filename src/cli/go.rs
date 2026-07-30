//! `forager go`: the zero-friction wrapper for direct agent work.
//!
//! One command replaces the add/start/attach dance: resolve the current
//! directory against the project registry, refresh the project's wiki brief,
//! then find-or-create a session for (cwd, tool) and attach to it. Typing
//! cost matches running the agent directly (`alias cc='forager go claude'`),
//! but the session is supervised and the knowledge plane rides along.

use anyhow::{bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::session::project_registry::{load_registry, resolve_project_for_path};
use crate::session::{repo_config, GroupTree, Instance, Storage};

#[derive(Args)]
pub struct GoArgs {
    /// Agent tool to run (e.g. 'claude', 'codex', 'gemini', 'opencode').
    /// Defaults to `session.default_tool` from the profile config, else 'claude'.
    tool: Option<String>,

    /// Extra arguments appended to the tool command (after `--`),
    /// e.g. `forager go claude -- --continue`
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    tool_args: Vec<String>,

    /// Project directory (defaults to current directory)
    #[arg(long, default_value = ".")]
    path: PathBuf,

    /// Enable YOLO mode when creating a new session
    #[arg(short = 'y', long)]
    yolo: bool,

    /// Automatically trust repository hooks without prompting
    #[arg(long = "trust-hooks")]
    trust_hooks: bool,

    /// Register this directory in the project registry before launching.
    /// The project key (see --key) doubles as the wiki profile, so a fresh
    /// knowledge plane comes up with the project.
    #[arg(long = "register")]
    register: bool,

    /// Project key for --register (default: slugified folder name)
    #[arg(long = "key", requires = "register")]
    key: Option<String>,

    /// Skip the wiki brief refresh
    #[arg(long = "no-brief")]
    no_brief: bool,

    /// Create/start the session but do not attach (for scripts and tests)
    #[arg(long = "no-attach")]
    no_attach: bool,
}

pub async fn run(profile: &str, args: GoArgs) -> Result<()> {
    if profile != "default" {
        // A stray FORAGER_PROFILE silently splits session storage; make the
        // active profile impossible to miss before creating anything.
        println!("Profile: {profile} (from -p/--profile or FORAGER_PROFILE)");
    }
    let path = if args.path.as_os_str() == "." {
        std::env::current_dir()?
    } else {
        args.path.canonicalize()?
    };
    if !path.is_dir() {
        bail!("Path is not a directory: {}", path.display());
    }

    // Explicit argument wins; otherwise honor the configured default tool
    // (settings TUI: session.default_tool) before falling back to claude.
    let tool_name = args.tool.clone().or_else(|| {
        crate::session::profile_config::resolve_config(profile)
            .ok()
            .and_then(|config| config.session.default_tool.clone())
            .map(|tool| tool.trim().to_string())
            .filter(|tool| !tool.is_empty())
    });
    let tool_name = tool_name.unwrap_or_else(|| "claude".to_string());
    let command = if args.tool_args.is_empty() {
        tool_name.clone()
    } else {
        format!("{} {}", tool_name, args.tool_args.join(" "))
    };
    let tool = super::add::detect_tool(&command)?;

    let mut registry = load_registry();
    let mut project = resolve_project_for_path(&path, &registry);

    if args.register {
        if let Some(entry) = &project {
            println!(
                "Already registered as {} ({}); --register ignored.",
                entry.display_name, entry.key
            );
        } else {
            let key = args.key.as_deref().filter(|value| !value.trim().is_empty());
            let entry = crate::session::project_registry::default_entry_for_path(&path, key);
            let registry_file = crate::session::project_registry::registry_path();
            crate::session::project_registry::append_project(&registry_file, &entry)?;
            println!(
                "Registered project: {} (key: {}, wiki plane: {})",
                entry.display_name,
                entry.key,
                entry.wiki_profile.as_deref().unwrap_or("-")
            );
            registry = load_registry();
            project = resolve_project_for_path(&path, &registry);
        }
    }

    match &project {
        Some(entry) => println!("Project: {} ({})", entry.display_name, entry.key),
        None => {
            if registry.is_empty() {
                println!(
                    "Project registry not found; running unregistered.\n\
                     Tip: rerun with --register to create it and onboard this project."
                );
            } else {
                println!(
                    "Path is not in the project registry; running unregistered.\n\
                     Tip: rerun with --register [--key <key>] to onboard it (registry: {})",
                    crate::session::project_registry::registry_path().display()
                );
            }
        }
    }

    if !args.no_brief {
        refresh_wiki_brief(project.as_ref(), &path).await;
    }

    let storage = Storage::new(profile)?;
    let (mut instances, groups) = storage.load_with_groups()?;
    let normalized_path = path.to_string_lossy();
    let normalized_path = normalized_path.trim_end_matches('/');

    let existing_idx = instances.iter().position(|inst| {
        inst.project_path.trim_end_matches('/') == normalized_path && inst.tool == tool
    });

    let idx = if let Some(idx) = existing_idx {
        let running =
            crate::tmux::Session::new(&instances[idx].id, &instances[idx].title)?.exists();
        if !args.tool_args.is_empty() && instances[idx].command != command {
            if running {
                println!(
                    "Session is already running; extra arguments were ignored: {}",
                    args.tool_args.join(" ")
                );
            } else {
                instances[idx].command = command.clone();
                println!("Updated session command: {}", command);
            }
        }
        println!(
            "Reusing session: {} ({})",
            instances[idx].title, instances[idx].id
        );
        idx
    } else {
        run_hook_check(&path, args.trust_hooks)?;
        let title = unique_session_title(&instances, &tool, normalized_path);
        let mut instance = Instance::new(&title, normalized_path);
        instance.command = command.clone();
        instance.tool = tool.clone();
        instance.yolo_mode = args.yolo;
        if let Some(group) = project
            .as_ref()
            .and_then(|entry| entry.session_group.clone())
        {
            instance.group_path = group;
        }
        println!("Created session: {} ({})", instance.title, instance.id);
        instances.push(instance);
        instances.len() - 1
    };

    let running = crate::tmux::Session::new(&instances[idx].id, &instances[idx].title)?.exists();
    if !running {
        instances[idx].start_with_size(crate::terminal::get_size())?;
        println!("Started: {}", instances[idx].command);
    }

    let mut group_tree = GroupTree::new_with_groups(&instances, &groups);
    if !instances[idx].group_path.is_empty() {
        group_tree.create_group(&instances[idx].group_path);
    }
    storage.save_with_groups(&instances, &group_tree)?;

    if args.no_attach {
        println!("Session ready (not attaching): {}", instances[idx].id);
        return Ok(());
    }
    let tmux_session = crate::tmux::Session::new(&instances[idx].id, &instances[idx].title)?;
    tmux_session.attach()?;
    Ok(())
}

/// Refresh `.wiki-brief.md` in the project directory from its knowledge
/// plane. Best-effort: a missing profile or store must never block launch.
async fn refresh_wiki_brief(
    project: Option<&crate::session::project_registry::ProjectRegistryEntry>,
    path: &std::path::Path,
) {
    let Some(entry) = project else {
        return;
    };
    let Some(wiki_profile) = entry.wiki_profile.clone() else {
        return;
    };
    let out = path.join(".wiki-brief.md");
    let brief_args = super::offdesk::WikiBriefArgs::scoped_to_file(entry.key.clone(), out.clone());
    match super::offdesk::wiki_brief(&wiki_profile, brief_args).await {
        Ok(()) => println!(
            "Wiki brief refreshed: {} (plane: {})",
            out.display(),
            wiki_profile
        ),
        Err(error) => println!("Wiki brief skipped: {error}"),
    }
}

fn run_hook_check(path: &std::path::Path, trust_hooks: bool) -> Result<()> {
    match repo_config::check_hook_trust(path) {
        Ok(repo_config::HookTrustStatus::NeedsTrust { hooks, hooks_hash }) => {
            let should_trust = if trust_hooks {
                true
            } else {
                println!("\nRepository hooks detected in repo config.");
                print!("Trust and run these hooks? [y/N] ");
                use std::io::Write;
                std::io::stdout().flush()?;
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                input.trim().eq_ignore_ascii_case("y")
            };
            if should_trust {
                super::add::trust_and_run_on_create(path, &hooks_hash, &hooks)?;
            } else {
                println!("Hooks skipped (session created without running hooks)");
            }
        }
        Ok(repo_config::HookTrustStatus::Trusted(hooks)) => {
            if !hooks.on_create.is_empty() {
                println!("Running on_create hooks...");
                repo_config::execute_hooks(&hooks.on_create, path)?;
            }
        }
        Ok(repo_config::HookTrustStatus::NoHooks) => {}
        Err(error) => {
            tracing::warn!("Failed to check repo hooks: {}", error);
        }
    }
    Ok(())
}

/// Title for a fresh go-session: capitalized tool name, suffixed only when a
/// same-titled session already exists at this path (different tool there).
fn unique_session_title(instances: &[Instance], tool: &str, path: &str) -> String {
    let mut chars = tool.chars();
    let base = match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => "Agent".to_string(),
    };
    if !super::add::is_duplicate_session(instances, &base, path) {
        return base;
    }
    for counter in 2..100 {
        let candidate = format!("{base}-{counter}");
        if !super::add::is_duplicate_session(instances, &candidate, path) {
            return candidate;
        }
    }
    format!("{base}-{}", uuid::Uuid::new_v4())
}
