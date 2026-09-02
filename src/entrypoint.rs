use crate::cli::{self, Cli, Commands, LEGACY_BINARY_NAME, PRIMARY_BINARY_NAME};
use crate::migrations;
use crate::session::{load_config_read_only, normalize_profile_name};
use crate::tui;
use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;

pub async fn run_cli() -> Result<()> {
    if debug_logging_enabled() {
        tracing_subscriber::fmt()
            .with_env_filter("forager=debug")
            .init();
    }

    let cli = Cli::parse();
    let explicit_profile = cli.profile.clone();
    let profile_independent_read_only = matches!(
        cli.command.as_ref(),
        Some(Commands::Offdesk { command })
            if matches!(
                command.as_ref(),
                cli::offdesk::OffdeskCommands::RemoteSession { .. }
            )
    );
    let profile = explicit_profile
        .clone()
        .or_else(|| std::env::var("AGENT_OF_EMPIRES_PROFILE").ok())
        .or_else(|| {
            if profile_independent_read_only {
                None
            } else {
                configured_default_profile()
            }
        })
        .unwrap_or_default();
    let profile = normalize_profile_name(&profile)?;
    maybe_warn_legacy_alias(&cli);

    // Handle commands that don't need app data or migrations.
    // These work in read-only/sandboxed environments (e.g. Nix builds).
    match cli.command {
        Some(Commands::Completion { shell }) => {
            let command_name = completion_binary_name();
            let mut command = Cli::command().name(command_name);
            generate(shell, &mut command, command_name, &mut std::io::stdout());
            return Ok(());
        }
        Some(Commands::Init(args)) => return cli::init::run(args).await,
        Some(Commands::Doctor(args)) => {
            return cli::doctor::run(explicit_profile.as_deref(), args).await;
        }
        Some(Commands::Migrate { command }) => return cli::migrate::run(command).await,
        Some(Commands::Tmux { command }) => {
            use cli::tmux::TmuxCommands;
            return match command {
                TmuxCommands::Status(args) => cli::tmux::run_status(args),
            };
        }
        Some(Commands::Offdesk { command })
            if matches!(
                command.as_ref(),
                cli::offdesk::OffdeskCommands::DebugBundle(_)
                    | cli::offdesk::OffdeskCommands::RemoteSession { .. }
            ) =>
        {
            return cli::offdesk::run(&profile, *command).await;
        }
        Some(Commands::Sounds { command }) => return cli::sounds::run(command).await,
        Some(Commands::Uninstall(args)) => return cli::uninstall::run(args).await,
        _ => {}
    }

    // TUI mode handles migrations with a spinner; CLI runs them silently.
    if cli.command.is_some() {
        migrations::run_migrations()?;
    }

    match cli.command {
        Some(Commands::Add(args)) => cli::add::run(&profile, args).await,
        Some(Commands::Go(args)) => cli::go::run(&profile, args).await,
        Some(Commands::List(args)) => cli::list::run(&profile, args).await,
        Some(Commands::Remove(args)) => cli::remove::run(&profile, args).await,
        Some(Commands::Status(args)) => cli::status::run(&profile, args).await,
        Some(Commands::Session { command }) => cli::session::run(&profile, command).await,
        Some(Commands::Group { command }) => cli::group::run(&profile, command).await,
        Some(Commands::Profile { command }) => cli::profile::run(command).await,
        Some(Commands::Project { command }) => cli::project::run(&profile, *command).await,
        Some(Commands::Worktree { command }) => cli::worktree::run(&profile, command).await,
        Some(Commands::Offdesk { command }) => cli::offdesk::run(&profile, *command).await,
        Some(Commands::Ondesk { command }) => cli::ondesk::run(&profile, command).await,
        None => tui::run(&profile).await,
        _ => unreachable!(),
    }
}

fn completion_binary_name() -> &'static str {
    if cli::invoked_binary_name() == LEGACY_BINARY_NAME {
        LEGACY_BINARY_NAME
    } else {
        PRIMARY_BINARY_NAME
    }
}

fn debug_logging_enabled() -> bool {
    std::env::var("FORAGER_DEBUG").is_ok() || std::env::var("AGENT_OF_EMPIRES_DEBUG").is_ok()
}

fn configured_default_profile() -> Option<String> {
    load_config_read_only()
        .ok()
        .flatten()
        .map(|config| config.default_profile)
        .filter(|profile| !profile.trim().is_empty())
}

fn maybe_warn_legacy_alias(cli: &Cli) {
    if cli::invoked_binary_name() != LEGACY_BINARY_NAME || !legacy_alias_warning_enabled(cli) {
        return;
    }

    eprintln!("warning: `aoe` is a legacy alias; use `forager` instead.");
}

fn legacy_alias_warning_enabled(cli: &Cli) -> bool {
    let Some(command) = &cli.command else {
        return false;
    };

    if matches!(command, Commands::Completion { .. } | Commands::Tmux { .. }) {
        return false;
    }

    let args: Vec<String> = std::env::args().skip(1).collect();
    !args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--json" | "--quiet" | "-q"))
}
