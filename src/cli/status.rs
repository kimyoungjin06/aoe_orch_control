//! `forager status` command implementation

use anyhow::Result;
use clap::Args;
use serde::Serialize;
use serde_json::Value;

use crate::offdesk::{
    ensure_resume_review_next_safe_action, load_offdesk_status_summary,
    OffdeskCloseoutStateSummary, OffdeskNextSafeAction, OffdeskStatusSummary, ResumeStatus,
    TaskResumeStore,
};
use crate::offdesk::{load_orchestration_signals, OrchestrationSignals};
use crate::session::{
    app_dir_resolution, get_profile_dir, project_registry, Instance, Status, Storage,
};

#[derive(Args)]
pub struct StatusArgs {
    /// Show detailed session list
    #[arg(short = 'v', long)]
    verbose: bool,

    /// Only output waiting count (for scripts)
    #[arg(short = 'q', long)]
    quiet: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Default)]
struct StatusCounts {
    running: usize,
    waiting: usize,
    idle: usize,
    stopped: usize,
    error: usize,
    total: usize,
}

#[derive(Serialize)]
struct SessionStatusJson {
    id: String,
    title: String,
    tool: String,
    path: String,
    status: String,
    /// Registry project key resolved from the session path, if any
    project: Option<String>,
}

#[derive(Serialize)]
struct ForeignSessionJson {
    tmux_session: String,
    session_id_suffix: String,
    owner_profile: Option<String>,
}

/// Live `forager_*` tmux sessions that this profile's storage does not know.
/// Divergence happens when sessions get created under another profile (stray
/// FORAGER_PROFILE, TUI profile switch); tmux is the runtime truth, so status
/// must surface the gap instead of letting sessions go invisible.
fn detect_foreign_sessions(profile: &str, instances: &[Instance]) -> Vec<ForeignSessionJson> {
    let Ok(output) = std::process::Command::new("tmux")
        .args(["list-sessions", "-F", "#S"])
        .output()
    else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let known: std::collections::HashSet<&str> = instances
        .iter()
        .filter_map(|inst| inst.id.get(..8))
        .collect();
    let mut foreign = Vec::new();
    for name in String::from_utf8_lossy(&output.stdout).lines() {
        let Some(rest) = name.strip_prefix("forager_") else {
            continue;
        };
        let Some(suffix) = rest.rsplit('_').next() else {
            continue;
        };
        if suffix.len() != 8 || !suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        if known.contains(suffix) {
            continue;
        }
        foreign.push(ForeignSessionJson {
            tmux_session: name.to_string(),
            session_id_suffix: suffix.to_string(),
            owner_profile: find_owner_profile(suffix, profile),
        });
    }
    foreign
}

fn find_owner_profile(suffix: &str, current_profile: &str) -> Option<String> {
    let current_dir = get_profile_dir(current_profile).ok()?;
    let profiles_root = current_dir.parent()?.to_path_buf();
    let entries = std::fs::read_dir(profiles_root).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name == current_profile {
            continue;
        }
        let sessions_file = entry.path().join("sessions.json");
        let Ok(raw) = std::fs::read_to_string(&sessions_file) else {
            continue;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        if let Some(rows) = parsed.as_array() {
            for row in rows {
                if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                    if id.starts_with(suffix) {
                        return Some(name);
                    }
                }
            }
        }
    }
    None
}

fn status_label(status: Status) -> &'static str {
    match status {
        Status::Running => "running",
        Status::Waiting => "waiting",
        Status::Idle => "idle",
        Status::Stopped => "stopped",
        Status::Error => "error",
        Status::Starting => "starting",
        Status::Deleting => "deleting",
    }
}

fn session_status_rows(
    instances: &[crate::session::Instance],
    registry: &[project_registry::ProjectRegistryEntry],
) -> Vec<SessionStatusJson> {
    instances
        .iter()
        .map(|inst| SessionStatusJson {
            id: inst.id.clone(),
            title: inst.title.clone(),
            tool: inst.tool.clone(),
            path: inst.project_path.clone(),
            status: status_label(inst.status).to_string(),
            project: project_registry::resolve_project_for_path(
                std::path::Path::new(&inst.project_path),
                registry,
            )
            .map(|entry| entry.key),
        })
        .collect()
}

#[derive(Serialize)]
struct StatusJson {
    profile: String,
    profile_dir: String,
    profile_dir_source: String,
    app_dir: String,
    app_dir_source: String,
    primary_app_dir: String,
    primary_app_dir_exists: bool,
    waiting: usize,
    running: usize,
    idle: usize,
    stopped: usize,
    error: usize,
    total: usize,
    resume_pending_fresh: usize,
    resume_pending_stale: usize,
    pending_approvals: usize,
    queued_offdesk_tasks: usize,
    active_offdesk_tasks: usize,
    offdesk_tasks_pending_approval: usize,
    failed_offdesk_tasks: usize,
    resume_pending_offdesk_tasks: usize,
    cancelled_offdesk_tasks: usize,
    stale_background_runs: usize,
    failed_background_runs: usize,
    closeout_required_offdesk_tasks: usize,
    closeout_state: OffdeskCloseoutStateSummary,
    offdesk_next_safe_actions: Vec<OffdeskNextSafeAction>,
    autonomy_armed: bool,
    registered_projects: usize,
    wiki_candidates: usize,
    sessions: Vec<SessionStatusJson>,
    foreign_sessions: Vec<ForeignSessionJson>,
}

#[derive(Default)]
struct ResumeCounts {
    fresh_pending: usize,
    stale_pending: usize,
}

fn build_status_json(
    profile: &str,
    counts: StatusCounts,
    resume_counts: ResumeCounts,
    offdesk_summary: OffdeskStatusSummary,
    sessions: Vec<SessionStatusJson>,
    foreign_sessions: Vec<ForeignSessionJson>,
) -> StatusJson {
    let offdesk_next_safe_actions =
        offdesk_next_safe_actions_for_status(&resume_counts, &offdesk_summary);
    let storage = status_storage_paths(profile);
    let signals = load_orchestration_signals(profile);
    StatusJson {
        profile: profile.to_string(),
        profile_dir: storage.profile_dir,
        profile_dir_source: storage.profile_dir_source,
        app_dir: storage.app_dir,
        app_dir_source: storage.app_dir_source,
        primary_app_dir: storage.primary_app_dir,
        primary_app_dir_exists: storage.primary_app_dir_exists,
        waiting: counts.waiting,
        running: counts.running,
        idle: counts.idle,
        stopped: counts.stopped,
        error: counts.error,
        total: counts.total,
        resume_pending_fresh: resume_counts.fresh_pending,
        resume_pending_stale: resume_counts.stale_pending,
        pending_approvals: offdesk_summary.pending_approvals,
        queued_offdesk_tasks: offdesk_summary.tasks.queued,
        active_offdesk_tasks: offdesk_summary.tasks.active,
        offdesk_tasks_pending_approval: offdesk_summary.tasks.pending_approval,
        failed_offdesk_tasks: offdesk_summary.tasks.failed,
        resume_pending_offdesk_tasks: offdesk_summary.tasks.resume_pending,
        cancelled_offdesk_tasks: offdesk_summary.tasks.cancelled,
        stale_background_runs: offdesk_summary.background_stale,
        failed_background_runs: offdesk_summary.background_failed,
        closeout_required_offdesk_tasks: offdesk_summary.closeout_required,
        closeout_state: offdesk_summary.closeout_state,
        offdesk_next_safe_actions,
        autonomy_armed: signals.autonomy_armed,
        registered_projects: signals.registered_projects,
        wiki_candidates: signals.wiki_candidates,
        sessions,
        foreign_sessions,
    }
}

struct StatusStoragePaths {
    profile_dir: String,
    profile_dir_source: String,
    app_dir: String,
    app_dir_source: String,
    primary_app_dir: String,
    primary_app_dir_exists: bool,
}

fn status_storage_paths(profile: &str) -> StatusStoragePaths {
    match app_dir_resolution() {
        Ok(resolution) => StatusStoragePaths {
            profile_dir: resolution
                .active_path
                .join("profiles")
                .join(profile)
                .display()
                .to_string(),
            profile_dir_source: resolution.active_source.to_string(),
            app_dir: resolution.active_path.display().to_string(),
            app_dir_source: resolution.active_source.to_string(),
            primary_app_dir: resolution.primary_path.display().to_string(),
            primary_app_dir_exists: resolution.primary_exists,
        },
        Err(error) => StatusStoragePaths {
            profile_dir: format!("unavailable: {error}"),
            profile_dir_source: "error".to_string(),
            app_dir: format!("unavailable: {error}"),
            app_dir_source: "error".to_string(),
            primary_app_dir: String::new(),
            primary_app_dir_exists: false,
        },
    }
}

fn offdesk_next_safe_actions_for_status(
    resume_counts: &ResumeCounts,
    offdesk_summary: &OffdeskStatusSummary,
) -> Vec<OffdeskNextSafeAction> {
    let mut actions = offdesk_summary.next_safe_actions.clone();
    if resume_counts.fresh_pending > 0 || resume_counts.stale_pending > 0 {
        ensure_resume_review_next_safe_action(&mut actions);
    }
    actions
}

#[cfg(test)]
pub(crate) fn status_json_value_for_test(profile: &str) -> serde_json::Value {
    let resume_counts = count_resume_state(profile);
    let offdesk_summary = count_offdesk_state(profile);
    serde_json::to_value(build_status_json(
        profile,
        StatusCounts::default(),
        resume_counts,
        offdesk_summary,
        Vec::new(),
        Vec::new(),
    ))
    .expect("status json should serialize")
}

pub(crate) fn current_status_json_value(profile: &str) -> Result<Value> {
    let storage = Storage::new(profile)?;
    let (mut instances, _) = storage.load_with_groups()?;
    if !instances.is_empty() {
        crate::tmux::refresh_session_cache();
        for inst in &mut instances {
            inst.update_status();
        }
    }
    let counts = count_by_status(&instances);
    let resume_counts = count_resume_state(storage.profile());
    let offdesk_summary = count_offdesk_state(storage.profile());
    let registry = project_registry::load_registry();
    let sessions = session_status_rows(&instances, &registry);
    let foreign_sessions = detect_foreign_sessions(storage.profile(), &instances);
    Ok(serde_json::to_value(build_status_json(
        storage.profile(),
        counts,
        resume_counts,
        offdesk_summary,
        sessions,
        foreign_sessions,
    ))?)
}

pub async fn run(profile: &str, args: StatusArgs) -> Result<()> {
    let storage = Storage::new(profile)?;
    let (mut instances, _) = storage.load_with_groups()?;

    if instances.is_empty() {
        if args.json {
            let resume_counts = count_resume_state(storage.profile());
            let offdesk_summary = count_offdesk_state(storage.profile());
            let status_json = build_status_json(
                storage.profile(),
                StatusCounts::default(),
                resume_counts,
                offdesk_summary,
                Vec::new(),
                detect_foreign_sessions(storage.profile(), &[]),
            );
            println!("{}", serde_json::to_string(&status_json)?);
        } else if args.quiet {
            println!("0");
        } else {
            println!("No sessions in profile '{}'.", storage.profile());
            print_profile_storage_hint(storage.profile());
            let resume_counts = count_resume_state(storage.profile());
            let offdesk_summary = count_offdesk_state(storage.profile());
            if resume_counts.fresh_pending > 0 || resume_counts.stale_pending > 0 {
                println!(
                    "{} fresh resume • {} stale resume",
                    resume_counts.fresh_pending, resume_counts.stale_pending
                );
            }
            let offdesk_next_safe_actions =
                offdesk_next_safe_actions_for_status(&resume_counts, &offdesk_summary);
            print_offdesk_summary(&offdesk_summary, &offdesk_next_safe_actions);
        }
        return Ok(());
    }

    // Refresh tmux session cache
    crate::tmux::refresh_session_cache();

    // Update status for all instances
    for inst in &mut instances {
        inst.update_status();
    }

    let counts = count_by_status(&instances);
    let resume_counts = count_resume_state(storage.profile());
    let offdesk_summary = count_offdesk_state(storage.profile());

    if args.json {
        let registry = project_registry::load_registry();
        let sessions = session_status_rows(&instances, &registry);
        let foreign_sessions = detect_foreign_sessions(storage.profile(), &instances);
        let status_json = build_status_json(
            storage.profile(),
            counts,
            resume_counts,
            offdesk_summary,
            sessions,
            foreign_sessions,
        );
        println!("{}", serde_json::to_string(&status_json)?);
    } else if args.quiet {
        println!("{}", counts.waiting);
    } else if args.verbose {
        print_status_group("WAITING", "◐", Status::Waiting, &instances);
        print_status_group("RUNNING", "●", Status::Running, &instances);
        print_status_group("IDLE", "○", Status::Idle, &instances);
        print_status_group("STOPPED", "□", Status::Stopped, &instances);
        print_status_group("ERROR", "✕", Status::Error, &instances);
        println!(
            "Total: {} sessions in profile '{}'",
            counts.total,
            storage.profile()
        );
        print_profile_storage_hint(storage.profile());
    } else {
        print_session_summary(&counts);
        print_project_rollup(&instances);
        for foreign in detect_foreign_sessions(storage.profile(), &instances) {
            match &foreign.owner_profile {
                Some(owner) => println!(
                    "WARNING: live session {} belongs to profile '{}' (view: forager -p {} status)",
                    foreign.tmux_session, owner, owner
                ),
                None => println!(
                    "WARNING: live session {} is not in any profile's storage",
                    foreign.tmux_session
                ),
            }
        }
        print_harness_signals(storage.profile());
        print_profile_storage_hint(storage.profile());
        if counts.error > 0 {
            println!(
                "Session recovery: run `forager status --verbose` to inspect errored sessions."
            );
        }
        if resume_counts.fresh_pending > 0 || resume_counts.stale_pending > 0 {
            println!(
                "{} fresh resume • {} stale resume",
                resume_counts.fresh_pending, resume_counts.stale_pending
            );
        }
        let offdesk_next_safe_actions =
            offdesk_next_safe_actions_for_status(&resume_counts, &offdesk_summary);
        print_offdesk_summary(&offdesk_summary, &offdesk_next_safe_actions);
    }

    // Show update notice if available (skip for JSON/quiet output)
    if !args.json && !args.quiet {
        crate::update::print_update_notice(profile).await;
    }

    Ok(())
}

fn print_profile_storage_hint(profile: &str) {
    let storage = status_storage_paths(profile);
    if storage.profile_dir_source == "legacy" {
        println!(
            "Profile data: {} ({})",
            storage.profile_dir, storage.profile_dir_source
        );
        println!("Storage hint: legacy AoE data is active; run `forager doctor` before migration.");
    }
}

/// One line per registered project that has sessions, with status icons,
/// plus an unregistered bucket. Silent when no registry is configured.
fn print_project_rollup(instances: &[crate::session::Instance]) {
    let registry = project_registry::load_registry();
    if registry.is_empty() || instances.is_empty() {
        return;
    }
    let mut rollup: std::collections::BTreeMap<String, StatusCounts> =
        std::collections::BTreeMap::new();
    let mut unregistered = 0usize;
    for inst in instances {
        let Some(entry) = project_registry::resolve_project_for_path(
            std::path::Path::new(&inst.project_path),
            &registry,
        ) else {
            unregistered += 1;
            continue;
        };
        let counts = rollup.entry(entry.key).or_default();
        match inst.status {
            Status::Running => counts.running += 1,
            Status::Waiting => counts.waiting += 1,
            Status::Error => counts.error += 1,
            _ => counts.idle += 1,
        }
    }
    if rollup.is_empty() {
        return;
    }
    let parts: Vec<String> = rollup
        .iter()
        .map(|(key, counts)| {
            let mut icons = String::new();
            if counts.waiting > 0 {
                icons.push_str(&format!(" ◐{}", counts.waiting));
            }
            if counts.running > 0 {
                icons.push_str(&format!(" ●{}", counts.running));
            }
            if counts.error > 0 {
                icons.push_str(&format!(" ✕{}", counts.error));
            }
            if counts.idle > 0 {
                icons.push_str(&format!(" ○{}", counts.idle));
            }
            format!("{key}{icons}")
        })
        .collect();
    let mut line = parts.join("  ·  ");
    if unregistered > 0 {
        line.push_str(&format!("  ·  (unregistered {unregistered})"));
    }
    println!("{line}");
}

/// Harness-wide signals: autonomy window state and the wiki candidate queue
/// across registered planes. Silent without a project registry.
fn print_harness_signals(profile: &str) {
    let signals: OrchestrationSignals = load_orchestration_signals(profile);
    if signals.registered_projects == 0 {
        return;
    }
    println!(
        "harness: autonomy {} · wiki candidates {} · projects {}",
        if signals.autonomy_armed {
            "ARMED"
        } else {
            "off"
        },
        signals.wiki_candidates,
        signals.registered_projects
    );
}

fn print_session_summary(counts: &StatusCounts) {
    let mut parts = vec![
        format!("{} waiting", counts.waiting),
        format!("{} running", counts.running),
        format!("{} idle", counts.idle),
    ];
    if counts.stopped > 0 {
        parts.push(format!("{} stopped", counts.stopped));
    }
    if counts.error > 0 {
        parts.push(format!("{} error", counts.error));
    }
    println!("{}", parts.join(" • "));
}

fn count_offdesk_state(profile: &str) -> crate::offdesk::OffdeskStatusSummary {
    let Ok(profile_dir) = get_profile_dir(profile) else {
        return crate::offdesk::OffdeskStatusSummary::default();
    };
    load_offdesk_status_summary(profile_dir, chrono::Utc::now()).unwrap_or_default()
}

fn print_offdesk_summary(
    summary: &crate::offdesk::OffdeskStatusSummary,
    next_safe_actions: &[OffdeskNextSafeAction],
) {
    let has_offdesk_activity = summary.pending_approvals != 0
        || summary.tasks.queued != 0
        || summary.tasks.active != 0
        || summary.tasks.pending_approval != 0
        || summary.tasks.failed != 0
        || summary.tasks.resume_pending != 0
        || summary.background_stale != 0
        || summary.background_failed != 0
        || summary.closeout_required != 0;
    if !has_offdesk_activity && next_safe_actions.is_empty() {
        return;
    }
    if has_offdesk_activity {
        println!(
            "{} approvals • {} queued offdesk • {} active offdesk • {} resume-pending offdesk • {} failed offdesk • {} stale background • {} failed background • {} closeout required",
            summary.pending_approvals,
            summary.tasks.queued,
            summary.tasks.active + summary.tasks.pending_approval,
            summary.tasks.resume_pending,
            summary.tasks.failed,
            summary.background_stale,
            summary.background_failed,
            summary.closeout_required
        );
    }
    if summary.tasks.failed > 0 || summary.tasks.resume_pending > 0 {
        println!("Recovery: run `forager offdesk tasks` for retry, resume, and abandon commands.");
    }
    if summary.closeout_required > 0 {
        println!(
            "Closeout state: {} missing, {} pending review, {} revise/blocked, {} approved with follow-ups, {} stale package, {} stale review, {} accepted, {} approved, {} retired incomplete.",
            summary.closeout_state.missing_closeout,
            summary.closeout_state.pending_review,
            summary.closeout_state.revision_required,
            summary.closeout_state.approved_with_followups,
            summary.closeout_state.stale_closeout,
            summary.closeout_state.stale_review,
            summary.closeout_state.accepted,
            summary.closeout_state.approved,
            summary.closeout_state.retired_incomplete
        );
        if summary.closeout_state.missing_closeout
            + summary.closeout_state.stale_closeout
            + summary.closeout_state.stale_review
            > 0
        {
            println!("Closeout: run `forager offdesk closeout` for tasks without a fresh closeout package.");
        }
        if summary.closeout_state.pending_review > 0 {
            println!(
                "Closeout review: read `COMMERCIAL_REVIEW_PACKET.md`, then record `forager offdesk closeout-review --verdict approved|revise|blocked`."
            );
        }
        if summary.closeout_state.revision_required > 0 {
            println!(
                "Closeout revision: address revise/blocked review notes before accepting output."
            );
        }
        if summary.closeout_state.approved_with_followups > 0 {
            println!(
                "Closeout receipt: approved review still has follow-ups; read `RETURN_PACKAGE.md` or `forager ondesk prompt-package` before treating output as accepted."
            );
        }
    }
    print_offdesk_next_safe_actions(next_safe_actions);
}

fn print_offdesk_next_safe_actions(actions: &[OffdeskNextSafeAction]) {
    if actions.is_empty() {
        return;
    }
    println!("Next safe actions:");
    for action in actions {
        println!("  next:    {}", action.detail);
        if !action.commands.is_empty() {
            println!("  command: {}", action.commands.join(" | "));
        }
        if action.requires_operator_review {
            println!("  review:  operator review required");
        }
    }
}

fn count_resume_state(profile: &str) -> ResumeCounts {
    let Ok(profile_dir) = get_profile_dir(profile) else {
        return ResumeCounts::default();
    };
    let Ok(states) = TaskResumeStore::new(profile_dir).load() else {
        return ResumeCounts::default();
    };
    let now = chrono::Utc::now();
    states
        .iter()
        .filter(|state| state.status == ResumeStatus::ResumePending)
        .fold(ResumeCounts::default(), |mut counts, state| {
            if state.is_fresh_at(now) {
                counts.fresh_pending += 1;
            } else {
                counts.stale_pending += 1;
            }
            counts
        })
}

fn count_by_status(instances: &[crate::session::Instance]) -> StatusCounts {
    let mut counts = StatusCounts::default();
    for inst in instances {
        match inst.status {
            Status::Running => counts.running += 1,
            Status::Waiting => counts.waiting += 1,
            Status::Idle => counts.idle += 1,
            Status::Stopped => counts.stopped += 1,
            Status::Error => counts.error += 1,
            Status::Starting => counts.idle += 1,
            Status::Deleting => {}
        }
        counts.total += 1;
    }
    counts
}

fn print_status_group(
    label: &str,
    symbol: &str,
    status: Status,
    instances: &[crate::session::Instance],
) {
    let matching: Vec<_> = instances.iter().filter(|i| i.status == status).collect();
    if matching.is_empty() {
        return;
    }

    println!("{} ({}):", label, matching.len());
    for inst in matching {
        let path = shorten_path(&inst.project_path);
        println!("  {} {:<16} {:<10} {}", symbol, inst.title, inst.tool, path);
    }
    println!();
}

fn shorten_path(path: &str) -> String {
    if let Some(home) = dirs::home_dir() {
        if let Some(home_str) = home.to_str() {
            if let Some(stripped) = path.strip_prefix(home_str) {
                return format!("~{}", stripped);
            }
        }
    }
    path.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Instance, Status};

    fn instance_with_status(status: Status) -> Instance {
        let mut inst = Instance::new("test", "/tmp/test");
        inst.status = status;
        inst
    }

    #[test]
    fn count_by_status_separates_stopped_from_error() {
        let instances = vec![
            instance_with_status(Status::Stopped),
            instance_with_status(Status::Error),
            instance_with_status(Status::Idle),
        ];

        let counts = count_by_status(&instances);

        assert_eq!(counts.stopped, 1);
        assert_eq!(counts.error, 1);
        assert_eq!(counts.idle, 1);
        assert_eq!(counts.total, 3);
    }
}
