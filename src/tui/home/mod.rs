//! Home view - main session list and navigation

mod input;
mod operations;
pub(super) mod projects;
mod render;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use tui_input::Input;

use crate::offdesk::{OffdeskCloseoutStateSummary, OffdeskNextSafeAction};
use crate::session::{
    config::{load_config, save_config},
    flatten_tree, get_profile_dir, resolve_config, Group, GroupTree, Instance, Item, Storage,
};
use crate::tmux::AvailableTools;

use super::creation_poller::{CreationPoller, CreationRequest};
use super::deletion_poller::DeletionPoller;
use super::dialogs::{
    ChangelogDialog, ConfirmDialog, GroupDeleteOptionsDialog, HookTrustDialog, InfoDialog,
    NewSessionData, NewSessionDialog, RenameDialog, UnifiedDeleteDialog, WelcomeDialog,
};
use super::diff::DiffView;
use super::preview_poller::{PreviewKind, PreviewPoller, PreviewRequest};
use super::settings::SettingsView;
use super::status_poller::StatusPoller;

/// View mode for the home screen
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ViewMode {
    #[default]
    Agent,
    Terminal,
}

/// Cached preview content to avoid subprocess calls on every frame
pub(super) struct PreviewCache {
    pub(super) session_id: Option<String>,
    pub(super) content: String,
    pub(super) last_refresh: Instant,
    pub(super) dimensions: (u16, u16),
    pub(super) terminal_running: bool,
}

impl Default for PreviewCache {
    fn default() -> Self {
        Self {
            session_id: None,
            content: String::new(),
            last_refresh: Instant::now(),
            dimensions: (0, 0),
            terminal_running: false,
        }
    }
}

impl PreviewCache {
    fn needs_refresh(&self, session_id: &str, dimensions: (u16, u16)) -> bool {
        const PREVIEW_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(750);

        self.session_id.as_deref() != Some(session_id)
            || self.dimensions != dimensions
            || self.last_refresh.elapsed() >= PREVIEW_REFRESH_INTERVAL
    }
}

pub(super) const INDENTS: [&str; 10] = [
    "",
    "  ",
    "    ",
    "      ",
    "        ",
    "          ",
    "            ",
    "              ",
    "                ",
    "                  ",
];

pub(super) fn get_indent(depth: usize) -> &'static str {
    INDENTS.get(depth).copied().unwrap_or(INDENTS[9])
}

pub(super) const ICON_RUNNING: &str = "●";
pub(super) const ICON_WAITING: &str = "◐";
pub(super) const ICON_IDLE: &str = "○";
pub(super) const ICON_ERROR: &str = "✕";
pub(super) const ICON_STARTING: &str = "◌";
pub(super) const ICON_DELETING: &str = "✗";
pub(super) const ICON_COLLAPSED: &str = "▶";
pub(super) const ICON_EXPANDED: &str = "▼";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct OffdeskResumeSummary {
    pub(super) fresh_pending: usize,
    pub(super) stale_pending: usize,
    pub(super) pending_approvals: usize,
    pub(super) queued_tasks: usize,
    pub(super) active_tasks: usize,
    pub(super) failed_tasks: usize,
    pub(super) resume_pending_tasks: usize,
    pub(super) cancelled_tasks: usize,
    pub(super) stale_background: usize,
    pub(super) failed_background: usize,
    pub(super) closeout_required: usize,
    pub(super) closeout_state: OffdeskCloseoutStateSummary,
    pub(super) next_safe_actions: Vec<OffdeskNextSafeAction>,
}

impl OffdeskResumeSummary {
    pub(super) fn has_offdesk_activity(&self) -> bool {
        self.pending_approvals > 0
            || self.queued_tasks > 0
            || self.active_tasks > 0
            || self.failed_tasks > 0
            || self.resume_pending_tasks > 0
            || self.cancelled_tasks > 0
            || self.stale_background > 0
            || self.failed_background > 0
            || self.closeout_required > 0
    }

    pub(super) fn has_morning_review(&self) -> bool {
        self.has_offdesk_activity() || self.fresh_pending > 0 || self.stale_pending > 0
    }

    pub(super) fn needs_operator_attention(&self) -> bool {
        self.pending_approvals > 0
            || self.failed_tasks > 0
            || self.resume_pending_tasks > 0
            || self.fresh_pending > 0
            || self.stale_pending > 0
            || self.stale_background > 0
            || self.failed_background > 0
            || self.closeout_required > 0
    }

    pub(super) fn focus_label(&self) -> &'static str {
        if self.pending_approvals > 0 {
            "approvals waiting"
        } else if self.failed_tasks > 0 || self.failed_background > 0 {
            "failed work needs review"
        } else if self.resume_pending_tasks > 0 || self.fresh_pending > 0 || self.stale_pending > 0
        {
            "resume decision pending"
        } else if self.stale_background > 0 {
            "stale background run"
        } else if self.closeout_required > 0 {
            self.closeout_focus_label()
        } else if self.active_tasks > 0 {
            "active offdesk work"
        } else if self.queued_tasks > 0 {
            "queued offdesk work"
        } else if self.cancelled_tasks > 0 {
            "cancelled work archived"
        } else {
            "no offdesk attention"
        }
    }

    pub(super) fn next_action_label(&self) -> String {
        if let Some(action) = self.next_safe_actions.first() {
            return tui_next_action_label(action);
        }
        self.fallback_next_action_label().to_string()
    }

    pub(super) fn next_action_command(&self) -> Option<&str> {
        self.next_safe_actions
            .first()
            .and_then(|action| action.commands.first())
            .map(String::as_str)
    }

    fn fallback_next_action_label(&self) -> &'static str {
        if self.pending_approvals > 0 {
            "Review: forager offdesk pending"
        } else if self.failed_tasks > 0 || self.resume_pending_tasks > 0 {
            "Recover: forager offdesk tasks"
        } else if self.fresh_pending > 0 || self.stale_pending > 0 {
            "Recover: forager offdesk resume"
        } else if self.stale_background > 0 || self.failed_background > 0 {
            "Inspect: forager offdesk poll"
        } else if self.closeout_required > 0 {
            "Closeout: forager offdesk closeout"
        } else if self.active_tasks > 0 || self.queued_tasks > 0 {
            "Monitor: forager offdesk poll"
        } else if self.cancelled_tasks > 0 {
            "Review: forager offdesk tasks"
        } else {
            "Plan: forager offdesk maintenance-report"
        }
    }

    pub(super) fn closeout_label(&self) -> String {
        if self.closeout_required == 0 {
            return "0 tasks require closeout".to_string();
        }

        let missing_or_stale = self.closeout_state.missing_closeout
            + self.closeout_state.stale_closeout
            + self.closeout_state.stale_review;
        let categories = [
            missing_or_stale,
            self.closeout_state.pending_review,
            self.closeout_state.revision_required,
            self.closeout_state.approved_with_followups,
        ]
        .into_iter()
        .filter(|count| *count > 0)
        .count();

        if categories == 1 {
            if self.closeout_state.approved_with_followups > 0 {
                return format!(
                    "{} follow-up receipt pending",
                    self.closeout_state.approved_with_followups
                );
            }
            if self.closeout_state.pending_review > 0 {
                return format!("{} review pending", self.closeout_state.pending_review);
            }
            if self.closeout_state.revision_required > 0 {
                return format!("{} revision/blocked", self.closeout_state.revision_required);
            }
            if missing_or_stale > 0 {
                return format!("{missing_or_stale} package missing/stale");
            }
        }

        if categories == 0 {
            return format!("{} tasks require closeout", self.closeout_required);
        }

        format!(
            "{} require closeout, see next action",
            self.closeout_required
        )
    }

    fn closeout_focus_label(&self) -> &'static str {
        let missing_or_stale = self.closeout_state.missing_closeout
            + self.closeout_state.stale_closeout
            + self.closeout_state.stale_review;
        let categories = [
            missing_or_stale,
            self.closeout_state.pending_review,
            self.closeout_state.revision_required,
            self.closeout_state.approved_with_followups,
        ]
        .into_iter()
        .filter(|count| *count > 0)
        .count();

        if categories != 1 {
            return "closeout required";
        }
        if self.closeout_state.approved_with_followups > 0 {
            "closeout follow-ups pending"
        } else if self.closeout_state.pending_review > 0 {
            "closeout review pending"
        } else if self.closeout_state.revision_required > 0 {
            "closeout revision pending"
        } else if missing_or_stale > 0 {
            "closeout package missing/stale"
        } else {
            "closeout required"
        }
    }
}

fn tui_next_action_label(action: &OffdeskNextSafeAction) -> String {
    let command = action
        .commands
        .first()
        .map(String::as_str)
        .unwrap_or(action.detail.as_str());
    let prefix = match action.kind.as_str() {
        "approval_pending" | "approval_expired" | "approval_denied" => "Review",
        "recovery_required" | "resume_review_required" => "Recover",
        "review_required" | "closeout_check" => "Review",
        "runtime_monitoring" => "Monitor",
        "dispatch_pending" => "Dispatch",
        "provider_attention" => "Provider",
        "cancelled" => "Review",
        _ if action.requires_operator_review => "Review",
        _ => "Next",
    };
    format!("{prefix}: {command}")
}

pub struct HomeView {
    pub(super) storage: Storage,
    pub(super) instances: Vec<Instance>,
    pub(super) instance_map: HashMap<String, Instance>,
    pub(super) groups: Vec<Group>,
    pub(super) group_tree: GroupTree,
    pub(super) flat_items: Vec<Item>,

    // UI state
    pub(super) cursor: usize,
    pub(super) selected_session: Option<String>,
    pub(super) selected_group: Option<String>,
    pub(super) view_mode: ViewMode,

    // Dialogs
    pub(super) show_help: bool,
    pub(super) new_dialog: Option<NewSessionDialog>,
    pub(super) confirm_dialog: Option<ConfirmDialog>,
    pub(super) unified_delete_dialog: Option<UnifiedDeleteDialog>,
    pub(super) group_delete_options_dialog: Option<GroupDeleteOptionsDialog>,
    pub(super) rename_dialog: Option<RenameDialog>,
    pub(super) hook_trust_dialog: Option<HookTrustDialog>,
    /// Session data pending hook trust approval
    pub(super) pending_hook_trust_data: Option<NewSessionData>,
    pub(super) welcome_dialog: Option<WelcomeDialog>,
    pub(super) changelog_dialog: Option<ChangelogDialog>,
    pub(super) info_dialog: Option<InfoDialog>,
    // Search
    pub(super) search_active: bool,
    pub(super) search_query: Input,
    pub(super) filtered_items: Option<Vec<usize>>,

    // Tool availability
    pub(super) available_tools: AvailableTools,

    // Performance: background status polling
    pub(super) status_poller: StatusPoller,

    // Performance: background deletion
    pub(super) deletion_poller: DeletionPoller,

    // Performance: background session creation for slow hook execution
    pub(super) creation_poller: CreationPoller,
    /// Set to true if user cancelled while creation was pending
    pub(super) creation_cancelled: bool,
    /// Sessions whose on_launch hooks already ran in the creation poller
    pub(super) on_launch_hooks_ran: HashSet<String>,

    // Performance: preview caching
    pub(super) preview_poller: PreviewPoller,
    pub(super) pending_preview_refresh: bool,
    pub(super) preview_cache: PreviewCache,
    pub(super) terminal_preview_cache: PreviewCache,

    // Sound config for state transition sounds
    pub(super) sound_config: crate::sound::SoundConfig,

    // Resolved active-profile config for all runtime decisions in this view
    pub(super) effective_config: crate::session::Config,
    pub(super) instance_configs: HashMap<String, crate::session::Config>,

    // Settings view
    pub(super) settings_view: Option<SettingsView>,
    pub(super) projects_view: Option<projects::ProjectsView>,
    /// Flag to indicate we're confirming settings close (unsaved changes)
    pub(super) settings_close_confirm: bool,

    // Diff view
    pub(super) diff_view: Option<DiffView>,

    // Resizable list column width (percentage-like units)
    pub(super) list_width: u16,

    // Offdesk durable artifact status
    pub(super) offdesk_resume: OffdeskResumeSummary,

    // Orchestration signals for the status bar
    pub(super) orchestration: OrchestrationSignals,

    // Path awaiting a "register project?" confirmation after session creation
    pub(super) pending_registry_path: Option<std::path::PathBuf>,

    // Session to attach once the registry confirmation resolves either way
    pub(super) pending_attach_after_registry: Option<String>,
}

pub(super) use crate::offdesk::{
    load_orchestration_signals as load_orchestration_summary, OrchestrationSignals,
};

impl HomeView {
    pub fn new(storage: Storage, available_tools: AvailableTools) -> anyhow::Result<Self> {
        let (mut instances, groups) = storage.load_with_groups()?;
        let resolved = resolve_config(storage.profile())?;

        for inst in &mut instances {
            inst.update_search_cache();
        }

        // Backfill orchestrator sessions for profiles created before this feature existed.
        let created = crate::session::auto_orchestrator::ensure_for_existing_sessions(
            &mut instances,
            &resolved,
        );
        if created > 0 {
            let group_tree = GroupTree::new_with_groups(&instances, &groups);
            storage.save_with_groups(&instances, &group_tree)?;
        }

        let instance_map: HashMap<String, Instance> = instances
            .iter()
            .map(|i| (i.id.clone(), i.clone()))
            .collect();
        let group_tree = GroupTree::new_with_groups(&instances, &groups);
        let flat_items = flatten_tree(&group_tree, &instances);

        // Load the resolved config to get sound config
        let sound_config = resolved.sound.clone();
        let offdesk_resume = load_offdesk_summary(storage.profile());
        let profile_name = storage.profile().to_string();
        let instance_configs = build_instance_configs(&profile_name, &instances, &resolved);

        let mut view = Self {
            storage,
            instances,
            instance_map,
            groups,
            group_tree,
            flat_items,
            cursor: 0,
            selected_session: None,
            selected_group: None,
            view_mode: ViewMode::default(),
            show_help: false,
            new_dialog: None,
            confirm_dialog: None,
            unified_delete_dialog: None,
            group_delete_options_dialog: None,
            rename_dialog: None,
            hook_trust_dialog: None,
            pending_hook_trust_data: None,
            welcome_dialog: None,
            changelog_dialog: None,
            info_dialog: None,
            search_active: false,
            search_query: Input::default(),
            filtered_items: None,
            available_tools,
            status_poller: StatusPoller::new(),
            deletion_poller: DeletionPoller::new(),
            creation_poller: CreationPoller::new(),
            creation_cancelled: false,
            on_launch_hooks_ran: HashSet::new(),
            preview_poller: PreviewPoller::new(),
            pending_preview_refresh: false,
            preview_cache: PreviewCache::default(),
            terminal_preview_cache: PreviewCache::default(),
            sound_config,
            effective_config: resolved,
            instance_configs,
            settings_view: None,
            projects_view: None,
            settings_close_confirm: false,
            diff_view: None,
            list_width: load_config()
                .ok()
                .flatten()
                .and_then(|c| c.app_state.home_list_width)
                .unwrap_or(35),
            offdesk_resume,
            orchestration: load_orchestration_summary(profile_name.as_str()),
            pending_registry_path: None,
            pending_attach_after_registry: None,
        };

        view.update_selected();
        Ok(view)
    }

    pub fn reload(&mut self) -> anyhow::Result<()> {
        let (mut instances, groups) = self.storage.load_with_groups()?;
        let resolved = resolve_config(self.storage.profile())?;

        for inst in &mut instances {
            if let Some(prev) = self.instance_map.get(&inst.id) {
                inst.status = prev.status;
                inst.last_error = prev.last_error.clone();
                inst.last_error_check = prev.last_error_check;
                inst.last_start_time = prev.last_start_time;
            }
            inst.update_search_cache();
        }

        // Backfill orchestrator sessions for pre-existing project sessions when enabled.
        let created = crate::session::auto_orchestrator::ensure_for_existing_sessions(
            &mut instances,
            &resolved,
        );
        if created > 0 {
            let group_tree = GroupTree::new_with_groups(&instances, &groups);
            self.storage.save_with_groups(&instances, &group_tree)?;
        }

        self.instances = instances;
        self.instance_map = self
            .instances
            .iter()
            .map(|i| (i.id.clone(), i.clone()))
            .collect();
        self.groups = groups;
        self.group_tree = GroupTree::new_with_groups(&self.instances, &self.groups);
        self.flat_items = flatten_tree(&self.group_tree, &self.instances);
        self.offdesk_resume = load_offdesk_summary(self.storage.profile());
        self.orchestration = load_orchestration_summary(self.storage.profile());
        self.sound_config = resolved.sound.clone();
        self.instance_configs =
            build_instance_configs(self.storage.profile(), &self.instances, &resolved);
        self.effective_config = resolved;

        if self.cursor >= self.flat_items.len() && !self.flat_items.is_empty() {
            self.cursor = self.flat_items.len() - 1;
        }

        self.update_selected();
        Ok(())
    }

    /// Request a status refresh in the background (non-blocking).
    /// Call `apply_status_updates` to check for and apply results.
    pub fn request_status_refresh(&mut self) {
        self.status_poller
            .request_refresh(&self.instances, self.selected_session.as_deref());
    }

    pub(super) fn request_preview_refresh_if_needed(
        &mut self,
        kind: PreviewKind,
        width: u16,
        height: u16,
    ) {
        if self.pending_preview_refresh || width == 0 || height == 0 {
            return;
        }

        let Some(session_id) = self.selected_session.clone() else {
            return;
        };
        let dimensions = (width, height);
        let cache = match kind {
            PreviewKind::Agent => &self.preview_cache,
            PreviewKind::Terminal => &self.terminal_preview_cache,
        };
        if !cache.needs_refresh(&session_id, dimensions) {
            return;
        }

        let Some(instance) = self.instance_map.get(&session_id).cloned() else {
            return;
        };
        self.pending_preview_refresh = self.preview_poller.request_refresh(PreviewRequest {
            instance,
            kind,
            width,
            height,
        });
    }

    pub fn apply_preview_update(&mut self) -> bool {
        let Some(update) = self.preview_poller.try_recv_update() else {
            return false;
        };
        self.pending_preview_refresh = false;

        if self.selected_session.as_deref() == Some(update.session_id.as_str())
            && self.view_mode
                == match update.kind {
                    PreviewKind::Agent => ViewMode::Agent,
                    PreviewKind::Terminal => ViewMode::Terminal,
                }
        {
            let cache = match update.kind {
                PreviewKind::Agent => &mut self.preview_cache,
                PreviewKind::Terminal => &mut self.terminal_preview_cache,
            };
            cache.session_id = Some(update.session_id);
            cache.content = update.content;
            cache.dimensions = update.dimensions;
            cache.terminal_running = update.terminal_running;
            cache.last_refresh = Instant::now();
        }

        true
    }

    /// Apply any pending status updates from the background poller.
    /// Returns true if updates were applied.
    pub fn apply_status_updates(&mut self) -> bool {
        use crate::session::Status;

        if let Some(updates) = self.status_poller.try_recv_updates() {
            for update in updates {
                if let Some(inst) = self.instances.iter_mut().find(|i| i.id == update.id) {
                    if inst.status != Status::Deleting {
                        let old_status = inst.status;
                        inst.status = update.status;
                        inst.last_error = update.last_error.clone();
                        if old_status != update.status {
                            crate::sound::play_for_transition(
                                old_status,
                                update.status,
                                &self.sound_config,
                            );
                        }
                    }
                }
                if let Some(inst) = self.instance_map.get_mut(&update.id) {
                    if inst.status != Status::Deleting {
                        inst.status = update.status;
                        inst.last_error = update.last_error;
                    }
                }
            }
            return true;
        }
        false
    }

    pub fn apply_deletion_results(&mut self) -> bool {
        use crate::session::Status;

        if let Some(result) = self.deletion_poller.try_recv_result() {
            if result.success {
                self.instances.retain(|i| i.id != result.session_id);
                self.instance_map.remove(&result.session_id);
                self.group_tree = GroupTree::new_with_groups(&self.instances, &self.groups);
                if let Some(group_path) = result.delete_empty_group_path.as_deref() {
                    let prefix = format!("{}/", group_path);
                    let group_still_has_sessions = self.instances.iter().any(|inst| {
                        inst.group_path == group_path || inst.group_path.starts_with(&prefix)
                    });
                    if !group_still_has_sessions {
                        self.group_tree.delete_group(group_path);
                        self.groups = self.group_tree.get_all_groups();
                    }
                }

                if let Err(e) = self
                    .storage
                    .save_with_groups(&self.instances, &self.group_tree)
                {
                    tracing::error!("Failed to save after deletion: {}", e);
                }
                let _ = self.reload();
            } else {
                if let Some(inst) = self
                    .instances
                    .iter_mut()
                    .find(|i| i.id == result.session_id)
                {
                    inst.status = Status::Error;
                    inst.last_error = result.error.clone();
                }
                if let Some(inst) = self.instance_map.get_mut(&result.session_id) {
                    inst.status = Status::Error;
                    inst.last_error = result.error;
                }
            }
            return true;
        }
        false
    }

    /// Request background session creation. Used for slow hooks to avoid blocking UI.
    pub fn request_creation(
        &mut self,
        mut data: NewSessionData,
        hooks: Option<crate::session::HooksConfig>,
    ) {
        let has_hooks = hooks
            .as_ref()
            .is_some_and(|h| !h.on_create.is_empty() || !h.on_launch.is_empty());
        if let Some(dialog) = &mut self.new_dialog {
            dialog.set_loading(true);
            dialog.set_has_hooks(has_hooks);
        }

        self.creation_cancelled = false;
        let config = match crate::session::resolve_config_with_repo(
            self.storage.profile(),
            std::path::Path::new(&data.path),
        ) {
            Ok(config) => config,
            Err(error) => {
                if let Some(dialog) = &mut self.new_dialog {
                    dialog.set_loading(false);
                    dialog.set_error(format!("Failed to resolve active profile: {error:#}"));
                }
                return;
            }
        };
        self.apply_effective_session_defaults(&mut data, &config);
        let request = CreationRequest {
            data,
            existing_instances: self.instances.clone(),
            hooks,
            config,
        };
        self.creation_poller.request_creation(request);
    }

    pub(super) fn apply_effective_session_defaults(
        &self,
        data: &mut NewSessionData,
        config: &crate::session::Config,
    ) {
        if !data.tool_explicitly_selected {
            if let Some(tool) = config
                .session
                .default_tool
                .as_deref()
                .map(str::trim)
                .filter(|tool| !tool.is_empty())
                .filter(|tool| self.available_tools.available_list().contains(tool))
            {
                data.tool = tool.to_string();
            }
        }
        if !data.yolo_explicitly_selected {
            data.yolo_mode = config.session.yolo_mode_default;
        }
    }

    /// Mark the current creation operation as cancelled (user pressed Esc)
    pub fn cancel_creation(&mut self) {
        if self.creation_poller.is_pending() {
            self.creation_cancelled = true;
        }
        self.new_dialog = None;
    }

    /// Apply any pending creation results from the background poller.
    /// Returns Some(session_id) if creation succeeded and we should attach.
    pub fn apply_creation_results(&mut self) -> Option<String> {
        use super::creation_poller::CreationResult;
        use crate::session::builder::{self, CreatedWorktree};
        use std::path::PathBuf;

        let result = self.creation_poller.try_recv_result()?;

        // Check if the user cancelled while waiting
        if self.creation_cancelled {
            self.creation_cancelled = false;
            if let CreationResult::Success {
                ref instance,
                ref created_worktree,
                ..
            } = result
            {
                let worktree = created_worktree.as_ref().map(|wt| CreatedWorktree {
                    path: PathBuf::from(&wt.path),
                    main_repo_path: PathBuf::from(&wt.main_repo_path),
                });
                builder::cleanup_instance(instance, worktree.as_ref());
            }
            return None;
        }

        match result {
            CreationResult::Success {
                session_id,
                instance,
                on_launch_hooks_ran,
                ..
            } => {
                let instance = *instance;
                self.instances.push(instance.clone());
                if let Ok(config) = crate::session::resolve_config_with_repo(
                    self.storage.profile(),
                    std::path::Path::new(&instance.project_path),
                ) {
                    let _ = crate::session::auto_orchestrator::maybe_create_for_instance(
                        &mut self.instances,
                        &instance,
                        &config,
                    );
                }
                self.group_tree = GroupTree::new_with_groups(&self.instances, &self.groups);
                if !instance.group_path.is_empty() {
                    self.group_tree.create_group(&instance.group_path);
                }

                if let Err(e) = self
                    .storage
                    .save_with_groups(&self.instances, &self.group_tree)
                {
                    tracing::error!("Failed to save after creation: {}", e);
                }

                if on_launch_hooks_ran {
                    self.on_launch_hooks_ran.insert(session_id.clone());
                }

                let _ = self.reload();
                self.new_dialog = None;

                Some(session_id)
            }
            CreationResult::Error(error) => {
                if let Some(dialog) = &mut self.new_dialog {
                    dialog.set_loading(false);
                    dialog.set_error(error);
                }
                None
            }
        }
    }

    /// Check if on_launch hooks already ran for this session (and consume the flag).
    pub fn take_on_launch_hooks_ran(&mut self, session_id: &str) -> bool {
        self.on_launch_hooks_ran.remove(session_id)
    }

    /// Check if there's a pending creation operation
    pub fn is_creation_pending(&self) -> bool {
        self.creation_poller.is_pending()
    }

    /// Tick the dialog spinner animation if loading, and drain hook progress
    pub fn tick_dialog(&mut self) {
        if let Some(dialog) = &mut self.new_dialog {
            if dialog.is_loading() {
                dialog.tick();
                // Drain all pending hook progress messages
                while let Some(progress) = self.creation_poller.try_recv_progress() {
                    dialog.push_hook_progress(progress);
                }
            }
        }
    }

    pub fn has_dialog(&self) -> bool {
        self.show_help
            || self.new_dialog.is_some()
            || self.confirm_dialog.is_some()
            || self.unified_delete_dialog.is_some()
            || self.group_delete_options_dialog.is_some()
            || self.rename_dialog.is_some()
            || self.hook_trust_dialog.is_some()
            || self.welcome_dialog.is_some()
            || self.changelog_dialog.is_some()
            || self.info_dialog.is_some()
            || self.settings_view.is_some()
            || self.diff_view.is_some()
    }

    pub fn shrink_list(&mut self) {
        self.list_width = self.list_width.saturating_sub(5).max(10);
        self.save_list_width();
    }

    pub fn grow_list(&mut self) {
        self.list_width = (self.list_width + 5).min(80);
        self.save_list_width();
    }

    fn save_list_width(&self) {
        if let Ok(mut config) = load_config().map(|c| c.unwrap_or_default()) {
            config.app_state.home_list_width = Some(self.list_width);
            let _ = save_config(&config);
        }
    }

    pub fn show_welcome(&mut self) {
        self.welcome_dialog = Some(WelcomeDialog::new());
    }

    pub fn show_changelog(&mut self, from_version: Option<String>) {
        self.changelog_dialog = Some(ChangelogDialog::new(from_version));
    }

    pub fn get_instance(&self, id: &str) -> Option<&Instance> {
        self.instance_map.get(id)
    }

    pub fn available_tools(&self) -> AvailableTools {
        self.available_tools.clone()
    }

    pub(super) fn get_next_profile(&self) -> Option<String> {
        use crate::session::list_profiles;

        let profiles = list_profiles().ok()?;
        if profiles.len() <= 1 {
            return None;
        }
        let current = self.storage.profile();
        let current_idx = profiles.iter().position(|p| p == current).unwrap_or(0);
        let next_idx = (current_idx + 1) % profiles.len();
        Some(profiles[next_idx].clone())
    }

    pub fn set_instance_error(&mut self, id: &str, error: Option<String>) {
        if let Some(inst) = self.instance_map.get_mut(id) {
            inst.last_error = error.clone();
        }
        if let Some(inst) = self.instances.iter_mut().find(|i| i.id == id) {
            inst.last_error = error;
        }
    }

    /// Session IDs in current list display order.
    ///
    /// If search is active, returns only sessions in the filtered list order.
    /// Otherwise returns sessions in the normal flattened tree order.
    pub(super) fn visible_session_ids(&self) -> Vec<String> {
        let indices: Vec<usize> = if let Some(ref filtered) = self.filtered_items {
            filtered.clone()
        } else {
            (0..self.flat_items.len()).collect()
        };

        indices
            .iter()
            .filter_map(|&idx| match self.flat_items.get(idx) {
                Some(Item::Session { id, .. }) => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn start_terminal_for_instance_with_size(
        &mut self,
        id: &str,
        size: Option<(u16, u16)>,
    ) -> anyhow::Result<()> {
        let config = self.resolved_config_for_instance(id)?;
        if let Some(inst) = self.instances.iter_mut().find(|i| i.id == id) {
            inst.start_terminal_with_size(size, &config)?;
        }
        if let Some(inst) = self.instance_map.get_mut(id) {
            inst.start_terminal_with_size(size, &config)?;
        }
        self.storage
            .save_with_groups(&self.instances, &self.group_tree)?;
        Ok(())
    }

    pub fn resolved_config_for_instance(&self, id: &str) -> anyhow::Result<crate::session::Config> {
        if let Some(config) = self.instance_configs.get(id) {
            return Ok(config.clone());
        }
        resolve_config(self.storage.profile())
    }

    pub fn select_session_by_id(&mut self, session_id: &str) {
        for (idx, item) in self.flat_items.iter().enumerate() {
            if let Item::Session { id, .. } = item {
                if id == session_id {
                    self.cursor = idx;
                    self.update_selected();
                    return;
                }
            }
        }
    }

    /// Select a session by ID while respecting the active filtered view.
    pub(super) fn select_session_by_id_in_current_view(&mut self, session_id: &str) {
        if let Some(ref filtered) = self.filtered_items {
            for (display_idx, &item_idx) in filtered.iter().enumerate() {
                if let Some(Item::Session { id, .. }) = self.flat_items.get(item_idx) {
                    if id == session_id {
                        self.cursor = display_idx;
                        self.update_selected();
                        return;
                    }
                }
            }
        }

        self.select_session_by_id(session_id);
    }

    /// Refresh all config-dependent state from the current profile's config.
    /// Call this after settings are saved to pick up any changes.
    pub fn refresh_from_config(&mut self) {
        if let Ok(config) = resolve_config(self.storage.profile()) {
            self.sound_config = config.sound.clone();
            self.instance_configs =
                build_instance_configs(self.storage.profile(), &self.instances, &config);
            self.effective_config = config;
        }
    }
}

fn build_instance_configs(
    profile: &str,
    instances: &[Instance],
    fallback: &crate::session::Config,
) -> HashMap<String, crate::session::Config> {
    instances
        .iter()
        .map(|instance| {
            let config = crate::session::resolve_config_with_repo(
                profile,
                std::path::Path::new(&instance.project_path),
            )
            .unwrap_or_else(|error| {
                tracing::warn!(
                    "Failed to resolve repo config for '{}': {error:#}",
                    instance.project_path
                );
                fallback.clone()
            });
            (instance.id.clone(), config)
        })
        .collect()
}

fn load_offdesk_summary(profile: &str) -> OffdeskResumeSummary {
    let Ok(profile_dir) = get_profile_dir(profile) else {
        return OffdeskResumeSummary::default();
    };
    let Ok(states) = crate::offdesk::TaskResumeStore::new(&profile_dir).load() else {
        return OffdeskResumeSummary::default();
    };
    let now = chrono::Utc::now();
    let mut summary = states
        .iter()
        .filter(|state| state.status == crate::offdesk::ResumeStatus::ResumePending)
        .fold(OffdeskResumeSummary::default(), |mut summary, state| {
            if state.is_fresh_at(now) {
                summary.fresh_pending += 1;
            } else {
                summary.stale_pending += 1;
            }
            summary
        });
    if let Ok(offdesk) = crate::offdesk::load_offdesk_status_summary(&profile_dir, now) {
        summary.pending_approvals = offdesk.pending_approvals;
        summary.queued_tasks = offdesk.tasks.queued;
        summary.active_tasks = offdesk.tasks.active + offdesk.tasks.pending_approval;
        summary.failed_tasks = offdesk.tasks.failed;
        summary.resume_pending_tasks = offdesk.tasks.resume_pending;
        summary.cancelled_tasks = offdesk.tasks.cancelled;
        summary.stale_background = offdesk.background_stale;
        summary.failed_background = offdesk.background_failed;
        summary.closeout_required = offdesk.closeout_required;
        summary.closeout_state = offdesk.closeout_state;
        summary.next_safe_actions = offdesk.next_safe_actions;
    }
    if summary.fresh_pending > 0 || summary.stale_pending > 0 {
        crate::offdesk::ensure_resume_review_next_safe_action(&mut summary.next_safe_actions);
    }
    summary
}
