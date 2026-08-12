//! Settings view - configuration management UI

mod fields;
mod input;
mod render;

use tui_input::Input;

use crate::session::{
    load_profile_config, load_repo_config, merge_configs, profile_to_repo_config,
    repo_config_to_profile, save_config, save_profile_config, save_repo_config, Config,
    ProfileConfig, RepoConfig,
};

pub use fields::{FieldKey, FieldValue, SettingField, SettingsCategory};
pub use input::SettingsAction;

/// Which scope of settings is being edited
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SettingsScope {
    #[default]
    Global,
    Profile,
    Repo,
}

/// Focus state for the settings view
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SettingsFocus {
    #[default]
    Categories,
    Fields,
}

/// State for editing a list field
#[derive(Debug, Clone, Default)]
pub struct ListEditState {
    pub selected_index: usize,
    pub editing_item: Option<Input>,
    pub adding_new: bool,
}

/// The settings view state
pub struct SettingsView {
    /// Current profile name
    pub(super) profile: String,

    /// Project path for repo-level settings (None if no session selected)
    pub(super) project_path: Option<String>,

    /// Repo-level config (original, for load/save)
    pub(super) repo_config: Option<RepoConfig>,

    /// Repo config converted to ProfileConfig for TUI editing (overrides relative to resolved base)
    pub(super) repo_as_profile: ProfileConfig,

    /// Resolved base config (global + profile merged) used as the "global" when editing Repo scope
    pub(super) resolved_base: Config,

    /// Which scope tab is selected
    pub(super) scope: SettingsScope,

    /// Which panel has focus
    pub(super) focus: SettingsFocus,

    /// Available categories
    pub(super) categories: Vec<SettingsCategory>,

    /// Currently selected category index
    pub(super) selected_category: usize,

    /// Fields for the current category
    pub(super) fields: Vec<SettingField>,

    /// Currently selected field index
    pub(super) selected_field: usize,

    /// Global config being edited
    pub(super) global_config: Config,

    /// Profile config being edited (overrides)
    pub(super) profile_config: ProfileConfig,

    /// Text input when editing a text/number field
    pub(super) editing_input: Option<Input>,

    /// State for list editing
    pub(super) list_edit_state: Option<ListEditState>,

    /// Scroll offset for the fields panel (in lines)
    pub(super) fields_scroll_offset: u16,

    /// Last known viewport height for the fields panel (set during render)
    pub(super) fields_viewport_height: u16,

    /// Whether there are unsaved changes
    pub(super) has_changes: bool,

    /// Error message to display
    pub(super) error_message: Option<String>,

    /// Success message to display
    pub(super) success_message: Option<String>,
}

impl SettingsView {
    pub fn new(profile: &str, project_path: Option<String>) -> anyhow::Result<Self> {
        let global_config = Config::load()?;
        let profile_config = load_profile_config(profile)?;

        let repo_config = project_path
            .as_ref()
            .and_then(|p| load_repo_config(std::path::Path::new(p)).ok().flatten());

        let resolved_base = merge_configs(global_config.clone(), &profile_config);
        let repo_as_profile = repo_config
            .as_ref()
            .map(repo_config_to_profile)
            .unwrap_or_default();

        let categories = Self::categories_for_scope(SettingsScope::Global);

        let mut view = Self {
            profile: profile.to_string(),
            project_path,
            repo_config,
            repo_as_profile,
            resolved_base,
            scope: SettingsScope::Global,
            focus: SettingsFocus::Categories,
            categories,
            selected_category: 0,
            fields: Vec::new(),
            selected_field: 0,
            global_config,
            profile_config,
            editing_input: None,
            list_edit_state: None,
            fields_scroll_offset: 0,
            fields_viewport_height: 0,
            has_changes: false,
            error_message: None,
            success_message: None,
        };

        view.rebuild_fields();
        Ok(view)
    }

    /// Rebuild the fields list based on current category and scope
    pub(super) fn rebuild_fields(&mut self) {
        let current_category = self.categories.get(self.selected_category).copied();
        let categories = Self::categories_for_scope(self.scope);
        if categories != self.categories {
            self.selected_category = current_category
                .and_then(|current| categories.iter().position(|item| *item == current))
                .unwrap_or(0);
            self.categories = categories;
        }

        let category = self.categories[self.selected_category];
        let (scope_for_fields, global_ref, profile_ref) = match self.scope {
            SettingsScope::Global => (
                SettingsScope::Global,
                &self.global_config,
                &self.profile_config,
            ),
            SettingsScope::Profile => (
                SettingsScope::Profile,
                &self.global_config,
                &self.profile_config,
            ),
            SettingsScope::Repo => (
                SettingsScope::Repo,
                &self.resolved_base,
                &self.repo_as_profile,
            ),
        };
        self.fields =
            fields::build_fields_for_category(category, scope_for_fields, global_ref, profile_ref);
        if self.selected_field >= self.fields.len() {
            self.selected_field = 0;
        }
        self.fields_scroll_offset = 0;
    }

    fn categories_for_scope(scope: SettingsScope) -> Vec<SettingsCategory> {
        if scope == SettingsScope::Repo {
            return vec![
                SettingsCategory::Session,
                SettingsCategory::Diff,
                SettingsCategory::Hooks,
                SettingsCategory::Worktree,
                SettingsCategory::Cleanup,
                SettingsCategory::Tmux,
            ];
        }

        let mut categories = vec![
            SettingsCategory::Session,
            SettingsCategory::Diff,
            SettingsCategory::Hooks,
            SettingsCategory::Worktree,
            SettingsCategory::Cleanup,
            SettingsCategory::Updates,
            SettingsCategory::Tmux,
            SettingsCategory::Sound,
        ];
        categories.insert(1, SettingsCategory::Claude);
        if scope == SettingsScope::Global {
            categories.insert(0, SettingsCategory::General);
        }
        categories
    }

    /// Ensure the selected field is visible within the given viewport height.
    /// Call this after changing `selected_field`.
    pub(super) fn ensure_field_visible(&mut self, viewport_height: u16) {
        let mut y = 0u16;
        let mut selected_y = 0u16;
        let mut selected_h = 0u16;

        for (i, field) in self.fields.iter().enumerate() {
            let h = self.field_height(field, i);
            if i == self.selected_field {
                selected_y = y;
                selected_h = h;
                break;
            }
            y += h + 1; // +1 spacing
        }

        // Scroll up if field starts above viewport
        if selected_y < self.fields_scroll_offset {
            self.fields_scroll_offset = selected_y;
        }
        // Scroll down if field ends below viewport
        let field_bottom = selected_y + selected_h;
        if field_bottom > self.fields_scroll_offset + viewport_height {
            self.fields_scroll_offset = field_bottom.saturating_sub(viewport_height);
        }
    }

    /// Apply the current field values back to the configs
    pub(super) fn apply_field_to_config(&mut self, field_index: usize) {
        if field_index >= self.fields.len() {
            return;
        }

        let field = &self.fields[field_index];

        match self.scope {
            SettingsScope::Global | SettingsScope::Profile => {
                fields::apply_field_to_config(
                    field,
                    self.scope,
                    &mut self.global_config,
                    &mut self.profile_config,
                );
            }
            SettingsScope::Repo => {
                // Use Profile logic but against resolved_base and repo_as_profile
                fields::apply_field_to_config(
                    field,
                    SettingsScope::Profile,
                    &mut self.resolved_base,
                    &mut self.repo_as_profile,
                );
                // Sync back to repo_config
                self.repo_config = Some(profile_to_repo_config(&self.repo_as_profile));
            }
        }
        self.has_changes = true;
    }

    /// Save the current configuration
    pub fn save(&mut self) -> anyhow::Result<()> {
        // Validate all fields before saving
        for field in &self.fields {
            if let Err(e) = field.validate() {
                self.error_message = Some(e);
                return Ok(());
            }
        }

        match self.scope {
            SettingsScope::Global => {
                save_config(&self.global_config)?;
            }
            SettingsScope::Profile => {
                save_profile_config(&self.profile, &self.profile_config)?;
            }
            SettingsScope::Repo => {
                if let (Some(ref project_path), Some(ref repo_config)) =
                    (&self.project_path, &self.repo_config)
                {
                    save_repo_config(std::path::Path::new(project_path), repo_config)?;
                }
            }
        }

        self.has_changes = false;
        self.success_message = Some("Settings saved".to_string());
        self.error_message = None;
        Ok(())
    }

    /// Check if there are unsaved changes
    pub fn has_unsaved_changes(&self) -> bool {
        self.has_changes
    }

    /// Check if currently in an editing state (text field, list, dialog, etc.)
    pub fn is_editing(&self) -> bool {
        self.editing_input.is_some() || self.list_edit_state.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_categories_hide_non_overridable_settings() {
        let global = SettingsView::categories_for_scope(SettingsScope::Global);
        assert!(global.contains(&SettingsCategory::General));
        assert!(global.contains(&SettingsCategory::Claude));

        let profile = SettingsView::categories_for_scope(SettingsScope::Profile);
        assert!(!profile.contains(&SettingsCategory::General));
        assert!(profile.contains(&SettingsCategory::Claude));

        let repo = SettingsView::categories_for_scope(SettingsScope::Repo);
        assert!(!repo.contains(&SettingsCategory::General));
        assert!(!repo.contains(&SettingsCategory::Claude));
        assert!(!repo.contains(&SettingsCategory::Updates));
        assert!(!repo.contains(&SettingsCategory::Sound));
        assert!(repo.contains(&SettingsCategory::Diff));
    }
}
