//! New session dialog

mod render;

#[cfg(test)]
mod tests;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tui_input::backend::crossterm::EventHandler;
use tui_input::Input;

use super::DialogResult;
use crate::session::repo_config::HookProgress;
#[cfg(test)]
use crate::session::Config;
use crate::session::{civilizations, resolve_config};
use crate::tmux::AvailableTools;
use crate::tui::components::{DirPicker, DirPickerResult, ListPicker, ListPickerResult};

pub(super) struct FieldHelp {
    pub(super) name: &'static str,
    pub(super) description: &'static str,
}

pub(super) const HELP_DIALOG_WIDTH: u16 = 85;

pub(super) const FIELD_HELP: &[FieldHelp] = &[
    FieldHelp {
        name: "Title",
        description: "Session name (auto-generates if empty)",
    },
    FieldHelp {
        name: "Path",
        description: "Working directory for the session",
    },
    FieldHelp {
        name: "Tool",
        description: "Which AI tool to use",
    },
    FieldHelp {
        name: "YOLO Mode",
        description:
            "Skip permission prompts for autonomous operation (--dangerously-skip-permissions)",
    },
    FieldHelp {
        name: "Worktree Branch",
        description: "Branch name for git worktree (Ctrl+P to browse existing branches)",
    },
    FieldHelp {
        name: "New Branch",
        description:
            "Checked: create new branch. Unchecked: use existing (creates worktree if needed)",
    },
    FieldHelp {
        name: "Group",
        description: "Optional grouping for organization (Ctrl+P to browse existing groups)",
    },
];

#[derive(Clone)]
pub struct NewSessionData {
    pub title: String,
    pub path: String,
    pub group: String,
    pub tool: String,
    pub worktree_branch: Option<String>,
    pub create_new_branch: bool,
    pub yolo_mode: bool,
    pub tool_explicitly_selected: bool,
    pub yolo_explicitly_selected: bool,
}

/// Spinner frames for loading animation
pub(super) const SPINNER_FRAMES: &[&str] = &["◐", "◓", "◑", "◒"];

pub struct NewSessionDialog {
    pub(super) title: Input,
    pub(super) path: Input,
    pub(super) group: Input,
    pub(super) tool_index: usize,
    pub(super) focused_field: usize,
    pub(super) available_tools: Vec<&'static str>,
    pub(super) existing_titles: Vec<String>,
    pub(super) worktree_branch: Input,
    pub(super) create_new_branch: bool,
    pub(super) yolo_mode: bool,
    pub(super) tool_explicitly_selected: bool,
    pub(super) yolo_explicitly_selected: bool,
    pub(super) existing_groups: Vec<String>,
    pub(super) group_picker: ListPicker,
    pub(super) branch_picker: ListPicker,
    pub(super) dir_picker: DirPicker,
    pub(super) error_message: Option<String>,
    pub(super) show_help: bool,
    /// Whether the dialog is in loading state (creating session in background)
    pub(super) loading: bool,
    /// Spinner animation frame counter
    pub(super) spinner_frame: usize,
    /// Whether hooks are being executed during loading
    pub(super) has_hooks: bool,
    /// The currently running hook command
    pub(super) current_hook: Option<String>,
    /// Accumulated output lines from hook execution
    pub(super) hook_output: Vec<String>,
}

impl NewSessionDialog {
    pub fn new(
        tools: AvailableTools,
        existing_titles: Vec<String>,
        existing_groups: Vec<String>,
        profile: &str,
    ) -> Self {
        let current_dir = std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        let available_tools = tools.available_list();

        // Load resolved config (global merged with profile overrides)
        let config = resolve_config(profile).unwrap_or_default();

        // Determine default tool index based on config
        let tool_index = if let Some(ref default_tool) = config.session.default_tool {
            available_tools
                .iter()
                .position(|&t| t == default_tool.as_str())
                .unwrap_or(0)
        } else {
            0
        };

        let yolo_mode = config.session.yolo_mode_default;

        Self {
            title: Input::default(),
            path: Input::new(current_dir),
            group: Input::default(),
            tool_index,
            focused_field: 0,
            available_tools,
            existing_titles,
            existing_groups,
            group_picker: ListPicker::new("Select Group"),
            branch_picker: ListPicker::new("Select Branch"),
            dir_picker: DirPicker::new(),
            worktree_branch: Input::default(),
            create_new_branch: true,
            yolo_mode,
            tool_explicitly_selected: false,
            yolo_explicitly_selected: false,
            error_message: None,
            show_help: false,
            loading: false,
            spinner_frame: 0,
            has_hooks: false,
            current_hook: None,
            hook_output: Vec::new(),
        }
    }

    /// Set whether hooks will be executed during session creation
    pub fn set_has_hooks(&mut self, has_hooks: bool) {
        self.has_hooks = has_hooks;
    }

    /// Push a hook progress message into the dialog state
    pub fn push_hook_progress(&mut self, progress: HookProgress) {
        match progress {
            HookProgress::Started(cmd) => {
                self.current_hook = Some(cmd);
            }
            HookProgress::Output(line) => {
                self.hook_output.push(line);
            }
        }
    }

    /// Set the dialog to loading state
    pub fn set_loading(&mut self, loading: bool) {
        self.loading = loading;
        if loading {
            self.error_message = None;
        }
    }

    /// Check if the dialog is in loading state
    pub fn is_loading(&self) -> bool {
        self.loading
    }

    /// Advance the spinner animation frame. Call this periodically when loading.
    pub fn tick(&mut self) {
        self.spinner_frame = (self.spinner_frame + 1) % SPINNER_FRAMES.len();
    }

    #[cfg(test)]
    pub(super) fn new_with_config(tools: Vec<&'static str>, path: String, config: Config) -> Self {
        let tool_index = if let Some(ref default_tool) = config.session.default_tool {
            tools
                .iter()
                .position(|&t| t == default_tool.as_str())
                .unwrap_or(0)
        } else {
            0
        };

        Self {
            title: Input::default(),
            path: Input::new(path),
            group: Input::default(),
            tool_index,
            focused_field: 0,
            available_tools: tools,
            existing_titles: Vec::new(),
            existing_groups: Vec::new(),
            group_picker: ListPicker::new("Select Group"),
            branch_picker: ListPicker::new("Select Branch"),
            dir_picker: DirPicker::new(),
            worktree_branch: Input::default(),
            create_new_branch: true,
            yolo_mode: false,
            tool_explicitly_selected: false,
            yolo_explicitly_selected: false,
            error_message: None,
            show_help: false,
            loading: false,
            spinner_frame: 0,
            has_hooks: false,
            current_hook: None,
            hook_output: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn new_with_tools(tools: Vec<&'static str>, path: String) -> Self {
        Self {
            title: Input::default(),
            path: Input::new(path),
            group: Input::default(),
            tool_index: 0,
            focused_field: 0,
            available_tools: tools,
            existing_titles: Vec::new(),
            existing_groups: Vec::new(),
            group_picker: ListPicker::new("Select Group"),
            branch_picker: ListPicker::new("Select Branch"),
            dir_picker: DirPicker::new(),
            worktree_branch: Input::default(),
            create_new_branch: true,
            yolo_mode: false,
            tool_explicitly_selected: false,
            yolo_explicitly_selected: false,
            error_message: None,
            show_help: false,
            loading: false,
            spinner_frame: 0,
            has_hooks: false,
            current_hook: None,
            hook_output: Vec::new(),
        }
    }

    pub fn set_error(&mut self, error: String) {
        self.error_message = Some(error);
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> DialogResult<NewSessionData> {
        // When loading, only allow Esc to cancel
        if self.loading {
            if matches!(key.code, KeyCode::Esc) {
                self.loading = false;
                return DialogResult::Cancel;
            }
            return DialogResult::Continue;
        }

        if self.show_help {
            if matches!(key.code, KeyCode::Esc | KeyCode::Char('?')) {
                self.show_help = false;
            }
            return DialogResult::Continue;
        }

        if self.group_picker.is_active() {
            if let ListPickerResult::Selected(value) = self.group_picker.handle_key(key) {
                self.group = Input::new(value);
            }
            return DialogResult::Continue;
        }

        if self.branch_picker.is_active() {
            if let ListPickerResult::Selected(value) = self.branch_picker.handle_key(key) {
                self.worktree_branch = Input::new(value);
            }
            return DialogResult::Continue;
        }

        if self.dir_picker.is_active() {
            match self.dir_picker.handle_key(key) {
                DirPickerResult::Selected(path) => {
                    self.path = Input::new(path);
                }
                DirPickerResult::Cancelled | DirPickerResult::Continue => {}
            }
            return DialogResult::Continue;
        }

        let has_tool_selection = self.available_tools.len() > 1;
        let has_worktree = !self.worktree_branch.value().is_empty();
        // Field order: title(0), path(1), [tool(2)], yolo, worktree,
        //   [new_branch], group.
        let tool_field = if has_tool_selection { 2 } else { usize::MAX };
        let yolo_mode_field = if has_tool_selection { 3 } else { 2 };
        let worktree_field = yolo_mode_field + 1;
        let new_branch_field = if has_worktree {
            worktree_field + 1
        } else {
            usize::MAX
        };
        let next = if has_worktree {
            new_branch_field + 1
        } else {
            worktree_field + 1
        };
        let group_field = next;
        let max_field = group_field + 1;

        // Ctrl+P opens a context-sensitive picker
        if key.code == KeyCode::Char('p') && key.modifiers.contains(KeyModifiers::CONTROL) {
            if self.focused_field == 1 {
                let path_value = self.path.value().trim().to_string();
                self.dir_picker.activate(&path_value);
                return DialogResult::Continue;
            }
            if self.focused_field == group_field && !self.existing_groups.is_empty() {
                self.group_picker.activate(self.existing_groups.clone());
                return DialogResult::Continue;
            }
            if self.focused_field == worktree_field {
                let path = std::path::Path::new(self.path.value().trim());
                if let Ok(branches) = crate::git::diff::list_branches(path) {
                    if !branches.is_empty() {
                        self.branch_picker.activate(branches);
                    }
                }
                return DialogResult::Continue;
            }
        }

        match key.code {
            KeyCode::Char('?') => {
                self.show_help = true;
                DialogResult::Continue
            }
            KeyCode::Esc => {
                self.error_message = None;
                DialogResult::Cancel
            }
            KeyCode::Enter => {
                self.error_message = None;
                let title_value = self.title.value().trim();
                let final_title = if title_value.is_empty() {
                    let refs: Vec<&str> = self.existing_titles.iter().map(|s| s.as_str()).collect();
                    civilizations::generate_random_title(&refs)
                } else {
                    title_value.to_string()
                };
                let worktree_value = self.worktree_branch.value().trim();
                let worktree_branch = if worktree_value.is_empty() {
                    None
                } else {
                    Some(worktree_value.to_string())
                };
                DialogResult::Submit(NewSessionData {
                    title: final_title,
                    path: self.path.value().trim().to_string(),
                    group: self.group.value().trim().to_string(),
                    tool: self.available_tools[self.tool_index].to_string(),
                    worktree_branch,
                    create_new_branch: self.create_new_branch,
                    yolo_mode: self.yolo_mode,
                    tool_explicitly_selected: self.tool_explicitly_selected,
                    yolo_explicitly_selected: self.yolo_explicitly_selected,
                })
            }
            KeyCode::Tab | KeyCode::Down => {
                self.focused_field = (self.focused_field + 1) % max_field;
                DialogResult::Continue
            }
            KeyCode::BackTab | KeyCode::Up => {
                self.focused_field = if self.focused_field == 0 {
                    max_field - 1
                } else {
                    self.focused_field - 1
                };
                DialogResult::Continue
            }
            KeyCode::Left | KeyCode::Right if self.focused_field == tool_field => {
                self.tool_index = (self.tool_index + 1) % self.available_tools.len();
                self.tool_explicitly_selected = true;
                DialogResult::Continue
            }
            KeyCode::Char(' ') if self.focused_field == tool_field => {
                self.tool_index = (self.tool_index + 1) % self.available_tools.len();
                self.tool_explicitly_selected = true;
                DialogResult::Continue
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Char(' ')
                if self.focused_field == new_branch_field =>
            {
                self.create_new_branch = !self.create_new_branch;
                DialogResult::Continue
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Char(' ')
                if self.focused_field == yolo_mode_field =>
            {
                self.yolo_mode = !self.yolo_mode;
                self.yolo_explicitly_selected = true;
                DialogResult::Continue
            }
            _ => {
                if self.focused_field != tool_field
                    && self.focused_field != new_branch_field
                    && self.focused_field != yolo_mode_field
                {
                    self.current_input_mut()
                        .handle_event(&crossterm::event::Event::Key(key));
                    self.error_message = None;
                }
                DialogResult::Continue
            }
        }
    }

    fn current_input_mut(&mut self) -> &mut Input {
        let has_tool_selection = self.available_tools.len() > 1;
        let has_worktree = !self.worktree_branch.value().is_empty();

        let yolo_mode_field = if has_tool_selection { 3 } else { 2 };
        let worktree_field = yolo_mode_field + 1;
        let new_branch_field = if has_worktree {
            worktree_field + 1
        } else {
            usize::MAX
        };
        let next = if has_worktree {
            new_branch_field + 1
        } else {
            worktree_field + 1
        };
        let group_field = next;

        match self.focused_field {
            0 => &mut self.title,
            1 => &mut self.path,
            n if n == worktree_field => &mut self.worktree_branch,
            n if n == group_field => &mut self.group,
            _ => &mut self.title,
        }
    }
}
