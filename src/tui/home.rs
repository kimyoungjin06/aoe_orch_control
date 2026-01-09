//! Home view - main session list and navigation

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::prelude::*;
use ratatui::widgets::*;
use std::collections::HashMap;

use super::app::Action;
use super::components::{HelpOverlay, Preview};
use super::dialogs::{ConfirmDialog, NewSessionDialog};
use super::styles::Theme;
use crate::session::{flatten_tree, Group, GroupTree, Instance, Item, Status, Storage};

pub struct HomeView {
    storage: Storage,
    instances: Vec<Instance>,
    instance_map: HashMap<String, Instance>,
    groups: Vec<Group>,
    group_tree: GroupTree,
    flat_items: Vec<Item>,

    // UI state
    cursor: usize,
    selected_session: Option<String>,
    selected_group: Option<String>,

    // Dialogs
    show_help: bool,
    new_dialog: Option<NewSessionDialog>,
    confirm_dialog: Option<ConfirmDialog>,

    // Search
    search_active: bool,
    search_query: String,
    filtered_items: Option<Vec<usize>>,
}

impl HomeView {
    pub fn new(storage: Storage) -> anyhow::Result<Self> {
        let (instances, groups) = storage.load_with_groups()?;
        let instance_map: HashMap<String, Instance> = instances
            .iter()
            .map(|i| (i.id.clone(), i.clone()))
            .collect();
        let group_tree = GroupTree::new_with_groups(&instances, &groups);
        let flat_items = flatten_tree(&group_tree, &instances);

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
            show_help: false,
            new_dialog: None,
            confirm_dialog: None,
            search_active: false,
            search_query: String::new(),
            filtered_items: None,
        };

        view.update_selected();
        Ok(view)
    }

    pub fn reload(&mut self) -> anyhow::Result<()> {
        let (instances, groups) = self.storage.load_with_groups()?;
        self.instances = instances;
        self.instance_map = self
            .instances
            .iter()
            .map(|i| (i.id.clone(), i.clone()))
            .collect();
        self.groups = groups;
        self.group_tree = GroupTree::new_with_groups(&self.instances, &self.groups);
        self.flat_items = flatten_tree(&self.group_tree, &self.instances);

        // Ensure cursor is valid
        if self.cursor >= self.flat_items.len() && !self.flat_items.is_empty() {
            self.cursor = self.flat_items.len() - 1;
        }

        self.update_selected();
        Ok(())
    }

    pub fn refresh_status(&mut self) {
        crate::tmux::refresh_session_cache();
        for inst in &mut self.instances {
            inst.update_status();
        }
        self.instance_map = self
            .instances
            .iter()
            .map(|i| (i.id.clone(), i.clone()))
            .collect();
    }

    pub fn has_dialog(&self) -> bool {
        self.show_help || self.new_dialog.is_some() || self.confirm_dialog.is_some()
    }

    pub fn get_instance(&self, id: &str) -> Option<&Instance> {
        self.instance_map.get(id)
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        // Handle dialog input first
        if self.show_help {
            if matches!(
                key.code,
                KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q')
            ) {
                self.show_help = false;
            }
            return None;
        }

        if let Some(dialog) = &mut self.new_dialog {
            match dialog.handle_key(key) {
                super::dialogs::DialogResult::Continue => {}
                super::dialogs::DialogResult::Cancel => {
                    self.new_dialog = None;
                }
                super::dialogs::DialogResult::Submit(data) => {
                    self.new_dialog = None;
                    match self.create_session(data) {
                        Ok(session_id) => {
                            return Some(Action::AttachSession(session_id));
                        }
                        Err(e) => {
                            tracing::error!("Failed to create session: {}", e);
                        }
                    }
                }
            }
            return None;
        }

        if let Some(dialog) = &mut self.confirm_dialog {
            match dialog.handle_key(key) {
                super::dialogs::DialogResult::Continue => {}
                super::dialogs::DialogResult::Cancel => {
                    self.confirm_dialog = None;
                }
                super::dialogs::DialogResult::Submit(_) => {
                    let action = dialog.action().to_string();
                    self.confirm_dialog = None;
                    if action == "delete" {
                        if let Err(e) = self.delete_selected() {
                            tracing::error!("Failed to delete session: {}", e);
                        }
                    } else if action == "delete_group" {
                        if let Err(e) = self.delete_selected_group() {
                            tracing::error!("Failed to delete group: {}", e);
                        }
                    }
                }
            }
            return None;
        }

        // Search mode
        if self.search_active {
            match key.code {
                KeyCode::Esc => {
                    self.search_active = false;
                    self.search_query.clear();
                    self.filtered_items = None;
                }
                KeyCode::Enter => {
                    self.search_active = false;
                }
                KeyCode::Backspace => {
                    self.search_query.pop();
                    self.update_filter();
                }
                KeyCode::Char(c) => {
                    self.search_query.push(c);
                    self.update_filter();
                }
                _ => {}
            }
            return None;
        }

        // Normal mode keybindings
        match key.code {
            KeyCode::Char('q') => return Some(Action::Quit),
            KeyCode::Char('?') => {
                self.show_help = true;
            }
            KeyCode::Char('/') => {
                self.search_active = true;
                self.search_query.clear();
            }
            KeyCode::Char('n') => {
                self.new_dialog = Some(NewSessionDialog::new());
            }
            KeyCode::Char('d') => {
                if self.selected_session.is_some() {
                    self.confirm_dialog = Some(ConfirmDialog::new(
                        "Delete Session",
                        "Are you sure you want to delete this session?",
                        "delete",
                    ));
                } else if let Some(group_path) = &self.selected_group {
                    let session_count = self
                        .instances
                        .iter()
                        .filter(|i| {
                            i.group_path == *group_path
                                || i.group_path.starts_with(&format!("{}/", group_path))
                        })
                        .count();
                    let message = if session_count > 0 {
                        format!(
                            "Delete group '{}'? It contains {} session(s) which will be moved to the default group.",
                            group_path, session_count
                        )
                    } else {
                        format!("Are you sure you want to delete group '{}'?", group_path)
                    };
                    self.confirm_dialog =
                        Some(ConfirmDialog::new("Delete Group", &message, "delete_group"));
                }
            }
            KeyCode::Char('r') | KeyCode::F(5) => {
                return Some(Action::Refresh);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_cursor(-1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_cursor(1);
            }
            KeyCode::PageUp => {
                self.move_cursor(-10);
            }
            KeyCode::PageDown => {
                self.move_cursor(10);
            }
            KeyCode::Home | KeyCode::Char('g') if key.modifiers.contains(KeyModifiers::NONE) => {
                self.cursor = 0;
                self.update_selected();
            }
            KeyCode::End | KeyCode::Char('G') => {
                if !self.flat_items.is_empty() {
                    self.cursor = self.flat_items.len() - 1;
                    self.update_selected();
                }
            }
            KeyCode::Enter => {
                if let Some(id) = &self.selected_session {
                    return Some(Action::AttachSession(id.clone()));
                } else if let Some(item) = self.flat_items.get(self.cursor) {
                    if let Item::Group { path, .. } = item {
                        self.group_tree.toggle_collapsed(path);
                        self.flat_items = flatten_tree(&self.group_tree, &self.instances);
                    }
                }
            }
            KeyCode::Left | KeyCode::Char('h') => {
                // Collapse current group or go to parent
                if let Some(item) = self.flat_items.get(self.cursor) {
                    if let Item::Group {
                        path, collapsed, ..
                    } = item
                    {
                        if !collapsed {
                            self.group_tree.toggle_collapsed(path);
                            self.flat_items = flatten_tree(&self.group_tree, &self.instances);
                        }
                    }
                }
            }
            KeyCode::Right | KeyCode::Char('l') => {
                // Expand current group
                if let Some(item) = self.flat_items.get(self.cursor) {
                    if let Item::Group {
                        path, collapsed, ..
                    } = item
                    {
                        if *collapsed {
                            self.group_tree.toggle_collapsed(path);
                            self.flat_items = flatten_tree(&self.group_tree, &self.instances);
                        }
                    }
                }
            }
            _ => {}
        }

        None
    }

    fn move_cursor(&mut self, delta: i32) {
        let items = if let Some(ref filtered) = self.filtered_items {
            filtered.len()
        } else {
            self.flat_items.len()
        };

        if items == 0 {
            return;
        }

        let new_cursor = if delta < 0 {
            self.cursor.saturating_sub((-delta) as usize)
        } else {
            (self.cursor + delta as usize).min(items - 1)
        };

        self.cursor = new_cursor;
        self.update_selected();
    }

    fn update_selected(&mut self) {
        let item_idx = if let Some(ref filtered) = self.filtered_items {
            filtered.get(self.cursor).copied()
        } else {
            Some(self.cursor)
        };

        if let Some(idx) = item_idx {
            if let Some(item) = self.flat_items.get(idx) {
                match item {
                    Item::Session { id, .. } => {
                        self.selected_session = Some(id.clone());
                        self.selected_group = None;
                    }
                    Item::Group { path, .. } => {
                        self.selected_session = None;
                        self.selected_group = Some(path.clone());
                    }
                }
            }
        }
    }

    fn update_filter(&mut self) {
        if self.search_query.is_empty() {
            self.filtered_items = None;
            return;
        }

        let query = self.search_query.to_lowercase();
        let mut matches = Vec::new();

        for (idx, item) in self.flat_items.iter().enumerate() {
            match item {
                Item::Session { id, .. } => {
                    if let Some(inst) = self.instance_map.get(id) {
                        if inst.title.to_lowercase().contains(&query)
                            || inst.project_path.to_lowercase().contains(&query)
                        {
                            matches.push(idx);
                        }
                    }
                }
                Item::Group { name, path, .. } => {
                    if name.to_lowercase().contains(&query) || path.to_lowercase().contains(&query)
                    {
                        matches.push(idx);
                    }
                }
            }
        }

        self.filtered_items = Some(matches);
        self.cursor = 0;
        self.update_selected();
    }

    fn create_session(&mut self, data: super::dialogs::NewSessionData) -> anyhow::Result<String> {
        let mut instance = Instance::new(&data.title, &data.path);
        instance.group_path = data.group;
        instance.command = data.command.clone();
        instance.tool = if data.command.to_lowercase().contains("claude") {
            "claude".to_string()
        } else if data.command.to_lowercase().contains("opencode") {
            "opencode".to_string()
        } else {
            "shell".to_string()
        };

        let session_id = instance.id.clone();
        self.instances.push(instance.clone());
        self.group_tree = GroupTree::new_with_groups(&self.instances, &self.groups);
        if !instance.group_path.is_empty() {
            self.group_tree.create_group(&instance.group_path);
        }
        self.storage
            .save_with_groups(&self.instances, &self.group_tree)?;

        self.reload()?;
        Ok(session_id)
    }

    fn delete_selected(&mut self) -> anyhow::Result<()> {
        if let Some(id) = &self.selected_session {
            let id = id.clone();
            self.instances.retain(|i| i.id != id);

            // Kill tmux session
            if let Some(inst) = self.instance_map.get(&id) {
                let _ = inst.kill();
            }

            self.group_tree = GroupTree::new_with_groups(&self.instances, &self.groups);
            self.storage
                .save_with_groups(&self.instances, &self.group_tree)?;

            self.reload()?;
        }
        Ok(())
    }

    fn delete_selected_group(&mut self) -> anyhow::Result<()> {
        if let Some(group_path) = self.selected_group.take() {
            let prefix = format!("{}/", group_path);
            for inst in &mut self.instances {
                if inst.group_path == group_path || inst.group_path.starts_with(&prefix) {
                    inst.group_path = String::new();
                }
            }

            self.group_tree = GroupTree::new_with_groups(&self.instances, &self.groups);
            self.group_tree.delete_group(&group_path);
            self.storage
                .save_with_groups(&self.instances, &self.group_tree)?;

            self.reload()?;
        }
        Ok(())
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        // Layout: left panel (list) and right panel (preview)
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
            .split(area);

        self.render_list(frame, chunks[0], theme);
        self.render_preview(frame, chunks[1], theme);

        // Render dialogs on top
        if self.show_help {
            HelpOverlay::render(frame, area, theme);
        }

        if let Some(dialog) = &self.new_dialog {
            dialog.render(frame, area, theme);
        }

        if let Some(dialog) = &self.confirm_dialog {
            dialog.render(frame, area, theme);
        }
    }

    fn render_list(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .title(format!(" Agent of Empires [{}] ", self.storage.profile()))
            .title_style(Style::default().fg(theme.title).bold());

        let inner = block.inner(area);
        frame.render_widget(block, area);

        if self.instances.is_empty() && self.groups.is_empty() {
            let empty_text = vec![
                Line::from(""),
                Line::from("No sessions yet").style(Style::default().fg(theme.dimmed)),
                Line::from(""),
                Line::from("Press 'n' to create one").style(Style::default().fg(theme.hint)),
                Line::from("or 'agent-of-empires add .'").style(Style::default().fg(theme.hint)),
            ];
            let para = Paragraph::new(empty_text).alignment(Alignment::Center);
            frame.render_widget(para, inner);
            return;
        }

        // Render session tree
        let items_to_show = if let Some(ref filtered) = self.filtered_items {
            filtered
                .iter()
                .filter_map(|&idx| self.flat_items.get(idx))
                .cloned()
                .collect()
        } else {
            self.flat_items.clone()
        };

        let list_items: Vec<ListItem> = items_to_show
            .iter()
            .enumerate()
            .map(|(idx, item)| {
                let is_selected = idx == self.cursor;
                self.render_item(item, is_selected, theme)
            })
            .collect();

        let list = List::new(list_items).highlight_style(Style::default().bg(theme.selection));

        frame.render_widget(list, inner);

        // Render search bar if active
        if self.search_active {
            let search_area = Rect {
                x: inner.x,
                y: inner.y + inner.height.saturating_sub(1),
                width: inner.width,
                height: 1,
            };
            let search_text = format!("/{}", self.search_query);
            let search_para = Paragraph::new(search_text).style(Style::default().fg(theme.search));
            frame.render_widget(search_para, search_area);
        }
    }

    fn render_item(&self, item: &Item, is_selected: bool, theme: &Theme) -> ListItem<'_> {
        let indent = "  ".repeat(item.depth());

        let (icon, text, style) = match item {
            Item::Group {
                name,
                collapsed,
                session_count,
                ..
            } => {
                let icon = if *collapsed { "▶" } else { "▼" };
                let text = format!("{} ({}) ", name, session_count);
                let style = Style::default().fg(theme.group).bold();
                (icon, text, style)
            }
            Item::Session { id, .. } => {
                if let Some(inst) = self.instance_map.get(id) {
                    let icon = match inst.status {
                        Status::Running => "●",
                        Status::Waiting => "◐",
                        Status::Idle => "○",
                        Status::Error => "✕",
                        Status::Starting => "◌",
                    };
                    let color = match inst.status {
                        Status::Running => theme.running,
                        Status::Waiting => theme.waiting,
                        Status::Idle => theme.idle,
                        Status::Error => theme.error,
                        Status::Starting => theme.dimmed,
                    };
                    let style = Style::default().fg(color);
                    (icon, inst.title.clone(), style)
                } else {
                    ("?", id.clone(), Style::default().fg(theme.dimmed))
                }
            }
        };

        let line = Line::from(vec![
            Span::raw(indent),
            Span::styled(format!("{} ", icon), style),
            Span::styled(text, if is_selected { style.bold() } else { style }),
        ]);

        if is_selected {
            ListItem::new(line).style(Style::default().bg(theme.selection))
        } else {
            ListItem::new(line)
        }
    }

    fn render_preview(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .title(" Preview ")
            .title_style(Style::default().fg(theme.title));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        if let Some(id) = &self.selected_session {
            if let Some(inst) = self.instance_map.get(id) {
                Preview::render(frame, inner, inst, theme);
            }
        } else {
            let hint = Paragraph::new("Select a session to preview")
                .style(Style::default().fg(theme.dimmed))
                .alignment(Alignment::Center);
            frame.render_widget(hint, inner);
        }
    }
}
