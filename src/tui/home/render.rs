//! Rendering for HomeView

use ratatui::prelude::*;
use ratatui::widgets::*;
use std::time::Instant;

use super::{
    get_indent, HomeView, ViewMode, ICON_COLLAPSED, ICON_DELETING, ICON_ERROR, ICON_EXPANDED,
    ICON_IDLE, ICON_RUNNING, ICON_STARTING, ICON_WAITING,
};
use crate::session::{Item, Status};
use crate::tui::components::{HelpOverlay, Preview};
use crate::tui::styles::Theme;
use crate::update::UpdateInfo;

fn truncate_tab_label(value: &str, max_chars: usize) -> String {
    let len = value.chars().count();
    if len <= max_chars {
        return value.to_string();
    }

    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }

    format!(
        "{}...",
        value.chars().take(max_chars - 3).collect::<String>()
    )
}

impl HomeView {
    pub fn render(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        theme: &Theme,
        update_info: Option<&UpdateInfo>,
    ) {
        // Settings view takes over the whole screen
        if let Some(ref mut settings) = self.settings_view {
            settings.render(frame, area, theme);
            // Render unsaved changes confirmation dialog over settings
            if self.settings_close_confirm {
                if let Some(dialog) = &self.confirm_dialog {
                    dialog.render(frame, area, theme);
                }
            }
            return;
        }

        // Projects view takes over the whole screen
        if let Some(ref mut projects) = self.projects_view {
            projects.render(frame, area, theme);
            return;
        }

        // Diff view takes over the whole screen
        if let Some(ref mut diff) = self.diff_view {
            // Compute diff for selected file if not cached
            let _ = diff.get_current_diff();

            diff.render(frame, area, theme);
            return;
        }

        // Layout: main area + status bar + optional update bar at bottom
        let constraints = if update_info.is_some() {
            vec![
                Constraint::Min(0),
                Constraint::Length(1),
                Constraint::Length(1),
            ]
        } else {
            vec![Constraint::Min(0), Constraint::Length(1)]
        };
        let main_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(area);

        // Layout: left panel (list) and right panel (preview)
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(self.list_width), Constraint::Min(40)])
            .split(main_chunks[0]);

        self.render_list(frame, chunks[0], theme);
        self.render_preview(frame, chunks[1], theme);
        self.render_status_bar(frame, main_chunks[1], theme);

        if let Some(info) = update_info {
            self.render_update_bar(frame, main_chunks[2], theme, info);
        }

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

        if let Some(dialog) = &self.unified_delete_dialog {
            dialog.render(frame, area, theme);
        }

        if let Some(dialog) = &self.group_delete_options_dialog {
            dialog.render(frame, area, theme);
        }

        if let Some(dialog) = &self.rename_dialog {
            dialog.render(frame, area, theme);
        }

        if let Some(dialog) = &self.hook_trust_dialog {
            dialog.render(frame, area, theme);
        }

        if let Some(dialog) = &self.welcome_dialog {
            dialog.render(frame, area, theme);
        }

        if let Some(dialog) = &self.changelog_dialog {
            dialog.render(frame, area, theme);
        }

        if let Some(dialog) = &self.info_dialog {
            dialog.render(frame, area, theme);
        }
    }

    fn render_list(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let title = match self.view_mode {
            ViewMode::Agent => format!(" Forager [{}] ", self.storage.profile()),
            ViewMode::Terminal => format!(" Terminals [{}] ", self.storage.profile()),
        };
        let (border_color, title_color) = match self.view_mode {
            ViewMode::Agent => (theme.border, theme.title),
            ViewMode::Terminal => (theme.terminal_border, theme.terminal_border),
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color))
            .title(title)
            .title_style(Style::default().fg(title_color).bold());

        let inner = block.inner(area);
        frame.render_widget(block, area);

        if self.instances.is_empty() && self.groups.is_empty() {
            let empty_text = vec![
                Line::from(""),
                Line::from("No sessions yet").style(Style::default().fg(theme.dimmed)),
                Line::from(""),
                Line::from("Press 'n' to create one").style(Style::default().fg(theme.hint)),
                Line::from("or 'forager add .'").style(Style::default().fg(theme.hint)),
            ];
            let para = Paragraph::new(empty_text).alignment(Alignment::Center);
            frame.render_widget(para, inner);
            return;
        }

        let indices: Vec<usize> = if let Some(ref filtered) = self.filtered_items {
            filtered.clone()
        } else {
            (0..self.flat_items.len()).collect()
        };

        let list_items: Vec<ListItem> = indices
            .iter()
            .enumerate()
            .filter_map(|(display_idx, &item_idx)| {
                self.flat_items.get(item_idx).map(|item| {
                    let is_selected = display_idx == self.cursor;
                    self.render_item(item, is_selected, theme)
                })
            })
            .collect();

        let list =
            List::new(list_items).highlight_style(Style::default().bg(theme.session_selection));

        frame.render_widget(list, inner);

        // Render search bar if active
        if self.search_active {
            let search_area = Rect {
                x: inner.x,
                y: inner.y + inner.height.saturating_sub(1),
                width: inner.width,
                height: 1,
            };

            let value = self.search_query.value();
            let cursor_pos = self.search_query.visual_cursor();
            let cursor_style = Style::default().fg(theme.background).bg(theme.search);
            let text_style = Style::default().fg(theme.search);

            // Split value into: before cursor, char at cursor, after cursor
            let before: String = value.chars().take(cursor_pos).collect();
            let cursor_char: String = value
                .chars()
                .nth(cursor_pos)
                .map(|c| c.to_string())
                .unwrap_or_else(|| " ".to_string());
            let after: String = value.chars().skip(cursor_pos + 1).collect();

            let mut spans = vec![Span::styled("/", text_style)];
            if !before.is_empty() {
                spans.push(Span::styled(before, text_style));
            }
            spans.push(Span::styled(cursor_char, cursor_style));
            if !after.is_empty() {
                spans.push(Span::styled(after, text_style));
            }

            frame.render_widget(Paragraph::new(Line::from(spans)), search_area);
        }
    }

    fn render_item(&self, item: &Item, is_selected: bool, theme: &Theme) -> ListItem<'_> {
        let indent = get_indent(item.depth());

        use std::borrow::Cow;

        let (icon, text, style): (&str, Cow<str>, Style) = match item {
            Item::Group {
                name,
                collapsed,
                session_count,
                ..
            } => {
                let icon = if *collapsed {
                    ICON_COLLAPSED
                } else {
                    ICON_EXPANDED
                };
                let text = Cow::Owned(format!("{} ({})", name, session_count));
                let style = Style::default().fg(theme.group).bold();
                (icon, text, style)
            }
            Item::Session { id, .. } => {
                if let Some(inst) = self.instance_map.get(id) {
                    match self.view_mode {
                        ViewMode::Agent => {
                            let icon = match inst.status {
                                Status::Running => ICON_RUNNING,
                                Status::Waiting => ICON_WAITING,
                                Status::Idle => ICON_IDLE,
                                Status::Stopped => ICON_IDLE,
                                Status::Error => ICON_ERROR,
                                Status::Starting => ICON_STARTING,
                                Status::Deleting => ICON_DELETING,
                            };
                            let color = match inst.status {
                                Status::Running => theme.running,
                                Status::Waiting => theme.waiting,
                                Status::Idle => theme.idle,
                                Status::Stopped => theme.dimmed,
                                Status::Error => theme.error,
                                Status::Starting => theme.dimmed,
                                Status::Deleting => theme.waiting,
                            };
                            let style = Style::default().fg(color);
                            (icon, Cow::Borrowed(&inst.title), style)
                        }
                        ViewMode::Terminal => {
                            let terminal_running = inst
                                .terminal_tmux_session()
                                .map(|s| s.exists())
                                .unwrap_or(false);
                            let (icon, color) = if terminal_running {
                                (ICON_RUNNING, theme.terminal_active)
                            } else {
                                (ICON_IDLE, theme.dimmed)
                            };
                            let style = Style::default().fg(color);
                            (icon, Cow::Borrowed(&inst.title), style)
                        }
                    }
                } else {
                    (
                        "?",
                        Cow::Borrowed(id.as_str()),
                        Style::default().fg(theme.dimmed),
                    )
                }
            }
        };

        let mut line_spans = Vec::with_capacity(5);
        line_spans.push(Span::raw(indent));
        line_spans.push(Span::styled(format!("{} ", icon), style));
        line_spans.push(Span::styled(
            text.into_owned(),
            if is_selected { style.bold() } else { style },
        ));

        if let Item::Session { id, .. } = item {
            if let Some(inst) = self.instance_map.get(id) {
                if let Some(wt_info) = &inst.worktree_info {
                    line_spans.push(Span::styled(
                        format!("  {}", wt_info.branch),
                        Style::default().fg(Color::Cyan),
                    ));
                }
                if inst.is_sandboxed() {
                    line_spans.push(Span::styled(
                        " [legacy sandbox]",
                        Style::default().fg(Color::Magenta),
                    ));
                }
            }
        }

        let line = Line::from(line_spans);

        if is_selected {
            ListItem::new(line).style(Style::default().bg(theme.session_selection))
        } else {
            ListItem::new(line)
        }
    }

    /// Refresh preview cache if needed (session changed, dimensions changed, or timer expired)
    fn refresh_preview_cache_if_needed(&mut self, width: u16, height: u16) {
        const PREVIEW_REFRESH_MS: u128 = 250; // Refresh preview 4x/second max

        let needs_refresh = match &self.selected_session {
            Some(id) => {
                self.preview_cache.session_id.as_ref() != Some(id)
                    || self.preview_cache.dimensions != (width, height)
                    || self.preview_cache.last_refresh.elapsed().as_millis() > PREVIEW_REFRESH_MS
            }
            None => false,
        };

        if needs_refresh {
            if let Some(id) = &self.selected_session {
                if let Some(inst) = self.instance_map.get(id) {
                    self.preview_cache.content = inst
                        .capture_output_with_size(height as usize, width, height)
                        .unwrap_or_default();
                    self.preview_cache.session_id = Some(id.clone());
                    self.preview_cache.dimensions = (width, height);
                    self.preview_cache.last_refresh = Instant::now();
                }
            }
        }
    }

    /// Refresh terminal preview cache if needed (for host terminals)
    fn refresh_terminal_preview_cache_if_needed(&mut self, width: u16, height: u16) {
        const PREVIEW_REFRESH_MS: u128 = 250;

        let needs_refresh = match &self.selected_session {
            Some(id) => {
                self.terminal_preview_cache.session_id.as_ref() != Some(id)
                    || self.terminal_preview_cache.dimensions != (width, height)
                    || self
                        .terminal_preview_cache
                        .last_refresh
                        .elapsed()
                        .as_millis()
                        > PREVIEW_REFRESH_MS
            }
            None => false,
        };

        if needs_refresh {
            if let Some(id) = &self.selected_session {
                if let Some(inst) = self.instance_map.get(id) {
                    self.terminal_preview_cache.content = inst
                        .terminal_tmux_session()
                        .and_then(|s| s.capture_pane(height as usize))
                        .unwrap_or_default();
                    self.terminal_preview_cache.session_id = Some(id.clone());
                    self.terminal_preview_cache.dimensions = (width, height);
                    self.terminal_preview_cache.last_refresh = Instant::now();
                }
            }
        }
    }

    fn render_session_switcher_bar(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        if area.width == 0 || area.height == 0 {
            return;
        }

        let label_style = Style::default().fg(theme.dimmed);
        let key_style = Style::default().fg(theme.accent).bold();
        let session_style = Style::default().fg(theme.text);
        let selected_style = Style::default()
            .fg(theme.background)
            .bg(theme.accent)
            .bold();

        let session_ids = self.visible_session_ids();
        let mut spans = vec![Span::styled(" Sessions ", label_style)];

        if session_ids.is_empty() {
            spans.push(Span::styled("None", Style::default().fg(theme.dimmed)));
        } else {
            for (idx, id) in session_ids.iter().enumerate() {
                if let Some(inst) = self.instance_map.get(id) {
                    spans.push(Span::raw(" "));

                    let label = format!("{}:{}", idx + 1, truncate_tab_label(&inst.title, 14));
                    let is_selected = self.selected_session.as_deref() == Some(id.as_str());
                    let style = if is_selected {
                        selected_style
                    } else {
                        session_style
                    };
                    spans.push(Span::styled(format!("[{}]", label), style));
                }
            }

            spans.extend([
                Span::raw("  "),
                Span::styled("1..9/Alt+1..9", key_style),
                Span::styled("/", label_style),
                Span::styled("Tab/Ctrl+Tab", key_style),
            ]);
        }

        let bar = Paragraph::new(Line::from(spans)).style(Style::default().bg(theme.selection));
        frame.render_widget(bar, area);
    }

    fn render_preview(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let title = match self.view_mode {
            ViewMode::Agent => " Preview ",
            ViewMode::Terminal => " Terminal Preview ",
        };
        let (border_color, title_color) = match self.view_mode {
            ViewMode::Agent => (theme.border, theme.title),
            ViewMode::Terminal => (theme.terminal_border, theme.terminal_border),
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color))
            .title(title)
            .title_style(Style::default().fg(title_color));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Reserve one row for quick session tabs when there is enough space.
        let show_switcher = inner.height >= 2;
        let content_area = if show_switcher {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(1), Constraint::Min(1)])
                .split(inner);
            self.render_session_switcher_bar(frame, chunks[0], theme);
            chunks[1]
        } else {
            inner
        };

        let content_area = self.render_offdesk_morning_review_if_needed(frame, content_area, theme);

        match self.view_mode {
            ViewMode::Agent => {
                // Refresh cache before borrowing from instance_map to avoid borrow conflicts
                self.refresh_preview_cache_if_needed(content_area.width, content_area.height);

                if let Some(id) = &self.selected_session {
                    if let Some(inst) = self.instance_map.get(id) {
                        Preview::render_with_cache(
                            frame,
                            content_area,
                            inst,
                            &self.preview_cache.content,
                            theme,
                        );
                    }
                } else {
                    let hint = Paragraph::new("Select a session to preview")
                        .style(Style::default().fg(theme.dimmed))
                        .alignment(Alignment::Center);
                    frame.render_widget(hint, content_area);
                }
            }
            ViewMode::Terminal => {
                let selected_id = self.selected_session.clone();

                if let Some(id) = selected_id {
                    self.refresh_terminal_preview_cache_if_needed(
                        content_area.width,
                        content_area.height,
                    );

                    if let Some(inst) = self.instance_map.get(&id) {
                        let terminal_running = inst
                            .terminal_tmux_session()
                            .map(|s| s.exists())
                            .unwrap_or(false);

                        Preview::render_terminal_preview(
                            frame,
                            content_area,
                            inst,
                            terminal_running,
                            &self.terminal_preview_cache.content,
                            theme,
                        );
                    }
                } else {
                    let hint = Paragraph::new("Select a session to preview terminal")
                        .style(Style::default().fg(theme.dimmed))
                        .alignment(Alignment::Center);
                    frame.render_widget(hint, content_area);
                }
            }
        }
    }

    fn render_offdesk_morning_review_if_needed(
        &self,
        frame: &mut Frame,
        area: Rect,
        theme: &Theme,
    ) -> Rect {
        const REVIEW_HEIGHT: u16 = 7;

        if !self.offdesk_resume.has_morning_review() || area.height < REVIEW_HEIGHT + 3 {
            return area;
        }

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(REVIEW_HEIGHT), Constraint::Min(1)])
            .split(area);

        self.render_offdesk_morning_review(frame, chunks[0], theme);
        chunks[1]
    }

    fn render_offdesk_morning_review(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let summary = &self.offdesk_resume;
        let accent_style = if summary.needs_operator_attention() {
            Style::default().fg(theme.waiting).bold()
        } else if summary.active_tasks > 0 {
            Style::default().fg(theme.running).bold()
        } else {
            Style::default().fg(theme.dimmed)
        };
        let label_style = Style::default().fg(theme.dimmed);
        let value_style = Style::default().fg(theme.text);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .title(" Offdesk Morning Review ")
            .title_style(Style::default().fg(theme.title).bold());
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let lines = vec![
            Line::from(vec![
                Span::styled("Focus: ", label_style),
                Span::styled(summary.focus_label(), accent_style),
            ]),
            Line::from(vec![
                Span::styled("Work:  ", label_style),
                Span::styled(
                    format!(
                        "approval {}  queued {}  active {}",
                        summary.pending_approvals, summary.queued_tasks, summary.active_tasks
                    ),
                    value_style,
                ),
            ]),
            Line::from(vec![
                Span::styled("Review: ", label_style),
                Span::styled(
                    format!(
                        "resume {}  failed {}  cancelled {}",
                        summary.resume_pending_tasks, summary.failed_tasks, summary.cancelled_tasks
                    ),
                    value_style,
                ),
            ]),
            Line::from(vec![
                Span::styled("Closeout: ", label_style),
                Span::styled(
                    summary.closeout_label(),
                    if summary.closeout_required > 0 {
                        accent_style
                    } else {
                        value_style
                    },
                ),
            ]),
            Line::from(vec![
                Span::styled("Next action: ", label_style),
                Span::styled(summary.next_action_label(), accent_style),
            ]),
        ];

        frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), inner);
    }

    fn render_status_bar(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let key_style = Style::default().fg(theme.accent).bold();
        let desc_style = Style::default().fg(theme.dimmed);
        let sep_style = Style::default().fg(theme.border);

        let (mode_indicator, mode_color) = match self.view_mode {
            ViewMode::Agent => ("[Agent]", theme.waiting),
            ViewMode::Terminal => ("[Term]", theme.terminal_border),
        };
        let mode_style = Style::default().fg(mode_color).bold();

        let mut spans = vec![
            Span::styled(format!(" {} ", mode_indicator), mode_style),
            Span::styled("│", sep_style),
            Span::styled(" j/k", key_style),
            Span::styled(" Nav ", desc_style),
        ];
        if let Some(enter_action_text) = match self.flat_items.get(self.cursor) {
            Some(Item::Group {
                collapsed: true, ..
            }) => Some(" Expand "),
            Some(Item::Group {
                collapsed: false, ..
            }) => Some(" Collapse "),
            Some(Item::Session { .. }) => Some(" Attach "),
            None => None,
        } {
            spans.extend([
                Span::styled("│", sep_style),
                Span::styled(" Enter", key_style),
                Span::styled(enter_action_text, desc_style),
            ])
        }
        spans.extend([
            Span::styled("│", sep_style),
            Span::styled(" t", key_style),
            Span::styled(" View ", desc_style),
        ]);

        if self.orchestration.autonomy_armed {
            spans.extend([
                Span::styled("│", sep_style),
                Span::styled(
                    " Autonomy ARMED ",
                    Style::default().fg(theme.waiting).bold(),
                ),
            ]);
        }
        if self.orchestration.wiki_candidates > 0 {
            spans.extend([
                Span::styled("│", sep_style),
                Span::styled(
                    format!(" Wiki cand {} ", self.orchestration.wiki_candidates),
                    Style::default().fg(theme.accent),
                ),
            ]);
        }

        if self.offdesk_resume.fresh_pending > 0 || self.offdesk_resume.stale_pending > 0 {
            let resume_style = if self.offdesk_resume.fresh_pending > 0 {
                Style::default().fg(theme.waiting).bold()
            } else {
                Style::default().fg(theme.dimmed)
            };
            spans.extend([
                Span::styled("│", sep_style),
                Span::styled(
                    format!(
                        " Resume {}/{} ",
                        self.offdesk_resume.fresh_pending, self.offdesk_resume.stale_pending
                    ),
                    resume_style,
                ),
            ]);
        }

        if self.offdesk_resume.has_offdesk_activity() {
            let offdesk_style = if self.offdesk_resume.needs_operator_attention() {
                Style::default().fg(theme.waiting).bold()
            } else {
                Style::default().fg(theme.dimmed)
            };
            spans.extend([
                Span::styled("│", sep_style),
                Span::styled(
                    format!(
                        " Offdesk approval {} active {} failed {} closeout {} ",
                        self.offdesk_resume.pending_approvals,
                        self.offdesk_resume.active_tasks,
                        self.offdesk_resume.failed_tasks,
                        self.offdesk_resume.closeout_required
                    ),
                    offdesk_style,
                ),
            ]);
        }

        if let Some(command) = self.offdesk_resume.next_action_command() {
            let action_style = if self.offdesk_resume.needs_operator_attention() {
                Style::default().fg(theme.waiting).bold()
            } else {
                Style::default().fg(theme.dimmed)
            };
            spans.extend([
                Span::styled("│", sep_style),
                Span::styled(format!(" Action {command} "), action_style),
            ]);
        }

        spans.extend([
            Span::styled("│", sep_style),
            Span::styled(" Tab", key_style),
            Span::styled(" Next ", desc_style),
            Span::styled("│", sep_style),
            Span::styled(" 1..9", key_style),
            Span::styled(" Jump ", desc_style),
        ]);

        spans.extend([
            Span::styled("│", sep_style),
            Span::styled(" n", key_style),
            Span::styled(" New ", desc_style),
        ]);

        if !self.flat_items.is_empty() {
            spans.extend([
                Span::styled("│", sep_style),
                Span::styled(" d", key_style),
                Span::styled(" Del ", desc_style),
            ]);
        }

        spans.extend([
            Span::styled("│", sep_style),
            Span::styled(" /", key_style),
            Span::styled(" Search ", desc_style),
            Span::styled("│", sep_style),
            Span::styled(" D", key_style),
            Span::styled(" Diff ", desc_style),
            Span::styled("│", sep_style),
            Span::styled(" ?", key_style),
            Span::styled(" Help ", desc_style),
            Span::styled("│", sep_style),
            Span::styled(" q", key_style),
            Span::styled(" Quit", desc_style),
        ]);

        let status = Paragraph::new(Line::from(spans)).style(Style::default().bg(theme.selection));
        frame.render_widget(status, area);
    }

    fn render_update_bar(&self, frame: &mut Frame, area: Rect, theme: &Theme, info: &UpdateInfo) {
        let update_style = Style::default().fg(theme.waiting).bold();
        let text = format!(
            " update available {} -> {}",
            info.current_version, info.latest_version
        );
        let bar = Paragraph::new(Line::from(Span::styled(text, update_style)))
            .style(Style::default().bg(theme.selection));
        frame.render_widget(bar, area);
    }
}
