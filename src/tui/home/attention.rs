//! Read-only full-screen view of the current operator attention queue.

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use super::OffdeskResumeSummary;
use crate::offdesk::OffdeskNextSafeAction;
use crate::tui::styles::Theme;

pub enum AttentionAction {
    Continue,
    Close,
}

pub struct AttentionView {
    summary: OffdeskResumeSummary,
    cursor: usize,
    detail_open: bool,
    detail_scroll: u16,
}

impl AttentionView {
    pub fn new(summary: &OffdeskResumeSummary) -> Self {
        Self {
            summary: summary.clone(),
            cursor: 0,
            detail_open: false,
            detail_scroll: 0,
        }
    }

    pub fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> AttentionAction {
        use crossterm::event::KeyCode;

        if self.detail_open {
            return match key.code {
                KeyCode::Char('q') | KeyCode::Char('a') => AttentionAction::Close,
                KeyCode::Esc | KeyCode::Enter | KeyCode::Backspace | KeyCode::Left => {
                    self.detail_open = false;
                    self.detail_scroll = 0;
                    AttentionAction::Continue
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.detail_scroll = self.detail_scroll.saturating_sub(1);
                    AttentionAction::Continue
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.detail_scroll = self.detail_scroll.saturating_add(1);
                    AttentionAction::Continue
                }
                KeyCode::Home | KeyCode::Char('g') => {
                    self.detail_scroll = 0;
                    AttentionAction::Continue
                }
                _ => AttentionAction::Continue,
            };
        }

        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('a') => AttentionAction::Close,
            KeyCode::Up | KeyCode::Char('k') => {
                self.cursor = self.cursor.saturating_sub(1);
                AttentionAction::Continue
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.cursor + 1 < self.summary.next_safe_actions.len() {
                    self.cursor += 1;
                }
                AttentionAction::Continue
            }
            KeyCode::Home | KeyCode::Char('g') => {
                self.cursor = 0;
                AttentionAction::Continue
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.cursor = self.summary.next_safe_actions.len().saturating_sub(1);
                AttentionAction::Continue
            }
            KeyCode::Enter if self.summary.next_safe_actions.get(self.cursor).is_some() => {
                self.detail_open = true;
                self.detail_scroll = 0;
                AttentionAction::Continue
            }
            _ => AttentionAction::Continue,
        }
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        if area.width == 0 || area.height == 0 {
            return;
        }

        frame.render_widget(Clear, area);
        let review_count = self
            .summary
            .next_safe_actions
            .iter()
            .filter(|action| action.requires_operator_review)
            .count();
        let title = format!(
            " Attention ({} actions · {} need review) ",
            self.summary.next_safe_actions.len(),
            review_count
        );
        let block = Block::default()
            .title(title)
            .title_style(Style::default().fg(theme.title).bold())
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let [overview_area, body_area, hint_area] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .areas(inner);
        self.render_overview(frame, overview_area, theme);
        self.render_body(frame, body_area, theme);
        let hint = if self.detail_open {
            " j/k: scroll · Enter/Esc: queue · a/q: close · read-only"
        } else {
            " Enter: details · j/k: move · a/Esc: close · read-only"
        };
        frame.render_widget(
            Line::from(Span::styled(hint, Style::default().fg(theme.hint))),
            hint_area,
        );
    }

    fn render_overview(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let state = if self.summary.needs_operator_attention() {
            "operator review required"
        } else if self.summary.has_offdesk_activity() {
            "work active; no review gate"
        } else {
            "clear; no work pending"
        };
        let state_style = if self.summary.needs_operator_attention() {
            Style::default().fg(theme.waiting).bold()
        } else {
            Style::default().fg(theme.text)
        };
        let label_style = Style::default().fg(theme.dimmed);
        let value_style = Style::default().fg(theme.text);
        let counts = format!(
            "approval {}  queued {}  active {}  resume {}  failed {}  closeout {}",
            self.summary.pending_approvals,
            self.summary.queued_tasks,
            self.summary.active_tasks,
            self.summary.resume_pending_tasks,
            self.summary.failed_tasks,
            self.summary.closeout_required
        );
        let lines = vec![
            Line::from(vec![
                Span::styled("State: ", label_style),
                Span::styled(state, state_style),
            ]),
            Line::from(vec![
                Span::styled("Focus: ", label_style),
                Span::styled(self.summary.focus_label(), state_style),
            ]),
            Line::from(vec![
                Span::styled("Work:  ", label_style),
                Span::styled(counts, value_style),
            ]),
        ];
        frame.render_widget(Paragraph::new(lines), area);
    }

    fn render_body(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        if area.height == 0 || area.width == 0 {
            return;
        }

        if self.detail_open {
            self.render_detail(frame, area, theme);
        } else if area.width >= 100 && area.height >= 8 {
            let [list_area, detail_area] =
                Layout::horizontal([Constraint::Percentage(42), Constraint::Percentage(58)])
                    .areas(area);
            self.render_list(frame, list_area, theme);
            self.render_detail(frame, detail_area, theme);
        } else if area.height >= 10 {
            let list_height = (area.height / 3).max(3);
            let [list_area, detail_area] =
                Layout::vertical([Constraint::Length(list_height), Constraint::Min(1)]).areas(area);
            self.render_list(frame, list_area, theme);
            self.render_detail(frame, detail_area, theme);
        } else {
            self.render_list(frame, area, theme);
        }
    }

    fn render_list(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title(" Queue ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        if self.summary.next_safe_actions.is_empty() {
            frame.render_widget(
                Paragraph::new("No operator action is currently queued.")
                    .style(Style::default().fg(theme.dimmed))
                    .alignment(Alignment::Center),
                inner,
            );
            return;
        }

        let items: Vec<ListItem> = self
            .summary
            .next_safe_actions
            .iter()
            .map(|action| {
                let rail_style = if action.requires_operator_review {
                    Style::default().fg(theme.waiting).bold()
                } else {
                    Style::default().fg(theme.border)
                };
                ListItem::new(Line::from(vec![
                    Span::styled("▌ ", rail_style),
                    Span::styled(
                        format!("{:<10}", action_kind_label(&action.kind)),
                        Style::default().fg(if action.requires_operator_review {
                            theme.waiting
                        } else {
                            theme.accent
                        }),
                    ),
                    Span::styled(primary_action_text(action), Style::default().fg(theme.text)),
                ]))
            })
            .collect();
        let list = List::new(items).highlight_style(
            Style::default()
                .bg(theme.selection)
                .add_modifier(Modifier::BOLD),
        );
        let mut state = ListState::default();
        state.select(Some(self.cursor));
        frame.render_stateful_widget(list, inner, &mut state);
    }

    fn render_detail(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title(" Selected action ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let Some(action) = self.summary.next_safe_actions.get(self.cursor) else {
            frame.render_widget(
                Paragraph::new("The queue is clear. Reopen this panel after state changes.")
                    .style(Style::default().fg(theme.dimmed))
                    .alignment(Alignment::Center),
                inner,
            );
            return;
        };

        let label_style = Style::default().fg(theme.dimmed);
        let value_style = Style::default().fg(theme.text);
        let command_style = Style::default().fg(theme.accent);
        let boundary_style = Style::default().fg(theme.waiting);
        let mut lines = vec![
            Line::from(vec![
                Span::styled("Action: ", label_style),
                Span::styled(action_kind_label(&action.kind), boundary_style.bold()),
            ]),
            Line::from(Span::styled(action.detail.clone(), value_style)),
            Line::from(vec![
                Span::styled("Scope: ", label_style),
                Span::styled(action.scope.clone(), value_style),
            ]),
            Line::from(vec![
                Span::styled("Review: ", label_style),
                Span::styled(
                    if action.requires_operator_review {
                        "required"
                    } else {
                        "not required"
                    },
                    if action.requires_operator_review {
                        boundary_style
                    } else {
                        value_style
                    },
                ),
            ]),
        ];
        if action.commands.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("Command: ", label_style),
                Span::styled("none", value_style),
            ]));
        } else {
            lines.push(Line::from(Span::styled(
                "Commands (not executed):",
                label_style,
            )));
            lines.extend(
                action
                    .commands
                    .iter()
                    .map(|command| Line::from(Span::styled(format!("› {command}"), command_style))),
            );
        }
        lines.push(Line::from(Span::styled(
            "Boundary: this panel is read-only.",
            boundary_style,
        )));
        if let Some(boundary) = action.does_not_authorize.first() {
            lines.push(Line::from(Span::styled(
                format!("Does not authorize: {boundary}"),
                Style::default().fg(theme.dimmed),
            )));
        }

        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: true })
                .scroll((self.detail_scroll, 0)),
            inner,
        );
    }
}

fn action_kind_label(kind: &str) -> &'static str {
    match kind {
        "approval_pending" | "approval_expired" | "approval_denied" => "APPROVAL",
        "recovery_required" | "resume_review_required" | "result_artifact_missing" => "RECOVERY",
        "review_required" | "closeout_check" => "REVIEW",
        "provider_attention" => "PROVIDER",
        "runtime_monitoring" => "MONITOR",
        "dispatch_pending" => "DISPATCH",
        "cancelled" => "ARCHIVED",
        _ => "NOTICE",
    }
}

fn primary_action_text(action: &OffdeskNextSafeAction) -> String {
    action
        .commands
        .first()
        .cloned()
        .unwrap_or_else(|| action.detail.clone())
}
