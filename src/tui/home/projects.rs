//! Full-screen project registry view.
//!
//! The home list is session-centric, so registered projects without a
//! session were invisible in the TUI. This view lists every project in the
//! registry with its live sessions; Enter jumps to a project's first session
//! or, for session-less projects, shows the exact `forager go` onboarding
//! command.

use std::path::Path;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState};
use ratatui::Frame;

use crate::session::project_registry::{load_registry, resolve_project_for_path};
use crate::session::{Instance, Status};
use crate::tui::styles::Theme;

pub enum ProjectsAction {
    Continue,
    Close,
    JumpToSession(String),
    ShowHint(String, String),
}

struct ProjectRow {
    key: String,
    display_name: String,
    wiki_profile: Option<String>,
    pattern: String,
    running: usize,
    waiting: usize,
    total: usize,
    first_session_id: Option<String>,
}

pub struct ProjectsView {
    rows: Vec<ProjectRow>,
    cursor: usize,
}

impl ProjectsView {
    pub fn new(instances: &[Instance]) -> Self {
        let registry = load_registry();
        let mut rows: Vec<ProjectRow> = registry
            .iter()
            .map(|entry| {
                let sessions: Vec<&Instance> = instances
                    .iter()
                    .filter(|inst| {
                        resolve_project_for_path(Path::new(&inst.project_path), &registry)
                            .map(|found| found.key == entry.key)
                            .unwrap_or(false)
                    })
                    .collect();
                ProjectRow {
                    key: entry.key.clone(),
                    display_name: entry.display_name.clone(),
                    wiki_profile: entry.wiki_profile.clone(),
                    pattern: entry
                        .workspace_patterns
                        .first()
                        .cloned()
                        .unwrap_or_default(),
                    running: sessions
                        .iter()
                        .filter(|s| matches!(s.status, Status::Running | Status::Starting))
                        .count(),
                    waiting: sessions
                        .iter()
                        .filter(|s| matches!(s.status, Status::Waiting))
                        .count(),
                    total: sessions.len(),
                    first_session_id: sessions.first().map(|s| s.id.clone()),
                }
            })
            .collect();
        // Active projects first, then alphabetical, so the list reads as
        // "what is live" before "what exists".
        rows.sort_by(|a, b| {
            (b.total > 0).cmp(&(a.total > 0)).then_with(|| {
                a.display_name
                    .to_lowercase()
                    .cmp(&b.display_name.to_lowercase())
            })
        });
        Self { rows, cursor: 0 }
    }

    pub fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> ProjectsAction {
        use crossterm::event::KeyCode;
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('p') => ProjectsAction::Close,
            KeyCode::Up | KeyCode::Char('k') => {
                self.cursor = self.cursor.saturating_sub(1);
                ProjectsAction::Continue
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.cursor + 1 < self.rows.len() {
                    self.cursor += 1;
                }
                ProjectsAction::Continue
            }
            KeyCode::Home | KeyCode::Char('g') => {
                self.cursor = 0;
                ProjectsAction::Continue
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.cursor = self.rows.len().saturating_sub(1);
                ProjectsAction::Continue
            }
            KeyCode::Enter => match self.rows.get(self.cursor) {
                Some(row) => match &row.first_session_id {
                    Some(id) => ProjectsAction::JumpToSession(id.clone()),
                    None => ProjectsAction::ShowHint(
                        row.display_name.clone(),
                        format!(
                            "No session yet. Onboard it with:\n\ncd <workspace>/{}\nforager go",
                            row.pattern
                        ),
                    ),
                },
                None => ProjectsAction::Continue,
            },
            _ => ProjectsAction::Continue,
        }
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) {
        frame.render_widget(Clear, area);
        let with_sessions = self.rows.iter().filter(|r| r.total > 0).count();
        let title = format!(
            " Projects ({} registered · {} with sessions) ",
            self.rows.len(),
            with_sessions
        );
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let [list_area, hint_area] =
            Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);

        let items: Vec<ListItem> = self
            .rows
            .iter()
            .map(|row| {
                let mut spans = vec![Span::styled(
                    format!("{:<28}", truncate(&row.display_name, 27)),
                    Style::default().fg(if row.total > 0 {
                        theme.text
                    } else {
                        theme.dimmed
                    }),
                )];
                if row.total > 0 {
                    if row.running > 0 {
                        spans.push(Span::styled(
                            format!("{} running ", row.running),
                            Style::default().fg(theme.running),
                        ));
                    }
                    if row.waiting > 0 {
                        spans.push(Span::styled(
                            format!("{} waiting ", row.waiting),
                            Style::default().fg(theme.waiting),
                        ));
                    }
                    spans.push(Span::styled(
                        format!("{} total", row.total),
                        Style::default().fg(theme.dimmed),
                    ));
                } else {
                    spans.push(Span::styled(
                        "no session".to_string(),
                        Style::default().fg(theme.dimmed),
                    ));
                }
                spans.push(Span::styled(
                    format!("  wiki:{}", row.wiki_profile.as_deref().unwrap_or("-")),
                    Style::default().fg(theme.hint),
                ));
                spans.push(Span::styled(
                    format!("  [{}]", row.key),
                    Style::default().fg(theme.hint),
                ));
                ListItem::new(Line::from(spans))
            })
            .collect();
        let list = List::new(items).highlight_style(
            Style::default()
                .bg(theme.selection)
                .add_modifier(Modifier::BOLD),
        );
        let mut state = ListState::default();
        state.select(Some(self.cursor));
        frame.render_stateful_widget(list, list_area, &mut state);

        frame.render_widget(
            Line::from(Span::styled(
                " Enter: open session / onboarding hint · j/k: move · p/Esc: close",
                Style::default().fg(theme.hint),
            )),
            hint_area,
        );
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}
