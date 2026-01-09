//! New session dialog

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use super::DialogResult;
use crate::tui::styles::Theme;

pub struct NewSessionData {
    pub title: String,
    pub path: String,
    pub group: String,
    pub command: String,
}

pub struct NewSessionDialog {
    title: String,
    path: String,
    group: String,
    command: String,
    focused_field: usize,
}

impl NewSessionDialog {
    pub fn new() -> Self {
        let current_dir = std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        Self {
            title: String::new(),
            path: current_dir,
            group: String::new(),
            command: String::new(),
            focused_field: 0,
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> DialogResult<NewSessionData> {
        match key.code {
            KeyCode::Esc => DialogResult::Cancel,
            KeyCode::Enter => {
                if self.title.is_empty() {
                    // Use directory name as title
                    self.title = std::path::Path::new(&self.path)
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| "untitled".to_string());
                }
                DialogResult::Submit(NewSessionData {
                    title: self.title.clone(),
                    path: self.path.clone(),
                    group: self.group.clone(),
                    command: self.command.clone(),
                })
            }
            KeyCode::Tab => {
                self.focused_field = (self.focused_field + 1) % 4;
                DialogResult::Continue
            }
            KeyCode::BackTab => {
                self.focused_field = if self.focused_field == 0 {
                    3
                } else {
                    self.focused_field - 1
                };
                DialogResult::Continue
            }
            KeyCode::Backspace => {
                self.current_field_mut().pop();
                DialogResult::Continue
            }
            KeyCode::Char(c) => {
                self.current_field_mut().push(c);
                DialogResult::Continue
            }
            _ => DialogResult::Continue,
        }
    }

    fn current_field_mut(&mut self) -> &mut String {
        match self.focused_field {
            0 => &mut self.title,
            1 => &mut self.path,
            2 => &mut self.group,
            3 => &mut self.command,
            _ => &mut self.title,
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        // Center the dialog
        let dialog_width = 60;
        let dialog_height = 14;
        let x = area.x + (area.width.saturating_sub(dialog_width)) / 2;
        let y = area.y + (area.height.saturating_sub(dialog_height)) / 2;

        let dialog_area = Rect {
            x,
            y,
            width: dialog_width.min(area.width),
            height: dialog_height.min(area.height),
        };

        // Clear background
        let clear = Clear;
        frame.render_widget(clear, dialog_area);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.accent))
            .title(" New Session ")
            .title_style(Style::default().fg(theme.title).bold());

        let inner = block.inner(dialog_area);
        frame.render_widget(block, dialog_area);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(2),
                Constraint::Length(2),
                Constraint::Length(2),
                Constraint::Min(1),
            ])
            .split(inner);

        // Fields
        let fields = [
            ("Title:", &self.title),
            ("Path:", &self.path),
            ("Group:", &self.group),
            ("Command:", &self.command),
        ];

        for (idx, (label, value)) in fields.iter().enumerate() {
            let is_focused = idx == self.focused_field;
            let style = if is_focused {
                Style::default().fg(theme.accent)
            } else {
                Style::default().fg(theme.text)
            };

            let display_value = if value.is_empty() {
                match idx {
                    0 => "(directory name)",
                    3 => "(default: claude)",
                    _ => "",
                }
            } else {
                value.as_str()
            };

            let text = format!("{} {}", label, display_value);
            let cursor = if is_focused { "█" } else { "" };
            let line = Line::from(vec![
                Span::styled(text, style),
                Span::styled(cursor, Style::default().fg(theme.accent)),
            ]);

            frame.render_widget(Paragraph::new(line), chunks[idx]);
        }

        // Hint
        let hint = Line::from(vec![
            Span::styled("Tab", Style::default().fg(theme.hint)),
            Span::raw(" next field  "),
            Span::styled("Enter", Style::default().fg(theme.hint)),
            Span::raw(" create  "),
            Span::styled("Esc", Style::default().fg(theme.hint)),
            Span::raw(" cancel"),
        ]);
        frame.render_widget(Paragraph::new(hint), chunks[4]);
    }
}

impl Default for NewSessionDialog {
    fn default() -> Self {
        Self::new()
    }
}
