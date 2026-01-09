//! Preview panel component

use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::session::Instance;
use crate::tui::styles::Theme;

pub struct Preview;

impl Preview {
    pub fn render(frame: &mut Frame, area: Rect, instance: &Instance, theme: &Theme) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6), // Info section
                Constraint::Min(1),    // Output section
            ])
            .split(area);

        Self::render_info(frame, chunks[0], instance, theme);
        Self::render_output(frame, chunks[1], instance, theme);
    }

    fn render_info(frame: &mut Frame, area: Rect, instance: &Instance, theme: &Theme) {
        let info_lines = vec![
            Line::from(vec![
                Span::styled("Title:   ", Style::default().fg(theme.dimmed)),
                Span::styled(&instance.title, Style::default().fg(theme.text).bold()),
            ]),
            Line::from(vec![
                Span::styled("Path:    ", Style::default().fg(theme.dimmed)),
                Span::styled(
                    shorten_path(&instance.project_path),
                    Style::default().fg(theme.text),
                ),
            ]),
            Line::from(vec![
                Span::styled("Tool:    ", Style::default().fg(theme.dimmed)),
                Span::styled(&instance.tool, Style::default().fg(theme.accent)),
            ]),
            Line::from(vec![
                Span::styled("Status:  ", Style::default().fg(theme.dimmed)),
                Span::styled(
                    format!("{:?}", instance.status),
                    Style::default().fg(match instance.status {
                        crate::session::Status::Running => theme.running,
                        crate::session::Status::Waiting => theme.waiting,
                        crate::session::Status::Idle => theme.idle,
                        crate::session::Status::Error => theme.error,
                        crate::session::Status::Starting => theme.dimmed,
                    }),
                ),
            ]),
            Line::from(vec![
                Span::styled("Group:   ", Style::default().fg(theme.dimmed)),
                Span::styled(
                    if instance.group_path.is_empty() {
                        "(none)"
                    } else {
                        &instance.group_path
                    },
                    Style::default().fg(theme.group),
                ),
            ]),
        ];

        let paragraph = Paragraph::new(info_lines);
        frame.render_widget(paragraph, area);
    }

    fn render_output(frame: &mut Frame, area: Rect, instance: &Instance, theme: &Theme) {
        let block = Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(theme.border))
            .title(" Output ")
            .title_style(Style::default().fg(theme.dimmed));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Try to capture output from tmux
        let output = instance
            .capture_output(inner.height as usize)
            .unwrap_or_default();

        if output.is_empty() {
            let hint = Paragraph::new("No output available")
                .style(Style::default().fg(theme.dimmed))
                .alignment(Alignment::Center);
            frame.render_widget(hint, inner);
        } else {
            let output_lines: Vec<Line> = output
                .lines()
                .map(|line| Line::from(Span::raw(line)))
                .collect();

            let paragraph = Paragraph::new(output_lines)
                .style(Style::default().fg(theme.text))
                .wrap(Wrap { trim: false });

            frame.render_widget(paragraph, inner);
        }
    }
}

fn shorten_path(path: &str) -> String {
    if let Some(home) = dirs::home_dir() {
        if let Some(home_str) = home.to_str() {
            if path.starts_with(home_str) {
                return format!("~{}", &path[home_str.len()..]);
            }
        }
    }
    path.to_string()
}
