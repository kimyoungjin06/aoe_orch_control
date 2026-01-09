//! Main TUI application

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::prelude::*;
use std::io::Write;
use std::time::Duration;

use super::home::HomeView;
use super::styles::Theme;
use crate::session::Storage;

pub struct App {
    home: HomeView,
    should_quit: bool,
    theme: Theme,
    needs_redraw: bool,
}

impl App {
    pub fn new(profile: &str) -> Result<Self> {
        let storage = Storage::new(profile)?;
        let home = HomeView::new(storage)?;
        let theme = Theme::default();

        Ok(Self {
            home,
            should_quit: false,
            theme,
            needs_redraw: true,
        })
    }

    pub async fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    ) -> Result<()> {
        // Initial render
        terminal.clear()?;
        terminal.draw(|f| self.render(f))?;

        // Refresh tmux session cache
        crate::tmux::refresh_session_cache();

        let mut last_status_refresh = std::time::Instant::now();
        const STATUS_REFRESH_INTERVAL: Duration = Duration::from_millis(500);

        loop {
            // Force full redraw if needed (e.g., after returning from tmux)
            if self.needs_redraw {
                terminal.clear()?;
                self.needs_redraw = false;
            }

            // Poll with short timeout for responsive input
            if event::poll(Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    self.handle_key(key, terminal).await?;

                    // Draw immediately after input for responsiveness
                    terminal.draw(|f| self.render(f))?;

                    if self.should_quit {
                        break;
                    }
                    continue; // Skip status refresh this iteration for responsiveness
                }
            }

            // Periodic status refresh (only when no input pending)
            if last_status_refresh.elapsed() >= STATUS_REFRESH_INTERVAL {
                self.home.refresh_status();
                last_status_refresh = std::time::Instant::now();
                terminal.draw(|f| self.render(f))?;
            }

            if self.should_quit {
                break;
            }
        }

        Ok(())
    }

    fn render(&mut self, frame: &mut Frame) {
        self.home.render(frame, frame.area(), &self.theme);
    }

    async fn handle_key(
        &mut self,
        key: KeyEvent,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    ) -> Result<()> {
        // Global keybindings
        match (key.code, key.modifiers) {
            (KeyCode::Char('c'), KeyModifiers::CONTROL) | (KeyCode::Char('q'), _) => {
                if !self.home.has_dialog() {
                    self.should_quit = true;
                    return Ok(());
                }
            }
            _ => {}
        }

        // Delegate to home view
        if let Some(action) = self.home.handle_key(key) {
            match action {
                Action::Quit => self.should_quit = true,
                Action::AttachSession(id) => {
                    self.attach_session(&id, terminal)?;
                }
                Action::Refresh => {
                    crate::tmux::refresh_session_cache();
                    self.home.reload()?;
                }
            }
        }

        Ok(())
    }

    fn attach_session(
        &mut self,
        session_id: &str,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    ) -> Result<()> {
        let instance = match self.home.get_instance(session_id) {
            Some(inst) => inst.clone(),
            None => return Ok(()),
        };

        let tmux_session = instance.tmux_session()?;

        if !tmux_session.exists() {
            // Start the session first
            let mut inst = instance.clone();
            inst.start()?;
        }

        // Leave TUI mode completely
        crossterm::terminal::disable_raw_mode()?;
        crossterm::execute!(
            std::io::stdout(),
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::event::DisableMouseCapture,
            crossterm::cursor::Show
        )?;
        std::io::stdout().flush()?;

        // Attach to tmux session (this blocks until user detaches with Ctrl+b d)
        let attach_result = tmux_session.attach();

        // Re-enter TUI mode
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(
            std::io::stdout(),
            crossterm::terminal::EnterAlternateScreen,
            crossterm::event::EnableMouseCapture,
            crossterm::cursor::Hide
        )?;
        std::io::stdout().flush()?;

        // Drain any stale events that accumulated during tmux session
        while event::poll(Duration::from_millis(0))? {
            let _ = event::read();
        }

        // Force terminal to clear and redraw completely
        terminal.clear()?;
        self.needs_redraw = true;

        // Refresh session state since things may have changed
        crate::tmux::refresh_session_cache();
        self.home.reload()?;

        // Log any attach errors but don't fail
        if let Err(e) = attach_result {
            tracing::warn!("tmux attach returned error: {}", e);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Quit,
    AttachSession(String),
    Refresh,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_enum() {
        let quit = Action::Quit;
        let attach = Action::AttachSession("test-id".to_string());
        let refresh = Action::Refresh;

        assert_eq!(quit, Action::Quit);
        assert_eq!(attach, Action::AttachSession("test-id".to_string()));
        assert_eq!(refresh, Action::Refresh);
    }

    #[test]
    fn test_action_clone() {
        let original = Action::AttachSession("session-123".to_string());
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }
}
