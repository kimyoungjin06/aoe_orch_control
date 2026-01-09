//! TUI theme and styling

use ratatui::style::Color;

#[derive(Debug, Clone)]
pub struct Theme {
    // Background and borders
    pub background: Color,
    pub border: Color,
    pub selection: Color,

    // Text colors
    pub title: Color,
    pub text: Color,
    pub dimmed: Color,
    pub hint: Color,

    // Status colors
    pub running: Color,
    pub waiting: Color,
    pub idle: Color,
    pub error: Color,

    // UI elements
    pub group: Color,
    pub search: Color,
    pub accent: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self::tokyo_night()
    }
}

impl Theme {
    pub fn tokyo_night() -> Self {
        Self {
            background: Color::Rgb(26, 27, 38),
            border: Color::Rgb(59, 66, 97),
            selection: Color::Rgb(41, 46, 66),

            title: Color::Rgb(122, 162, 247),
            text: Color::Rgb(192, 202, 245),
            dimmed: Color::Rgb(86, 95, 137),
            hint: Color::Rgb(125, 133, 168),

            running: Color::Rgb(158, 206, 106),
            waiting: Color::Rgb(224, 175, 104),
            idle: Color::Rgb(86, 95, 137),
            error: Color::Rgb(247, 118, 142),

            group: Color::Rgb(187, 154, 247),
            search: Color::Rgb(125, 207, 255),
            accent: Color::Rgb(122, 162, 247),
        }
    }
}
