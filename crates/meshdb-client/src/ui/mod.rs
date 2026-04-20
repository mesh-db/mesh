pub mod graph;
pub mod repl;
pub mod schema;

use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Paragraph, Tabs},
    Frame,
};

use crate::app::{App, Tab};

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(0),    // body
            Constraint::Length(1), // status bar
        ])
        .split(f.area());

    draw_header(f, app, chunks[0]);
    match app.tab {
        Tab::Repl => repl::draw(f, app, chunks[1]),
        Tab::Schema => schema::draw(f, app, chunks[1]),
        Tab::Graph => graph::draw(f, app, chunks[1]),
    }
    draw_status(f, app, chunks[2]);
}

fn draw_header(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let titles: Vec<Line> = [Tab::Repl, Tab::Schema, Tab::Graph]
        .iter()
        .map(|t| Line::from(t.title()))
        .collect();
    let header = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title(format!(
            " meshdb-client — {} [{}] ",
            app.profile_name,
            app.backend.label()
        )))
        .select(app.tab.index())
        .style(Style::default().fg(Color::Gray))
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(header, area);
}

fn draw_status(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let hint = match app.tab {
        Tab::Repl => {
            "F5 run · Alt+↑/↓ history · F6 / Esc switch pane · F1/F2/F3 tabs · F8 schema · F10 quit"
        }
        Tab::Schema => "↑/↓ scroll · F8 refresh · F1/F2/F3 tabs · F10 quit",
        Tab::Graph => "h/j/k/l pan · Home center · F1/F2/F3 tabs · F10 quit",
    };
    let text = format!("{}  │  {}", app.status, hint);
    let p = Paragraph::new(text).style(Style::default().fg(Color::DarkGray));
    f.render_widget(p, area);
}
