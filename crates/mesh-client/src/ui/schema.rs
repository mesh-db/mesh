use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem},
    Frame,
};

use crate::app::App;

pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(33),
            Constraint::Percentage(34),
        ])
        .split(area);

    let Some(schema) = &app.schema else {
        let block = Block::default().borders(Borders::ALL).title(" Schema ");
        let hint = ratatui::widgets::Paragraph::new("Press Ctrl+R to load schema.").block(block);
        f.render_widget(hint, area);
        return;
    };

    render_list(
        f,
        chunks[0],
        " Labels ",
        &schema.labels,
        Color::Green,
        app.schema_scroll,
    );
    render_list(
        f,
        chunks[1],
        " Relationship Types ",
        &schema.relationship_types,
        Color::Magenta,
        app.schema_scroll,
    );
    render_list(
        f,
        chunks[2],
        " Property Keys ",
        &schema.property_keys,
        Color::Blue,
        app.schema_scroll,
    );
}

fn render_list(f: &mut Frame, area: Rect, title: &str, items: &[String], color: Color, scroll: usize) {
    let items: Vec<ListItem> = items
        .iter()
        .skip(scroll)
        .map(|s| {
            ListItem::new(Line::from(vec![
                Span::styled("• ", Style::default().fg(color)),
                Span::styled(s.clone(), Style::default().add_modifier(Modifier::BOLD)),
            ]))
        })
        .collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title.to_string()));
    f.render_widget(list, area);
}
