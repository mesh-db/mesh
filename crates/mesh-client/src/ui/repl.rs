use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Frame,
};

use crate::app::{App, Focus};

pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(0)])
        .split(area);

    draw_editor(f, app, chunks[0]);
    draw_results(f, app, chunks[1]);
}

fn draw_editor(f: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Editor;
    let mut ta = app.editor.clone();
    let title = if focused {
        "Query — F5 to run (focused)"
    } else {
        "Query — F5 to run"
    };
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    ta.set_block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(title),
    );
    f.render_widget(&ta, area);
}

fn draw_results(f: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Results;
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title(" Results ");

    let Some(result) = &app.result else {
        let hint = Line::from(vec![
            Span::styled("No results yet. ", Style::default().fg(Color::DarkGray)),
            Span::raw("Type a query and press "),
            Span::styled("F5", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("."),
        ]);
        let p = Paragraph::new(hint).block(block);
        f.render_widget(p, area);
        return;
    };

    if result.columns.is_empty() && result.rows.is_empty() {
        let summary = result.summary.clone().unwrap_or_else(|| "done".into());
        let p = Paragraph::new(summary).block(block);
        f.render_widget(p, area);
        return;
    }

    let header_cells: Vec<Cell> = result
        .columns
        .iter()
        .map(|c| Cell::from(c.as_str()).style(Style::default().add_modifier(Modifier::BOLD)))
        .collect();
    let header = Row::new(header_cells).style(Style::default().fg(Color::Yellow));

    let visible = result
        .rows
        .iter()
        .skip(app.result_scroll)
        .map(|r| {
            let cells: Vec<Cell> = r.iter().map(|v| Cell::from(truncate(v.render(), 80))).collect();
            Row::new(cells)
        });

    let col_count = result.columns.len().max(1);
    let widths: Vec<Constraint> = (0..col_count)
        .map(|_| Constraint::Percentage((100 / col_count.max(1)) as u16))
        .collect();

    let table = Table::new(visible, widths)
        .header(header)
        .block(block)
        .row_highlight_style(Style::default().bg(Color::DarkGray));

    f.render_widget(table, area);
}

fn truncate(s: String, max: usize) -> String {
    if s.chars().count() <= max {
        s
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}
