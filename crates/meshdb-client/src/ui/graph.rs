use ratatui::{
    layout::Rect,
    style::{Color, Style},
    symbols::Marker,
    text::Line,
    widgets::{
        canvas::{Canvas, Line as CLine, Points},
        Block, Borders, Paragraph,
    },
    Frame,
};
use std::collections::HashMap;

use crate::app::App;
use crate::backend::GraphNode;

pub fn draw(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Graph — h/j/k/l to pan ");

    let Some(result) = &app.result else {
        let hint = Paragraph::new(Line::from(
            "Run a query on the REPL tab — nodes and edges from the result show here.",
        ))
        .block(block);
        f.render_widget(hint, area);
        return;
    };
    if result.nodes.is_empty() && result.edges.is_empty() {
        let hint = Paragraph::new(Line::from(
            "No graph elements in the last result. Try `MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 20`.",
        ))
        .block(block);
        f.render_widget(hint, area);
        return;
    }

    let positions = layout_nodes(&result.nodes);
    let (pan_x, pan_y) = app.graph_scroll;
    let pan_x = pan_x as f64;
    let pan_y = pan_y as f64;

    let nodes_for_canvas = result.nodes.clone();
    let edges_for_canvas = result.edges.clone();

    let canvas = Canvas::default()
        .block(block)
        .marker(Marker::Braille)
        .x_bounds([-100.0 + pan_x, 100.0 + pan_x])
        .y_bounds([-50.0 + pan_y, 50.0 + pan_y])
        .paint(move |ctx| {
            for edge in &edges_for_canvas {
                if let (Some(&(x1, y1)), Some(&(x2, y2))) =
                    (positions.get(&edge.source), positions.get(&edge.target))
                {
                    ctx.draw(&CLine {
                        x1,
                        y1,
                        x2,
                        y2,
                        color: Color::DarkGray,
                    });
                    let mx = (x1 + x2) / 2.0;
                    let my = (y1 + y2) / 2.0;
                    ctx.print(mx, my, edge.edge_type.clone());
                }
            }
            for node in &nodes_for_canvas {
                if let Some(&(x, y)) = positions.get(&node.id) {
                    ctx.draw(&Points {
                        coords: &[(x, y)],
                        color: Color::Cyan,
                    });
                    let label = node_label(node);
                    ctx.print(x + 2.0, y, label);
                }
            }
        })
        .background_color(Color::Reset);

    f.render_widget(canvas, area);
}

fn node_label(node: &GraphNode) -> Line<'static> {
    let mut s = String::new();
    if !node.labels.is_empty() {
        s.push(':');
        s.push_str(&node.labels.join(":"));
    }
    // Try to show a name-ish property if present.
    for key in ["name", "title", "id", "label"] {
        if let Some(v) = node.properties.get(key) {
            s.push(' ');
            s.push_str(&v.render());
            break;
        }
    }
    if s.is_empty() {
        s = format!("#{}", shorten(&node.id));
    }
    Line::styled(s, Style::default().fg(Color::White))
}

fn shorten(s: &str) -> String {
    if s.len() <= 6 {
        s.to_string()
    } else {
        s[..6].to_string()
    }
}

/// Place nodes on a circle, scaled to the canvas bounds. Keep it
/// deterministic so panning feels stable.
fn layout_nodes(nodes: &[GraphNode]) -> HashMap<String, (f64, f64)> {
    let mut out = HashMap::new();
    let n = nodes.len().max(1) as f64;
    let radius = if nodes.len() <= 1 { 0.0 } else { 60.0 };
    for (i, node) in nodes.iter().enumerate() {
        let theta = (i as f64 / n) * std::f64::consts::TAU;
        let x = radius * theta.cos();
        let y = radius * theta.sin() * 0.45; // aspect compensation
        out.insert(node.id.clone(), (x, y));
    }
    out
}
