use anyhow::Result;
use tui_textarea::TextArea;

/// Format an anyhow error as `top: cause: deeper-cause` so the status
/// bar shows the full chain instead of just the outermost context.
fn fmt_error(err: &anyhow::Error) -> String {
    let mut parts: Vec<String> = vec![err.to_string()];
    let mut src = err.source();
    while let Some(s) = src {
        parts.push(s.to_string());
        src = s.source();
    }
    parts.join(": ")
}

use crate::backend::{GraphBackend, QueryResult, Schema};

pub struct App<'a> {
    pub backend: Box<dyn GraphBackend>,
    pub profile_name: String,
    pub tab: Tab,
    pub editor: TextArea<'a>,
    pub result: Option<QueryResult>,
    pub schema: Option<Schema>,
    pub status: String,
    pub running: bool,
    pub history: Vec<String>,
    pub history_idx: Option<usize>,
    pub result_scroll: usize,
    pub schema_scroll: usize,
    pub graph_scroll: (i32, i32),
    pub focus: Focus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Repl,
    Schema,
    Graph,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Editor,
    Results,
}

impl Tab {
    pub fn title(self) -> &'static str {
        match self {
            Tab::Repl => "REPL",
            Tab::Schema => "Schema",
            Tab::Graph => "Graph",
        }
    }
    pub fn index(self) -> usize {
        match self {
            Tab::Repl => 0,
            Tab::Schema => 1,
            Tab::Graph => 2,
        }
    }
}

impl<'a> App<'a> {
    pub fn new(backend: Box<dyn GraphBackend>, profile_name: String) -> Self {
        let mut editor = TextArea::default();
        editor.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Query — F5 to run"),
        );
        editor.insert_str("MATCH (n) RETURN n LIMIT 25");
        Self {
            backend,
            profile_name,
            tab: Tab::Repl,
            editor,
            result: None,
            schema: None,
            status: "ready".into(),
            running: true,
            history: Vec::new(),
            history_idx: None,
            result_scroll: 0,
            schema_scroll: 0,
            graph_scroll: (0, 0),
            focus: Focus::Editor,
        }
    }

    pub fn current_query(&self) -> String {
        self.editor.lines().join("\n")
    }

    pub async fn run_query(&mut self) {
        let q = self.current_query().trim().to_string();
        if q.is_empty() {
            self.status = "empty query".into();
            return;
        }
        self.status = format!("running against {}…", self.backend.label());
        match self.backend.query(&q).await {
            Ok(r) => {
                let summary = r
                    .summary
                    .clone()
                    .unwrap_or_else(|| format!("{} row(s)", r.rows.len()));
                self.status = format!("ok — {summary}");
                self.result = Some(r);
                self.result_scroll = 0;
                self.history.push(q);
                self.history_idx = None;
            }
            Err(e) => {
                self.status = format!("error: {}", fmt_error(&e));
            }
        }
    }

    pub async fn refresh_schema(&mut self) {
        self.status = "loading schema…".into();
        match self.backend.schema().await {
            Ok(s) => {
                self.schema = Some(s);
                self.status = "schema loaded".into();
            }
            Err(e) => self.status = format!("schema error: {}", fmt_error(&e)),
        }
    }

    pub fn recall_prev(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let idx = match self.history_idx {
            None => self.history.len() - 1,
            Some(0) => 0,
            Some(i) => i - 1,
        };
        let text = self.history[idx].clone();
        self.set_editor(&text);
        self.history_idx = Some(idx);
    }

    pub fn recall_next(&mut self) {
        let Some(idx) = self.history_idx else { return };
        if idx + 1 >= self.history.len() {
            self.history_idx = None;
            self.set_editor("");
        } else {
            let text = self.history[idx + 1].clone();
            self.set_editor(&text);
            self.history_idx = Some(idx + 1);
        }
    }

    fn set_editor(&mut self, text: &str) {
        let block = self.editor.block().cloned();
        let mut ta = TextArea::from(text.lines().map(|s| s.to_string()).collect::<Vec<_>>());
        if let Some(b) = block {
            ta.set_block(b);
        }
        self.editor = ta;
    }

    pub fn scroll_results(&mut self, delta: isize) -> Result<()> {
        let max = self.result.as_ref().map(|r| r.rows.len()).unwrap_or(0);
        let new = self.result_scroll as isize + delta;
        self.result_scroll = new.clamp(0, max as isize) as usize;
        Ok(())
    }
}
