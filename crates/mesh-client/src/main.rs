mod app;
mod backend;
mod config;
mod ui;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{io, path::PathBuf, time::Duration};

use crate::app::{App, Focus, Tab};
use crate::config::Config;

#[derive(Parser, Debug)]
#[command(name = "mesh-client", version, about = "TUI for graph databases")]
struct Args {
    /// Profile name from config (e.g. mesh-local, neo4j-local, falkor-local)
    #[arg(short, long)]
    profile: Option<String>,

    /// Path to config file (defaults to ~/.config/mesh-client/config.toml)
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // rustls 0.23 requires a crypto provider to be installed before any
    // ClientConfig::builder() call — aws-lc-rs is the default provider
    // that rustls pulls in with its default features.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("installing rustls crypto provider"))?;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(io::stderr)
        .init();

    let config_path = args.config.unwrap_or_else(Config::default_path);
    let config = Config::load(&config_path)?;

    let profile_name = args
        .profile
        .or_else(|| config.profiles.first().map(|p| p.name.clone()))
        .ok_or_else(|| anyhow!("no profile specified and config has none"))?;

    let profile = config
        .profiles
        .iter()
        .find(|p| p.name == profile_name)
        .ok_or_else(|| anyhow!("profile '{profile_name}' not found in config"))?
        .clone();

    eprintln!("connecting to {} ({})…", profile.name, profile.uri);
    let backend = backend::connect(&profile)
        .await
        .with_context(|| format!("connecting to profile '{}'", profile.name))?;

    let mut app = App::new(backend, profile.name.clone());

    // Kick off an initial schema load so the Schema tab has content.
    app.refresh_schema().await;

    run_tui(&mut app).await?;
    Ok(())
}

async fn run_tui(app: &mut App<'_>) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;

    let result = event_loop(app, &mut term).await;

    disable_raw_mode()?;
    execute!(
        term.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    term.show_cursor()?;
    result
}

async fn event_loop<B: ratatui::backend::Backend>(
    app: &mut App<'_>,
    term: &mut Terminal<B>,
) -> Result<()> {
    while app.running {
        term.draw(|f| ui::draw(f, app))?;

        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        let ev = event::read()?;
        handle_event(app, ev).await?;
    }
    Ok(())
}

async fn handle_event(app: &mut App<'_>, ev: Event) -> Result<()> {
    let Event::Key(key) = ev else { return Ok(()) };
    if key.kind != KeyEventKind::Press {
        return Ok(());
    }

    // Global shortcuts — F-keys are terminal-safe (iTerm doesn't steal them).
    match key.code {
        KeyCode::F(1) => {
            app.tab = Tab::Repl;
            return Ok(());
        }
        KeyCode::F(2) => {
            app.tab = Tab::Schema;
            return Ok(());
        }
        KeyCode::F(3) => {
            app.tab = Tab::Graph;
            return Ok(());
        }
        KeyCode::F(5) => {
            app.run_query().await;
            return Ok(());
        }
        KeyCode::F(8) => {
            app.refresh_schema().await;
            return Ok(());
        }
        KeyCode::F(10) => {
            app.running = false;
            return Ok(());
        }
        _ => {}
    }

    match app.tab {
        Tab::Repl => handle_repl_key(app, key).await?,
        Tab::Schema => handle_schema_key(app, key)?,
        Tab::Graph => handle_graph_key(app, key)?,
    }
    Ok(())
}

async fn handle_repl_key(app: &mut App<'_>, key: crossterm::event::KeyEvent) -> Result<()> {
    let alt = key.modifiers.contains(KeyModifiers::ALT);

    // F6 toggles focus between editor and results pane.
    if key.code == KeyCode::F(6) {
        app.focus = match app.focus {
            Focus::Editor => Focus::Results,
            Focus::Results => Focus::Editor,
        };
        return Ok(());
    }

    // Esc drops out of the editor into the results pane — lets you
    // navigate results without Ctrl or F-keys.
    if key.code == KeyCode::Esc && app.focus == Focus::Editor {
        app.focus = Focus::Results;
        return Ok(());
    }

    // Alt+Up / Alt+Down cycles query history while editing.
    if alt {
        match key.code {
            KeyCode::Up => {
                app.recall_prev();
                return Ok(());
            }
            KeyCode::Down => {
                app.recall_next();
                return Ok(());
            }
            _ => {}
        }
    }

    if app.focus == Focus::Results {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => app.scroll_results(1)?,
            KeyCode::Up | KeyCode::Char('k') => app.scroll_results(-1)?,
            KeyCode::PageDown => app.scroll_results(10)?,
            KeyCode::PageUp => app.scroll_results(-10)?,
            KeyCode::Home => app.result_scroll = 0,
            KeyCode::Enter | KeyCode::Char('i') => app.focus = Focus::Editor,
            _ => {}
        }
        return Ok(());
    }

    app.editor.input(key);
    Ok(())
}

fn handle_schema_key(app: &mut App, key: crossterm::event::KeyEvent) -> Result<()> {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            app.schema_scroll = app.schema_scroll.saturating_add(1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.schema_scroll = app.schema_scroll.saturating_sub(1);
        }
        KeyCode::Home => app.schema_scroll = 0,
        _ => {}
    }
    Ok(())
}

fn handle_graph_key(app: &mut App, key: crossterm::event::KeyEvent) -> Result<()> {
    match key.code {
        KeyCode::Left | KeyCode::Char('h') => app.graph_scroll.0 -= 2,
        KeyCode::Right | KeyCode::Char('l') => app.graph_scroll.0 += 2,
        KeyCode::Up | KeyCode::Char('k') => app.graph_scroll.1 -= 1,
        KeyCode::Down | KeyCode::Char('j') => app.graph_scroll.1 += 1,
        KeyCode::Home => app.graph_scroll = (0, 0),
        _ => {}
    }
    Ok(())
}
