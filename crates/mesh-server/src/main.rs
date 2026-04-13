use anyhow::Result;
use clap::Parser;
use mesh_server::config::ServerConfig;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "mesh-server", about = "Mesh graph database server", version)]
struct Cli {
    /// Path to the TOML config file.
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let config = ServerConfig::from_path(&cli.config)?;
    tracing::info!(
        self_id = config.self_id,
        listen_address = %config.listen_address,
        data_dir = %config.data_dir.display(),
        peers = config.peers.len(),
        "starting mesh-server"
    );

    mesh_server::serve(config).await
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
