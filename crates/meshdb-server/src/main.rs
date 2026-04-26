use anyhow::{anyhow, Result};
use clap::Parser;
use meshdb_server::config::{ClusterMode, ServerConfig};
use std::path::PathBuf;

/// Common-case options exposed on the CLI / environment. Anything
/// involving structured data (peer lists, bolt auth tables, TLS paths)
/// stays TOML-only — that's what `--config` is for.
///
/// Resolution order:
///
///   1. If `--config <path>` is given, parse that file as the base.
///      Otherwise build the config purely from CLI flags / env vars.
///   2. Any CLI flag / env var that's set overrides the corresponding
///      field (or, for `Option<_>` fields, populates it).
///
/// Every flag also has a `MESHDB_*` env var fallback so Docker users
/// can `-e MESHDB_SELF_ID=1` instead of mounting a config file.
#[derive(Parser, Debug)]
#[command(name = "meshdb-server", about = "Mesh graph database server", version)]
struct Cli {
    /// Path to the TOML config file. Omit to configure the server
    /// entirely from --flags / MESHDB_* env vars.
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Peer id for this server. Required when `--config` is absent.
    #[arg(long, env = "MESHDB_SELF_ID")]
    self_id: Option<u64>,

    /// Address to bind for gRPC traffic, e.g. `0.0.0.0:7001`.
    #[arg(long, env = "MESHDB_LISTEN_ADDRESS")]
    listen_address: Option<String>,

    /// Directory the RocksDB store lives in. Created if missing.
    #[arg(long, env = "MESHDB_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Address for the Bolt protocol listener, e.g. `0.0.0.0:7687`.
    /// Omit to disable Bolt entirely.
    #[arg(long, env = "MESHDB_BOLT_ADDRESS")]
    bolt_address: Option<String>,

    /// Address for the Prometheus metrics endpoint.
    #[arg(long, env = "MESHDB_METRICS_ADDRESS")]
    metrics_address: Option<String>,

    /// Number of hash partitions. Ignored in single-node mode.
    #[arg(long, env = "MESHDB_NUM_PARTITIONS")]
    num_partitions: Option<u32>,

    /// Mark this peer as the Raft bootstrap seed.
    /// Pass bare `--bootstrap` for true, or `--bootstrap=false` /
    /// `MESHDB_BOOTSTRAP=false` to force false on top of a TOML
    /// config that set it true.
    #[arg(
        long,
        env = "MESHDB_BOOTSTRAP",
        num_args = 0..=1,
        default_missing_value = "true",
    )]
    bootstrap: Option<bool>,

    /// Cluster mode: `single`, `routing`, or `raft`.
    #[arg(long, env = "MESHDB_MODE", value_enum)]
    mode: Option<ClusterMode>,
}

impl Cli {
    /// Build a [`ServerConfig`] straight from CLI flags / env vars,
    /// with no TOML file involved. `self_id`, `listen_address`, and
    /// `data_dir` are required in this mode; everything else falls
    /// back to the same defaults `from_toml_str` would use.
    fn into_config(self) -> Result<ServerConfig> {
        let self_id = self.self_id.ok_or_else(|| {
            anyhow!("--self-id (or MESHDB_SELF_ID) is required when --config is not provided")
        })?;
        let listen_address = self.listen_address.ok_or_else(|| {
            anyhow!(
                "--listen-address (or MESHDB_LISTEN_ADDRESS) is required when --config is not provided"
            )
        })?;
        let data_dir = self.data_dir.ok_or_else(|| {
            anyhow!("--data-dir (or MESHDB_DATA_DIR) is required when --config is not provided")
        })?;
        Ok(ServerConfig {
            self_id,
            listen_address,
            data_dir,
            num_partitions: self.num_partitions.unwrap_or(4),
            peers: vec![],
            bootstrap: self.bootstrap.unwrap_or(false),
            bolt_address: self.bolt_address,
            metrics_address: self.metrics_address,
            bolt_auth: None,
            bolt_tls: None,
            bolt_advertised_versions: None,
            bolt_advertised_address: None,
            grpc_tls: None,
            mode: self.mode,
            replication_factor: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
        })
    }

    /// Apply any CLI flags / env vars that were set as overrides on
    /// top of a config loaded from TOML. Unset flags are left alone.
    fn apply_to(&self, cfg: &mut ServerConfig) {
        if let Some(v) = self.self_id {
            cfg.self_id = v;
        }
        if let Some(v) = &self.listen_address {
            cfg.listen_address = v.clone();
        }
        if let Some(v) = &self.data_dir {
            cfg.data_dir = v.clone();
        }
        if let Some(v) = self.num_partitions {
            cfg.num_partitions = v;
        }
        if let Some(v) = self.bootstrap {
            cfg.bootstrap = v;
        }
        if let Some(v) = &self.bolt_address {
            cfg.bolt_address = Some(v.clone());
        }
        if let Some(v) = &self.metrics_address {
            cfg.metrics_address = Some(v.clone());
        }
        if let Some(v) = self.mode {
            cfg.mode = Some(v);
        }
    }
}

fn build_config(cli: Cli) -> Result<ServerConfig> {
    match cli.config.as_ref() {
        Some(path) => {
            let mut cfg = ServerConfig::from_path(path)?;
            cli.apply_to(&mut cfg);
            Ok(cfg)
        }
        None => cli.into_config(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let config = build_config(cli)?;
    tracing::info!(
        self_id = config.self_id,
        listen_address = %config.listen_address,
        data_dir = %config.data_dir.display(),
        peers = config.peers.len(),
        "starting meshdb-server"
    );

    meshdb_server::serve(config).await
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthesize a Cli struct from argv. clap is strict about env
    /// vars bleeding into tests, so every test zeroes them via
    /// `with_env_cleared` first where relevant.
    fn parse(argv: &[&str]) -> Cli {
        Cli::try_parse_from(argv).expect("cli parse")
    }

    #[test]
    fn config_only_requires_config_flag() {
        let cli = parse(&["meshdb-server", "--config", "/tmp/mesh.toml"]);
        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("/tmp/mesh.toml"))
        );
        assert!(cli.self_id.is_none());
    }

    #[test]
    fn pure_cli_config_requires_three_required_fields() {
        let cli = parse(&[
            "meshdb-server",
            "--self-id",
            "1",
            "--listen-address",
            "0.0.0.0:7001",
            "--data-dir",
            "/tmp/data",
            "--bolt-address",
            "0.0.0.0:7687",
        ]);
        let cfg = cli.into_config().expect("build config");
        assert_eq!(cfg.self_id, 1);
        assert_eq!(cfg.listen_address, "0.0.0.0:7001");
        assert_eq!(cfg.bolt_address.as_deref(), Some("0.0.0.0:7687"));
        assert_eq!(cfg.num_partitions, 4); // default
        assert!(!cfg.bootstrap); // default
    }

    #[test]
    fn pure_cli_config_errors_without_self_id() {
        let cli = parse(&[
            "meshdb-server",
            "--listen-address",
            "0.0.0.0:7001",
            "--data-dir",
            "/tmp/data",
        ]);
        let err = cli.into_config().unwrap_err().to_string();
        assert!(err.contains("--self-id"), "unexpected error: {err}");
    }

    #[test]
    fn bare_bootstrap_flag_parses_as_true() {
        let cli = parse(&["meshdb-server", "--bootstrap"]);
        assert_eq!(cli.bootstrap, Some(true));
    }

    #[test]
    fn explicit_bootstrap_false_wins_over_config() {
        let cli = parse(&["meshdb-server", "--bootstrap=false"]);
        assert_eq!(cli.bootstrap, Some(false));

        let mut cfg = ServerConfig {
            self_id: 1,
            listen_address: "127.0.0.1:7001".into(),
            data_dir: "/tmp/d".into(),
            num_partitions: 4,
            peers: vec![],
            bootstrap: true,
            bolt_address: None,
            metrics_address: None,
            bolt_auth: None,
            bolt_tls: None,
            bolt_advertised_versions: None,
            bolt_advertised_address: None,
            grpc_tls: None,
            mode: None,
            replication_factor: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
        };
        cli.apply_to(&mut cfg);
        assert!(!cfg.bootstrap);
    }

    #[test]
    fn apply_to_leaves_unset_fields_alone() {
        let cli = parse(&["meshdb-server", "--listen-address", "0.0.0.0:9000"]);
        let mut cfg = ServerConfig {
            self_id: 42,
            listen_address: "127.0.0.1:7001".into(),
            data_dir: "/tmp/d".into(),
            num_partitions: 8,
            peers: vec![],
            bootstrap: false,
            bolt_address: Some("127.0.0.1:7687".into()),
            metrics_address: None,
            bolt_auth: None,
            bolt_tls: None,
            bolt_advertised_versions: None,
            bolt_advertised_address: None,
            grpc_tls: None,
            mode: None,
            replication_factor: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
        };
        cli.apply_to(&mut cfg);
        assert_eq!(cfg.listen_address, "0.0.0.0:9000"); // overridden
        assert_eq!(cfg.self_id, 42); // preserved
        assert_eq!(cfg.num_partitions, 8); // preserved
        assert_eq!(cfg.bolt_address.as_deref(), Some("127.0.0.1:7687"));
    }

    #[test]
    fn mode_flag_accepts_each_variant() {
        for (arg, expected) in [
            ("single", ClusterMode::Single),
            ("routing", ClusterMode::Routing),
            ("raft", ClusterMode::Raft),
        ] {
            let cli = parse(&["meshdb-server", "--mode", arg]);
            assert_eq!(cli.mode, Some(expected));
        }
    }
}
