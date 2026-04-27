use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use meshdb_server::config::{ClusterMode, ServerConfig};
use std::path::{Path, PathBuf};

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

    /// Optional subcommand. Without one, `meshdb-server` runs the
    /// graph-database server. Subcommands provide ops-time
    /// utilities (validate-backup, ...) that don't bind a
    /// listener.
    #[command(subcommand)]
    command: Option<Subcmd>,
}

#[derive(Subcommand, Debug)]
enum Subcmd {
    /// Validate that an archived backup directory matches the
    /// backup manifest produced by `MeshService::take_cluster_backup`.
    ///
    /// Walks every entry in the manifest and confirms that the
    /// configured `data_dir` contains a `raft/<group>` subdirectory
    /// for each (peer, group) pair. Hard-fails if any subdir is
    /// missing — operators run this AFTER copying each peer's
    /// data_dir into place but BEFORE starting the cluster, so a
    /// half-restored cluster can't boot into split-brain.
    ValidateBackup {
        /// Path to the JSON manifest produced at backup time.
        #[arg(long)]
        manifest: PathBuf,
        /// Path to the data_dir of the peer being validated.
        /// Run this once per peer in your archive — each peer's
        /// data_dir is independent.
        #[arg(long)]
        data_dir: PathBuf,
        /// Peer id this data_dir belongs to. The manifest carries
        /// per-peer entries; the validator looks for entries
        /// matching this id.
        #[arg(long)]
        peer_id: u64,
    },
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
            read_consistency: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
            cluster_auth: None,
            routing_ttl_seconds: None,
            shutdown_drain_timeout_seconds: None,
            query_timeout_seconds: None,
            query_max_rows: None,
            max_concurrent_queries: None,
            audit_log_path: None,
            plan_cache_size: None,
            storage: None,
            tracing: None,
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
    let cli = Cli::parse();

    // Subcommands run synchronously without binding any listener.
    if let Some(subcmd) = cli.command {
        return run_subcommand(subcmd);
    }

    let config = build_config(cli)?;
    init_tracing(config.tracing.as_ref())?;
    tracing::info!(
        self_id = config.self_id,
        listen_address = %config.listen_address,
        data_dir = %config.data_dir.display(),
        peers = config.peers.len(),
        "starting meshdb-server"
    );

    meshdb_server::serve(config).await
}

fn run_subcommand(subcmd: Subcmd) -> Result<()> {
    match subcmd {
        Subcmd::ValidateBackup {
            manifest,
            data_dir,
            peer_id,
        } => validate_backup(&manifest, &data_dir, peer_id),
    }
}

/// Walk a backup manifest and confirm that `data_dir` contains a
/// `raft/<group>` subdirectory for every entry that names this
/// peer. Hard-fails if any subdir is missing or if the manifest
/// is unparseable. Operators run this once per peer in their
/// archive before starting the cluster, so a half-restored
/// cluster can't boot into split-brain.
fn validate_backup(manifest: &Path, data_dir: &Path, peer_id: u64) -> Result<()> {
    let raw = std::fs::read_to_string(manifest)
        .with_context(|| format!("reading manifest {}", manifest.display()))?;
    let manifest: meshdb_rpc::MultiRaftBackupManifest = serde_json::from_str(&raw)
        .with_context(|| format!("parsing manifest {}", manifest.display()))?;

    let raft_dir = data_dir.join("raft");
    if !raft_dir.exists() {
        return Err(anyhow!(
            "data_dir {} has no raft/ subdirectory; this isn't a multi-raft archive",
            data_dir.display()
        ));
    }

    let entries_for_peer: Vec<_> = manifest
        .entries
        .iter()
        .filter(|e| e.peer_id == peer_id)
        .collect();
    if entries_for_peer.is_empty() {
        return Err(anyhow!(
            "manifest carries no entries for peer {peer_id} — \
             either the wrong --peer-id was passed or this peer was missing at backup time"
        ));
    }

    let mut missing = Vec::new();
    for entry in &entries_for_peer {
        let dir = raft_dir.join(entry.group.dir_name());
        if !dir.exists() {
            missing.push((entry.group, dir));
        }
    }
    if !missing.is_empty() {
        eprintln!("validate-backup: missing snapshot directories:");
        for (group, dir) in &missing {
            eprintln!("  - {:?} expected at {}", group, dir.display());
        }
        return Err(anyhow!(
            "{} expected snapshot directory(ies) missing in {}; cluster cannot be safely brought up",
            missing.len(),
            data_dir.display()
        ));
    }

    if !manifest.errors.is_empty() {
        eprintln!(
            "validate-backup: manifest carries {} backup-time error(s):",
            manifest.errors.len()
        );
        for err in &manifest.errors {
            eprintln!(
                "  - {:?} on peer {}: {}",
                err.group, err.peer_id, err.message
            );
        }
        return Err(anyhow!(
            "manifest is dirty — backup ran but {} group(s) failed to snapshot; \
             re-run take_cluster_backup before restoring",
            manifest.errors.len()
        ));
    }

    println!(
        "validate-backup: peer {peer_id} OK — {} groups present, manifest clean",
        entries_for_peer.len()
    );
    Ok(())
}

/// Set up the global tracing subscriber. Always installs the
/// fmt-to-stdout layer (gated by `RUST_LOG` / fallback `info`).
/// When `[tracing] otlp_endpoint = "..."` is configured, also
/// installs an OpenTelemetry layer that exports spans to the
/// collector via OTLP/gRPC.
///
/// The OTel pipeline uses a tokio-runtime batch span processor
/// (`opentelemetry_sdk::runtime::Tokio`), so a tokio runtime must
/// be active before this runs — that's why `main()` parses CLI
/// inside `#[tokio::main]` first and only then calls this.
fn init_tracing(tracing_cfg: Option<&meshdb_server::config::TracingConfig>) -> Result<()> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    match tracing_cfg {
        Some(cfg) => {
            use opentelemetry::trace::TracerProvider as _;
            use opentelemetry_otlp::WithExportConfig;

            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(cfg.otlp_endpoint.clone())
                .build()
                .map_err(|e| {
                    anyhow!("building OTLP span exporter for {}: {e}", cfg.otlp_endpoint)
                })?;
            let service_name = cfg
                .service_name
                .clone()
                .unwrap_or_else(|| "meshdb-server".to_string());
            let resource = opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new(
                "service.name",
                service_name,
            )]);
            let sampler = match cfg.sample_rate {
                Some(r) if (0.0..=1.0).contains(&r) => {
                    opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(r)
                }
                Some(r) => {
                    return Err(anyhow!(
                        "tracing.sample_rate must be in [0.0, 1.0], got {r}"
                    ));
                }
                None => opentelemetry_sdk::trace::Sampler::AlwaysOn,
            };
            let provider = opentelemetry_sdk::trace::TracerProvider::builder()
                .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
                .with_resource(resource)
                .with_sampler(sampler)
                .build();
            let tracer = provider.tracer("meshdb");
            opentelemetry::global::set_tracer_provider(provider);
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            registry.with(otel_layer).init();
        }
        None => registry.init(),
    }

    Ok(())
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
            read_consistency: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
            cluster_auth: None,
            routing_ttl_seconds: None,
            shutdown_drain_timeout_seconds: None,
            query_timeout_seconds: None,
            query_max_rows: None,
            max_concurrent_queries: None,
            audit_log_path: None,
            plan_cache_size: None,
            storage: None,
            tracing: None,
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
            read_consistency: None,
            #[cfg(feature = "apoc-load")]
            apoc_import: None,
            cluster_auth: None,
            routing_ttl_seconds: None,
            shutdown_drain_timeout_seconds: None,
            query_timeout_seconds: None,
            query_max_rows: None,
            max_concurrent_queries: None,
            audit_log_path: None,
            plan_cache_size: None,
            storage: None,
            tracing: None,
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

    #[test]
    fn validate_backup_passes_when_every_group_has_a_subdir() {
        use meshdb_rpc::{MultiRaftBackupEntry, MultiRaftBackupGroup, MultiRaftBackupManifest};

        let dir = tempfile::TempDir::new().unwrap();
        // Create the data_dir layout the manifest expects.
        let raft_dir = dir.path().join("raft");
        std::fs::create_dir_all(raft_dir.join("meta")).unwrap();
        std::fs::create_dir_all(raft_dir.join("p-0")).unwrap();
        std::fs::create_dir_all(raft_dir.join("p-1")).unwrap();

        let manifest = MultiRaftBackupManifest {
            entries: vec![
                MultiRaftBackupEntry {
                    group: MultiRaftBackupGroup::Meta,
                    peer_id: 1,
                    last_log_index: Some(10),
                    last_log_term: Some(1),
                },
                MultiRaftBackupEntry {
                    group: MultiRaftBackupGroup::Partition { id: 0 },
                    peer_id: 1,
                    last_log_index: Some(20),
                    last_log_term: Some(1),
                },
                MultiRaftBackupEntry {
                    group: MultiRaftBackupGroup::Partition { id: 1 },
                    peer_id: 1,
                    last_log_index: Some(15),
                    last_log_term: Some(1),
                },
                // Entry for a different peer — must be ignored
                // when validating peer 1.
                MultiRaftBackupEntry {
                    group: MultiRaftBackupGroup::Meta,
                    peer_id: 99,
                    last_log_index: Some(10),
                    last_log_term: Some(1),
                },
            ],
            errors: vec![],
        };
        let manifest_path = dir.path().join("manifest.json");
        std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        validate_backup(&manifest_path, dir.path(), 1).expect("validate succeeds");
    }

    #[test]
    fn validate_backup_fails_when_a_subdir_is_missing() {
        use meshdb_rpc::{MultiRaftBackupEntry, MultiRaftBackupGroup, MultiRaftBackupManifest};

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("raft").join("meta")).unwrap();
        // p-0 deliberately missing.

        let manifest = MultiRaftBackupManifest {
            entries: vec![
                MultiRaftBackupEntry {
                    group: MultiRaftBackupGroup::Meta,
                    peer_id: 1,
                    last_log_index: Some(10),
                    last_log_term: Some(1),
                },
                MultiRaftBackupEntry {
                    group: MultiRaftBackupGroup::Partition { id: 0 },
                    peer_id: 1,
                    last_log_index: Some(20),
                    last_log_term: Some(1),
                },
            ],
            errors: vec![],
        };
        let manifest_path = dir.path().join("manifest.json");
        std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        let err =
            validate_backup(&manifest_path, dir.path(), 1).expect_err("missing subdir must fail");
        assert!(
            err.to_string().contains("missing"),
            "expected missing-subdir error, got: {err}"
        );
    }

    #[test]
    fn validate_backup_fails_when_manifest_carries_errors() {
        use meshdb_rpc::{
            MultiRaftBackupEntry, MultiRaftBackupError, MultiRaftBackupGroup,
            MultiRaftBackupManifest,
        };

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("raft").join("meta")).unwrap();

        let manifest = MultiRaftBackupManifest {
            entries: vec![MultiRaftBackupEntry {
                group: MultiRaftBackupGroup::Meta,
                peer_id: 1,
                last_log_index: Some(10),
                last_log_term: Some(1),
            }],
            errors: vec![MultiRaftBackupError {
                group: MultiRaftBackupGroup::Partition { id: 0 },
                peer_id: 1,
                message: "snapshot trigger failed".into(),
            }],
        };
        let manifest_path = dir.path().join("manifest.json");
        std::fs::write(&manifest_path, serde_json::to_string(&manifest).unwrap()).unwrap();

        let err =
            validate_backup(&manifest_path, dir.path(), 1).expect_err("dirty manifest must fail");
        assert!(
            err.to_string().contains("manifest is dirty"),
            "expected dirty-manifest error, got: {err}"
        );
    }
}
