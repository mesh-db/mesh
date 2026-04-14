use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// This server's peer id. Must match one of the entries in `peers` when
    /// `peers` is non-empty.
    pub self_id: u64,

    /// Address to bind for gRPC traffic, e.g. `127.0.0.1:7001`. Use
    /// `127.0.0.1:0` to bind an ephemeral port (mostly useful for tests).
    pub listen_address: String,

    /// Directory that the RocksDB store lives in. Created if it does not
    /// already exist.
    pub data_dir: PathBuf,

    /// Number of hash partitions. Ignored when `peers` is empty (single-node
    /// mode). Defaults to 4.
    #[serde(default = "default_num_partitions")]
    pub num_partitions: u32,

    /// Cluster member list. Empty for a single-node deployment.
    #[serde(default)]
    pub peers: Vec<PeerConfig>,

    /// Marks this peer as the seed that calls `Raft::initialize` to bootstrap
    /// the cluster. Exactly one peer per cluster should have this set on its
    /// first start; on subsequent starts it should be `false`. Ignored in
    /// single-node (empty peers) deployments.
    #[serde(default)]
    pub bootstrap: bool,

    /// Optional address for the Bolt protocol listener (e.g.
    /// `127.0.0.1:7687`). When present, the server additionally binds a
    /// Bolt 4.4-speaking listener on this address alongside the gRPC
    /// one, so Neo4j drivers and cypher-shell can connect directly.
    /// Omit or set to `None` to disable Bolt entirely.
    #[serde(default)]
    pub bolt_address: Option<String>,
}

fn default_num_partitions() -> u32 {
    4
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub id: u64,
    pub address: String,
}

impl ServerConfig {
    pub fn from_toml_str(input: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(input)
    }

    pub fn from_path(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config file {}: {}", path.display(), e))?;
        Self::from_toml_str(&contents)
            .map_err(|e| anyhow::anyhow!("parsing config file {}: {}", path.display(), e))
    }
}
