use serde::Deserialize;
use std::path::{Path, PathBuf};

/// How a multi-peer server operates on top of its peer list. See
/// [`ServerConfig::resolved_mode`] for the defaulting rules that apply
/// when the TOML config omits `mode` entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterMode {
    /// Single-node: no peers, no replication, no partitioning. The
    /// store is local and every query hits it directly. Implicit when
    /// `peers` is empty and `mode` is unset.
    Single,
    /// Hash-partitioned routing: nodes are sharded across peers by
    /// `owner_of(node.id)`, reads go through `PartitionedGraphReader`,
    /// writes commit through the 2PC coordinator + durable recovery
    /// log. No consensus — each partition lives on exactly one peer,
    /// and a peer crash loses that peer's shard until it restarts.
    /// Selected explicitly via `mode = "routing"`.
    Routing,
    /// Single-Raft-group replication: every peer holds the full graph
    /// and every write is replicated through a single Raft log. Reads
    /// are cheap-local; writes go through `propose_graph`. Implicit
    /// when `peers` is non-empty and `mode` is unset, for
    /// backward compatibility with configs from before `mode` existed.
    Raft,
}

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
    /// single-node (empty peers) deployments and in routing mode.
    #[serde(default)]
    pub bootstrap: bool,

    /// Optional address for the Bolt protocol listener (e.g.
    /// `127.0.0.1:7687`). When present, the server additionally binds a
    /// Bolt 4.4-speaking listener on this address alongside the gRPC
    /// one, so Neo4j drivers and cypher-shell can connect directly.
    /// Omit or set to `None` to disable Bolt entirely.
    #[serde(default)]
    pub bolt_address: Option<String>,

    /// Cluster operating mode. Omitted → inferred from `peers` (empty
    /// → Single, non-empty → Raft) for backward compatibility with
    /// configs from before this field existed. Set explicitly to
    /// `"routing"` to run a hash-partitioned non-Raft cluster that
    /// uses the 2PC coordinator + recovery log.
    #[serde(default)]
    pub mode: Option<ClusterMode>,
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

    /// Resolve the cluster mode, falling back to the legacy implicit
    /// rules when `mode` is unset. Empty `peers` implies Single;
    /// non-empty `peers` implies Raft. The fallback preserves every
    /// pre-`mode` config's behavior unchanged.
    pub fn resolved_mode(&self) -> ClusterMode {
        if let Some(mode) = self.mode {
            return mode;
        }
        if self.peers.is_empty() {
            ClusterMode::Single
        } else {
            ClusterMode::Raft
        }
    }

    /// Check that the chosen mode and the peer list are consistent.
    /// Returns a human-readable error for any mismatch so the server
    /// binary surfaces configuration mistakes at startup instead of
    /// silently ignoring them.
    pub fn validate(&self) -> Result<(), String> {
        let mode = self.resolved_mode();
        let has_peers = !self.peers.is_empty();
        match (mode, has_peers) {
            (ClusterMode::Single, true) => Err(
                "mode = \"single\" but `peers` is non-empty; omit peers or pick a \
                 multi-peer mode"
                    .into(),
            ),
            (ClusterMode::Routing, false) => {
                Err("mode = \"routing\" requires a non-empty `peers` list".into())
            }
            (ClusterMode::Raft, false) => {
                Err("mode = \"raft\" requires a non-empty `peers` list".into())
            }
            _ => {
                if has_peers && !self.peers.iter().any(|p| p.id == self.self_id) {
                    return Err(format!("self_id {} is not in the peers list", self.self_id));
                }
                Ok(())
            }
        }
    }
}
