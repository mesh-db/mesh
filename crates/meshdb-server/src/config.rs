use serde::Deserialize;
use std::path::{Path, PathBuf};

/// How a multi-peer server operates on top of its peer list. See
/// [`ServerConfig::resolved_mode`] for the defaulting rules that apply
/// when the TOML config omits `mode` entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
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

    /// Optional address for the Prometheus metrics endpoint (e.g.
    /// `127.0.0.1:9090`). When present, the server binds a tiny
    /// HTTP listener serving `GET /metrics` with the Prometheus
    /// text encoding of every registered counter / gauge /
    /// histogram. Omit to disable metrics export entirely; the
    /// in-process counters still increment regardless.
    #[serde(default)]
    pub metrics_address: Option<String>,

    /// Optional Bolt protocol authentication. When set, every
    /// incoming Bolt `HELLO` is validated against the configured
    /// user table: `scheme = "basic"` with a matching username +
    /// password succeeds, anything else responds with a
    /// `Neo.ClientError.Security.Unauthorized` failure and the
    /// connection is closed. Omitted → accept-any, which is the
    /// pre-auth default behaviour and what the existing configs
    /// in the repo rely on.
    #[serde(default)]
    pub bolt_auth: Option<BoltAuthConfig>,

    /// Optional TLS termination for the Bolt listener. When set, the
    /// listener negotiates TLS on every accepted socket before entering
    /// the Bolt handshake — clients connect with `bolt+s://` (or
    /// `bolt+ssc://` with self-signed certs) rather than plain `bolt://`.
    /// Omitted → plaintext Bolt, which is the pre-TLS default and fine
    /// for `127.0.0.1` dev setups.
    #[serde(default)]
    pub bolt_tls: Option<BoltTlsConfig>,

    /// Optional clamp on which Bolt versions the listener advertises.
    /// Each entry is one of `"4.4"`, `"5.0"`, `"5.1"`, `"5.2"`, `"5.3"`,
    /// `"5.4"`. Omitted (or empty) → advertise the full
    /// [`meshdb_bolt::SUPPORTED`] set, which is the default behaviour
    /// every existing config relies on.
    ///
    /// Used by the driver-matrix harness to pin a connecting driver to
    /// a specific Bolt version per test cell — Neo4j drivers don't
    /// expose a wire-version flag, so the only way to force them to
    /// negotiate down is to trim what the server advertises.
    #[serde(default)]
    pub bolt_advertised_versions: Option<Vec<String>>,

    /// Optional TLS for the gRPC listener and outgoing inter-peer gRPC
    /// channels. Every peer is both a gRPC server and a gRPC client
    /// (Raft heartbeats, leader forwarding, scatter-gather reads), so
    /// the same section describes both the identity this peer presents
    /// and the CA it uses to verify remote peers. When set on one peer
    /// of a cluster it must be set on all peers; mixed-mode clusters
    /// are not supported.
    #[serde(default)]
    pub grpc_tls: Option<GrpcTlsConfig>,

    /// Cluster operating mode. Omitted → inferred from `peers` (empty
    /// → Single, non-empty → Raft) for backward compatibility with
    /// configs from before this field existed. Set explicitly to
    /// `"routing"` to run a hash-partitioned non-Raft cluster that
    /// uses the 2PC coordinator + recovery log.
    #[serde(default)]
    pub mode: Option<ClusterMode>,

    /// Configuration for `apoc.load.*` (and, in the future,
    /// `apoc.export.*`). Omitted → every load call fails with a
    /// message pointing at the config key. A `[apoc_import]`
    /// section opts in, setting `enabled`, `allow_file`,
    /// `allow_http`, `file_root`, and `url_allowlist` per the
    /// [`ImportConfig`](meshdb_executor::ImportConfig) docs.
    #[cfg(feature = "apoc-load")]
    #[serde(default)]
    pub apoc_import: Option<meshdb_executor::ImportConfig>,
}

fn default_num_partitions() -> u32 {
    4
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub id: u64,
    pub address: String,
    /// Optional Bolt listener address for this peer. When present,
    /// cluster-aware Neo4j drivers (`neo4j://`-scheme connections)
    /// see this peer in the routing table the server returns on
    /// ROUTE messages. Peers without `bolt_address` are silently
    /// skipped from the routing table — existing configs keep
    /// working and just advertise a smaller set of endpoints.
    ///
    /// Format matches `bolt_address` at the top level of the
    /// config (e.g. `"peer-b.internal:7687"`). Needed because
    /// the gRPC `address` runs on a different port than the Bolt
    /// listener, so drivers can't derive one from the other.
    #[serde(default)]
    pub bolt_address: Option<String>,
}

/// Authentication table for the Bolt listener. Enabled by adding a
/// `[bolt_auth]` section to the TOML config with one or more
/// `[[bolt_auth.users]]` entries. Absent → unauthenticated
/// (accept-any), which matches the pre-auth behavior.
///
/// The `password` field accepts either:
///
///   * A bcrypt hash — any string starting with `$2a$`, `$2b$`,
///     `$2y$`, or `$2x$`. Verified with [`bcrypt::verify`]. This
///     is the recommended form for anything beyond local dev.
///   * Plain text — any string that doesn't match a bcrypt
///     prefix. Compared directly. Fine for local dev, not safe
///     for shared infrastructure since the config file holds
///     the credential.
///
/// Both forms are supported so existing dev configs keep working
/// and operators can migrate a single user at a time without a
/// big-bang rewrite.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoltAuthConfig {
    #[serde(default)]
    pub users: Vec<BoltUser>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoltUser {
    pub username: String,
    pub password: String,
}

/// TLS material for the Bolt listener. Both files are PEM-encoded:
///
///   * `cert_path` → one or more X.509 certificates. The server presents
///     them as the chain, leaf first.
///   * `key_path` → the private key matching the leaf certificate.
///     PKCS#8, SEC1 (EC), and RSA formats are all accepted — the first
///     private key found in the file wins.
///
/// Generate a self-signed cert for local dev with:
/// ```sh
/// openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
///   -keyout key.pem -out cert.pem -days 365 -nodes \
///   -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost'
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoltTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

/// TLS material for the gRPC listener and outbound peer channels.
/// Every field is PEM-encoded:
///
///   * `cert_path` → the certificate chain this peer presents to both
///     inbound connections (as the server identity) and to peer
///     verification (the chain must validate against every other
///     peer's `ca_path`). Leaf first.
///   * `key_path` → the private key matching the leaf certificate.
///   * `ca_path` → the bundle of trusted CA certificates this peer
///     uses when *connecting* to remote peers as a client. For small
///     setups this is usually the same self-signed cert that every
///     peer presents; for PKI-backed clusters it's the root / issuing
///     CA that signed each peer's cert.
///
/// All three fields must point at readable files when the section is
/// present — validated at startup so a typo in the path surfaces
/// immediately rather than on the first inter-peer RPC.
///
/// Peer certificates should carry Subject Alternative Names covering
/// every address peers reach this server by. For a loopback dev setup
/// on `127.0.0.1:7001` that means `DNS:localhost, IP:127.0.0.1`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
}

impl BoltAuthConfig {
    /// Returns `true` when `username` / `password` matches any
    /// configured user. Routes to [`bcrypt::verify`] when the
    /// stored password looks like a bcrypt hash, or plain-text
    /// equality otherwise.
    pub fn verify(&self, username: &str, password: &str) -> bool {
        self.users
            .iter()
            .any(|u| u.username == username && password_matches(&u.password, password))
    }
}

/// True when `provided` matches `stored`. Sniffs bcrypt hashes
/// by their canonical 4-character prefix (`$2a$` / `$2b$` /
/// `$2y$` / `$2x$`) — anything else is treated as plain text.
/// A malformed bcrypt hash produces `false` rather than
/// panicking so a typo in the config file can't crash the
/// server on the first login attempt.
fn password_matches(stored: &str, provided: &str) -> bool {
    if is_bcrypt_hash(stored) {
        bcrypt::verify(provided, stored).unwrap_or(false)
    } else {
        stored == provided
    }
}

fn is_bcrypt_hash(s: &str) -> bool {
    matches!(
        s.get(..4),
        Some("$2a$") | Some("$2b$") | Some("$2y$") | Some("$2x$")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bcrypt at its minimum valid cost (4) — fast enough for
    /// tests to generate a fresh hash per run without slowing
    /// down `cargo test`.
    fn hash(pw: &str) -> String {
        bcrypt::hash(pw, 4).expect("bcrypt hash")
    }

    #[test]
    fn verify_accepts_plaintext_password() {
        let cfg = BoltAuthConfig {
            users: vec![BoltUser {
                username: "ada".into(),
                password: "secret".into(),
            }],
        };
        assert!(cfg.verify("ada", "secret"));
        assert!(!cfg.verify("ada", "wrong"));
        assert!(!cfg.verify("bob", "secret"));
    }

    #[test]
    fn verify_accepts_bcrypt_hashed_password() {
        let cfg = BoltAuthConfig {
            users: vec![BoltUser {
                username: "ada".into(),
                password: hash("secret"),
            }],
        };
        assert!(cfg.verify("ada", "secret"));
        assert!(!cfg.verify("ada", "wrong"));
    }

    #[test]
    fn verify_handles_malformed_bcrypt_hash_gracefully() {
        // Starts with `$2a$` so the sniffer routes to bcrypt,
        // but the rest is garbage. Should return false, not
        // panic.
        let cfg = BoltAuthConfig {
            users: vec![BoltUser {
                username: "ada".into(),
                password: "$2a$not-a-real-hash".into(),
            }],
        };
        assert!(!cfg.verify("ada", "anything"));
    }

    #[test]
    fn verify_mixes_plain_and_bcrypt_users() {
        // Operators can migrate users one at a time. Both
        // forms live in the same table and work independently.
        let cfg = BoltAuthConfig {
            users: vec![
                BoltUser {
                    username: "ada".into(),
                    password: "plain".into(),
                },
                BoltUser {
                    username: "bob".into(),
                    password: hash("hashed"),
                },
            ],
        };
        assert!(cfg.verify("ada", "plain"));
        assert!(cfg.verify("bob", "hashed"));
        assert!(!cfg.verify("ada", "hashed"));
        assert!(!cfg.verify("bob", "plain"));
    }

    #[test]
    #[ignore]
    fn print_hash_for_password_literal() {
        // Manual helper: run with `cargo test --lib \
        // print_hash_for_password_literal -- --ignored --nocapture`
        // to dump a freshly-salted bcrypt hash for the literal
        // string "password". Used to seed the sample configs in
        // the repo root; left `#[ignore]` so the regular suite
        // isn't slowed down.
        let hash = bcrypt::hash("password", 12).unwrap();
        println!("HASH={}", hash);
    }

    #[test]
    fn is_bcrypt_hash_recognizes_all_canonical_prefixes() {
        assert!(is_bcrypt_hash("$2a$10$abc"));
        assert!(is_bcrypt_hash("$2b$10$abc"));
        assert!(is_bcrypt_hash("$2y$10$abc"));
        assert!(is_bcrypt_hash("$2x$10$abc"));
        assert!(!is_bcrypt_hash("plaintext"));
        assert!(!is_bcrypt_hash("$1$md5-ish"));
        assert!(!is_bcrypt_hash(""));
    }
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

    /// Resolve `bolt_advertised_versions` to the wire-format byte
    /// triples consumed by [`meshdb_bolt::perform_server_handshake_with`].
    /// Returns `None` when the config doesn't restrict the set (the
    /// listener should advertise the full [`meshdb_bolt::SUPPORTED`]
    /// list); returns `Some(vec)` ordered newest-first so the
    /// negotiation honours preference even on a clamped set.
    /// `Err` on an unrecognised label.
    pub fn resolved_bolt_versions(&self) -> Result<Option<Vec<[u8; 4]>>, String> {
        let Some(labels) = &self.bolt_advertised_versions else {
            return Ok(None);
        };
        if labels.is_empty() {
            return Ok(None);
        }
        let mut bytes = Vec::with_capacity(labels.len());
        for label in labels {
            let v = match label.as_str() {
                "4.4" => meshdb_bolt::BOLT_4_4,
                "5.0" => meshdb_bolt::BOLT_5_0,
                "5.1" => meshdb_bolt::BOLT_5_1,
                "5.2" => meshdb_bolt::BOLT_5_2,
                "5.3" => meshdb_bolt::BOLT_5_3,
                "5.4" => meshdb_bolt::BOLT_5_4,
                other => {
                    return Err(format!(
                        "bolt_advertised_versions entry `{other}` is not one of \
                         4.4, 5.0, 5.1, 5.2, 5.3, 5.4"
                    ))
                }
            };
            bytes.push(v);
        }
        // Sort newest-first to match SUPPORTED's preference order.
        bytes.sort_by(|a, b| (b[3], b[2]).cmp(&(a[3], a[2])));
        bytes.dedup();
        Ok(Some(bytes))
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
                if self.bolt_tls.is_some() && self.bolt_address.is_none() {
                    return Err("`bolt_tls` set but `bolt_address` is missing; \
                                TLS only applies when the Bolt listener is enabled"
                        .into());
                }
                if let Some(tls) = &self.grpc_tls {
                    for (role, path) in [
                        ("grpc_tls.cert_path", &tls.cert_path),
                        ("grpc_tls.key_path", &tls.key_path),
                        ("grpc_tls.ca_path", &tls.ca_path),
                    ] {
                        if !path.is_file() {
                            return Err(format!(
                                "{role} points at {}, which is not a readable file",
                                path.display()
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }
}
