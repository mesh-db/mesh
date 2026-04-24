use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PeerId(pub u64);

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub id: PeerId,
    pub address: String,
    /// Optional Bolt endpoint for this peer. When present, a
    /// cluster-aware Neo4j driver sees this peer in the routing
    /// table the server returns on ROUTE messages. `serde(default)`
    /// + `skip_serializing_if` keeps the wire shape backward-compat:
    /// absent in old Raft snapshots → `None`; `None` in fresh
    /// records → no field emitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bolt_address: Option<String>,
}

impl Peer {
    pub fn new(id: PeerId, address: impl Into<String>) -> Self {
        Self {
            id,
            address: address.into(),
            bolt_address: None,
        }
    }

    /// Convenience builder for peers that advertise a Bolt endpoint.
    pub fn with_bolt_address(mut self, bolt_address: impl Into<String>) -> Self {
        self.bolt_address = Some(bolt_address.into());
        self
    }
}

/// Static membership list: peer id -> {gRPC address, optional Bolt address}.
///
/// Stored as a `BTreeMap<PeerId, PeerRecord>` for O(log n) address lookup,
/// serialized as a `Vec<Peer>` so the wire format is independent of JSON's
/// string-key constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    peers: BTreeMap<PeerId, PeerRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerRecord {
    address: String,
    bolt_address: Option<String>,
}

impl Membership {
    pub fn new<I: IntoIterator<Item = Peer>>(peers: I) -> Self {
        let peers = peers
            .into_iter()
            .map(|p| {
                (
                    p.id,
                    PeerRecord {
                        address: p.address,
                        bolt_address: p.bolt_address,
                    },
                )
            })
            .collect();
        Self { peers }
    }

    pub fn address(&self, id: PeerId) -> Option<&str> {
        self.peers.get(&id).map(|r| r.address.as_str())
    }

    /// Bolt endpoint for `id`, if any. `None` is returned both for
    /// unknown peers and for peers without a configured bolt endpoint;
    /// callers that need to distinguish the two should check
    /// [`contains`] first.
    pub fn bolt_address(&self, id: PeerId) -> Option<&str> {
        self.peers.get(&id).and_then(|r| r.bolt_address.as_deref())
    }

    pub fn contains(&self, id: PeerId) -> bool {
        self.peers.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn peer_ids(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.keys().copied()
    }

    /// Iterate `(id, gRPC address)` pairs. Kept for call sites that
    /// only care about the gRPC surface.
    pub fn iter(&self) -> impl Iterator<Item = (PeerId, &str)> {
        self.peers.iter().map(|(id, r)| (*id, r.address.as_str()))
    }

    /// Iterate `(id, gRPC address, optional Bolt address)` triples.
    /// Used by the ROUTE handler to assemble multi-peer routing
    /// tables for cluster-aware Neo4j drivers.
    pub fn iter_full(&self) -> impl Iterator<Item = (PeerId, &str, Option<&str>)> {
        self.peers
            .iter()
            .map(|(id, r)| (*id, r.address.as_str(), r.bolt_address.as_deref()))
    }
}

impl Serialize for Membership {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let peers: Vec<Peer> = self
            .peers
            .iter()
            .map(|(id, r)| Peer {
                id: *id,
                address: r.address.clone(),
                bolt_address: r.bolt_address.clone(),
            })
            .collect();
        peers.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Membership {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let peers: Vec<Peer> = Vec::deserialize(deserializer)?;
        Ok(Membership::new(peers))
    }
}
