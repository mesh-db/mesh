use crate::{Error, Membership, PartitionMap, Peer, PeerId, Result};
use meshdb_core::{Edge, EdgeId, Node, NodeId};
use serde::{Deserialize, Serialize};

/// The replicated portion of a cluster: who the members are and which peer
/// owns each partition. This is the type a consensus layer (Raft) will
/// snapshot, apply entries against, and hand out to readers.
///
/// Invariants (maintained by [`ClusterState::apply`]):
/// - `membership` is non-empty.
/// - Every partition in `partition_map` is assigned to a peer that exists in
///   `membership`.
/// - The partitioner (held by [`crate::Cluster`]) is fixed; `apply` never
///   changes the number of partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterState {
    pub membership: Membership,
    pub partition_map: PartitionMap,
}

/// Log entries that a consensus layer will replay against [`ClusterState`].
///
/// Adding a peer does **not** automatically rebalance — callers that want
/// partitions redistributed must follow with [`ClusterCommand::Rebalance`].
/// Removing a peer, however, must reassign that peer's partitions (otherwise
/// they'd have no owner), so `RemovePeer` implicitly reassigns them
/// round-robin across the remaining members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterCommand {
    AddPeer { id: PeerId, address: String },
    RemovePeer { id: PeerId },
    UpdatePeerAddress { id: PeerId, address: String },
    Rebalance,
}

/// Mutations applied to the graph store. Replicated through Raft so every
/// peer's local store ends up with the same data.
///
/// `PutNode` / `PutEdge` carry the full entity bytes; `DeleteEdge` /
/// `DetachDeleteNode` carry only the id. `Batch` carries a sequence of
/// mutations that must be applied as one Raft entry — used to give a
/// multi-write Cypher query single-commit atomicity. Float properties on
/// `Node`/`Edge` mean this enum can only derive `PartialEq`, not `Eq`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GraphCommand {
    PutNode(Node),
    PutEdge(Edge),
    DeleteEdge(EdgeId),
    DetachDeleteNode(NodeId),
    Batch(Vec<GraphCommand>),
    /// Declare a property index on every replica. Reaches every peer
    /// through the Raft log, so the resulting `(label, property)`
    /// index is consistent across the cluster and each peer's
    /// `StoreGraphApplier` runs its own backfill against the local
    /// graph copy.
    CreateIndex {
        label: String,
        property: String,
    },
    /// Tear down a property index across every replica. Mirrors
    /// `CreateIndex` — idempotent when the index doesn't exist.
    DropIndex {
        label: String,
        property: String,
    },
    /// Declare a property constraint on every replica. Reaches every
    /// peer through the Raft log / routing-mode fan-out, so the
    /// resulting registry entry and its on-write enforcement agree
    /// across the cluster. `name` is `None` when the user omitted it
    /// at the surface; each peer resolves the default name
    /// deterministically during apply, so the log entry stays
    /// idempotent under re-replay.
    CreateConstraint {
        name: Option<String>,
        label: String,
        /// Non-empty list of property names the constraint applies
        /// to. Single-property kinds (`Unique`, `NotNull`,
        /// `PropertyType`) carry a one-element list; `NodeKey`
        /// carries the full composite tuple.
        properties: Vec<String>,
        kind: ConstraintKind,
        if_not_exists: bool,
    },
    /// Tear down a constraint by name across every replica. Mirrors
    /// `CreateConstraint`. `if_exists` lets replicas treat a missing
    /// constraint as a no-op rather than erroring — needed for
    /// routing-mode rollback so a DROP that only reached some peers
    /// can be safely retried / inverted.
    DropConstraint {
        name: String,
        if_exists: bool,
    },
}

/// Cluster-visible constraint kind. Mirrors
/// `meshdb_storage::PropertyConstraintKind` but kept in the
/// cluster crate so the Raft log entry doesn't need to depend on the
/// storage crate. Converters on both sides keep the two enums in
/// lockstep. `PropertyType` carries the target type inline so a
/// single variant covers every `IS :: <TYPE>` flavour instead of
/// exploding into one per type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintKind {
    Unique,
    NotNull,
    PropertyType(PropertyType),
    /// Composite `REQUIRE (n.a, n.b) IS NODE KEY` — tuple uniqueness
    /// plus a NOT NULL obligation on every listed property. The
    /// property list lives on the enclosing `GraphCommand::CreateConstraint`
    /// so replicas see the exact tuple the proposer sent.
    NodeKey,
}

/// Property types recognised by `IS :: <TYPE>`. Kept numeric-neutral
/// (Integer vs Float distinct, no coercion) to match the storage
/// layer's strict semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropertyType {
    String,
    Integer,
    Float,
    Boolean,
}

impl PropertyType {
    pub fn name_tag(&self) -> &'static str {
        match self {
            PropertyType::String => "string",
            PropertyType::Integer => "integer",
            PropertyType::Float => "float",
            PropertyType::Boolean => "boolean",
        }
    }
}

impl ConstraintKind {
    /// Lower-case tag used to build the auto-generated constraint
    /// name. Must match `meshdb_storage::PropertyConstraintKind`'s
    /// naming scheme — the storage layer uses the same tag — so
    /// `GraphCommand::DropConstraint` rollbacks can reconstruct the
    /// resolved name without a cross-crate lookup.
    pub fn name_tag(&self) -> String {
        match self {
            ConstraintKind::Unique => "unique".into(),
            ConstraintKind::NotNull => "not_null".into(),
            ConstraintKind::PropertyType(t) => format!("type_{}", t.name_tag()),
            ConstraintKind::NodeKey => "node_key".into(),
        }
    }
}

/// Deterministic auto-name for an un-named constraint. Matches the
/// format the storage layer uses so followers resolving a
/// `CreateConstraint { name: None, ... }` entry produce the same
/// final name as the proposer. Exposed so the routing-mode DDL
/// fan-out can compute the drop name during rollback.
pub fn resolved_constraint_name(
    name: &Option<String>,
    label: &str,
    properties: &[String],
    kind: ConstraintKind,
) -> String {
    match name {
        Some(n) => n.clone(),
        None => {
            let joined = properties.join("_");
            format!("constraint_{label}_{joined}_{}", kind.name_tag())
        }
    }
}

/// Top-level Raft log entry: either a cluster-metadata mutation or a graph
/// mutation. `MeshRaftConfig::D` is bound to this so a single Raft group
/// replicates both layers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MeshLogEntry {
    Cluster(ClusterCommand),
    Graph(GraphCommand),
}

impl ClusterState {
    pub fn new(membership: Membership, partition_map: PartitionMap) -> Self {
        Self {
            membership,
            partition_map,
        }
    }

    pub fn apply(&mut self, command: &ClusterCommand) -> Result<()> {
        match command {
            ClusterCommand::AddPeer { id, address } => self.apply_add_peer(*id, address.clone()),
            ClusterCommand::RemovePeer { id } => self.apply_remove_peer(*id),
            ClusterCommand::UpdatePeerAddress { id, address } => {
                self.apply_update_address(*id, address.clone())
            }
            ClusterCommand::Rebalance => self.apply_rebalance(),
        }
    }

    fn apply_add_peer(&mut self, id: PeerId, address: String) -> Result<()> {
        if self.membership.contains(id) {
            return Err(Error::PeerAlreadyExists(id));
        }
        let mut peers: Vec<Peer> = self.current_peers();
        peers.push(Peer::new(id, address));
        self.membership = Membership::new(peers);
        Ok(())
    }

    fn apply_remove_peer(&mut self, id: PeerId) -> Result<()> {
        if !self.membership.contains(id) {
            return Err(Error::UnknownPeer(id));
        }
        if self.membership.len() == 1 {
            return Err(Error::NoPeers);
        }

        let remaining: Vec<Peer> = self
            .current_peers()
            .into_iter()
            .filter(|p| p.id != id)
            .collect();
        let remaining_ids: Vec<PeerId> = remaining.iter().map(|p| p.id).collect();
        self.membership = Membership::new(remaining);

        // Reassign partitions previously owned by `id` round-robin across the
        // remaining peers, preserving all other assignments unchanged.
        let current = self.partition_map.assignments().to_vec();
        let mut next = current.clone();
        let mut rotation = 0usize;
        for (idx, owner) in current.iter().enumerate() {
            if *owner == id {
                next[idx] = remaining_ids[rotation % remaining_ids.len()];
                rotation += 1;
            }
        }
        self.partition_map = PartitionMap::new(next);
        Ok(())
    }

    fn apply_update_address(&mut self, id: PeerId, address: String) -> Result<()> {
        if !self.membership.contains(id) {
            return Err(Error::UnknownPeer(id));
        }
        let peers: Vec<Peer> = self
            .current_peers()
            .into_iter()
            .map(|p| {
                if p.id == id {
                    Peer::new(p.id, address.clone())
                } else {
                    p
                }
            })
            .collect();
        self.membership = Membership::new(peers);
        Ok(())
    }

    fn apply_rebalance(&mut self) -> Result<()> {
        let peer_ids: Vec<PeerId> = self.membership.peer_ids().collect();
        let num = self.partition_map.num_partitions();
        self.partition_map = PartitionMap::round_robin(&peer_ids, num)?;
        Ok(())
    }

    fn current_peers(&self) -> Vec<Peer> {
        self.membership
            .iter()
            .map(|(id, addr)| Peer::new(id, addr.to_string()))
            .collect()
    }
}
