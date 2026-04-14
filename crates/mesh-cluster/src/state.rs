use crate::{Error, Membership, PartitionMap, Peer, PeerId, Result};
use mesh_core::{Edge, EdgeId, Node, NodeId};
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
