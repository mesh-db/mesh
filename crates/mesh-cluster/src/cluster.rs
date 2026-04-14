use crate::{
    ClusterCommand, ClusterState, Error, Membership, PartitionId, PartitionMap, Partitioner,
    Peer, PeerId, Result,
};
use mesh_core::NodeId;

/// Cluster configuration and routing facade.
///
/// Holds a [`Partitioner`] (hash → partition) plus a [`ClusterState`]
/// (partition → owner, and peer → address). Combines them to answer the
/// central routing question: given a `NodeId`, which peer owns it?
///
/// State-mutating operations go through [`Cluster::apply`], which is the
/// hook a Raft-backed consensus layer will eventually drive.
#[derive(Debug, Clone)]
pub struct Cluster {
    self_id: PeerId,
    partitioner: Partitioner,
    state: ClusterState,
}

impl Cluster {
    /// Build a cluster with a round-robin partition map across the given peers.
    pub fn new(self_id: PeerId, num_partitions: u32, peers: Vec<Peer>) -> Result<Self> {
        if peers.is_empty() {
            return Err(Error::NoPeers);
        }
        if num_partitions == 0 {
            return Err(Error::ZeroPartitions);
        }

        let partitioner = Partitioner::new(num_partitions);
        let peer_ids: Vec<PeerId> = peers.iter().map(|p| p.id).collect();
        let partition_map = PartitionMap::round_robin(&peer_ids, num_partitions)?;
        let membership = Membership::new(peers);

        if !membership.contains(self_id) {
            return Err(Error::UnknownPeer(self_id));
        }

        Ok(Self {
            self_id,
            partitioner,
            state: ClusterState::new(membership, partition_map),
        })
    }

    /// Construct from pre-built components (useful once a real partition
    /// assignment strategy replaces round-robin).
    pub fn from_parts(
        self_id: PeerId,
        partitioner: Partitioner,
        partition_map: PartitionMap,
        membership: Membership,
    ) -> Result<Self> {
        if membership.is_empty() {
            return Err(Error::NoPeers);
        }
        if partition_map.num_partitions() != partitioner.num_partitions() {
            return Err(Error::PartitionMapLengthMismatch {
                map_len: partition_map.num_partitions() as usize,
                expected: partitioner.num_partitions(),
            });
        }
        if !membership.contains(self_id) {
            return Err(Error::UnknownPeer(self_id));
        }
        for assignment in partition_map.assignments() {
            if !membership.contains(*assignment) {
                return Err(Error::UnknownPeer(*assignment));
            }
        }
        Ok(Self {
            self_id,
            partitioner,
            state: ClusterState::new(membership, partition_map),
        })
    }

    pub fn self_id(&self) -> PeerId {
        self.self_id
    }

    pub fn partitioner(&self) -> &Partitioner {
        &self.partitioner
    }

    pub fn partition_map(&self) -> &PartitionMap {
        &self.state.partition_map
    }

    pub fn membership(&self) -> &Membership {
        &self.state.membership
    }

    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    pub fn num_partitions(&self) -> u32 {
        self.partitioner.num_partitions()
    }

    /// Which partition would own this node id?
    pub fn partition_for(&self, node_id: NodeId) -> PartitionId {
        self.partitioner.partition_for(node_id)
    }

    /// Which peer currently owns the partition this node id hashes to?
    pub fn owner_of(&self, node_id: NodeId) -> PeerId {
        self.state.partition_map.owner(self.partition_for(node_id))
    }

    /// Does this node id belong to the local peer?
    pub fn is_local(&self, node_id: NodeId) -> bool {
        self.owner_of(node_id) == self.self_id
    }

    /// Network address of the peer that owns this node id.
    pub fn owner_address(&self, node_id: NodeId) -> Option<&str> {
        self.state.membership.address(self.owner_of(node_id))
    }

    /// Network address of a specific peer.
    pub fn peer_address(&self, id: PeerId) -> Option<&str> {
        self.state.membership.address(id)
    }

    /// Apply a command to the cluster state. Rejects commands that would
    /// remove the local peer (that has to go through a leadership handoff
    /// flow that doesn't exist yet). All other failures come from the
    /// underlying [`ClusterState::apply`] transition.
    pub fn apply(&mut self, command: &ClusterCommand) -> Result<()> {
        if let ClusterCommand::RemovePeer { id } = command {
            if *id == self.self_id {
                return Err(Error::CannotRemoveSelf);
            }
        }
        self.state.apply(command)
    }
}
