use mesh_cluster::{
    Cluster, ClusterCommand, Error, PartitionId, PartitionMap, Partitioner, Peer, PeerId,
};
use mesh_core::NodeId;
use std::collections::HashMap;

fn sample_peers() -> Vec<Peer> {
    vec![
        Peer::new(PeerId(1), "127.0.0.1:7001"),
        Peer::new(PeerId(2), "127.0.0.1:7002"),
        Peer::new(PeerId(3), "127.0.0.1:7003"),
    ]
}

#[test]
fn partitioner_is_deterministic() {
    let p = Partitioner::new(16);
    let id = NodeId::new();
    let first = p.partition_for(id);
    for _ in 0..100 {
        assert_eq!(p.partition_for(id), first);
    }
}

#[test]
fn partitioner_distributes_across_partitions() {
    let p = Partitioner::new(8);
    let mut counts: HashMap<PartitionId, usize> = HashMap::new();
    for _ in 0..2000 {
        let id = NodeId::new();
        *counts.entry(p.partition_for(id)).or_insert(0) += 1;
    }
    // With 2000 ids across 8 partitions, expected ~250 each. Require
    // all 8 partitions non-empty and each >= 50 as a loose sanity check.
    assert_eq!(counts.len(), 8, "some partitions were empty: {counts:?}");
    for (pid, n) in counts {
        assert!(n >= 50, "partition {pid} only received {n} ids");
    }
}

#[test]
fn partitioner_hashes_same_bytes_to_same_partition() {
    // Fabricate two NodeIds from the same bytes: they should hash the same.
    let raw = [1u8; 16];
    let a = NodeId::from_bytes(raw);
    let b = NodeId::from_bytes(raw);
    let p = Partitioner::new(13);
    assert_eq!(p.partition_for(a), p.partition_for(b));
}

#[test]
fn partition_map_round_robin_assigns_all_partitions() {
    let peers = vec![PeerId(1), PeerId(2), PeerId(3)];
    let map = PartitionMap::round_robin(&peers, 6).unwrap();
    assert_eq!(map.num_partitions(), 6);
    assert_eq!(map.owner(PartitionId(0)), PeerId(1));
    assert_eq!(map.owner(PartitionId(1)), PeerId(2));
    assert_eq!(map.owner(PartitionId(2)), PeerId(3));
    assert_eq!(map.owner(PartitionId(3)), PeerId(1));
    assert_eq!(map.owner(PartitionId(4)), PeerId(2));
    assert_eq!(map.owner(PartitionId(5)), PeerId(3));
}

#[test]
fn partition_map_empty_peers_rejected() {
    let err = PartitionMap::round_robin(&[], 4).unwrap_err();
    assert!(matches!(err, Error::NoPeers));
}

#[test]
fn partition_map_zero_partitions_rejected() {
    let err = PartitionMap::round_robin(&[PeerId(1)], 0).unwrap_err();
    assert!(matches!(err, Error::ZeroPartitions));
}

#[test]
fn cluster_owner_is_deterministic_for_same_node_id() {
    let cluster = Cluster::new(PeerId(1), 8, sample_peers()).unwrap();
    let id = NodeId::new();
    let owner = cluster.owner_of(id);
    for _ in 0..100 {
        assert_eq!(cluster.owner_of(id), owner);
    }
}

#[test]
fn cluster_is_local_only_when_self_owns() {
    let cluster = Cluster::new(PeerId(1), 6, sample_peers()).unwrap();
    let mut seen_local = false;
    let mut seen_remote = false;
    for _ in 0..200 {
        let id = NodeId::new();
        if cluster.is_local(id) {
            seen_local = true;
            assert_eq!(cluster.owner_of(id), PeerId(1));
        } else {
            seen_remote = true;
            assert_ne!(cluster.owner_of(id), PeerId(1));
        }
    }
    assert!(seen_local && seen_remote);
}

#[test]
fn cluster_owner_address_resolves() {
    let cluster = Cluster::new(PeerId(2), 6, sample_peers()).unwrap();
    let id = NodeId::new();
    let owner = cluster.owner_of(id);
    let expected_addr = match owner {
        PeerId(1) => "127.0.0.1:7001",
        PeerId(2) => "127.0.0.1:7002",
        PeerId(3) => "127.0.0.1:7003",
        other => panic!("unexpected owner {other:?}"),
    };
    assert_eq!(cluster.owner_address(id), Some(expected_addr));
}

#[test]
fn cluster_rejects_self_id_not_in_membership() {
    let err = Cluster::new(PeerId(99), 4, sample_peers()).unwrap_err();
    assert!(matches!(err, Error::UnknownPeer(PeerId(99))));
}

#[test]
fn cluster_rejects_empty_peers() {
    let err = Cluster::new(PeerId(1), 4, Vec::new()).unwrap_err();
    assert!(matches!(err, Error::NoPeers));
}

#[test]
fn cluster_rejects_zero_partitions() {
    let err = Cluster::new(PeerId(1), 0, sample_peers()).unwrap_err();
    assert!(matches!(err, Error::ZeroPartitions));
}

#[test]
fn from_parts_rejects_partition_map_referencing_unknown_peer() {
    use mesh_cluster::{Membership, Partitioner};
    let membership = Membership::new(sample_peers());
    let partitioner = Partitioner::new(4);
    // Assignment references a peer that isn't a member.
    let bad_map = PartitionMap::new(vec![PeerId(1), PeerId(1), PeerId(99), PeerId(2)]);
    let err = Cluster::from_parts(PeerId(1), partitioner, bad_map, membership).unwrap_err();
    assert!(matches!(err, Error::UnknownPeer(PeerId(99))));
}

#[test]
fn from_parts_rejects_mismatched_map_length() {
    use mesh_cluster::{Membership, Partitioner};
    let membership = Membership::new(sample_peers());
    let partitioner = Partitioner::new(4);
    let map = PartitionMap::new(vec![PeerId(1), PeerId(2)]); // wrong length
    let err = Cluster::from_parts(PeerId(1), partitioner, map, membership).unwrap_err();
    assert!(matches!(
        err,
        Error::PartitionMapLengthMismatch {
            map_len: 2,
            expected: 4
        }
    ));
}

#[test]
fn apply_add_peer_extends_membership_without_rebalancing() {
    let mut cluster = Cluster::new(PeerId(1), 6, sample_peers()).unwrap();
    let before_partitions = cluster.partition_map().assignments().to_vec();
    cluster
        .apply(&ClusterCommand::AddPeer {
            id: PeerId(4),
            address: "127.0.0.1:7004".into(),
        })
        .unwrap();
    assert_eq!(cluster.membership().len(), 4);
    assert_eq!(cluster.peer_address(PeerId(4)), Some("127.0.0.1:7004"));
    // AddPeer doesn't touch the partition map — Rebalance is required.
    assert_eq!(cluster.partition_map().assignments(), &before_partitions[..]);
}

#[test]
fn apply_add_peer_duplicate_rejected() {
    let mut cluster = Cluster::new(PeerId(1), 4, sample_peers()).unwrap();
    let err = cluster
        .apply(&ClusterCommand::AddPeer {
            id: PeerId(2),
            address: "elsewhere".into(),
        })
        .unwrap_err();
    assert!(matches!(err, Error::PeerAlreadyExists(PeerId(2))));
}

#[test]
fn apply_remove_peer_reassigns_its_partitions() {
    let mut cluster = Cluster::new(PeerId(1), 6, sample_peers()).unwrap();
    cluster
        .apply(&ClusterCommand::RemovePeer { id: PeerId(3) })
        .unwrap();
    assert_eq!(cluster.membership().len(), 2);
    assert!(!cluster.membership().contains(PeerId(3)));
    for owner in cluster.partition_map().assignments() {
        assert_ne!(*owner, PeerId(3), "partition still owned by removed peer");
        assert!(cluster.membership().contains(*owner));
    }
    assert_eq!(
        cluster.partition_map().num_partitions(),
        6,
        "partition count must be invariant"
    );
}

#[test]
fn apply_remove_peer_unknown_rejected() {
    let mut cluster = Cluster::new(PeerId(1), 4, sample_peers()).unwrap();
    let err = cluster
        .apply(&ClusterCommand::RemovePeer { id: PeerId(99) })
        .unwrap_err();
    assert!(matches!(err, Error::UnknownPeer(PeerId(99))));
}

#[test]
fn apply_remove_peer_rejects_removing_self() {
    let mut cluster = Cluster::new(PeerId(1), 4, sample_peers()).unwrap();
    let err = cluster
        .apply(&ClusterCommand::RemovePeer { id: PeerId(1) })
        .unwrap_err();
    assert!(matches!(err, Error::CannotRemoveSelf));
    // State untouched.
    assert_eq!(cluster.membership().len(), 3);
}

#[test]
fn apply_update_peer_address_keeps_partitions() {
    let mut cluster = Cluster::new(PeerId(1), 6, sample_peers()).unwrap();
    let before = cluster.partition_map().assignments().to_vec();
    cluster
        .apply(&ClusterCommand::UpdatePeerAddress {
            id: PeerId(2),
            address: "10.0.0.2:9999".into(),
        })
        .unwrap();
    assert_eq!(cluster.peer_address(PeerId(2)), Some("10.0.0.2:9999"));
    assert_eq!(cluster.partition_map().assignments(), &before[..]);
}

#[test]
fn apply_update_peer_address_unknown_rejected() {
    let mut cluster = Cluster::new(PeerId(1), 4, sample_peers()).unwrap();
    let err = cluster
        .apply(&ClusterCommand::UpdatePeerAddress {
            id: PeerId(42),
            address: "x".into(),
        })
        .unwrap_err();
    assert!(matches!(err, Error::UnknownPeer(PeerId(42))));
}

#[test]
fn apply_rebalance_distributes_across_current_members() {
    let mut cluster = Cluster::new(PeerId(1), 6, sample_peers()).unwrap();
    cluster
        .apply(&ClusterCommand::AddPeer {
            id: PeerId(4),
            address: "d".into(),
        })
        .unwrap();
    // Before rebalance: partitions still split across {1,2,3}.
    assert!(cluster
        .partition_map()
        .assignments()
        .iter()
        .all(|p| *p != PeerId(4)));

    cluster.apply(&ClusterCommand::Rebalance).unwrap();

    // After rebalance: every current member should own at least one partition.
    for pid in [PeerId(1), PeerId(2), PeerId(3), PeerId(4)] {
        assert!(
            cluster.partition_map().assignments().contains(&pid),
            "{pid} missing from rebalanced partition map"
        );
    }
}

#[test]
fn apply_command_sequence_preserves_invariants() {
    let mut cluster = Cluster::new(PeerId(1), 6, sample_peers()).unwrap();

    let commands = vec![
        ClusterCommand::AddPeer {
            id: PeerId(4),
            address: "d".into(),
        },
        ClusterCommand::Rebalance,
        ClusterCommand::RemovePeer { id: PeerId(3) },
        ClusterCommand::UpdatePeerAddress {
            id: PeerId(4),
            address: "new-d".into(),
        },
    ];
    for cmd in &commands {
        cluster.apply(cmd).unwrap();
    }

    // Self still present.
    assert!(cluster.membership().contains(cluster.self_id()));
    // Every partition owner is a known member.
    for owner in cluster.partition_map().assignments() {
        assert!(cluster.membership().contains(*owner));
    }
    // Applied update stuck.
    assert_eq!(cluster.peer_address(PeerId(4)), Some("new-d"));
    // Removed peer is absent.
    assert!(!cluster.membership().contains(PeerId(3)));
    // Partition count is invariant.
    assert_eq!(cluster.partition_map().num_partitions(), 6);
}

#[test]
fn apply_routing_reflects_state_changes() {
    // Build a small cluster, record the owner of a specific NodeId, then
    // rebalance after adding a new peer and verify `owner_of` can now return
    // the new peer for at least one of a batch of node ids.
    let mut cluster = Cluster::new(PeerId(1), 8, sample_peers()).unwrap();
    cluster
        .apply(&ClusterCommand::AddPeer {
            id: PeerId(4),
            address: "d".into(),
        })
        .unwrap();
    cluster.apply(&ClusterCommand::Rebalance).unwrap();

    let mut saw_new_peer_owner = false;
    for _ in 0..200 {
        let id = NodeId::new();
        if cluster.owner_of(id) == PeerId(4) {
            saw_new_peer_owner = true;
            break;
        }
    }
    assert!(
        saw_new_peer_owner,
        "Rebalance should allow the new peer to own at least one partition"
    );
}

#[test]
fn membership_address_lookup_and_iter() {
    use mesh_cluster::Membership;
    let m = Membership::new(sample_peers());
    assert_eq!(m.len(), 3);
    assert_eq!(m.address(PeerId(2)), Some("127.0.0.1:7002"));
    assert_eq!(m.address(PeerId(42)), None);
    let ids: Vec<PeerId> = m.peer_ids().collect();
    assert_eq!(ids, vec![PeerId(1), PeerId(2), PeerId(3)]);
}
