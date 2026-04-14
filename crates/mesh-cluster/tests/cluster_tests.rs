use mesh_cluster::raft::{ApplyResponse, MeshRaftConfig, NodeId as RaftNodeId};
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

#[tokio::test]
async fn raft_single_node_bootstraps_and_applies_commands() {
    use mesh_cluster::raft::RaftCluster;
    use mesh_cluster::{ClusterState, Membership, PartitionMap};

    // Initial state: one peer (self), one partition.
    let initial = ClusterState::new(
        Membership::new(vec![Peer::new(PeerId(1), "127.0.0.1:7001")]),
        PartitionMap::round_robin(&[PeerId(1)], 1).unwrap(),
    );

    let cluster = RaftCluster::new_single_node(1, initial)
        .await
        .expect("raft should bootstrap");

    // Drive a command through the full client_write → commit → apply cycle.
    cluster
        .propose(ClusterCommand::AddPeer {
            id: PeerId(2),
            address: "127.0.0.1:7002".into(),
        })
        .await
        .expect("propose should succeed");

    // State machine applied the change.
    let state = cluster.current_state().await;
    assert_eq!(state.membership.len(), 2);
    assert_eq!(state.membership.address(PeerId(2)), Some("127.0.0.1:7002"));

    cluster.shutdown().await.ok();
}

#[tokio::test]
async fn raft_persistent_store_reloads_log_and_vote_after_restart() {
    use mesh_cluster::raft::{MemStore, MeshRaftConfig, NoOpNetwork, RaftCluster};
    use mesh_cluster::{ClusterState, Membership, PartitionMap};
    use openraft::storage::{RaftLogReader, RaftStorage};
    use openraft::Vote;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("raft");

    let initial = || {
        ClusterState::new(
            Membership::new(vec![Peer::new(PeerId(1), "127.0.0.1:7001")]),
            PartitionMap::round_robin(&[PeerId(1)], 1).unwrap(),
        )
    };

    // Bootstrap a single-node cluster against the persistent store and
    // drive a few commands through it so the log has non-trivial content.
    {
        let cluster =
            RaftCluster::open_persistent(1, &path, initial(), NoOpNetwork, None)
                .await
                .expect("open persistent raft");
        let mut members = std::collections::BTreeSet::new();
        members.insert(1u64);
        cluster
            .raft
            .initialize(members)
            .await
            .expect("initialize single-node cluster");
        cluster
            .propose(ClusterCommand::AddPeer {
                id: PeerId(2),
                address: "127.0.0.1:7002".into(),
            })
            .await
            .expect("propose add peer");
        cluster
            .propose(ClusterCommand::AddPeer {
                id: PeerId(3),
                address: "127.0.0.1:7003".into(),
            })
            .await
            .expect("propose add peer");
        cluster.shutdown().await.ok();
    }

    // Re-open the store directly (bypassing RaftCluster, whose startup
    // would race with the NoOpNetwork-backed election loop) and confirm
    // the log and vote survived the drop.
    let mut store = MemStore::open_persistent(&path, initial(), None)
        .expect("reopen persistent store");
    let entries = <MemStore as RaftLogReader<MeshRaftConfig>>::try_get_log_entries(
        &mut store,
        ..,
    )
    .await
    .expect("read log entries");
    assert!(
        entries.len() >= 2,
        "expected at least 2 entries (initial membership + 2 AddPeer), got {}",
        entries.len()
    );
    let vote = <MemStore as RaftStorage<MeshRaftConfig>>::read_vote(&mut store)
        .await
        .expect("read vote");
    assert!(vote.is_some(), "vote should have been persisted");
    let _: Vote<RaftNodeId> = vote.unwrap();
}

#[tokio::test]
async fn raft_single_node_applies_command_sequence_in_order() {
    use mesh_cluster::raft::RaftCluster;
    use mesh_cluster::{ClusterState, Membership, PartitionMap};

    let initial = ClusterState::new(
        Membership::new(vec![Peer::new(PeerId(1), "127.0.0.1:7001")]),
        PartitionMap::round_robin(&[PeerId(1)], 4).unwrap(),
    );
    let cluster = RaftCluster::new_single_node(1, initial).await.unwrap();

    cluster
        .propose(ClusterCommand::AddPeer {
            id: PeerId(2),
            address: "b".into(),
        })
        .await
        .unwrap();
    cluster
        .propose(ClusterCommand::AddPeer {
            id: PeerId(3),
            address: "c".into(),
        })
        .await
        .unwrap();
    cluster.propose(ClusterCommand::Rebalance).await.unwrap();
    cluster
        .propose(ClusterCommand::UpdatePeerAddress {
            id: PeerId(2),
            address: "b-new".into(),
        })
        .await
        .unwrap();

    let state = cluster.current_state().await;
    assert_eq!(state.membership.len(), 3);
    assert_eq!(state.membership.address(PeerId(2)), Some("b-new"));
    // All current members referenced by the partition map after Rebalance.
    for owner in state.partition_map.assignments() {
        assert!(state.membership.contains(*owner));
    }
    // Every peer should own at least one partition after the 3-peer rebalance.
    for pid in [PeerId(1), PeerId(2), PeerId(3)] {
        assert!(
            state.partition_map.assignments().contains(&pid),
            "{pid} missing from partition map"
        );
    }

    cluster.shutdown().await.ok();
}

#[test]
fn openraft_type_config_binds_domain_types() {
    // The declare_raft_types! macro at mesh_cluster::raft ties ClusterCommand
    // to D and ApplyResponse to R. If this compiles, the TypeConfig is usable
    // as an openraft input and every follow-up trait impl can rely on the
    // same type bindings.
    fn _assert_config_type<C: openraft::RaftTypeConfig>() {}
    _assert_config_type::<MeshRaftConfig>();
}

#[test]
fn openraft_config_defaults_tuned_for_tests() {
    let config = mesh_cluster::raft::default_config();
    assert_eq!(config.heartbeat_interval, 250);
    assert!(config.election_timeout_min < config.election_timeout_max);
}

#[test]
fn raft_node_id_is_isomorphic_to_peer_id() {
    let pid = PeerId(42);
    let nid: RaftNodeId = pid.into();
    assert_eq!(nid, 42);
    let back: PeerId = nid.into();
    assert_eq!(back, pid);
}

#[test]
fn apply_response_roundtrips_through_serde() {
    let r = ApplyResponse::default();
    let bytes = serde_json::to_vec(&r).unwrap();
    let back: ApplyResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(r, back);
}

#[test]
fn cluster_command_roundtrips_through_serde_json() {
    let commands = vec![
        ClusterCommand::AddPeer {
            id: PeerId(7),
            address: "10.0.0.7:9000".into(),
        },
        ClusterCommand::RemovePeer { id: PeerId(3) },
        ClusterCommand::UpdatePeerAddress {
            id: PeerId(2),
            address: "new-addr:1234".into(),
        },
        ClusterCommand::Rebalance,
    ];
    for cmd in commands {
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ClusterCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
    }
}

#[test]
fn cluster_state_roundtrips_through_serde_json() {
    use mesh_cluster::{ClusterState, Membership, PartitionMap};
    let membership = Membership::new(sample_peers());
    let partition_map =
        PartitionMap::round_robin(&[PeerId(1), PeerId(2), PeerId(3)], 6).unwrap();
    let state = ClusterState::new(membership, partition_map);

    let json = serde_json::to_string(&state).unwrap();
    let back: ClusterState = serde_json::from_str(&json).unwrap();
    assert_eq!(state, back);
}

#[test]
fn cluster_state_serializes_membership_as_vec_of_peers() {
    // Verify the wire format of Membership is a JSON array, not an
    // object-keyed-by-int — that's the property we wanted when switching to
    // the custom impl, and we want to lock it in so future changes are caught.
    use mesh_cluster::{ClusterState, Membership, PartitionMap};
    let membership = Membership::new(sample_peers());
    let partition_map =
        PartitionMap::round_robin(&[PeerId(1), PeerId(2), PeerId(3)], 3).unwrap();
    let state = ClusterState::new(membership, partition_map);
    let json = serde_json::to_value(&state).unwrap();
    let membership_json = &json["membership"];
    assert!(
        membership_json.is_array(),
        "expected membership to serialize as array, got {membership_json}"
    );
    assert_eq!(membership_json.as_array().unwrap().len(), 3);
}

#[test]
fn cluster_state_after_apply_still_roundtrips() {
    use mesh_cluster::{ClusterState, Membership, PartitionMap};
    let membership = Membership::new(sample_peers());
    let partition_map =
        PartitionMap::round_robin(&[PeerId(1), PeerId(2), PeerId(3)], 6).unwrap();
    let mut state = ClusterState::new(membership, partition_map);
    state
        .apply(&ClusterCommand::AddPeer {
            id: PeerId(4),
            address: "d".into(),
        })
        .unwrap();
    state.apply(&ClusterCommand::Rebalance).unwrap();
    state
        .apply(&ClusterCommand::RemovePeer { id: PeerId(2) })
        .unwrap();

    let json = serde_json::to_string(&state).unwrap();
    let back: ClusterState = serde_json::from_str(&json).unwrap();
    assert_eq!(state, back);
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
