//! End-to-end 2-peer Raft integration test over gRPC.
//!
//! Spins up two `RaftCluster` instances, each with a `GrpcNetwork` pointing
//! at the other peer, registers a `MeshRaftService` on each peer's tonic
//! server, bootstraps the cluster, proposes a `ClusterCommand` on the
//! leader, and verifies the follower's state machine replays it.

use mesh_cluster::raft::RaftCluster;
use mesh_cluster::{ClusterCommand, ClusterState, Membership, PartitionMap, Peer, PeerId};
use mesh_rpc::{GrpcNetwork, MeshRaftService};
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct RaftPeer {
    cluster: RaftCluster,
}

async fn bind_listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

fn initial_state(peer_addrs: &[(PeerId, String)]) -> ClusterState {
    let peers: Vec<Peer> = peer_addrs
        .iter()
        .map(|(id, addr)| Peer::new(*id, addr.clone()))
        .collect();
    let membership = Membership::new(peers);
    let ids: Vec<PeerId> = membership.peer_ids().collect();
    let partition_map = PartitionMap::round_robin(&ids, 4).unwrap();
    ClusterState::new(membership, partition_map)
}

async fn spawn_peer(
    id: u64,
    listener: TcpListener,
    peer_map: Vec<(u64, String)>,
    initial: ClusterState,
) -> RaftPeer {
    let network = GrpcNetwork::new(peer_map).expect("valid endpoints");
    let cluster = RaftCluster::new(id, initial, network).await.unwrap();
    let raft_service = MeshRaftService::new(cluster.raft.clone());

    tokio::spawn(async move {
        Server::builder()
            .add_service(raft_service.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    RaftPeer { cluster }
}

#[tokio::test]
async fn two_peer_raft_replicates_commands() {
    // Bind both listeners first so we know each peer's address before either
    // RaftCluster is constructed.
    let (listener_a, addr_a) = bind_listener().await;
    let (listener_b, addr_b) = bind_listener().await;

    let peer_addrs_for_state: Vec<(PeerId, String)> = vec![
        (PeerId(1), addr_a.to_string()),
        (PeerId(2), addr_b.to_string()),
    ];
    let initial = initial_state(&peer_addrs_for_state);

    // Peer A: network has only B (we never send to self).
    let peer_a = spawn_peer(
        1,
        listener_a,
        vec![(2u64, addr_b.to_string())],
        initial.clone(),
    )
    .await;
    // Peer B: network has only A.
    let peer_b = spawn_peer(
        2,
        listener_b,
        vec![(1u64, addr_a.to_string())],
        initial,
    )
    .await;

    // Only peer A calls initialize — B joins via replication.
    let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
    members.insert(1, BasicNode::new(addr_a.to_string()));
    members.insert(2, BasicNode::new(addr_b.to_string()));
    peer_a
        .cluster
        .initialize(members)
        .await
        .expect("initialize seed peer");

    // Give Raft time to elect a leader and replicate the initial membership
    // entry to B.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let metrics = peer_a.cluster.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(1) {
            break;
        }
    }

    // Propose a real command from peer A (the leader).
    peer_a
        .cluster
        .propose(ClusterCommand::AddPeer {
            id: PeerId(3),
            address: "10.0.0.3:7003".into(),
        })
        .await
        .expect("propose should succeed on leader");

    // Peer A's state machine applied the command.
    let state_a = peer_a.cluster.current_state().await;
    assert!(state_a.membership.contains(PeerId(3)));
    assert_eq!(
        state_a.membership.address(PeerId(3)),
        Some("10.0.0.3:7003")
    );

    // Peer B should eventually see the same state via log replication.
    let mut replicated = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let state_b = peer_b.cluster.current_state().await;
        if state_b.membership.contains(PeerId(3)) {
            assert_eq!(
                state_b.membership.address(PeerId(3)),
                Some("10.0.0.3:7003")
            );
            replicated = true;
            break;
        }
    }
    assert!(
        replicated,
        "peer B did not receive the replicated command within 4 seconds"
    );

    peer_a.cluster.shutdown().await.ok();
    peer_b.cluster.shutdown().await.ok();
}
