//! End-to-end test: graph mutations replicated through Raft.
//!
//! Spins up two peers, each backed by its own `Store` + `RaftCluster` with
//! a [`StoreGraphApplier`] plugged in. Bootstraps via peer A, proposes a
//! [`GraphCommand::PutNode`] on the leader, then asserts the node shows up
//! in **peer B's `Store`** via the Raft `apply_to_state_machine` pipeline.

use meshdb_cluster::raft::{GraphStateMachine, RaftCluster};
use meshdb_cluster::{
    ClusterState, ConstraintKind, ConstraintScope, GraphCommand, Membership, PartitionMap, Peer,
    PeerId,
};
use meshdb_core::{Edge, Node};
use meshdb_rpc::{GrpcNetwork, MeshRaftService, StoreGraphApplier};
use meshdb_storage::{RocksDbStorageEngine as Store, StorageEngine};
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct GraphPeer {
    cluster: RaftCluster,
    #[allow(dead_code)]
    store: Arc<dyn StorageEngine>,
}

async fn spawn_graph_peer(
    id: u64,
    listener: TcpListener,
    store: Arc<dyn StorageEngine>,
    peer_endpoints: Vec<(u64, String)>,
    initial: ClusterState,
) -> GraphPeer {
    let applier: Arc<dyn GraphStateMachine> = Arc::new(StoreGraphApplier::new(store.clone()));
    let network = GrpcNetwork::new(peer_endpoints).expect("valid endpoints");
    let cluster = RaftCluster::new_with_applier(id, initial, network, Some(applier))
        .await
        .expect("raft cluster builds");

    let raft_service = MeshRaftService::new(cluster.raft.clone());
    tokio::spawn(async move {
        Server::builder()
            .add_service(raft_service.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    GraphPeer { cluster, store }
}

#[tokio::test]
async fn graph_command_replicates_to_follower_store() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_b.path()).unwrap());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    // Initial cluster state — same on both peers.
    let initial = ClusterState::new(
        Membership::new(vec![
            Peer::new(PeerId(1), addr_a.to_string()),
            Peer::new(PeerId(2), addr_b.to_string()),
        ]),
        PartitionMap::round_robin(&[PeerId(1), PeerId(2)], 4).unwrap(),
    );

    let peer_a = spawn_graph_peer(
        1,
        listener_a,
        store_a.clone(),
        vec![(2u64, addr_b.to_string())],
        initial.clone(),
    )
    .await;
    let peer_b = spawn_graph_peer(
        2,
        listener_b,
        store_b.clone(),
        vec![(1u64, addr_a.to_string())],
        initial,
    )
    .await;

    // Bootstrap via peer A only; peer B joins via replication.
    let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
    members.insert(1, BasicNode::new(addr_a.to_string()));
    members.insert(2, BasicNode::new(addr_b.to_string()));
    peer_a
        .cluster
        .initialize(members)
        .await
        .expect("seed peer should bootstrap");

    // Wait for leadership.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let metrics = peer_a.cluster.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(1) {
            break;
        }
    }

    // Propose a graph write on the leader.
    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let node_id = node.id;
    peer_a
        .cluster
        .propose_graph(GraphCommand::PutNode(node))
        .await
        .expect("propose_graph should commit on leader");

    // Peer A's local store has the node immediately (state machine applied
    // it as part of the commit on the leader).
    let on_a = store_a.get_node(node_id).unwrap();
    assert!(on_a.is_some(), "leader's store should hold the new node");
    assert_eq!(on_a.unwrap().labels, vec!["Person"]);

    // Peer B should see the node via Raft replication.
    let mut replicated = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(Some(node)) = store_b.get_node(node_id) {
            assert_eq!(node.labels, vec!["Person"]);
            replicated = true;
            break;
        }
    }
    assert!(
        replicated,
        "follower's store did not see the replicated graph write"
    );
    let _ = peer_b;

    peer_a.cluster.shutdown().await.ok();
}

#[tokio::test]
async fn put_edge_and_delete_edge_replicate() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_b.path()).unwrap());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let initial = ClusterState::new(
        Membership::new(vec![
            Peer::new(PeerId(1), addr_a.to_string()),
            Peer::new(PeerId(2), addr_b.to_string()),
        ]),
        PartitionMap::round_robin(&[PeerId(1), PeerId(2)], 4).unwrap(),
    );

    let peer_a = spawn_graph_peer(
        1,
        listener_a,
        store_a.clone(),
        vec![(2u64, addr_b.to_string())],
        initial.clone(),
    )
    .await;
    let _peer_b = spawn_graph_peer(
        2,
        listener_b,
        store_b.clone(),
        vec![(1u64, addr_a.to_string())],
        initial,
    )
    .await;

    let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
    members.insert(1, BasicNode::new(addr_a.to_string()));
    members.insert(2, BasicNode::new(addr_b.to_string()));
    peer_a.cluster.initialize(members).await.unwrap();

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if peer_a.cluster.raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
    }

    // Two nodes + an edge, all proposed via Raft.
    let src = Node::new();
    let dst = Node::new();
    let edge = Edge::new("KNOWS", src.id, dst.id);
    let edge_id = edge.id;

    peer_a
        .cluster
        .propose_graph(GraphCommand::PutNode(src.clone()))
        .await
        .unwrap();
    peer_a
        .cluster
        .propose_graph(GraphCommand::PutNode(dst.clone()))
        .await
        .unwrap();
    peer_a
        .cluster
        .propose_graph(GraphCommand::PutEdge(edge))
        .await
        .unwrap();

    // Wait for the edge to land on peer B.
    let mut edge_replicated = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if store_b.get_edge(edge_id).unwrap().is_some() {
            edge_replicated = true;
            break;
        }
    }
    assert!(edge_replicated, "edge did not replicate to follower");
    // Forward adjacency is rebuilt on B too because put_edge writes both
    // forward and reverse adjacency in the local store.
    assert_eq!(store_b.outgoing(src.id).unwrap().len(), 1);

    // Now propose a delete and watch it replicate.
    peer_a
        .cluster
        .propose_graph(GraphCommand::DeleteEdge(edge_id))
        .await
        .unwrap();

    let mut edge_gone = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if store_b.get_edge(edge_id).unwrap().is_none() {
            edge_gone = true;
            break;
        }
    }
    assert!(edge_gone, "delete did not replicate to follower");
    assert!(store_a.get_edge(edge_id).unwrap().is_none());
}

/// CREATE CONSTRAINT on the leader replicates through Raft so the
/// follower's constraint registry sees the same entry. Then DROP
/// CONSTRAINT replicates the removal. Confirms the Raft log entry
/// path for constraint DDL works end-to-end across replicas.
#[tokio::test]
async fn constraint_ddl_replicates_to_follower_store() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_b.path()).unwrap());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let initial = ClusterState::new(
        Membership::new(vec![
            Peer::new(PeerId(1), addr_a.to_string()),
            Peer::new(PeerId(2), addr_b.to_string()),
        ]),
        PartitionMap::round_robin(&[PeerId(1), PeerId(2)], 4).unwrap(),
    );

    let peer_a = spawn_graph_peer(
        1,
        listener_a,
        store_a.clone(),
        vec![(2u64, addr_b.to_string())],
        initial.clone(),
    )
    .await;
    let _peer_b = spawn_graph_peer(
        2,
        listener_b,
        store_b.clone(),
        vec![(1u64, addr_a.to_string())],
        initial,
    )
    .await;

    let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
    members.insert(1, BasicNode::new(addr_a.to_string()));
    members.insert(2, BasicNode::new(addr_b.to_string()));
    peer_a.cluster.initialize(members).await.unwrap();

    // Wait for leadership.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if peer_a.cluster.raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
    }

    // Propose CREATE CONSTRAINT on the leader.
    peer_a
        .cluster
        .propose_graph(GraphCommand::CreateConstraint {
            name: Some("email_uniq".into()),
            scope: ConstraintScope::Node("Person".into()),
            properties: vec!["email".into()],
            kind: ConstraintKind::Unique,
            if_not_exists: false,
        })
        .await
        .expect("propose CreateConstraint should commit");

    // Leader's local store has the constraint immediately.
    let on_a = store_a.list_property_constraints();
    assert_eq!(on_a.len(), 1);
    assert_eq!(on_a[0].name, "email_uniq");

    // Follower sees it via Raft replication.
    let mut replicated = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let specs = store_b.list_property_constraints();
        if !specs.is_empty() {
            assert_eq!(specs[0].name, "email_uniq");
            assert_eq!(
                specs[0].scope,
                meshdb_storage::ConstraintScope::Node("Person".into())
            );
            assert_eq!(specs[0].properties, vec!["email".to_string()]);
            replicated = true;
            break;
        }
    }
    assert!(
        replicated,
        "follower did not observe the replicated constraint"
    );

    // Now drop it and watch the drop replicate too.
    peer_a
        .cluster
        .propose_graph(GraphCommand::DropConstraint {
            name: "email_uniq".into(),
            if_exists: false,
        })
        .await
        .expect("propose DropConstraint should commit");

    let mut dropped = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if store_b.list_property_constraints().is_empty() {
            dropped = true;
            break;
        }
    }
    assert!(dropped, "follower did not observe the replicated drop");
    assert!(store_a.list_property_constraints().is_empty());

    peer_a.cluster.shutdown().await.ok();
}

/// CREATE CONSTRAINT carrying no user-supplied name resolves to the
/// same deterministic auto-name on every replica. Without this
/// property a follower could end up with a differently-named entry
/// than the leader; DROP by name would then silently miss on some
/// peers.
#[tokio::test]
async fn auto_named_constraint_resolves_consistently() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_b.path()).unwrap());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let initial = ClusterState::new(
        Membership::new(vec![
            Peer::new(PeerId(1), addr_a.to_string()),
            Peer::new(PeerId(2), addr_b.to_string()),
        ]),
        PartitionMap::round_robin(&[PeerId(1), PeerId(2)], 4).unwrap(),
    );

    let peer_a = spawn_graph_peer(
        1,
        listener_a,
        store_a.clone(),
        vec![(2u64, addr_b.to_string())],
        initial.clone(),
    )
    .await;
    let _peer_b = spawn_graph_peer(
        2,
        listener_b,
        store_b.clone(),
        vec![(1u64, addr_a.to_string())],
        initial,
    )
    .await;

    let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
    members.insert(1, BasicNode::new(addr_a.to_string()));
    members.insert(2, BasicNode::new(addr_b.to_string()));
    peer_a.cluster.initialize(members).await.unwrap();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if peer_a.cluster.raft.metrics().borrow().current_leader == Some(1) {
            break;
        }
    }

    peer_a
        .cluster
        .propose_graph(GraphCommand::CreateConstraint {
            name: None,
            scope: ConstraintScope::Node("Widget".into()),
            properties: vec!["sku".into()],
            kind: ConstraintKind::NotNull,
            if_not_exists: false,
        })
        .await
        .unwrap();

    // Wait for replication.
    let mut saw = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !store_b.list_property_constraints().is_empty() {
            saw = true;
            break;
        }
    }
    assert!(saw);

    let on_a = store_a.list_property_constraints();
    let on_b = store_b.list_property_constraints();
    assert_eq!(on_a, on_b, "replicas diverged on auto-generated name");
    assert_eq!(on_a[0].name, "constraint_node_Widget_sku_not_null");
}
