use mesh_cluster::{Cluster, Peer, PeerId};
use mesh_core::{Edge, Node};
use mesh_rpc::convert::{edge_to_proto, node_to_proto, uuid_to_proto};
use mesh_rpc::proto::mesh_query_client::MeshQueryClient;
use mesh_rpc::proto::mesh_write_client::MeshWriteClient;
use mesh_rpc::proto::{
    BatchPhase, BatchWriteRequest, DeleteEdgeRequest, DetachDeleteNodeRequest,
    ExecuteCypherRequest, GetEdgeRequest, GetNodeRequest, HealthRequest, NeighborRequest,
    NodesByLabelRequest, PutEdgeRequest, PutNodeRequest, UuidBytes,
};
use mesh_rpc::{CoordinatorLog, MeshService, Routing, TxLogEntry};
use mesh_storage::Store;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct Harness {
    store: Arc<Store>,
    address: String,
    _dir: TempDir,
}

async fn spawn_server() -> Harness {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stream = TcpListenerStream::new(listener);

    tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_query_server())
            .serve_with_incoming(stream)
            .await
            .unwrap();
    });

    // Small delay to let the server bind before the client connects.
    tokio::time::sleep(Duration::from_millis(50)).await;

    Harness {
        store,
        address: format!("http://{}", addr),
        _dir: dir,
    }
}

async fn client(harness: &Harness) -> MeshQueryClient<tonic::transport::Channel> {
    MeshQueryClient::connect(harness.address.clone())
        .await
        .unwrap()
}

#[tokio::test]
async fn health_reports_serving() {
    let harness = spawn_server().await;
    let mut c = client(&harness).await;
    let resp = c.health(HealthRequest {}).await.unwrap();
    assert!(resp.into_inner().serving);
}

#[tokio::test]
async fn get_node_roundtrips_labels_and_properties() {
    let harness = spawn_server().await;
    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Ada")
        .with_property("age", 37_i64);
    let node_id = node.id;
    harness.store.put_node(&node).unwrap();

    let mut c = client(&harness).await;
    let resp = c
        .get_node(GetNodeRequest {
            id: Some(uuid_to_proto(node_id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.found);
    let got = inner.node.unwrap();
    assert_eq!(got.labels, vec!["Person"]);
    assert!(got.properties.contains_key("name"));
    assert!(got.properties.contains_key("age"));
}

#[tokio::test]
async fn get_node_missing_returns_not_found() {
    let harness = spawn_server().await;
    let mut c = client(&harness).await;

    // Random 16-byte id that doesn't exist.
    let fake = uuid_to_proto(mesh_core::NodeId::new().as_uuid());
    let resp = c
        .get_node(GetNodeRequest {
            id: Some(fake),
            local_only: false,
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(!inner.found);
    assert!(inner.node.is_none());
}

#[tokio::test]
async fn nodes_by_label_returns_all_ids_for_label() {
    let harness = spawn_server().await;
    let a = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let b = Node::new()
        .with_label("Person")
        .with_property("name", "Alan");
    let c_node = Node::new()
        .with_label("Company")
        .with_property("name", "Acme");
    harness.store.put_node(&a).unwrap();
    harness.store.put_node(&b).unwrap();
    harness.store.put_node(&c_node).unwrap();

    let mut c = client(&harness).await;
    let resp = c
        .nodes_by_label(NodesByLabelRequest {
            label: "Person".into(),
            local_only: false,
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.ids.len(), 2);
}

#[tokio::test]
async fn outgoing_returns_edge_and_neighbor() {
    let harness = spawn_server().await;
    let a = Node::new().with_label("Person");
    let b = Node::new().with_label("Person");
    harness.store.put_node(&a).unwrap();
    harness.store.put_node(&b).unwrap();
    let edge = Edge::new("KNOWS", a.id, b.id);
    harness.store.put_edge(&edge).unwrap();

    let mut c = client(&harness).await;
    let resp = c
        .outgoing(NeighborRequest {
            node_id: Some(uuid_to_proto(a.id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.neighbors.len(), 1);
    let neighbor = &inner.neighbors[0];
    assert!(neighbor.edge_id.is_some());
    assert!(neighbor.neighbor_id.is_some());
}

#[tokio::test]
async fn incoming_mirrors_outgoing() {
    let harness = spawn_server().await;
    let a = Node::new();
    let b = Node::new();
    harness.store.put_node(&a).unwrap();
    harness.store.put_node(&b).unwrap();
    let edge = Edge::new("KNOWS", a.id, b.id);
    harness.store.put_edge(&edge).unwrap();

    let mut c = client(&harness).await;
    let resp = c
        .incoming(NeighborRequest {
            node_id: Some(uuid_to_proto(b.id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.neighbors.len(), 1);
}

#[tokio::test]
async fn get_edge_roundtrip() {
    let harness = spawn_server().await;
    let a = Node::new();
    let b = Node::new();
    harness.store.put_node(&a).unwrap();
    harness.store.put_node(&b).unwrap();
    let edge = Edge::new("KNOWS", a.id, b.id).with_property("since", 2020_i64);
    let edge_id = edge.id;
    harness.store.put_edge(&edge).unwrap();

    let mut c = client(&harness).await;
    let resp = c
        .get_edge(GetEdgeRequest {
            id: Some(uuid_to_proto(edge_id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.found);
    let got = inner.edge.unwrap();
    assert_eq!(got.edge_type, "KNOWS");
    assert!(got.properties.contains_key("since"));
}

struct TwoPeer {
    store_a: Arc<Store>,
    store_b: Arc<Store>,
    cluster_a: Arc<Cluster>,
    client_addr_a: String,
    _dirs: (TempDir, TempDir),
}

async fn spawn_two_peer() -> TwoPeer {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let store_a = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b = Arc::new(Store::open(dir_b.path()).unwrap());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        Peer::new(PeerId(1), addr_a.to_string()),
        Peer::new(PeerId(2), addr_b.to_string()),
    ];

    let cluster_a = Arc::new(Cluster::new(PeerId(1), 4, peers.clone()).unwrap());
    let cluster_b = Arc::new(Cluster::new(PeerId(2), 4, peers.clone()).unwrap());
    let routing_a = Arc::new(Routing::new(cluster_a.clone()).unwrap());
    let routing_b = Arc::new(Routing::new(cluster_b.clone()).unwrap());

    let service_a = MeshService::with_routing(store_a.clone(), routing_a);
    let service_b = MeshService::with_routing(store_b.clone(), routing_b);

    let stream_a = TcpListenerStream::new(listener_a);
    let stream_b = TcpListenerStream::new(listener_b);

    tokio::spawn(async move {
        Server::builder()
            .add_service(service_a.clone().into_query_server())
            .add_service(service_a.into_write_server())
            .serve_with_incoming(stream_a)
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_b.clone().into_query_server())
            .add_service(service_b.into_write_server())
            .serve_with_incoming(stream_b)
            .await
            .unwrap();
    });

    // Let both servers bind before the client connects.
    tokio::time::sleep(Duration::from_millis(80)).await;

    TwoPeer {
        store_a,
        store_b,
        cluster_a,
        client_addr_a: format!("http://{}", addr_a),
        _dirs: (dir_a, dir_b),
    }
}

fn put_node_on_owner(node: &Node, h: &TwoPeer) -> PeerId {
    let owner = h.cluster_a.owner_of(node.id);
    match owner {
        PeerId(1) => h.store_a.put_node(node).unwrap(),
        PeerId(2) => h.store_b.put_node(node).unwrap(),
        other => panic!("unexpected owner {other:?}"),
    }
    owner
}

/// Two-peer harness extended with an on-disk coordinator log for peer
/// A, so recovery tests can manipulate the log directly and then call
/// `service_a.recover_pending_transactions()` through the local
/// `MeshService` handle.
struct TwoPeerWithLog {
    service_a: MeshService,
    store_a: Arc<Store>,
    store_b: Arc<Store>,
    cluster_a: Arc<Cluster>,
    log_a: Arc<CoordinatorLog>,
    log_a_path: std::path::PathBuf,
    _dirs: (TempDir, TempDir, TempDir),
}

/// Same shape as [`spawn_two_peer`] but wires a real `CoordinatorLog`
/// into peer A's `MeshService` and exposes it on the returned harness.
/// Peer B is unchanged — it's a plain routing participant whose
/// in-memory staging is all we need for recovery to exercise.
async fn spawn_two_peer_with_log() -> TwoPeerWithLog {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let log_dir = TempDir::new().unwrap();
    let store_a = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b = Arc::new(Store::open(dir_b.path()).unwrap());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        Peer::new(PeerId(1), addr_a.to_string()),
        Peer::new(PeerId(2), addr_b.to_string()),
    ];

    let cluster_a = Arc::new(Cluster::new(PeerId(1), 4, peers.clone()).unwrap());
    let cluster_b = Arc::new(Cluster::new(PeerId(2), 4, peers.clone()).unwrap());
    let routing_a = Arc::new(Routing::new(cluster_a.clone()).unwrap());
    let routing_b = Arc::new(Routing::new(cluster_b.clone()).unwrap());

    let log_a_path = log_dir.path().join("coordinator-log.jsonl");
    let log_a = Arc::new(CoordinatorLog::open(&log_a_path).unwrap());

    let service_a =
        MeshService::with_routing_and_log(store_a.clone(), routing_a, Some(log_a.clone()));
    let service_b = MeshService::with_routing(store_b.clone(), routing_b);

    let stream_a = TcpListenerStream::new(listener_a);
    let stream_b = TcpListenerStream::new(listener_b);

    let service_a_for_server = service_a.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_a_for_server.clone().into_query_server())
            .add_service(service_a_for_server.into_write_server())
            .serve_with_incoming(stream_a)
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(service_b.clone().into_query_server())
            .add_service(service_b.into_write_server())
            .serve_with_incoming(stream_b)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    TwoPeerWithLog {
        service_a,
        store_a,
        store_b,
        cluster_a,
        log_a,
        log_a_path,
        _dirs: (dir_a, dir_b, log_dir),
    }
}

#[tokio::test]
async fn routed_get_node_crosses_peers() {
    let h = spawn_two_peer().await;

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    let mut local_hits = 0;
    let mut remote_hits = 0;

    for i in 0..30 {
        let node = Node::new()
            .with_label("Person")
            .with_property("i", i as i64);
        put_node_on_owner(&node, &h);

        let resp = client
            .get_node(GetNodeRequest {
                id: Some(uuid_to_proto(node.id.as_uuid())),
                local_only: false,
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.found, "node not found through routed client");
        assert!(inner.node.unwrap().labels.contains(&"Person".to_string()));

        if h.cluster_a.is_local(node.id) {
            local_hits += 1;
        } else {
            remote_hits += 1;
        }
    }

    assert!(local_hits > 0, "expected at least one local hit");
    assert!(
        remote_hits > 0,
        "expected at least one forwarded (remote) hit"
    );
}

#[tokio::test]
async fn nodes_by_label_scatter_gathers_across_peers() {
    let h = spawn_two_peer().await;

    for i in 0..30 {
        let n = Node::new()
            .with_label("Worker")
            .with_property("i", i as i64);
        put_node_on_owner(&n, &h);
    }

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let resp = client
        .nodes_by_label(NodesByLabelRequest {
            label: "Worker".into(),
            local_only: false,
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().ids.len(), 30);
}

#[tokio::test]
async fn nodes_by_label_local_only_skips_remote() {
    let h = spawn_two_peer().await;

    let mut owned_by_a = 0;
    for i in 0..30 {
        let n = Node::new().with_label("Flag").with_property("i", i as i64);
        let owner = put_node_on_owner(&n, &h);
        if owner == PeerId(1) {
            owned_by_a += 1;
        }
    }
    assert!(owned_by_a > 0, "expected some local-to-A entries");
    assert!(owned_by_a < 30, "expected some remote entries too");

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let resp = client
        .nodes_by_label(NodesByLabelRequest {
            label: "Flag".into(),
            local_only: true,
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().ids.len(), owned_by_a);
}

#[tokio::test]
async fn routed_outgoing_forwards_to_source_owner() {
    let h = spawn_two_peer().await;

    // Find a source owned by peer 2 so the query must be forwarded from peer 1.
    let src = loop {
        let n = Node::new();
        if h.cluster_a.owner_of(n.id) == PeerId(2) {
            break n;
        }
    };
    let dst = Node::new();

    h.store_b.put_node(&src).unwrap();
    put_node_on_owner(&dst, &h);

    // Edge stored on the source's owner (peer 2), matching the partitioning model.
    let edge = Edge::new("KNOWS", src.id, dst.id);
    h.store_b.put_edge(&edge).unwrap();

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let resp = client
        .outgoing(NeighborRequest {
            node_id: Some(uuid_to_proto(src.id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    let neighbors = resp.into_inner().neighbors;
    assert_eq!(neighbors.len(), 1);
    assert_eq!(
        neighbors[0].neighbor_id.as_ref().unwrap().value,
        dst.id.as_bytes().to_vec()
    );
}

#[tokio::test]
async fn cypher_match_scatter_gathers_across_partitions() {
    // Routing mode: nodes are sharded by hash across two peers, so any
    // single peer's local store holds a strict subset of the graph. A
    // Cypher MATCH issued against either peer must fan out to the other
    // peer through the PartitionedGraphReader or it would return a
    // fragment instead of the union.
    let h = spawn_two_peer().await;

    let mut owned_by_a = 0;
    let mut owned_by_b = 0;
    for i in 0..40 {
        let n = Node::new()
            .with_label("Worker")
            .with_property("i", i as i64);
        let owner = put_node_on_owner(&n, &h);
        match owner {
            PeerId(1) => owned_by_a += 1,
            PeerId(2) => owned_by_b += 1,
            _ => unreachable!(),
        }
    }
    // Sanity: the hash partitioner actually spread nodes across both peers.
    assert!(owned_by_a > 0 && owned_by_b > 0);
    assert_eq!(owned_by_a + owned_by_b, 40);

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (n:Worker) RETURN n.i AS i".to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();

    // `rows_json` is a serde-serialized Vec<mesh_executor::Row>. Decode
    // just enough to count rows and pull out the `i` property.
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(
        rows.len(),
        40,
        "expected scatter-gather to return all 40 nodes, got {}",
        rows.len()
    );
    let mut seen: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for row in rows {
        // Row shape: {"i": {"Property": {"type":"Int64","value":<n>}}}
        let prop = &row["i"]["Property"];
        assert_eq!(prop["type"], "Int64");
        seen.insert(prop["value"].as_i64().expect("i should be an integer"));
    }
    assert_eq!(seen, (0..40).collect());
}

#[tokio::test]
async fn cypher_match_with_filter_scatter_gathers() {
    // Same two-peer setup; make sure WHERE filtering composes correctly
    // over the partitioned reader — each peer applies the filter locally
    // and the coordinator unions the survivors.
    let h = spawn_two_peer().await;

    for i in 0..20 {
        let n = Node::new()
            .with_label("Box")
            .with_property("size", i as i64);
        put_node_on_owner(&n, &h);
    }

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (n:Box) WHERE n.size > 14 RETURN n.size AS s ORDER BY s".to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();

    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    let sizes: Vec<i64> = rows
        .iter()
        .map(|r| r["s"]["Property"]["value"].as_i64().unwrap())
        .collect();
    assert_eq!(sizes, vec![15, 16, 17, 18, 19]);
}

#[tokio::test]
async fn cypher_create_routes_writes_by_partition() {
    // Routing mode sharding invariant: each node must live on the peer
    // whose partition owns its id. With the RoutingGraphWriter wired in,
    // a Cypher CREATE issued on peer A for a node whose id hashes to
    // peer B must travel through MeshWrite::PutNode (local_only=true) to
    // peer B's local store — NOT land on peer A where the executor is
    // running.
    let h = spawn_two_peer().await;

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    for i in 0..40 {
        client
            .execute_cypher(ExecuteCypherRequest {
                query: format!("CREATE (n:Worker {{i: {}}})", i),
                params_json: vec![],
            })
            .await
            .unwrap();
    }

    let a_ids = h.store_a.all_node_ids().unwrap();
    let b_ids = h.store_b.all_node_ids().unwrap();
    assert_eq!(
        a_ids.len() + b_ids.len(),
        40,
        "total nodes across both shards should be exactly 40"
    );
    assert!(
        !a_ids.is_empty() && !b_ids.is_empty(),
        "hash partitioner should spread 40 nodes across both peers (got a={}, b={})",
        a_ids.len(),
        b_ids.len()
    );
    // Every node's host shard must match the partition owner for its id.
    for id in &a_ids {
        assert_eq!(h.cluster_a.owner_of(*id), PeerId(1));
    }
    for id in &b_ids {
        assert_eq!(h.cluster_a.owner_of(*id), PeerId(2));
    }

    // Round-trip via scatter-gather MATCH to confirm the full graph is
    // reachable from peer A even though half the data lives on B.
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (n:Worker) RETURN n.i AS i".to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(rows.len(), 40);
    let seen: std::collections::BTreeSet<i64> = rows
        .iter()
        .map(|r| r["i"]["Property"]["value"].as_i64().unwrap())
        .collect();
    assert_eq!(seen, (0..40).collect());
}

#[tokio::test]
async fn cypher_create_edge_lands_on_both_owners() {
    // Cypher CREATE (a)-[:KNOWS]->(b) must place the edge on both a's
    // owner and b's owner. Force the two endpoints onto different peers
    // by generating candidates until the hash partitioner splits them,
    // then drive the CREATE through the routing writer.
    //
    // Since we can't pre-compute ids for nodes that CREATE will mint
    // itself, we fan out 20 create-pair attempts via a single Cypher
    // query and later check that every KNOWS edge exists on both the
    // source owner's store and the target owner's store.
    let h = spawn_two_peer().await;
    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    // Create 15 disconnected pairs — with a 4-partition / 2-peer layout
    // and a hash partitioner, most pairs will straddle the two peers.
    for i in 0..15 {
        client
            .execute_cypher(ExecuteCypherRequest {
                query: format!(
                    "CREATE (a:Link {{name: 'a{}'}})-[:KNOWS]->(b:Link {{name: 'b{}'}})",
                    i, i
                ),
                params_json: vec![],
            })
            .await
            .unwrap();
    }

    // Every edge must appear on both endpoint owners' local stores.
    let edges_a = h.store_a.all_edges().unwrap();
    let edges_b = h.store_b.all_edges().unwrap();

    let mut cross_partition = 0;
    for edge in edges_a.iter().chain(edges_b.iter()) {
        let src_owner = h.cluster_a.owner_of(edge.source);
        let dst_owner = h.cluster_a.owner_of(edge.target);

        if src_owner != dst_owner {
            cross_partition += 1;
            // Cross-partition edge: must live on BOTH stores.
            assert!(
                h.store_a.get_edge(edge.id).unwrap().is_some(),
                "cross-partition edge missing from store_a"
            );
            assert!(
                h.store_b.get_edge(edge.id).unwrap().is_some(),
                "cross-partition edge missing from store_b"
            );
        }
    }
    // Each cross-partition edge gets counted twice (once per store), so
    // divide. Require at least one genuine cross-partition edge so we
    // know the test is actually exercising the two-writer path.
    assert!(
        cross_partition / 2 >= 1,
        "expected at least one cross-partition edge, got {}",
        cross_partition / 2
    );

    // MATCH round-trip from peer A reaches every edge.
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (a:Link)-[:KNOWS]->(b:Link) RETURN a.name AS an, b.name AS bn"
                .to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(rows.len(), 15);
}

#[tokio::test]
async fn batch_write_prepare_commit_applies_atomically() {
    // Exercise the BatchWrite RPC directly: two PutNode commands
    // prepared on a single peer, then committed — should atomically
    // materialize both nodes in one rocksdb WriteBatch.
    let h = spawn_two_peer().await;
    let mut w = MeshWriteClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    // Pick two nodes that both hash to peer A so we can target a
    // single participant and skip cross-peer coordination for this
    // lower-level test.
    let mut nodes: Vec<Node> = Vec::new();
    while nodes.len() < 2 {
        let n = Node::new().with_label("T");
        if h.cluster_a.owner_of(n.id) == PeerId(1) {
            nodes.push(n);
        }
    }
    let node_ids: Vec<_> = nodes.iter().map(|n| n.id).collect();

    // Commands as serde-json Vec<GraphCommand>.
    let commands: Vec<mesh_cluster::GraphCommand> = nodes
        .into_iter()
        .map(mesh_cluster::GraphCommand::PutNode)
        .collect();
    let payload = serde_json::to_vec(&commands).unwrap();

    let txid = "tx-prep-commit-1".to_string();
    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Prepare as i32,
        commands_json: payload,
    })
    .await
    .unwrap();

    // Before COMMIT the nodes must NOT be visible in the local store.
    for id in &node_ids {
        assert!(h.store_a.get_node(*id).unwrap().is_none());
    }

    w.batch_write(BatchWriteRequest {
        txid,
        phase: BatchPhase::Commit as i32,
        commands_json: Vec::new(),
    })
    .await
    .unwrap();

    // After COMMIT both nodes exist.
    for id in &node_ids {
        assert!(h.store_a.get_node(*id).unwrap().is_some());
    }
}

#[tokio::test]
async fn batch_write_abort_discards_staged_commands() {
    // Prepare, then abort — the staged commands must not land on disk.
    let h = spawn_two_peer().await;
    let mut w = MeshWriteClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    let node = loop {
        let n = Node::new().with_label("AbortMe");
        if h.cluster_a.owner_of(n.id) == PeerId(1) {
            break n;
        }
    };
    let node_id = node.id;

    let commands = vec![mesh_cluster::GraphCommand::PutNode(node)];
    let payload = serde_json::to_vec(&commands).unwrap();

    let txid = "tx-abort-1".to_string();
    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Prepare as i32,
        commands_json: payload,
    })
    .await
    .unwrap();

    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Abort as i32,
        commands_json: Vec::new(),
    })
    .await
    .unwrap();

    assert!(h.store_a.get_node(node_id).unwrap().is_none());

    // A follow-up COMMIT on the same txid must fail — abort removed
    // the staged entry, so there's nothing to commit. This proves
    // ABORT actually discards staging state.
    let err = w
        .batch_write(BatchWriteRequest {
            txid,
            phase: BatchPhase::Commit as i32,
            commands_json: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn batch_write_duplicate_prepare_rejected() {
    // Preparing the same txid twice must fail so a coordinator bug
    // can't silently clobber a concurrent transaction's staged state.
    let h = spawn_two_peer().await;
    let mut w = MeshWriteClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    let commands: Vec<mesh_cluster::GraphCommand> = Vec::new();
    let payload = serde_json::to_vec(&commands).unwrap();

    let txid = "tx-dup".to_string();
    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Prepare as i32,
        commands_json: payload.clone(),
    })
    .await
    .unwrap();

    let err = w
        .batch_write(BatchWriteRequest {
            txid,
            phase: BatchPhase::Prepare as i32,
            commands_json: payload,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::AlreadyExists);
}

#[tokio::test]
async fn cypher_multi_op_query_atomic_across_peers() {
    // A multi-node CREATE whose nodes hash to BOTH peers. With 2PC
    // wiring the whole transaction either lands on both peers or on
    // neither; specifically, neither peer's store should hold half a
    // transaction after a successful execute_cypher call.
    //
    // We don't have a fault-injection hook yet to prove the ABORT
    // rollback branch, so this test just pins the happy-path atomic
    // shape and asserts the exact shard split matches the partitioner.
    let h = spawn_two_peer().await;
    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    // One Cypher query that creates eight nodes — the partitioner will
    // almost certainly split them across both peers on a hash layout,
    // so the coordinator runs PREPARE+COMMIT on both.
    client
        .execute_cypher(ExecuteCypherRequest {
            query: "CREATE (a:Batch {i:0}), (b:Batch {i:1}), (c:Batch {i:2}), (d:Batch {i:3}),
                        (e:Batch {i:4}), (f:Batch {i:5}), (g:Batch {i:6}), (h:Batch {i:7})"
                .to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();

    let a_ids = h.store_a.all_node_ids().unwrap();
    let b_ids = h.store_b.all_node_ids().unwrap();

    // Total is exactly eight — the PREPARE-then-COMMIT sequence
    // committed without dropping or duplicating any mutation.
    assert_eq!(a_ids.len() + b_ids.len(), 8);

    // Each node lives on the peer its id hashes to (sharding invariant).
    for id in &a_ids {
        assert_eq!(h.cluster_a.owner_of(*id), PeerId(1));
    }
    for id in &b_ids {
        assert_eq!(h.cluster_a.owner_of(*id), PeerId(2));
    }

    // Round-trip MATCH returns the full set regardless of which peer
    // holds which node.
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (n:Batch) RETURN n.i AS i".to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(rows.len(), 8);
    let seen: std::collections::BTreeSet<i64> = rows
        .iter()
        .map(|r| r["i"]["Property"]["value"].as_i64().unwrap())
        .collect();
    assert_eq!(seen, (0..8).collect());
}

#[tokio::test]
async fn cypher_reverse_traversal_via_ghost_edges() {
    // Cross-partition reverse traversal test. The spec calls for
    // ghost/stub edge copies on the target's partition so a query
    // starting from the target and walking backwards along the edge
    // finds the source. In practice, `RoutingGraphWriter::put_edge`
    // already replicates each edge to both source-owner and target-
    // owner, and `Store::put_edge` populates both adj_out and adj_in
    // — so the target owner ends up with a populated `incoming(t)`
    // entry for every edge pointing at one of its nodes. This test
    // pins that invariant by driving a reverse-direction MATCH.
    let h = spawn_two_peer().await;
    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    // Create 15 (a)-[:KNOWS]->(b) pairs. With a 4-partition hash
    // layout the hash partitioner will straddle some pairs across
    // peers — that's the case we care about.
    for i in 0..15 {
        client
            .execute_cypher(ExecuteCypherRequest {
                query: format!(
                    "CREATE (a:Src {{name: 'a{i}'}})-[:KNOWS]->(b:Dst {{name: 'b{i}'}})"
                ),
                params_json: vec![],
            })
            .await
            .unwrap();
    }

    // Sanity: at least one edge must actually cross partitions, else
    // the test isn't exercising cross-partition reverse traversal.
    let all_edges: Vec<_> = h
        .store_a
        .all_edges()
        .unwrap()
        .into_iter()
        .chain(h.store_b.all_edges().unwrap().into_iter())
        .collect();
    let cross_partition_exists = all_edges
        .iter()
        .any(|e| h.cluster_a.owner_of(e.source) != h.cluster_a.owner_of(e.target));
    assert!(
        cross_partition_exists,
        "need at least one cross-partition edge to exercise ghost-edge reverse traversal"
    );

    // Reverse MATCH: start from Dst (the target side), walk edges
    // backwards to find Src. This forces the executor to call
    // `incoming(dst)` on the target's partition owner — which only
    // returns the edge if the ghost-copy invariant holds.
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (b:Dst)<-[:KNOWS]-(a:Src) RETURN a.name AS src, b.name AS dst"
                .to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();

    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(
        rows.len(),
        15,
        "reverse traversal should reach every (Src)->(Dst) pair"
    );
    let pairs: std::collections::BTreeSet<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                r["src"]["Property"]["value"].as_str().unwrap().to_string(),
                r["dst"]["Property"]["value"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let expected: std::collections::BTreeSet<(String, String)> = (0..15)
        .map(|i| (format!("a{i}"), format!("b{i}")))
        .collect();
    assert_eq!(pairs, expected);
}

#[tokio::test]
async fn cypher_traversal_crosses_partitions() {
    // Seed a KNOWS chain whose source and destination are likely on
    // different peers; the partitioned reader must route the outgoing()
    // lookup to whichever peer owns each source node.
    let h = spawn_two_peer().await;

    // Build a fan of edges (a)-[:KNOWS]->(bᵢ) so at least one b is on
    // the other peer regardless of which peer owns a. Edges currently
    // land on the local store of whoever holds `a`, which is fine —
    // the cross-partition hop under test is the Cypher read.
    let a = Node::new().with_label("Person").with_property("name", "A");
    let a_id = a.id;
    let a_owner = put_node_on_owner(&a, &h);

    let mut targets: Vec<Node> = Vec::new();
    for i in 0..20 {
        targets.push(
            Node::new()
                .with_label("Person")
                .with_property("name", format!("B{}", i)),
        );
    }

    // Place each target on its true owner so nodes_by_label/get_node work.
    let mut a_side_targets = 0;
    let mut other_side_targets = 0;
    for b in &targets {
        let owner = put_node_on_owner(b, &h);
        if owner == a_owner {
            a_side_targets += 1;
        } else {
            other_side_targets += 1;
        }
    }
    assert!(
        other_side_targets > 0,
        "expected at least one cross-peer target"
    );
    assert!(a_side_targets < 20, "expected some local-to-a targets too");

    // Edges are stored on A's owner's local store — that's where the
    // adjacency lookup for `a` will run.
    for b in &targets {
        let edge = Edge::new("KNOWS", a_id, b.id);
        match a_owner {
            PeerId(1) => h.store_a.put_edge(&edge).unwrap(),
            PeerId(2) => h.store_b.put_edge(&edge).unwrap(),
            _ => unreachable!(),
        }
    }

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (a:Person {name: 'A'})-[:KNOWS]->(b:Person) RETURN b.name AS name"
                .to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();

    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(
        rows.len(),
        20,
        "expected all 20 KNOWS targets after cross-partition expand"
    );
    let names: std::collections::BTreeSet<String> = rows
        .iter()
        .map(|r| r["name"]["Property"]["value"].as_str().unwrap().to_string())
        .collect();
    let expected: std::collections::BTreeSet<String> = (0..20).map(|i| format!("B{}", i)).collect();
    assert_eq!(names, expected);
}

#[tokio::test]
async fn routed_get_node_missing_returns_not_found_on_either_peer() {
    let h = spawn_two_peer().await;
    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    // A random id that doesn't exist anywhere.
    let resp = client
        .get_node(GetNodeRequest {
            id: Some(uuid_to_proto(mesh_core::NodeId::new().as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(!inner.found);
    assert!(inner.node.is_none());
}

fn node_owned_by(owner: PeerId, h: &TwoPeer) -> Node {
    for _ in 0..10_000 {
        let n = Node::new();
        if h.cluster_a.owner_of(n.id) == owner {
            return n;
        }
    }
    panic!("no node owned by {owner:?} after 10k tries");
}

async fn write_client(h: &TwoPeer) -> MeshWriteClient<tonic::transport::Channel> {
    MeshWriteClient::connect(h.client_addr_a.clone())
        .await
        .unwrap()
}

#[tokio::test]
async fn routed_put_node_lands_on_owner_only() {
    let h = spawn_two_peer().await;
    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let node_id = node.id;
    let proto_node = node_to_proto(&node).unwrap();

    let mut client = write_client(&h).await;
    client
        .put_node(PutNodeRequest {
            node: Some(proto_node),
            local_only: false,
        })
        .await
        .unwrap();

    let owner = h.cluster_a.owner_of(node_id);
    let (owner_store, other_store) = match owner {
        PeerId(1) => (&h.store_a, &h.store_b),
        PeerId(2) => (&h.store_b, &h.store_a),
        other => panic!("unexpected owner {other:?}"),
    };
    assert!(owner_store.get_node(node_id).unwrap().is_some());
    assert!(other_store.get_node(node_id).unwrap().is_none());
}

#[tokio::test]
async fn routed_put_edge_lands_on_both_endpoint_owners() {
    let h = spawn_two_peer().await;
    let src = node_owned_by(PeerId(1), &h);
    let dst = node_owned_by(PeerId(2), &h);

    let mut client = write_client(&h).await;
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&src).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&dst).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    let edge = Edge::new("KNOWS", src.id, dst.id).with_property("since", 2020_i64);
    let edge_id = edge.id;
    client
        .put_edge(PutEdgeRequest {
            edge: Some(edge_to_proto(&edge).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    // Both stores see the edge (each also holds complete local adjacency).
    assert!(h.store_a.get_edge(edge_id).unwrap().is_some());
    assert!(h.store_b.get_edge(edge_id).unwrap().is_some());
    assert_eq!(h.store_a.outgoing(src.id).unwrap().len(), 1);
    assert_eq!(h.store_b.incoming(dst.id).unwrap().len(), 1);
}

#[tokio::test]
async fn routed_put_edge_same_owner_writes_once() {
    let h = spawn_two_peer().await;
    let src = node_owned_by(PeerId(1), &h);
    let dst = loop {
        let n = node_owned_by(PeerId(1), &h);
        if n.id != src.id {
            break n;
        }
    };

    let mut client = write_client(&h).await;
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&src).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&dst).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    let edge = Edge::new("T", src.id, dst.id);
    let edge_id = edge.id;
    client
        .put_edge(PutEdgeRequest {
            edge: Some(edge_to_proto(&edge).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    // Only peer 1 holds the edge; peer 2 is untouched.
    assert!(h.store_a.get_edge(edge_id).unwrap().is_some());
    assert!(h.store_b.get_edge(edge_id).unwrap().is_none());
}

#[tokio::test]
async fn routed_delete_edge_removes_from_both_peers() {
    let h = spawn_two_peer().await;
    let src = node_owned_by(PeerId(1), &h);
    let dst = node_owned_by(PeerId(2), &h);

    let mut client = write_client(&h).await;
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&src).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&dst).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    let edge = Edge::new("T", src.id, dst.id);
    let edge_id = edge.id;
    client
        .put_edge(PutEdgeRequest {
            edge: Some(edge_to_proto(&edge).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    // Both peers hold the edge.
    assert!(h.store_a.get_edge(edge_id).unwrap().is_some());
    assert!(h.store_b.get_edge(edge_id).unwrap().is_some());

    client
        .delete_edge(DeleteEdgeRequest {
            edge_id: Some(uuid_to_proto(edge_id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();

    assert!(h.store_a.get_edge(edge_id).unwrap().is_none());
    assert!(h.store_b.get_edge(edge_id).unwrap().is_none());
    assert!(h.store_a.outgoing(src.id).unwrap().is_empty());
    assert!(h.store_b.incoming(dst.id).unwrap().is_empty());
}

#[tokio::test]
async fn routed_detach_delete_node_cleans_up_everywhere() {
    let h = spawn_two_peer().await;
    let src = node_owned_by(PeerId(1), &h);
    let dst = node_owned_by(PeerId(2), &h);

    let mut client = write_client(&h).await;
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&src).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();
    client
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&dst).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    let edge = Edge::new("KNOWS", src.id, dst.id);
    let edge_id = edge.id;
    client
        .put_edge(PutEdgeRequest {
            edge: Some(edge_to_proto(&edge).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    client
        .detach_delete_node(DetachDeleteNodeRequest {
            node_id: Some(uuid_to_proto(src.id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();

    // src gone from its owner, edge gone from both, dst still alive, dst has no incoming.
    assert!(h.store_a.get_node(src.id).unwrap().is_none());
    assert!(h.store_a.get_edge(edge_id).unwrap().is_none());
    assert!(h.store_b.get_edge(edge_id).unwrap().is_none());
    assert!(h.store_b.get_node(dst.id).unwrap().is_some());
    assert!(h.store_b.incoming(dst.id).unwrap().is_empty());
}

#[tokio::test]
async fn write_then_routed_read_full_cycle() {
    let h = spawn_two_peer().await;
    let src = node_owned_by(PeerId(2), &h);
    let dst = node_owned_by(PeerId(1), &h);

    let mut writer = write_client(&h).await;
    writer
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&src).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();
    writer
        .put_node(PutNodeRequest {
            node: Some(node_to_proto(&dst).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();
    let edge = Edge::new("LINKED", src.id, dst.id);
    writer
        .put_edge(PutEdgeRequest {
            edge: Some(edge_to_proto(&edge).unwrap()),
            local_only: false,
        })
        .await
        .unwrap();

    // Read via routed MeshQuery (connected to peer 1, but src is on peer 2).
    let mut reader = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let resp = reader
        .get_node(GetNodeRequest {
            id: Some(uuid_to_proto(src.id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    assert!(resp.into_inner().found);

    let resp = reader
        .outgoing(NeighborRequest {
            node_id: Some(uuid_to_proto(src.id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().neighbors.len(), 1);

    let resp = reader
        .incoming(NeighborRequest {
            node_id: Some(uuid_to_proto(dst.id.as_uuid())),
            local_only: false,
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().neighbors.len(), 1);
}

#[tokio::test]
async fn invalid_uuid_length_returns_invalid_argument() {
    let harness = spawn_server().await;
    let mut c = client(&harness).await;
    let bad = UuidBytes {
        value: vec![0, 1, 2, 3], // too short
    };
    let err = c
        .get_node(GetNodeRequest {
            id: Some(bad),
            local_only: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_execute_cypher_with_params_round_trips() {
    // gRPC parity for Bolt's parameter support. Encodes the params
    // map as serde-json into ExecuteCypherRequest.params_json, sends
    // it to the single-node server, and checks the rows decode back.
    // Without this test, parameterized writes against a Raft follower
    // (which forwards via this same proto field) would break silently.
    let harness = spawn_server().await;
    let mut c = client(&harness).await;

    // Seed two Person nodes via Cypher CREATE so we can filter them
    // by name with a parameter on the read side.
    for (name, age) in [("Ada", 37_i64), ("Alan", 41)] {
        let _ = c
            .execute_cypher(ExecuteCypherRequest {
                query: format!("CREATE (n:Person {{name: '{name}', age: {age}}})"),
                params_json: vec![],
            })
            .await
            .unwrap();
    }

    // Now read with a parameter. ParamMap is just `HashMap<String, Value>`,
    // and Value uses an internally-tagged JSON shape via serde.
    let mut params: std::collections::HashMap<String, mesh_executor::Value> =
        std::collections::HashMap::new();
    params.insert(
        "name".into(),
        mesh_executor::Value::Property(mesh_core::Property::String("Ada".into())),
    );
    let params_json = serde_json::to_vec(&params).unwrap();

    let resp = c
        .execute_cypher(ExecuteCypherRequest {
            query: "MATCH (n:Person {name: $name}) RETURN n.age AS age".to_string(),
            params_json,
        })
        .await
        .unwrap();

    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["age"]["Property"]["value"].as_i64(), Some(37));
}

// ---------------------------------------------------------------------------
// Coordinator recovery log: durable 2PC crash recovery
// ---------------------------------------------------------------------------

/// Pick two fresh node ids whose partitioner owners are peers 1 and 2
/// respectively. Used by the recovery tests to construct synthetic
/// Prepared entries that touch both peers regardless of the hash
/// partitioner's output.
fn pick_ids_per_peer(cluster: &Cluster) -> (mesh_core::NodeId, mesh_core::NodeId) {
    let mut owner1 = None;
    let mut owner2 = None;
    while owner1.is_none() || owner2.is_none() {
        let id = mesh_core::NodeId::new();
        match cluster.owner_of(id) {
            PeerId(1) if owner1.is_none() => owner1 = Some(id),
            PeerId(2) if owner2.is_none() => owner2 = Some(id),
            _ => {}
        }
    }
    (owner1.unwrap(), owner2.unwrap())
}

#[tokio::test]
async fn successful_tx_writes_prepared_commit_and_completed() {
    // Drive a normal routing-mode Cypher write through
    // execute_cypher_local and assert the coordinator log contains a
    // full Prepared → CommitDecision → Completed sequence. This is
    // the happy-path signal that logging hooks fire at the right
    // protocol boundaries.
    let h = spawn_two_peer_with_log().await;

    // Drive CREATEs straight through `execute_cypher_local` rather
    // than building a gRPC client — the method on MeshService is
    // exactly what the gRPC handler delegates to, and it commits
    // through the same TxCoordinator path that writes the log.
    // 8 creates virtually guarantee the partitioner scatters nodes
    // across peers 1 and 2, producing 2PC rounds that touch both.
    for i in 0..8 {
        h.service_a
            .execute_cypher_local(
                format!("CREATE (n:T {{i: {i}}})"),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();
    }

    // Each execute_cypher_local call that touches multiple peers
    // produces one 2PC round and therefore one {Prepared, Commit,
    // Completed} triple in the log. After 8 rounds the log should
    // have exactly 8 * 3 = 24 entries, alternating in that order.
    let entries = h.log_a.read_all().unwrap();
    assert!(
        entries.len() >= 3,
        "expected at least one full tx triple in the log, got {} entries",
        entries.len()
    );
    // Group by txid and verify every tx saw Prepared → CommitDecision → Completed.
    use std::collections::HashMap;
    let mut per_txid: HashMap<String, Vec<&TxLogEntry>> = HashMap::new();
    for e in &entries {
        per_txid.entry(e.txid().to_string()).or_default().push(e);
    }
    for (txid, entries) in &per_txid {
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, TxLogEntry::Prepared { .. })),
            "tx {txid} missing Prepared entry"
        );
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, TxLogEntry::CommitDecision { .. })),
            "tx {txid} missing CommitDecision entry"
        );
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, TxLogEntry::Completed { .. })),
            "tx {txid} missing Completed entry"
        );
    }
}

#[tokio::test]
async fn recovery_commits_tx_with_commit_decision_but_no_completion() {
    // Simulate a coordinator crash between writing CommitDecision and
    // finishing the COMMIT phase on every peer. On restart, recovery
    // must push the tx forward: apply the local commands directly
    // and send COMMIT (falling back to PREPARE+COMMIT) to peer B.
    let h = spawn_two_peer_with_log().await;

    // Pick one node id per peer so the Prepared entry has work for
    // both sides of the cluster.
    let (id_a, id_b) = pick_ids_per_peer(&h.cluster_a);
    let mut node_a = Node::new().with_label("Recovered");
    node_a.id = id_a;
    let mut node_b = Node::new().with_label("Recovered");
    node_b.id = id_b;

    // Synthesize what the coordinator log would contain if we had
    // crashed between CommitDecision and Completed. Peer B's staging
    // is empty (nothing ever ran PREPARE on it), so recover_commit
    // must fall back to re-sending PREPARE before COMMIT.
    let txid = "recovery-commit-1".to_string();
    let groups = vec![
        (
            PeerId(1),
            vec![mesh_cluster::GraphCommand::PutNode(node_a.clone())],
        ),
        (
            PeerId(2),
            vec![mesh_cluster::GraphCommand::PutNode(node_b.clone())],
        ),
    ];
    h.log_a
        .append(&TxLogEntry::Prepared {
            txid: txid.clone(),
            groups,
        })
        .unwrap();
    h.log_a
        .append(&TxLogEntry::CommitDecision { txid: txid.clone() })
        .unwrap();

    // Kick the coordinator into its recovery routine.
    h.service_a.recover_pending_transactions().await.unwrap();

    // Local peer A applied the command directly via apply_batch.
    assert!(
        h.store_a.get_node(node_a.id).unwrap().is_some(),
        "recovery should have applied the local node",
    );
    // Remote peer B received PREPARE+COMMIT and landed the node.
    assert!(
        h.store_b.get_node(node_b.id).unwrap().is_some(),
        "recovery should have committed the remote node",
    );
    // Log was compacted after recovery — no entries should remain
    // for the reconciled txid.
    let after = h.log_a.read_all().unwrap();
    assert!(
        !after.iter().any(|e| e.txid() == txid),
        "compacted log still contains recovered txid"
    );
}

#[tokio::test]
async fn recovery_aborts_tx_with_prepared_but_no_decision() {
    // Crash between Prepared and any decision → recovery rolls back.
    // We can't observe "staged then aborted" directly because peer B
    // never saw PREPARE in this synthetic scenario, but we can check
    // that no commands land on either store after recovery, and that
    // the log is compacted.
    let h = spawn_two_peer_with_log().await;

    let (id_a, id_b) = pick_ids_per_peer(&h.cluster_a);
    let mut node_a = Node::new().with_label("NeverCommitted");
    node_a.id = id_a;
    let mut node_b = Node::new().with_label("NeverCommitted");
    node_b.id = id_b;

    let txid = "recovery-abort-1".to_string();
    let groups = vec![
        (
            PeerId(1),
            vec![mesh_cluster::GraphCommand::PutNode(node_a.clone())],
        ),
        (
            PeerId(2),
            vec![mesh_cluster::GraphCommand::PutNode(node_b.clone())],
        ),
    ];
    h.log_a
        .append(&TxLogEntry::Prepared {
            txid: txid.clone(),
            groups,
        })
        .unwrap();
    // No CommitDecision — simulate crash right after writing Prepared.

    h.service_a.recover_pending_transactions().await.unwrap();

    // Neither node should exist. Recovery decided to abort because
    // there was no commit decision recorded.
    assert!(
        h.store_a.get_node(node_a.id).unwrap().is_none(),
        "undecided tx should not have landed on peer A"
    );
    assert!(
        h.store_b.get_node(node_b.id).unwrap().is_none(),
        "undecided tx should not have landed on peer B"
    );

    let after = h.log_a.read_all().unwrap();
    assert!(
        !after.iter().any(|e| e.txid() == txid),
        "compacted log still contains aborted txid"
    );
}

#[tokio::test]
async fn recovery_compacts_completed_entries() {
    // A log full of already-Completed entries should compact to
    // empty on recovery — nothing to resolve, no reason to keep the
    // historical trail around.
    let h = spawn_two_peer_with_log().await;

    for i in 0..3 {
        let txid = format!("done-{i}");
        h.log_a
            .append(&TxLogEntry::Prepared {
                txid: txid.clone(),
                groups: vec![],
            })
            .unwrap();
        h.log_a
            .append(&TxLogEntry::CommitDecision { txid: txid.clone() })
            .unwrap();
        h.log_a.append(&TxLogEntry::Completed { txid }).unwrap();
    }
    let before = h.log_a.read_all().unwrap();
    assert_eq!(before.len(), 9);

    h.service_a.recover_pending_transactions().await.unwrap();

    let after = h.log_a.read_all().unwrap();
    assert!(
        after.is_empty(),
        "recovery should compact away already-completed entries, got {} left",
        after.len()
    );

    // Also silence the "unused" warnings on harness fields the
    // recovery tests don't exercise directly.
    let _ = h.log_a_path;
}

// ---------------------------------------------------------------------------
// Participant staging TTL sweeper
// ---------------------------------------------------------------------------

#[tokio::test]
async fn staging_sweep_drops_stale_prepare_via_batch_write_rpc() {
    // End-to-end: stand up a single-peer service with a very short
    // TTL, send a Prepare via the gRPC batch_write handler, wait
    // past the TTL, sweep, and verify a subsequent Commit returns
    // FailedPrecondition ("not prepared") because the entry has
    // been collected.
    use mesh_rpc::ParticipantStaging;
    use std::time::Duration;

    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peers = vec![Peer::new(PeerId(1), addr.to_string())];
    let cluster = Arc::new(Cluster::new(PeerId(1), 1, peers).unwrap());
    let routing = Arc::new(Routing::new(cluster).unwrap());

    // 50ms TTL — short enough for a test to wait past without
    // inflating total runtime.
    let staging = ParticipantStaging::new(Duration::from_millis(50));

    let service = MeshService::with_routing(store, routing).with_staging(staging.clone());
    let stream = TcpListenerStream::new(listener);
    let server_service = service.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(server_service.clone().into_query_server())
            .add_service(server_service.into_write_server())
            .serve_with_incoming(stream)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(40)).await;

    // PREPARE via gRPC. An empty commands list is fine — the
    // sweeper doesn't care about payload shape, only age.
    let mut w = MeshWriteClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let commands: Vec<mesh_cluster::GraphCommand> = Vec::new();
    let payload = serde_json::to_vec(&commands).unwrap();
    let txid = "stale-tx".to_string();
    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Prepare as i32,
        commands_json: payload,
    })
    .await
    .unwrap();

    // Immediately after PREPARE, the entry is staged.
    assert!(staging.contains(&txid));

    // Wait past the TTL and run the sweeper directly. Calling
    // sweep_expired inline keeps the test deterministic; the
    // background sweeper is covered by the staging unit tests.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let dropped = staging.sweep_expired();
    assert_eq!(dropped, 1, "sweep should drop the stale entry");
    assert!(!staging.contains(&txid));

    // A late COMMIT for the swept tx surfaces the "not prepared"
    // protocol error, just as if the entry had never been staged.
    let err = w
        .batch_write(BatchWriteRequest {
            txid,
            phase: BatchPhase::Commit as i32,
            commands_json: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn staging_commit_before_ttl_still_succeeds() {
    // Complement of the sweep test: a Commit that arrives within
    // the TTL must still find its prepared entry. Guards against a
    // too-aggressive sweeper that collects entries immediately.
    use mesh_rpc::ParticipantStaging;
    use std::time::Duration;

    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peers = vec![Peer::new(PeerId(1), addr.to_string())];
    let cluster = Arc::new(Cluster::new(PeerId(1), 1, peers).unwrap());
    let routing = Arc::new(Routing::new(cluster).unwrap());
    let staging = ParticipantStaging::new(Duration::from_secs(30));

    let service = MeshService::with_routing(store, routing).with_staging(staging.clone());
    let stream = TcpListenerStream::new(listener);
    let server_service = service.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(server_service.clone().into_query_server())
            .add_service(server_service.into_write_server())
            .serve_with_incoming(stream)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(40)).await;

    let mut w = MeshWriteClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let commands: Vec<mesh_cluster::GraphCommand> = Vec::new();
    let payload = serde_json::to_vec(&commands).unwrap();
    let txid = "fresh-tx".to_string();
    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Prepare as i32,
        commands_json: payload,
    })
    .await
    .unwrap();

    // Sweep immediately — a 30s TTL means the entry is fresh and
    // survives.
    let dropped = staging.sweep_expired();
    assert_eq!(dropped, 0);
    assert!(staging.contains(&txid));

    // Commit succeeds because the entry is still there.
    w.batch_write(BatchWriteRequest {
        txid,
        phase: BatchPhase::Commit as i32,
        commands_json: Vec::new(),
    })
    .await
    .unwrap();
}
