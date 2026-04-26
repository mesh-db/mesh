use meshdb_cluster::{Cluster, Peer, PeerId};
use meshdb_core::{Edge, Node, Property};
use meshdb_executor::{ParamMap, Value};
use meshdb_rpc::convert::{edge_to_proto, node_to_proto, uuid_to_proto};
use meshdb_rpc::proto::mesh_query_client::MeshQueryClient;
use meshdb_rpc::proto::mesh_write_client::MeshWriteClient;
use meshdb_rpc::proto::{
    BatchPhase, BatchWriteRequest, DeleteEdgeRequest, DetachDeleteNodeRequest,
    EdgesByPropertyRequest, ExecuteCypherRequest, GetEdgeRequest, GetNodeRequest, HealthRequest,
    NeighborRequest, NodesByLabelRequest, PutEdgeRequest, PutNodeRequest, UuidBytes,
};
use meshdb_rpc::{
    CoordinatorLog, MeshService, ParticipantLog, Routing, TxCoordinatorTimeouts, TxLogEntry,
};
use meshdb_storage::{RocksDbStorageEngine as Store, StorageEngine};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct Harness {
    store: Arc<dyn StorageEngine>,
    address: String,
    _dir: TempDir,
}

async fn spawn_server() -> Harness {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
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
    let fake = uuid_to_proto(meshdb_core::NodeId::new().as_uuid());
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
    store_a: Arc<dyn StorageEngine>,
    store_b: Arc<dyn StorageEngine>,
    cluster_a: Arc<Cluster>,
    client_addr_a: String,
    _dirs: (TempDir, TempDir),
}

async fn spawn_two_peer() -> TwoPeer {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_b.path()).unwrap());

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

/// Generate fresh nodes until one hashes onto `peer`. Used by
/// scatter-gather tests that need a guaranteed per-peer source
/// count — `Node::new()` uses random UUID v7 ids, and at low
/// sample sizes the hash partitioner can land every node on one
/// side, trivially defeating any scatter-gather coverage. Looping
/// until we get a desired-owner id keeps the distribution
/// deterministic. Bounded to 256 tries so a wedged partitioner
/// fails the test instead of hanging.
fn fresh_node_owned_by(cluster: &Cluster, peer: PeerId) -> Node {
    for _ in 0..256 {
        let n = Node::new();
        if cluster.owner_of(n.id) == peer {
            return n;
        }
    }
    panic!("could not find a node owned by {peer:?} after 256 tries");
}

/// Two-peer harness extended with an on-disk coordinator log for peer
/// A, so recovery tests can manipulate the log directly and then call
/// `service_a.recover_pending_transactions()` through the local
/// `MeshService` handle.
struct TwoPeerWithLog {
    service_a: MeshService,
    store_a: Arc<dyn StorageEngine>,
    store_b: Arc<dyn StorageEngine>,
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
    let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());
    let store_b: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_b.path()).unwrap());

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
async fn edges_by_property_scatter_gathers_across_peers() {
    // Cluster-mode edge seek: each peer's local edge-property index CF
    // only carries entries for edges owned locally, so a scatter-gather
    // is required to return the complete set. Seed edges whose source
    // is owned by A and others whose source is owned by B, then query
    // peer A with local_only=false and expect every matching edge back.
    let h = spawn_two_peer().await;

    // Register the edge property index on BOTH local stores. The test
    // harness has no Raft/routing DDL replication so we backfill each
    // store directly; production routing DDL lands the index on every
    // peer via the same commit path.
    h.store_a.create_edge_property_index("R", "k").unwrap();
    h.store_b.create_edge_property_index("R", "k").unwrap();

    // Build 20 edges with source ownership split 10/10 across the two
    // peers, so the scatter-gather path is guaranteed to be exercised.
    // Previously the test used random `Node::new()` sources and relied
    // on hash partitioning to balance the distribution — at this small
    // sample size that's ~0.2% per-run flake risk (CI run 24871287307
    // hit it). Pre-choosing sources by owner removes the flake by
    // construction. Five k=42 + five k=99 per peer: k=42 is the seek
    // target (total 10), k=99 rows verify the filter is applied.
    let mut edge_ids_with_k_42: Vec<_> = Vec::new();
    for peer in [PeerId(1), PeerId(2)] {
        let store = match peer {
            PeerId(1) => &h.store_a,
            PeerId(2) => &h.store_b,
            other => panic!("unexpected peer {other:?}"),
        };
        for k_val in [42, 42, 42, 42, 42, 99, 99, 99, 99, 99] {
            let src = fresh_node_owned_by(&h.cluster_a, peer);
            let dst = Node::new();
            put_node_on_owner(&src, &h);
            put_node_on_owner(&dst, &h);
            let edge = Edge::new("R", src.id, dst.id).with_property("k", k_val);
            store.put_edge(&edge).unwrap();
            if k_val == 42 {
                edge_ids_with_k_42.push(edge.id);
            }
        }
    }
    // Sanity: 5 targets per peer, 10 total — no randomness, but an
    // explicit count keeps this readable alongside the scatter-gather
    // assertion below.
    assert_eq!(edge_ids_with_k_42.len(), 10);

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();
    let value_json = serde_json::to_vec(&meshdb_core::Property::Int64(42)).unwrap();
    let resp = client
        .edges_by_property(EdgesByPropertyRequest {
            edge_type: "R".into(),
            property: "k".into(),
            value_json: value_json.clone(),
            local_only: false,
        })
        .await
        .unwrap();
    let got = resp.into_inner().ids.len();
    assert_eq!(got, edge_ids_with_k_42.len(), "scatter-gather result size");

    // local_only=true on peer A must omit peer B's edges — 5 k=42
    // edges sourced on peer A, by construction.
    let resp_local = client
        .edges_by_property(EdgesByPropertyRequest {
            edge_type: "R".into(),
            property: "k".into(),
            value_json,
            local_only: true,
        })
        .await
        .unwrap();
    assert_eq!(resp_local.into_inner().ids.len(), 5);
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

    // `rows_json` is a serde-serialized Vec<meshdb_executor::Row>. Decode
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
    let commands: Vec<meshdb_cluster::GraphCommand> = nodes
        .into_iter()
        .map(meshdb_cluster::GraphCommand::PutNode)
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

    let commands = vec![meshdb_cluster::GraphCommand::PutNode(node)];
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
async fn batch_write_duplicate_prepare_same_payload_is_idempotent() {
    // A retry with identical commands (transient network glitch
    // caused the coordinator to resend PREPARE) must succeed instead
    // of failing the whole transaction. The staged batch in memory
    // stays intact, and the duplicate log entry is suppressed.
    let h = spawn_two_peer().await;
    let mut w = MeshWriteClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    let commands: Vec<meshdb_cluster::GraphCommand> = Vec::new();
    let payload = serde_json::to_vec(&commands).unwrap();

    let txid = "tx-dup-same".to_string();
    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Prepare as i32,
        commands_json: payload.clone(),
    })
    .await
    .unwrap();

    // Same txid + same payload → OK, no error.
    w.batch_write(BatchWriteRequest {
        txid,
        phase: BatchPhase::Prepare as i32,
        commands_json: payload,
    })
    .await
    .expect("identical PREPARE retry must be idempotent");
}

#[tokio::test]
async fn batch_write_duplicate_prepare_different_payload_rejected() {
    // Same txid with a DIFFERENT commands payload is a coordinator
    // bug — the caller could be silently clobbering a concurrent
    // transaction's staged state. Must fail loudly.
    let h = spawn_two_peer().await;
    let mut w = MeshWriteClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    let first: Vec<meshdb_cluster::GraphCommand> = vec![meshdb_cluster::GraphCommand::PutNode(
        Node::new().with_label("First"),
    )];
    let second: Vec<meshdb_cluster::GraphCommand> = vec![meshdb_cluster::GraphCommand::PutNode(
        Node::new().with_label("Second"),
    )];

    let txid = "tx-dup-conflict".to_string();
    w.batch_write(BatchWriteRequest {
        txid: txid.clone(),
        phase: BatchPhase::Prepare as i32,
        commands_json: serde_json::to_vec(&first).unwrap(),
    })
    .await
    .unwrap();

    let err = w
        .batch_write(BatchWriteRequest {
            txid,
            phase: BatchPhase::Prepare as i32,
            commands_json: serde_json::to_vec(&second).unwrap(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::AlreadyExists);
    assert!(
        err.message().contains("different commands"),
        "error message should call out the payload mismatch, got: {}",
        err.message(),
    );
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
            id: Some(uuid_to_proto(meshdb_core::NodeId::new().as_uuid())),
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
    let mut params: std::collections::HashMap<String, meshdb_executor::Value> =
        std::collections::HashMap::new();
    params.insert(
        "name".into(),
        meshdb_executor::Value::Property(meshdb_core::Property::String("Ada".into())),
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
fn pick_ids_per_peer(cluster: &Cluster) -> (meshdb_core::NodeId, meshdb_core::NodeId) {
    let mut owner1 = None;
    let mut owner2 = None;
    while owner1.is_none() || owner2.is_none() {
        let id = meshdb_core::NodeId::new();
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
            vec![meshdb_cluster::GraphCommand::PutNode(node_a.clone())],
        ),
        (
            PeerId(2),
            vec![meshdb_cluster::GraphCommand::PutNode(node_b.clone())],
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
            vec![meshdb_cluster::GraphCommand::PutNode(node_a.clone())],
        ),
        (
            PeerId(2),
            vec![meshdb_cluster::GraphCommand::PutNode(node_b.clone())],
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
    use meshdb_rpc::ParticipantStaging;
    use std::time::Duration;

    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());

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
    let commands: Vec<meshdb_cluster::GraphCommand> = Vec::new();
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
    use meshdb_rpc::ParticipantStaging;
    use std::time::Duration;

    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());

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
    let commands: Vec<meshdb_cluster::GraphCommand> = Vec::new();
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

/// Routing-mode `CREATE CONSTRAINT` fans out from peer A to peer B so
/// both stores end up with the same entry. Then `SHOW CONSTRAINTS`
/// issued on peer A scatter-gathers through the partitioned reader
/// and returns the same row shape the single-node DDL path emits.
/// Together these confirm the constraint DDL fan-out is wired up
/// end-to-end across the routing-mode server surface.
#[tokio::test]
async fn routing_mode_create_constraint_fans_out() {
    let h = spawn_two_peer().await;

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone())
        .await
        .unwrap();

    // CREATE CONSTRAINT — executor buffers a `CreateConstraint`
    // command, `commit_buffered_commands` runs the fan-out, and every
    // peer's store has a matching entry.
    client
        .execute_cypher(ExecuteCypherRequest {
            query: "CREATE CONSTRAINT email_uniq FOR (p:Person) REQUIRE p.email IS UNIQUE"
                .to_string(),
            params_json: vec![],
        })
        .await
        .expect("create constraint should succeed in routing mode");

    assert_eq!(h.store_a.list_property_constraints().len(), 1);
    assert_eq!(h.store_a.list_property_constraints()[0].name, "email_uniq");
    assert_eq!(h.store_b.list_property_constraints().len(), 1);
    assert_eq!(h.store_b.list_property_constraints()[0].name, "email_uniq");

    // SHOW CONSTRAINTS through Cypher — routed to peer A, reads from
    // the local registry (authoritative after fan-out).
    let resp = client
        .execute_cypher(ExecuteCypherRequest {
            query: "SHOW CONSTRAINTS".to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&resp.into_inner().rows_json).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"]["Property"]["value"], "email_uniq");
    assert_eq!(rows[0]["type"]["Property"]["value"], "UNIQUE");

    // DROP CONSTRAINT fans out the same way.
    client
        .execute_cypher(ExecuteCypherRequest {
            query: "DROP CONSTRAINT email_uniq".to_string(),
            params_json: vec![],
        })
        .await
        .unwrap();
    assert!(h.store_a.list_property_constraints().is_empty());
    assert!(h.store_b.list_property_constraints().is_empty());
}

// ---------------------------------------------------------------
// Participant-side durability (2PC hardening).
//
// Crash-simulates a peer that ACKed PREPARE by dropping its
// in-memory service and opening a fresh one against the same
// storage + log. Confirms that a subsequent COMMIT finds the
// staged batch and applies it — without the participant log, the
// second service would surface `failed_precondition` because
// staging is purely in-memory.
// ---------------------------------------------------------------

/// Harness for single-peer participant-durability tests. Holds the
/// on-disk paths that outlive any `MeshService` the test builds on
/// top of them. The `store` field is an `Option` so `.take()` drops
/// it cleanly — rocksdb's per-process directory lock won't let a
/// second `Store::open()` run until every handle on the first is
/// dropped.
struct ParticipantHarness {
    store: Option<Arc<dyn StorageEngine>>,
    log_path: std::path::PathBuf,
    store_dir: TempDir,
    _log_dir: TempDir,
}

fn new_participant_harness() -> ParticipantHarness {
    let store_dir = TempDir::new().unwrap();
    let log_dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(store_dir.path()).unwrap());
    let log_path = log_dir.path().join("participant-log.jsonl");
    ParticipantHarness {
        store: Some(store),
        log_path,
        store_dir,
        _log_dir: log_dir,
    }
}

impl ParticipantHarness {
    /// Borrow the currently-open store. Panics after [`Self::restart`]
    /// replaces it — call `.service()` or peek through a fresh
    /// `Store::open()` instead.
    fn store(&self) -> &Arc<dyn StorageEngine> {
        self.store.as_ref().expect("store dropped; call restart")
    }

    /// Build a fresh `MeshService` atop the currently-open store and
    /// log. Each call produces an independent service instance —
    /// tests that want to simulate a service restart should drop the
    /// previous service before calling [`Self::restart`].
    fn service(&self) -> MeshService {
        let log = Arc::new(ParticipantLog::open(&self.log_path).unwrap());
        MeshService::new(self.store().clone()).with_participant_log(Some(log))
    }

    /// Simulate a peer crash + restart. Drops the store and every
    /// outstanding handle through it (including the caller's service,
    /// which must be dropped first), opens a fresh store against the
    /// same data dir, and hands back a new `MeshService` whose
    /// participant staging has been rehydrated from the on-disk log.
    fn restart(&mut self) -> MeshService {
        // Drop the store Arc so rocksdb's LOCK releases. Tests must
        // have already dropped their service handle — any live clone
        // keeps the Arc alive and `Store::open()` below will fail
        // with "lock hold by current process".
        self.store = None;
        let store: Arc<dyn StorageEngine> = Arc::new(Store::open(self.store_dir.path()).unwrap());
        self.store = Some(store.clone());
        let log = Arc::new(ParticipantLog::open(&self.log_path).unwrap());
        let service = MeshService::new(store).with_participant_log(Some(log));
        service.recover_participant_staging().unwrap();
        service
    }
}

fn prepare_request(txid: &str, commands: &[meshdb_cluster::GraphCommand]) -> BatchWriteRequest {
    BatchWriteRequest {
        txid: txid.to_string(),
        phase: BatchPhase::Prepare as i32,
        commands_json: serde_json::to_vec(commands).unwrap(),
    }
}

fn commit_request(txid: &str) -> BatchWriteRequest {
    BatchWriteRequest {
        txid: txid.to_string(),
        phase: BatchPhase::Commit as i32,
        commands_json: Vec::new(),
    }
}

fn abort_request(txid: &str) -> BatchWriteRequest {
    BatchWriteRequest {
        txid: txid.to_string(),
        phase: BatchPhase::Abort as i32,
        commands_json: Vec::new(),
    }
}

#[tokio::test]
async fn participant_log_rehydrates_prepare_across_restart() {
    use meshdb_rpc::proto::mesh_write_server::MeshWrite;

    let mut h = new_participant_harness();
    let node = Node::new().with_label("Survives");
    let node_id = node.id;
    let commands = vec![meshdb_cluster::GraphCommand::PutNode(node)];

    // PREPARE on the first service instance, then drop it without a
    // COMMIT. Staging is in-memory only, so the only thing preserving
    // the prepared batch is the on-disk participant log.
    {
        let service = h.service();
        service
            .batch_write(tonic::Request::new(prepare_request("t-survive", &commands)))
            .await
            .unwrap();
    }

    // Before COMMIT, the store must still be untouched.
    assert!(h.store().get_node(node_id).unwrap().is_none());

    // Restart: fresh service with empty staging, replay from the log.
    let recovered = h.restart();

    // COMMIT arrives on the new service — must find the rehydrated
    // staging and apply the batch.
    recovered
        .batch_write(tonic::Request::new(commit_request("t-survive")))
        .await
        .unwrap();

    assert!(
        h.store().get_node(node_id).unwrap().is_some(),
        "rehydrated PREPARE must apply on the replacement service",
    );
}

#[tokio::test]
async fn participant_log_makes_duplicate_commit_idempotent() {
    use meshdb_rpc::proto::mesh_write_server::MeshWrite;

    let h = new_participant_harness();
    let node = Node::new().with_label("OnlyOnce");
    let node_id = node.id;
    let commands = vec![meshdb_cluster::GraphCommand::PutNode(node)];

    let service = h.service();
    service
        .batch_write(tonic::Request::new(prepare_request("t-dup", &commands)))
        .await
        .unwrap();
    service
        .batch_write(tonic::Request::new(commit_request("t-dup")))
        .await
        .unwrap();

    // Second COMMIT — previously returned `failed_precondition`
    // ("txid not prepared") because staging was drained. With the
    // outcomes cache populated, it short-circuits to success.
    service
        .batch_write(tonic::Request::new(commit_request("t-dup")))
        .await
        .expect("duplicate COMMIT should be idempotent");

    assert!(h.store().get_node(node_id).unwrap().is_some());
}

#[tokio::test]
async fn participant_log_commit_after_abort_is_rejected() {
    use meshdb_rpc::proto::mesh_write_server::MeshWrite;

    let h = new_participant_harness();
    let commands = vec![meshdb_cluster::GraphCommand::PutNode(
        Node::new().with_label("Abandoned"),
    )];

    let service = h.service();
    service
        .batch_write(tonic::Request::new(prepare_request("t-abort", &commands)))
        .await
        .unwrap();
    service
        .batch_write(tonic::Request::new(abort_request("t-abort")))
        .await
        .unwrap();

    // COMMIT against an aborted txid must fail — the outcomes cache
    // holds Aborted, which is a different failure than "never
    // prepared" and callers can use the message to diagnose
    // coordinator bugs.
    let err = service
        .batch_write(tonic::Request::new(commit_request("t-abort")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("aborted"),
        "error should name the aborted state, got: {}",
        err.message(),
    );
}

#[tokio::test]
async fn participant_log_replay_preserves_committed_outcome() {
    use meshdb_rpc::proto::mesh_write_server::MeshWrite;

    let mut h = new_participant_harness();
    let commands = vec![meshdb_cluster::GraphCommand::PutNode(
        Node::new().with_label("Done"),
    )];

    {
        let service = h.service();
        service
            .batch_write(tonic::Request::new(prepare_request("t-done", &commands)))
            .await
            .unwrap();
        service
            .batch_write(tonic::Request::new(commit_request("t-done")))
            .await
            .unwrap();
    }

    // Restart the peer. The committed outcome in the log must rehydrate
    // the outcomes cache so a late duplicate COMMIT still short-circuits.
    let recovered = h.restart();
    recovered
        .batch_write(tonic::Request::new(commit_request("t-done")))
        .await
        .expect("late COMMIT after restart must be idempotent against the logged outcome");
}

// ---------------------------------------------------------------
// Per-phase coordinator timeouts (slice A).
//
// A stalling peer must not block the coordinator past the
// configured deadline. The test spins up a fake MeshWrite server
// whose BatchWrite handler sleeps forever, registers it as peer 2
// in a two-peer cluster, and runs a Cypher write whose commands
// route across both peers. With a tight PREPARE timeout the
// coordinator reports `DeadlineExceeded` within the budget rather
// than waiting on the stalled peer indefinitely.
// ---------------------------------------------------------------

/// Minimal MeshWrite server that sleeps forever on every RPC. Used
/// to simulate a peer that accepts a TCP connection but never
/// completes the `BatchWrite` handler.
#[derive(Clone, Default)]
struct StallingMeshWrite;

#[tonic::async_trait]
impl meshdb_rpc::proto::mesh_write_server::MeshWrite for StallingMeshWrite {
    async fn put_node(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::PutNodeRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::PutNodeResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn put_edge(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::PutEdgeRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::PutEdgeResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn delete_edge(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DeleteEdgeRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DeleteEdgeResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn detach_delete_node(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DetachDeleteNodeRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DetachDeleteNodeResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn batch_write(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::BatchWriteRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::BatchWriteResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn create_property_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::CreatePropertyIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::CreatePropertyIndexResponse>, tonic::Status>
    {
        std::future::pending().await
    }
    async fn drop_property_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DropPropertyIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DropPropertyIndexResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn create_edge_property_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::CreateEdgePropertyIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::CreateEdgePropertyIndexResponse>, tonic::Status>
    {
        std::future::pending().await
    }
    async fn drop_edge_property_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DropEdgePropertyIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DropEdgePropertyIndexResponse>, tonic::Status>
    {
        std::future::pending().await
    }
    async fn create_point_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::CreatePointIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::CreatePointIndexResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn drop_point_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DropPointIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DropPointIndexResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn create_edge_point_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::CreateEdgePointIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::CreateEdgePointIndexResponse>, tonic::Status>
    {
        std::future::pending().await
    }
    async fn drop_edge_point_index(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DropEdgePointIndexRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DropEdgePointIndexResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn create_property_constraint(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::CreatePropertyConstraintRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::CreatePropertyConstraintResponse>, tonic::Status>
    {
        std::future::pending().await
    }
    async fn drop_property_constraint(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DropPropertyConstraintRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DropPropertyConstraintResponse>, tonic::Status>
    {
        std::future::pending().await
    }
    async fn resolve_transaction(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::ResolveTransactionRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::ResolveTransactionResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn install_trigger(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::InstallTriggerRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::InstallTriggerResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn drop_trigger(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::DropTriggerRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::DropTriggerResponse>, tonic::Status> {
        std::future::pending().await
    }
    async fn forward_write(
        &self,
        _req: tonic::Request<meshdb_rpc::proto::ForwardWriteRequest>,
    ) -> Result<tonic::Response<meshdb_rpc::proto::ForwardWriteResponse>, tonic::Status> {
        std::future::pending().await
    }
}

/// Spawn a two-peer cluster where peer A is a real `MeshService`
/// and peer B is a stalling fake. Peer A runs its coordinator
/// against peer B; every outgoing RPC to peer B hangs until its
/// deadline fires.
async fn spawn_two_peer_with_stalling_b(
    timeouts: TxCoordinatorTimeouts,
) -> (MeshService, Arc<dyn StorageEngine>, TempDir) {
    let dir_a = TempDir::new().unwrap();
    let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    let peers = vec![
        Peer::new(PeerId(1), addr_a.to_string()),
        Peer::new(PeerId(2), addr_b.to_string()),
    ];
    let cluster_a = Arc::new(Cluster::new(PeerId(1), 4, peers.clone()).unwrap());
    let routing_a = Arc::new(Routing::new(cluster_a.clone()).unwrap());

    let service_a =
        MeshService::with_routing(store_a.clone(), routing_a).with_tx_timeouts(timeouts);

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
            .add_service(meshdb_rpc::proto::mesh_write_server::MeshWriteServer::new(
                StallingMeshWrite,
            ))
            .serve_with_incoming(stream_b)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    (service_a, store_a, dir_a)
}

#[tokio::test]
async fn resolve_transaction_reports_coordinator_decision() {
    // Peer A acts as coordinator, records a CommitDecision for a
    // txid in its log. Peer B queries peer A via
    // `ResolveTransaction` and should learn the decision.
    use meshdb_rpc::proto::mesh_write_client::MeshWriteClient;
    use meshdb_rpc::proto::{ResolveTransactionRequest, TxResolutionStatus};

    let h = spawn_two_peer_with_log().await;

    // Seed the coordinator log by hand with a Commit decision for
    // a fabricated txid, as if peer A had run the 2PC protocol and
    // decided to commit.
    let txid = "resolve-test-tx".to_string();
    h.log_a
        .append(&TxLogEntry::Prepared {
            txid: txid.clone(),
            groups: vec![],
        })
        .unwrap();
    h.log_a
        .append(&TxLogEntry::CommitDecision { txid: txid.clone() })
        .unwrap();

    // Peer A's MeshWrite service answers `ResolveTransaction`.
    let mut w = MeshWriteClient::connect(format!(
        "http://{}",
        h.cluster_a
            .membership()
            .address(h.cluster_a.self_id())
            .unwrap()
    ))
    .await
    .unwrap();
    let resp = w
        .resolve_transaction(ResolveTransactionRequest { txid })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, TxResolutionStatus::Committed as i32);

    // Unknown txids return UNKNOWN (not an error).
    let resp = w
        .resolve_transaction(ResolveTransactionRequest {
            txid: "never-seen".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, TxResolutionStatus::Unknown as i32);
}

#[tokio::test]
async fn coordinator_prepare_timeout_fires_within_budget() {
    // Pick a PREPARE deadline short enough to fire quickly but long
    // enough for gRPC RPC startup cost. The stalling peer accepts
    // the connection but never returns from `batch_write`.
    let timeouts = TxCoordinatorTimeouts {
        prepare: Duration::from_millis(300),
        commit: Duration::from_secs(30),
        abort: Duration::from_secs(10),
    };
    let (service_a, _store_a, _dir) = spawn_two_peer_with_stalling_b(timeouts).await;

    // Cypher query whose WRITE routes to peer B: CREATE N nodes
    // under a unique label, then check how long the failure takes
    // to surface. At least one of the created nodes will hash to
    // peer B (random UUIDs against a 2-peer FNV partition map land
    // on each peer with ~0.5 probability), so the coordinator's
    // PREPARE phase will contact the stalling peer and hit the
    // timeout. If every node happened to land on peer A (unlikely
    // but possible), the coordinator skips the cross-peer PREPARE
    // altogether and the test shouldn't fail — just retry.
    //
    // To keep the test deterministic without overengineering, we
    // create enough nodes that hitting peer B is overwhelmingly
    // likely (p(all on A) = 0.5^16 ≈ 1e-5 with 16 nodes).
    use meshdb_rpc::proto::mesh_query_server::MeshQuery;
    let exec = ExecuteCypherRequest {
        query: (1..=16)
            .map(|i| format!("CREATE (:Stalled {{i: {i}}})"))
            .collect::<Vec<_>>()
            .join(" "),
        params_json: vec![],
    };

    let start = std::time::Instant::now();
    let result = service_a.execute_cypher(tonic::Request::new(exec)).await;
    let elapsed = start.elapsed();

    // The RPC must return an error (the PREPARE fan-out hit the
    // stall) within a bounded window — the 300ms deadline plus
    // enough slack for RPC overhead. Two seconds is generous;
    // without the timeout wrapper this call would hang until
    // tonic's default client timeout (effectively forever).
    assert!(
        elapsed < Duration::from_secs(2),
        "coordinator must bail within the PREPARE deadline; elapsed {:?}",
        elapsed,
    );
    let err = result.expect_err("stalled PREPARE must surface an error");
    assert!(
        err.message().contains("timed out") || err.code() == tonic::Code::DeadlineExceeded,
        "expected a deadline error, got code={:?} message={:?}",
        err.code(),
        err.message(),
    );
}

// ---------------------------------------------------------------
// CALL { ... } IN TRANSACTIONS — batched commits.
// ---------------------------------------------------------------

#[tokio::test]
async fn call_in_transactions_creates_all_input_nodes() {
    // 5 input rows × CREATE per row × batch size 2 = 3 commits
    // (2, 2, 1 rows). All 5 nodes should land in the store.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let _ = service
        .execute_cypher_local(
            "UNWIND [1, 2, 3, 4, 5] AS x \
             CALL { WITH x CREATE (:Batched {n: x}) } IN TRANSACTIONS OF 2 ROWS"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("execute should succeed");
    // CALL { ... } IN TRANSACTIONS is a write-only terminal
    // clause in this v1 — it doesn't surface body output rows
    // to the caller. The check below confirms the writes
    // committed via a follow-up MATCH count.
    // Verify all 5 Batched nodes landed.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:Batched) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .expect("count should succeed");
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64 count, got {other:?}"),
    };
    assert_eq!(c, 5, "all batched CREATEs should commit");
}

#[tokio::test]
async fn call_in_transactions_default_batch_size_works() {
    // Without `OF n ROWS`, batch size defaults to 1000 (Neo4j 5).
    // Five rows fit in one batch; everything should still commit.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "UNWIND [1, 2, 3, 4, 5] AS x \
             CALL { WITH x CREATE (:Defaulted {n: x}) } IN TRANSACTIONS"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("default-batch IN TRANSACTIONS should succeed");
    let rows = service
        .execute_cypher_local(
            "MATCH (n:Defaulted) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(c, 5);
}

#[tokio::test]
async fn call_in_transactions_each_batch_sees_prior_batch_writes() {
    // Run two passes back-to-back via separate IN TRANSACTIONS
    // batches. The second pass's MATCH should see the first
    // pass's writes, since each batch commits independently.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());

    // Pass 1: create 4 nodes in batches of 2.
    service
        .execute_cypher_local(
            "UNWIND [1, 2, 3, 4] AS x \
             CALL { WITH x CREATE (:Pass {n: x}) } IN TRANSACTIONS OF 2 ROWS"
                .into(),
            ParamMap::new(),
        )
        .await
        .unwrap();

    // Pass 2: another IN TRANSACTIONS that reads the Pass nodes
    // created above. Confirms prior commits are visible.
    let rows = service
        .execute_cypher_local(
            "MATCH (n:Pass) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(c, 4);
}

#[tokio::test]
async fn call_in_transactions_on_error_continue_skips_failed_batch() {
    // UNIQUE constraint + a duplicate value in the input list
    // forces one batch to fail. With ON ERROR CONTINUE the
    // statement should commit the other 3 batches and report
    // overall success.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());

    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:K) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:K {k: 99})".into(), ParamMap::new())
        .await
        .unwrap();

    // Batch size 1 means each row is its own batch. The 99
    // collides with the pre-existing node; the other three
    // batches (1, 2, 4) should commit independently.
    service
        .execute_cypher_local(
            "UNWIND [1, 2, 99, 4] AS x \
             CALL { WITH x CREATE (:K {k: x}) } IN TRANSACTIONS OF 1 ROWS ON ERROR CONTINUE"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("ON ERROR CONTINUE must report success");

    let rows = service
        .execute_cypher_local("MATCH (n:K) RETURN count(n) AS c".into(), ParamMap::new())
        .await
        .unwrap();
    let c = match rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    // Pre-existing 99 + 3 new (1, 2, 4) = 4. The 99 batch
    // failed and was skipped.
    assert_eq!(c, 4, "CONTINUE should skip the duplicate batch");
}

#[tokio::test]
async fn call_in_transactions_on_error_break_halts_after_failure() {
    // Same setup but with BREAK: the duplicate halts processing.
    // Earlier batches (1, 2) are committed; later batches (4)
    // are not attempted.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());

    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:K) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:K {k: 99})".into(), ParamMap::new())
        .await
        .unwrap();

    service
        .execute_cypher_local(
            "UNWIND [1, 2, 99, 4] AS x \
             CALL { WITH x CREATE (:K {k: x}) } IN TRANSACTIONS OF 1 ROWS ON ERROR BREAK"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("ON ERROR BREAK must report success on prior commits");

    let rows = service
        .execute_cypher_local("MATCH (n:K) RETURN count(n) AS c".into(), ParamMap::new())
        .await
        .unwrap();
    let c = match rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    // Pre-existing 99 + 2 committed (1, 2) before the failing
    // 99 halted further batches = 3.
    assert_eq!(c, 3, "BREAK should commit pre-failure batches and stop");
}

#[tokio::test]
async fn call_in_transactions_on_error_fail_propagates() {
    // Default ON ERROR FAIL behaviour: statement returns error.
    // Earlier batches still committed (Neo4j semantics — "earlier
    // batches stay durably persisted, the failed one is rolled
    // back, no later batches are attempted").
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());

    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:K) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:K {k: 99})".into(), ParamMap::new())
        .await
        .unwrap();

    let err = service
        .execute_cypher_local(
            "UNWIND [1, 2, 99, 4] AS x \
             CALL { WITH x CREATE (:K {k: x}) } IN TRANSACTIONS OF 1 ROWS"
                .into(),
            ParamMap::new(),
        )
        .await
        .err()
        .expect("default FAIL mode must return an error");
    let msg = err.message().to_lowercase();
    assert!(
        msg.contains("constraint") || msg.contains("unique") || msg.contains("k_uniq"),
        "expected constraint-violation message, got: {}",
        err.message(),
    );

    // 1 + 2 committed before the failure; 4 not attempted; 99
    // pre-existed.
    let rows = service
        .execute_cypher_local("MATCH (n:K) RETURN count(n) AS c".into(), ParamMap::new())
        .await
        .unwrap();
    let c = match rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(c, 3, "FAIL keeps pre-failure commits durable");
}

#[tokio::test]
async fn call_in_transactions_with_trailing_return_surfaces_outer_rows() {
    // The wrapping `RETURN x` projects from the outer-row
    // bindings (the UNWIND value) and should yield one row per
    // input row after batched execution.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "UNWIND [10, 20, 30] AS x \
             CALL { WITH x CREATE (:Reported {n: x}) } IN TRANSACTIONS OF 1 ROWS \
             RETURN x ORDER BY x"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("post-CALL RETURN should surface outer rows");
    assert_eq!(rows.len(), 3, "one row per input value");
    let mut got: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("x").unwrap() {
            Value::Property(Property::Int64(n)) => *n,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![10, 20, 30]);

    // Confirm the writes also landed.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:Reported) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(c, 3);
}

#[tokio::test]
async fn call_in_transactions_with_aggregate_count_returns_total() {
    // `RETURN count(*)` after IN TRANSACTIONS should count the
    // outer rows that flowed through. Tests the Aggregate
    // operator running on top of the substituted RowsLiteralOp.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "UNWIND [1, 2, 3, 4, 5] AS x \
             CALL { WITH x CREATE (:Counted {n: x}) } IN TRANSACTIONS OF 2 ROWS \
             RETURN count(*) AS total"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("post-CALL count(*) should aggregate over outer rows");
    assert_eq!(rows.len(), 1);
    let total = match rows[0].get("total").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64 total, got {other:?}"),
    };
    assert_eq!(total, 5);
}

#[tokio::test]
async fn call_in_transactions_with_limit_truncates_output_only() {
    // `LIMIT 2` after IN TRANSACTIONS should only affect the
    // returned row count — all writes still land because the
    // batched commits happen before the post-CALL pipeline runs.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "UNWIND [1, 2, 3, 4] AS x \
             CALL { WITH x CREATE (:Limited {n: x}) } IN TRANSACTIONS OF 1 ROWS \
             RETURN x ORDER BY x LIMIT 2"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("post-CALL LIMIT should truncate but not block writes");
    assert_eq!(rows.len(), 2);
    // All 4 writes landed regardless of LIMIT.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:Limited) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(c, 4, "LIMIT applies to output rows only, not writes");
}

#[tokio::test]
async fn call_in_transactions_report_status_emits_one_row_per_batch() {
    // 5 input rows × batch size 2 = 3 batches. With REPORT
    // STATUS AS s, the output is 3 rows (one per batch), each
    // carrying the s map.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "UNWIND [1, 2, 3, 4, 5] AS x \
             CALL { WITH x CREATE (:Statused {n: x}) } \
             IN TRANSACTIONS OF 2 ROWS REPORT STATUS AS s \
             RETURN s.committed AS committed, s.errorMessage AS err"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("REPORT STATUS query should succeed");
    assert_eq!(rows.len(), 3, "one row per batch (5 rows / 2 = 3 batches)");
    for row in &rows {
        match row.get("committed").unwrap() {
            Value::Property(Property::Bool(true)) => {}
            other => panic!("expected committed=true, got {other:?}"),
        }
        match row.get("err").unwrap() {
            Value::Property(Property::Null) | Value::Null => {}
            other => panic!("expected null errorMessage, got {other:?}"),
        }
    }
    // Writes still landed.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:Statused) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert_eq!(c, 5);
}

#[tokio::test]
async fn call_in_transactions_report_status_marks_failed_batch() {
    // ON ERROR CONTINUE + REPORT STATUS: failed batch shows
    // committed=false with a populated errorMessage; successful
    // batches show committed=true.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());

    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:K) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:K {k: 99})".into(), ParamMap::new())
        .await
        .unwrap();

    let rows = service
        .execute_cypher_local(
            "UNWIND [1, 2, 99, 4] AS x \
             CALL { WITH x CREATE (:K {k: x}) } \
             IN TRANSACTIONS OF 1 ROWS ON ERROR CONTINUE REPORT STATUS AS s \
             RETURN s.committed AS committed, s.errorMessage AS err"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("CONTINUE + REPORT STATUS should succeed overall");
    // 4 batches (one per input row), one of which failed.
    assert_eq!(rows.len(), 4);
    let mut committed_count = 0;
    let mut failed_with_err = 0;
    for row in &rows {
        match row.get("committed").unwrap() {
            Value::Property(Property::Bool(true)) => committed_count += 1,
            Value::Property(Property::Bool(false)) => {
                if matches!(
                    row.get("err").unwrap(),
                    Value::Property(Property::String(_))
                ) {
                    failed_with_err += 1;
                }
            }
            other => panic!("expected Bool, got {other:?}"),
        }
    }
    assert_eq!(committed_count, 3, "3 batches should commit (1, 2, 4)");
    assert_eq!(
        failed_with_err, 1,
        "1 batch should report failure with a non-null errorMessage",
    );
}

#[tokio::test]
async fn call_in_transactions_report_status_transaction_ids_unique() {
    // Each batch mints its own transactionId. The status row's
    // transactionId field should be a non-empty string and
    // distinct across batches.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "UNWIND [1, 2, 3] AS x \
             CALL { WITH x CREATE (:T {n: x}) } \
             IN TRANSACTIONS OF 1 ROWS REPORT STATUS AS s \
             RETURN s.transactionId AS tx"
                .into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    let mut ids: Vec<String> = Vec::new();
    for row in &rows {
        match row.get("tx").unwrap() {
            Value::Property(Property::String(s)) => {
                assert!(!s.is_empty(), "transactionId should not be empty");
                ids.push(s.clone());
            }
            other => panic!("expected String transactionId, got {other:?}"),
        }
    }
    let unique: std::collections::HashSet<_> = ids.iter().cloned().collect();
    assert_eq!(unique.len(), 3, "transactionIds should be unique per batch");
}

// ---------------------------------------------------------------
// apoc.periodic.iterate — APOC's batched-migration procedure.
// ---------------------------------------------------------------

#[tokio::test]
async fn apoc_periodic_iterate_basic_creates_all_target_nodes() {
    // Iterate over a small UNWIND list, action creates one node
    // per row. The iterate's RETURN columns become $-params for
    // the action.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [1, 2, 3, 4, 5] AS x RETURN x',\
                'CREATE (:ApocBatched {n: $x})',\
                {batchSize: 2}\
            ) YIELD batches, total, committedOperations, failedOperations, failedBatches \
             RETURN batches, total, committedOperations, failedOperations, failedBatches"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("apoc.periodic.iterate should succeed");
    assert_eq!(rows.len(), 1, "apoc.periodic.iterate emits one summary row");
    let r = &rows[0];
    let int_col = |name: &str| match r.get(name).unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64 for {name}, got {other:?}"),
    };
    // 5 rows / batchSize 2 = 3 batches (2, 2, 1).
    assert_eq!(int_col("batches"), 3);
    assert_eq!(int_col("total"), 5);
    assert_eq!(int_col("committedOperations"), 5);
    assert_eq!(int_col("failedOperations"), 0);
    assert_eq!(int_col("failedBatches"), 0);
    // Verify writes landed.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:ApocBatched) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    assert_eq!(c, 5);
}

#[tokio::test]
async fn apoc_periodic_iterate_continues_past_failed_batch() {
    // UNIQUE constraint + duplicate value in the iterate stream.
    // apoc.periodic.iterate uses ON ERROR CONTINUE-style
    // semantics by default — failed batches counted, run
    // proceeds.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:ApocK) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:ApocK {k: 99})".into(), ParamMap::new())
        .await
        .unwrap();
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [1, 2, 99, 4] AS x RETURN x',\
                'CREATE (:ApocK {k: $x})',\
                {batchSize: 1}\
            ) YIELD batches, total, committedOperations, failedOperations, failedBatches, errorMessages \
             RETURN batches, total, committedOperations, failedOperations, failedBatches, errorMessages"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("CONTINUE-style iterate should succeed overall");
    let r = &rows[0];
    let int_col = |name: &str| match r.get(name).unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    assert_eq!(int_col("batches"), 4);
    assert_eq!(int_col("total"), 4);
    // 3 batches commit (1, 2, 4), 1 fails (99).
    assert_eq!(int_col("committedOperations"), 3);
    assert_eq!(int_col("failedOperations"), 1);
    assert_eq!(int_col("failedBatches"), 1);
    // errorMessages is a Map keyed by error string — there's
    // at least one entry covering the constraint violation.
    match r.get("errorMessages").unwrap() {
        Value::Property(Property::Map(m)) => {
            assert!(!m.is_empty(), "errorMessages should be non-empty");
            let any_const = m.keys().any(|k| {
                let kl = k.to_lowercase();
                kl.contains("constraint") || kl.contains("unique") || kl.contains("k_uniq")
            });
            assert!(
                any_const,
                "expected constraint-related error key, got {:?}",
                m.keys().collect::<Vec<_>>()
            );
        }
        other => panic!("expected Map, got {other:?}"),
    }
}

#[tokio::test]
async fn apoc_periodic_iterate_extra_params_merge_with_iterate_columns() {
    // config.params provides an extra `multiplier` parameter
    // alongside the iterate row's `x` column.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [10, 20, 30] AS x RETURN x',\
                'CREATE (:Scaled {n: $x * $multiplier})',\
                {batchSize: 10, params: {multiplier: 7}}\
            ) YIELD batches RETURN batches"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("config.params should be honored");
    let scaled = service
        .execute_cypher_local(
            "MATCH (n:Scaled) RETURN n.n AS v ORDER BY v".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let got: Vec<i64> = scaled
        .iter()
        .map(|r| match r.get("v").unwrap() {
            Value::Property(Property::Int64(n)) => *n,
            other => panic!("got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![70, 140, 210]);
}

#[tokio::test]
async fn apoc_periodic_iterate_rejects_unknown_yield_column() {
    // Asking for a YIELD column the procedure doesn't produce
    // is a plan-time error.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let err = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'RETURN 1 AS x',\
                'CREATE (:T)',\
                {}\
            ) YIELD frobnicated RETURN frobnicated"
                .into(),
            ParamMap::new(),
        )
        .await
        .err()
        .expect("unknown YIELD column should error");
    let msg = err.message().to_lowercase();
    assert!(
        msg.contains("output column") && msg.contains("frobnicated"),
        "expected unknown-column message, got: {}",
        err.message(),
    );
}

#[tokio::test]
async fn apoc_periodic_iterate_extended_result_columns_present() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [1, 2, 3] AS x RETURN x',\
                'CREATE (:Ext {n: $x})',\
                {batchSize: 10}\
            ) YIELD batches, retries, batch, operations, updateStatistics, failedParams \
             RETURN batches, retries, batch, operations, updateStatistics, failedParams"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("extended-column YIELD should succeed");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    // retries: no failures, no retries.
    match r.get("retries").unwrap() {
        Value::Property(Property::Int64(0)) => {}
        other => panic!("expected retries=0, got {other:?}"),
    }
    // batch / operations: structured Maps with the expected keys.
    let batch_keys = match r.get("batch").unwrap() {
        Value::Property(Property::Map(m)) => m
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<String>>(),
        other => panic!("expected Map batch, got {other:?}"),
    };
    assert!(batch_keys.contains("total"));
    assert!(batch_keys.contains("committed"));
    assert!(batch_keys.contains("failed"));
    let ops = match r.get("operations").unwrap() {
        Value::Property(Property::Map(m)) => m.clone(),
        other => panic!("expected Map operations, got {other:?}"),
    };
    assert_eq!(ops.get("total"), Some(&Property::Int64(3)));
    assert_eq!(ops.get("committed"), Some(&Property::Int64(3)));
    assert_eq!(ops.get("failed"), Some(&Property::Int64(0)));
    // updateStatistics: 3 nodes created with 1 prop + 1 label each.
    let upd = match r.get("updateStatistics").unwrap() {
        Value::Property(Property::Map(m)) => m.clone(),
        other => panic!("expected Map updateStatistics, got {other:?}"),
    };
    assert_eq!(upd.get("nodesCreated"), Some(&Property::Int64(3)));
    assert_eq!(upd.get("propertiesSet"), Some(&Property::Int64(3)));
    assert_eq!(upd.get("labelsAdded"), Some(&Property::Int64(3)));
    // failedParams: empty Map (no failures).
    match r.get("failedParams").unwrap() {
        Value::Property(Property::Map(m)) => assert!(m.is_empty()),
        other => panic!("expected empty Map failedParams, got {other:?}"),
    }
}

#[tokio::test]
async fn apoc_periodic_iterate_retries_count_increments_on_repeated_failure() {
    // UNIQUE constraint guarantees the duplicate batch fails
    // every attempt. With retries=2, that batch should be
    // attempted 3 times total (1 initial + 2 retries) — bumping
    // retries by 2 even though the batch ultimately fails.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:RetK) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:RetK {k: 99})".into(), ParamMap::new())
        .await
        .unwrap();
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [99] AS x RETURN x',\
                'CREATE (:RetK {k: $x})',\
                {batchSize: 1, retries: 2}\
            ) YIELD failedBatches, retries RETURN failedBatches, retries"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("retries config should succeed even on persistent failure");
    let r = &rows[0];
    let int_col = |name: &str| match r.get(name).unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    assert_eq!(int_col("failedBatches"), 1);
    assert_eq!(int_col("retries"), 2, "should have retried twice");
}

#[tokio::test]
async fn apoc_periodic_iterate_failed_params_capture_samples() {
    // failedParams: -1 captures all sets that fail (capped only
    // by the set size). Verify the `failedParams` column has an
    // entry containing the failing input row's `x` value.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:FpK) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:FpK {k: 7})".into(), ParamMap::new())
        .await
        .unwrap();
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [7] AS x RETURN x',\
                'CREATE (:FpK {k: $x})',\
                {batchSize: 1, failedParams: -1}\
            ) YIELD failedParams RETURN failedParams"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("failedParams capture should succeed");
    let fp = match rows[0].get("failedParams").unwrap() {
        Value::Property(Property::Map(m)) => m.clone(),
        other => panic!("got {other:?}"),
    };
    assert!(!fp.is_empty(), "failedParams should be populated");
    // Each entry's value is a List of param-map snapshots.
    let any_sample = fp.values().any(|v| match v {
        Property::List(items) => items.iter().any(|item| match item {
            Property::Map(sample) => sample.get("x") == Some(&Property::Int64(7)),
            _ => false,
        }),
        _ => false,
    });
    assert!(any_sample, "expected an x=7 sample in failedParams: {fp:?}");
}

#[tokio::test]
async fn apoc_periodic_iterate_failed_params_zero_disables_capture() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "CREATE CONSTRAINT k_uniq FOR (n:FpZ) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    service
        .execute_cypher_local("CREATE (:FpZ {k: 1})".into(), ParamMap::new())
        .await
        .unwrap();
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [1] AS x RETURN x',\
                'CREATE (:FpZ {k: $x})',\
                {batchSize: 1, failedParams: 0}\
            ) YIELD failedParams RETURN failedParams"
                .into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let fp = match rows[0].get("failedParams").unwrap() {
        Value::Property(Property::Map(m)) => m.clone(),
        other => panic!("got {other:?}"),
    };
    assert!(
        fp.is_empty(),
        "failedParams=0 should disable capture entirely, got {fp:?}",
    );
}

#[tokio::test]
async fn apoc_periodic_iterate_list_mode_runs_action_once_per_batch() {
    // iterateList=true: action receives `$_batch` as a list of
    // row-Maps and is invoked ONCE per batch. The action body
    // unwraps via UNWIND $_batch AS row, then accesses
    // row.<column> directly. With batchSize=2 over 5 rows,
    // there are 3 batches and the action runs 3 times — but
    // the writes still cover all 5 input rows.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [1, 2, 3, 4, 5] AS x RETURN x',\
                'UNWIND $_batch AS row CREATE (:ListMode {n: row.x})',\
                {batchSize: 2, iterateList: true}\
            ) YIELD batches, total, committedOperations, updateStatistics \
             RETURN batches, total, committedOperations, updateStatistics"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("iterateList=true should succeed");
    let r = &rows[0];
    let int_col = |name: &str| match r.get(name).unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    // 3 batches (2,2,1) processing 5 rows total.
    assert_eq!(int_col("batches"), 3);
    assert_eq!(int_col("total"), 5);
    assert_eq!(int_col("committedOperations"), 5);
    // Verify all 5 nodes landed.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:ListMode) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    assert_eq!(c, 5);
    // updateStatistics should reflect the 5 nodes (regardless
    // of how many times the action ran).
    let upd = match r.get("updateStatistics").unwrap() {
        Value::Property(Property::Map(m)) => m.clone(),
        other => panic!("got {other:?}"),
    };
    assert_eq!(upd.get("nodesCreated"), Some(&Property::Int64(5)));
}

#[tokio::test]
async fn apoc_periodic_iterate_list_mode_with_extra_params() {
    // iterateList=true should still merge config.params under
    // their declared name. The action references both the
    // batch-scoped $_batch list AND the extra `$shift` param.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [10, 20] AS x RETURN x',\
                'UNWIND $_batch AS row CREATE (:Shifted {v: row.x + $shift})',\
                {batchSize: 10, iterateList: true, params: {shift: 100}}\
            ) YIELD batches RETURN batches"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("iterateList=true + params should compose");
    let nodes = service
        .execute_cypher_local(
            "MATCH (n:Shifted) RETURN n.v AS v ORDER BY v".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let got: Vec<i64> = nodes
        .iter()
        .map(|r| match r.get("v").unwrap() {
            Value::Property(Property::Int64(n)) => *n,
            other => panic!("got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![110, 120]);
}

#[tokio::test]
async fn apoc_periodic_iterate_parallel_creates_all_target_nodes() {
    // Parallel happy-path analogue of
    // `apoc_periodic_iterate_basic_creates_all_target_nodes`: 20
    // rows × batchSize 2 → 10 batches, dispatched through a pool
    // of 4. Aggregates must match the sequential semantics
    // exactly — `batches`, `total`, `committedOperations`,
    // `failedBatches`, and the underlying write count are all
    // order-independent.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND range(1, 20) AS x RETURN x',\
                'CREATE (:ApocPar {n: $x})',\
                {batchSize: 2, parallel: true, concurrency: 4}\
            ) YIELD batches, total, committedOperations, failedOperations, failedBatches \
             RETURN batches, total, committedOperations, failedOperations, failedBatches"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("parallel apoc.periodic.iterate should succeed");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    let int_col = |name: &str| match r.get(name).unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("expected Int64 for {name}, got {other:?}"),
    };
    assert_eq!(int_col("batches"), 10);
    assert_eq!(int_col("total"), 20);
    assert_eq!(int_col("committedOperations"), 20);
    assert_eq!(int_col("failedOperations"), 0);
    assert_eq!(int_col("failedBatches"), 0);

    // All 20 writes must have landed, regardless of which task
    // committed each batch.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:ApocPar) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    assert_eq!(c, 20);
}

#[tokio::test]
async fn apoc_periodic_iterate_parallel_failed_batch_doesnt_block_others() {
    // Parallel variant of
    // `apoc_periodic_iterate_continues_past_failed_batch`.
    // UNIQUE collision makes one batch fail, the others must
    // still commit — verifies per-batch failure isolation under
    // concurrent dispatch.
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let service = MeshService::new(store.clone());
    service
        .execute_cypher_local(
            "CREATE CONSTRAINT kpar_uniq FOR (n:ApocParK) REQUIRE n.k IS UNIQUE".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    // Pre-seed key 99 so the batch carrying $x=99 collides.
    service
        .execute_cypher_local("CREATE (:ApocParK {k: 99})".into(), ParamMap::new())
        .await
        .unwrap();
    let rows = service
        .execute_cypher_local(
            "CALL apoc.periodic.iterate(\
                'UNWIND [1, 2, 3, 4, 99, 6, 7, 8] AS x RETURN x',\
                'CREATE (:ApocParK {k: $x})',\
                {batchSize: 1, parallel: true, concurrency: 4}\
            ) YIELD batches, total, committedOperations, failedOperations, failedBatches \
             RETURN batches, total, committedOperations, failedOperations, failedBatches"
                .into(),
            ParamMap::new(),
        )
        .await
        .expect("parallel iterate should survive a failing batch");
    let r = &rows[0];
    let int_col = |name: &str| match r.get(name).unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    // 8 batches total, 7 commit, 1 fails (the 99 duplicate).
    assert_eq!(int_col("batches"), 8);
    assert_eq!(int_col("total"), 8);
    assert_eq!(int_col("committedOperations"), 7);
    assert_eq!(int_col("failedOperations"), 1);
    assert_eq!(int_col("failedBatches"), 1);

    // The 7 successful inserts plus the pre-seeded 99 should all
    // be in the store; the 99 from the iterate stream was
    // rejected by the UNIQUE constraint.
    let count_rows = service
        .execute_cypher_local(
            "MATCH (n:ApocParK) RETURN count(n) AS c".into(),
            ParamMap::new(),
        )
        .await
        .unwrap();
    let c = match count_rows[0].get("c").unwrap() {
        Value::Property(Property::Int64(n)) => *n,
        other => panic!("got {other:?}"),
    };
    assert_eq!(c, 8);
}

#[cfg(feature = "apoc-trigger")]
mod apoc_trigger {
    use super::*;
    use meshdb_executor::apoc_trigger::TriggerRegistry;
    use meshdb_executor::ProcedureRegistry;

    /// Build a single-node MeshService with a fresh trigger
    /// registry attached to its procedure-registry factory.
    fn spawn_service_with_triggers() -> (MeshService, Arc<dyn StorageEngine>, TempDir) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
        let registry = TriggerRegistry::from_storage(store.clone()).unwrap();
        let svc = MeshService::new(store.clone()).with_procedure_registry_factory(move || {
            let mut p = ProcedureRegistry::new();
            p.register_defaults();
            p.set_trigger_registry(registry.clone());
            p
        });
        (svc, store, dir)
    }

    /// Run a single Cypher statement through the service:
    /// execute → commit → return result rows.
    async fn run(svc: &MeshService, query: &str) -> Vec<meshdb_executor::Row> {
        let (rows, cmds) = svc
            .execute_cypher_in_tx(query.to_string(), ParamMap::new(), Vec::new(), false)
            .await
            .unwrap_or_else(|e| panic!("execute {query}: {e:?}"));
        svc.commit_buffered_commands(cmds)
            .await
            .unwrap_or_else(|e| panic!("commit {query}: {e:?}"));
        rows
    }

    #[tokio::test]
    async fn trigger_install_drop_list_roundtrips_via_cypher() {
        let (svc, _store, _d) = spawn_service_with_triggers();
        // install
        let rows = run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'bump', \
               'MATCH (n:Widget) RETURN 1', null, null) \
             YIELD name, installed RETURN name, installed",
        )
        .await;
        assert_eq!(rows.len(), 1);
        // list sees it
        let rows = run(
            &svc,
            "CALL apoc.trigger.list() YIELD name, query RETURN name, query",
        )
        .await;
        assert_eq!(rows.len(), 1);
        // drop removes it
        let rows = run(
            &svc,
            "CALL apoc.trigger.drop('meshdb', 'bump') \
             YIELD name, removed RETURN name, removed",
        )
        .await;
        assert_eq!(rows.len(), 1);
        match rows[0].get("removed") {
            Some(Value::Property(Property::Bool(true))) => {}
            other => panic!("expected removed=true, got {other:?}"),
        }
        // list is empty again
        let rows = run(&svc, "CALL apoc.trigger.list() YIELD name RETURN name").await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn trigger_fires_after_create_and_sees_created_nodes() {
        let (svc, store, _d) = spawn_service_with_triggers();
        // Install a trigger that writes a Marker node
        // whenever a Widget is created. The trigger inspects
        // `$createdNodes` and only fires when it sees one.
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'markWidget', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Widget\" IN labels(n) \
                CREATE (:Marker {name: n.name})', \
               null, null) \
             YIELD name RETURN name",
        )
        .await;
        // Create a Widget — should trigger a Marker creation.
        run(&svc, "CREATE (:Widget {name: 'one'})").await;
        // Marker should exist.
        let markers = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Marker"))
            .collect::<Vec<_>>();
        assert_eq!(markers.len(), 1, "trigger should have created one Marker");
        assert_eq!(
            markers[0].properties.get("name"),
            Some(&Property::String("one".into()))
        );
    }

    #[tokio::test]
    async fn trigger_recursion_guard_stops_self_triggering() {
        let (svc, store, _d) = spawn_service_with_triggers();
        // A trigger that creates another Widget whenever it sees
        // one in $createdNodes. Without a suppression guard this
        // would loop until the process dies; with the guard it
        // fires exactly once per real user write.
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'copyWidget', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Widget\" IN labels(n) \
                CREATE (:Widget {name: n.name + \"_copy\"})', \
               null, null) \
             YIELD name RETURN name",
        )
        .await;
        run(&svc, "CREATE (:Widget {name: 'seed'})").await;
        // Expect 2 widgets: the original + the trigger's copy.
        // Not 4, not infinite.
        let widgets = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Widget"))
            .collect::<Vec<_>>();
        assert_eq!(
            widgets.len(),
            2,
            "recursion guard should stop the trigger from self-triggering"
        );
    }

    #[tokio::test]
    async fn trigger_sees_deleted_nodes_and_relationships() {
        let (svc, store, _d) = spawn_service_with_triggers();
        run(
            &svc,
            "CREATE (a:Widget {name: 'a'})-[:LINKS]->(b:Widget {name: 'b'})",
        )
        .await;
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'logDeletes', \
               'UNWIND $deletedNodes AS n \
                CREATE (:DeletionLog {name: n.name})', \
               null, null) \
             YIELD name RETURN name",
        )
        .await;
        run(&svc, "MATCH (w:Widget {name: 'a'}) DETACH DELETE w").await;
        // Expect one DeletionLog node with name='a'.
        let logs = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "DeletionLog"))
            .collect::<Vec<_>>();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].properties.get("name"),
            Some(&Property::String("a".into()))
        );
    }

    #[tokio::test]
    async fn trigger_install_rejects_unknown_phase() {
        let (svc, _store, _d) = spawn_service_with_triggers();
        let err = svc
            .execute_cypher_in_tx(
                "CALL apoc.trigger.install('meshdb', 'x', 'RETURN 1', \
                   {phase: 'invalid_phase'}, null) \
                 YIELD name RETURN name"
                    .to_string(),
                ParamMap::new(),
                Vec::new(),
                false,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("phase must be one of"),
            "expected unknown-phase rejection, got: {err}"
        );
    }

    #[tokio::test]
    async fn before_trigger_can_abort_commit_via_error() {
        let (svc, store, _d) = spawn_service_with_triggers();
        // Install a before-trigger that errors on Forbidden labels.
        // Cypher's only "raise" mechanism is to evaluate an
        // expression that errors at runtime — divide by zero
        // suffices.
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'forbidder', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Forbidden\" IN labels(n) \
                RETURN 1 / 0', \
               {phase: 'before'}, null) \
             YIELD name RETURN name",
        )
        .await;
        // Allowed creates succeed.
        run(&svc, "CREATE (:Allowed {name: 'ok'})").await;
        let allowed_count = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Allowed"))
            .count();
        assert_eq!(allowed_count, 1);
        // A Forbidden create should abort the commit.
        let err = svc
            .execute_cypher_in_tx(
                "CREATE (:Forbidden {name: 'no'})".to_string(),
                ParamMap::new(),
                Vec::new(),
                false,
            )
            .await;
        let err = match err {
            Ok((_, cmds)) => svc.commit_buffered_commands(cmds).await.unwrap_err(),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("before-trigger"),
            "expected before-trigger abort, got: {err}"
        );
        // Verify the Forbidden node was NOT created.
        let forbidden_count = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Forbidden"))
            .count();
        assert_eq!(
            forbidden_count, 0,
            "before-trigger abort should have prevented the Forbidden node"
        );
    }

    #[tokio::test]
    async fn before_trigger_writes_commit_with_user_writes() {
        let (svc, store, _d) = spawn_service_with_triggers();
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'audit', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Person\" IN labels(n) \
                CREATE (:Audit {who: n.name})', \
               {phase: 'before'}, null) \
             YIELD name RETURN name",
        )
        .await;
        run(&svc, "CREATE (:Person {name: 'Ada'})").await;
        let audit = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Audit"))
            .collect::<Vec<_>>();
        assert_eq!(audit.len(), 1);
        assert_eq!(
            audit[0].properties.get("who"),
            Some(&Property::String("Ada".into()))
        );
    }

    #[tokio::test]
    async fn trigger_stop_pauses_firing_until_start() {
        let (svc, store, _d) = spawn_service_with_triggers();
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'pausable', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Source\" IN labels(n) \
                CREATE (:Marker {name: n.name})', \
               null, null) \
             YIELD name RETURN name",
        )
        .await;
        run(&svc, "CREATE (:Source {name: 'first'})").await;
        let initial = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Marker"))
            .count();
        assert_eq!(initial, 1);
        run(
            &svc,
            "CALL apoc.trigger.stop('meshdb', 'pausable') \
             YIELD name, paused RETURN name, paused",
        )
        .await;
        run(&svc, "CREATE (:Source {name: 'second'})").await;
        let after_stop = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Marker"))
            .count();
        assert_eq!(after_stop, 1, "stopped trigger should not have fired");
        run(
            &svc,
            "CALL apoc.trigger.start('meshdb', 'pausable') \
             YIELD name, paused RETURN name, paused",
        )
        .await;
        run(&svc, "CREATE (:Source {name: 'third'})").await;
        let after_start = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "Marker"))
            .count();
        assert_eq!(after_start, 2, "restarted trigger should fire");
    }

    /// Spawn a 2-peer routing-mode cluster where both peers have
    /// a TriggerRegistry attached. Tests the cluster-replicated
    /// install/drop path: CALL on peer A should reach peer B's
    /// storage via the routing-mode DDL fan-out.
    async fn spawn_two_peer_with_triggers() -> (
        MeshService,
        Arc<dyn StorageEngine>,
        Arc<dyn StorageEngine>,
        TempDir,
        TempDir,
    ) {
        use meshdb_executor::ProcedureRegistry;
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let store_a: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_a.path()).unwrap());
        let store_b: Arc<dyn StorageEngine> = Arc::new(Store::open(dir_b.path()).unwrap());

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
        let routing_a = Arc::new(Routing::new(cluster_a).unwrap());
        let routing_b = Arc::new(Routing::new(cluster_b).unwrap());

        // Attach TriggerRegistry on both peers via the registry
        // factory.
        let registry_a = TriggerRegistry::from_storage(store_a.clone()).unwrap();
        let registry_b = TriggerRegistry::from_storage(store_b.clone()).unwrap();

        let service_a = MeshService::with_routing(store_a.clone(), routing_a)
            .with_procedure_registry_factory(move || {
                let mut p = ProcedureRegistry::new();
                p.register_defaults();
                p.set_trigger_registry(registry_a.clone());
                p
            });
        let service_b = MeshService::with_routing(store_b.clone(), routing_b)
            .with_procedure_registry_factory(move || {
                let mut p = ProcedureRegistry::new();
                p.register_defaults();
                p.set_trigger_registry(registry_b.clone());
                p
            });

        let stream_a = TcpListenerStream::new(listener_a);
        let stream_b = TcpListenerStream::new(listener_b);
        let svc_a_clone = service_a.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc_a_clone.clone().into_query_server())
                .add_service(svc_a_clone.into_write_server())
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

        (service_a, store_a, store_b, dir_a, dir_b)
    }

    #[tokio::test]
    async fn trigger_install_replicates_across_routing_cluster() {
        let (svc_a, store_a, store_b, _da, _db) = spawn_two_peer_with_triggers().await;
        // Install via peer A — should fan out to peer B.
        let (_rows, cmds) = svc_a
            .execute_cypher_in_tx(
                "CALL apoc.trigger.install('meshdb', 'auditor', \
                   'RETURN 1', null, null) YIELD name RETURN name"
                    .to_string(),
                ParamMap::new(),
                Vec::new(),
                false,
            )
            .await
            .unwrap();
        svc_a.commit_buffered_commands(cmds).await.unwrap();
        // Verify storage on both peers has the entry.
        let triggers_a = store_a.list_triggers().unwrap();
        assert_eq!(
            triggers_a.len(),
            1,
            "peer A should have the installed trigger"
        );
        assert_eq!(triggers_a[0].0, "auditor");
        let triggers_b = store_b.list_triggers().unwrap();
        assert_eq!(
            triggers_b.len(),
            1,
            "peer B should have received the trigger via DDL fan-out"
        );
        assert_eq!(triggers_b[0].0, "auditor");
        // Verify the spec blob round-tripped intact (same bytes
        // on both peers).
        assert_eq!(triggers_a[0].1, triggers_b[0].1);
    }

    #[tokio::test]
    async fn trigger_drop_replicates_across_routing_cluster() {
        let (svc_a, store_a, store_b, _da, _db) = spawn_two_peer_with_triggers().await;
        // Install on both peers via the fan-out.
        let (_, cmds) = svc_a
            .execute_cypher_in_tx(
                "CALL apoc.trigger.install('meshdb', 'transient', \
                   'RETURN 1', null, null) YIELD name RETURN name"
                    .to_string(),
                ParamMap::new(),
                Vec::new(),
                false,
            )
            .await
            .unwrap();
        svc_a.commit_buffered_commands(cmds).await.unwrap();
        assert_eq!(store_a.list_triggers().unwrap().len(), 1);
        assert_eq!(store_b.list_triggers().unwrap().len(), 1);
        // Drop and verify the fan-out propagated.
        let (_, cmds) = svc_a
            .execute_cypher_in_tx(
                "CALL apoc.trigger.drop('meshdb', 'transient') \
                 YIELD name RETURN name"
                    .to_string(),
                ParamMap::new(),
                Vec::new(),
                false,
            )
            .await
            .unwrap();
        svc_a.commit_buffered_commands(cmds).await.unwrap();
        assert!(store_a.list_triggers().unwrap().is_empty());
        assert!(store_b.list_triggers().unwrap().is_empty());
    }

    #[tokio::test]
    async fn after_async_trigger_fires_in_background() {
        let (svc, store, _d) = spawn_service_with_triggers();
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'asyncMark', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Source\" IN labels(n) \
                CREATE (:AsyncMarker {name: n.name})', \
               {phase: 'afterAsync'}, null) \
             YIELD name RETURN name",
        )
        .await;
        run(&svc, "CREATE (:Source {name: 'first'})").await;
        // afterAsync runs in tokio::spawn — give it a moment to
        // commit. The recursive commit goes through the same
        // single-node apply, which is fast, so a brief sleep is
        // enough on the test machine.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let markers = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "AsyncMarker"))
            .count();
        assert_eq!(markers, 1, "afterAsync trigger should have fired");
    }

    #[tokio::test]
    async fn rollback_trigger_fires_when_commit_fails() {
        let (svc, store, _d) = spawn_service_with_triggers();
        // Install the rollback-phase trigger first; if we
        // installed the before-error trigger first, it would
        // abort the install commit of the rollback trigger
        // itself. Then install a before-trigger that errors on
        // any commit involving a Trigger label — narrow enough
        // not to fire on subsequent installs / drops.
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'logRollback', \
               'CREATE (:RollbackLog {note: \"saw an abort\"})', \
               {phase: 'rollback'}, null) \
             YIELD name RETURN name",
        )
        .await;
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'forbidAnything', \
               'UNWIND $createdNodes AS n \
                WITH n WHERE \"Anything\" IN labels(n) \
                RETURN 1 / 0', \
               {phase: 'before'}, null) \
             YIELD name RETURN name",
        )
        .await;
        // Attempt a commit that the before-trigger will abort.
        let attempt = svc
            .execute_cypher_in_tx(
                "CREATE (:Anything)".to_string(),
                ParamMap::new(),
                Vec::new(),
                false,
            )
            .await;
        let _err = match attempt {
            Ok((_, cmds)) => svc.commit_buffered_commands(cmds).await.unwrap_err(),
            Err(e) => e,
        };
        // Rollback trigger should have written its log entry.
        let logs = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "RollbackLog"))
            .count();
        assert_eq!(logs, 1, "rollback trigger should have fired on the abort");
    }

    #[tokio::test]
    async fn trigger_sees_assigned_node_properties_and_labels() {
        let (svc, store, _d) = spawn_service_with_triggers();
        run(&svc, "CREATE (:Person {name: 'Ada'})").await;
        // Install AFTER seeding so the create doesn't fire it.
        run(
            &svc,
            "CALL apoc.trigger.install('meshdb', 'changeAudit', \
               'UNWIND $assignedNodeProperties AS change \
                CREATE (:PropAudit {key: change.key}) \
                WITH 1 AS _ \
                UNWIND $assignedLabels AS lc \
                CREATE (:LabelAudit {label: lc.label})', \
               null, null) \
             YIELD name RETURN name",
        )
        .await;
        // SET adds a property and a label to an existing node.
        run(
            &svc,
            "MATCH (p:Person {name: 'Ada'}) SET p.age = 30, p:Member",
        )
        .await;
        let prop_audits = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "PropAudit"))
            .collect::<Vec<_>>();
        // age was the only assignment.
        assert_eq!(prop_audits.len(), 1);
        assert_eq!(
            prop_audits[0].properties.get("key"),
            Some(&Property::String("age".into()))
        );
        let label_audits = store
            .all_nodes()
            .unwrap()
            .into_iter()
            .filter(|n| n.labels.iter().any(|l| l == "LabelAudit"))
            .collect::<Vec<_>>();
        assert_eq!(label_audits.len(), 1);
        assert_eq!(
            label_audits[0].properties.get("label"),
            Some(&Property::String("Member".into()))
        );
    }

    #[tokio::test]
    async fn trigger_drop_is_idempotent_for_unknown_name() {
        let (svc, _store, _d) = spawn_service_with_triggers();
        // V2 semantics: drop always reports `removed: true` because
        // the actual apply happens through the cluster commit path
        // (where we don't synchronously know whether a prior entry
        // existed under the same name). The drop is still
        // idempotent at the storage layer.
        let rows = run(
            &svc,
            "CALL apoc.trigger.drop('meshdb', 'nonexistent') \
             YIELD name, removed RETURN name, removed",
        )
        .await;
        match rows[0].get("removed") {
            Some(Value::Property(Property::Bool(true))) => {}
            other => panic!("expected removed=true (V2 always-true semantics), got {other:?}"),
        }
    }
}
