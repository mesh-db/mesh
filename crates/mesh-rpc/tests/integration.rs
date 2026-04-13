use mesh_cluster::{Cluster, Peer, PeerId};
use mesh_core::{Edge, Node};
use mesh_rpc::convert::{edge_to_proto, node_to_proto, uuid_to_proto};
use mesh_rpc::proto::mesh_query_client::MeshQueryClient;
use mesh_rpc::proto::mesh_write_client::MeshWriteClient;
use mesh_rpc::proto::{
    DeleteEdgeRequest, DetachDeleteNodeRequest, GetEdgeRequest, GetNodeRequest, HealthRequest,
    NeighborRequest, NodesByLabelRequest, PutEdgeRequest, PutNodeRequest, UuidBytes,
};
use mesh_rpc::{MeshService, Routing};
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
    let resp = c.get_node(GetNodeRequest { id: Some(fake) }).await.unwrap();
    let inner = resp.into_inner();
    assert!(!inner.found);
    assert!(inner.node.is_none());
}

#[tokio::test]
async fn nodes_by_label_returns_all_ids_for_label() {
    let harness = spawn_server().await;
    let a = Node::new().with_label("Person").with_property("name", "Ada");
    let b = Node::new().with_label("Person").with_property("name", "Alan");
    let c_node = Node::new().with_label("Company").with_property("name", "Acme");
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

#[tokio::test]
async fn routed_get_node_crosses_peers() {
    let h = spawn_two_peer().await;

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone()).await.unwrap();

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

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone()).await.unwrap();
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
        let n = Node::new()
            .with_label("Flag")
            .with_property("i", i as i64);
        let owner = put_node_on_owner(&n, &h);
        if owner == PeerId(1) {
            owned_by_a += 1;
        }
    }
    assert!(owned_by_a > 0, "expected some local-to-A entries");
    assert!(owned_by_a < 30, "expected some remote entries too");

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone()).await.unwrap();
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

    let mut client = MeshQueryClient::connect(h.client_addr_a.clone()).await.unwrap();
    let resp = client
        .outgoing(NeighborRequest {
            node_id: Some(uuid_to_proto(src.id.as_uuid())),
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
async fn routed_get_node_missing_returns_not_found_on_either_peer() {
    let h = spawn_two_peer().await;
    let mut client = MeshQueryClient::connect(h.client_addr_a.clone()).await.unwrap();
    // A random id that doesn't exist anywhere.
    let resp = client
        .get_node(GetNodeRequest {
            id: Some(uuid_to_proto(mesh_core::NodeId::new().as_uuid())),
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
        })
        .await
        .unwrap();
    assert!(resp.into_inner().found);

    let resp = reader
        .outgoing(NeighborRequest {
            node_id: Some(uuid_to_proto(src.id.as_uuid())),
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().neighbors.len(), 1);

    let resp = reader
        .incoming(NeighborRequest {
            node_id: Some(uuid_to_proto(dst.id.as_uuid())),
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
        .get_node(GetNodeRequest { id: Some(bad) })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
