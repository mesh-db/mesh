use mesh_core::{Edge, Node};
use mesh_rpc::convert::uuid_to_proto;
use mesh_rpc::proto::mesh_query_client::MeshQueryClient;
use mesh_rpc::proto::{
    GetEdgeRequest, GetNodeRequest, HealthRequest, NeighborRequest, NodesByLabelRequest,
    UuidBytes,
};
use mesh_rpc::MeshService;
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
            .add_service(service.into_server())
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
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.found);
    let got = inner.edge.unwrap();
    assert_eq!(got.edge_type, "KNOWS");
    assert!(got.properties.contains_key("since"));
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
