//! Verifies the gRPC handler instrumentation actually emits tracing
//! spans + events. Installs a per-thread subscriber that captures every
//! span name into a shared Vec, then drives a request through the
//! single-node MeshService and asserts the captured names include the
//! handler we hit.

use meshdb_core::Node;
use meshdb_rpc::convert::node_to_proto;
use meshdb_rpc::proto::mesh_query_server::MeshQuery;
use meshdb_rpc::proto::mesh_write_server::MeshWrite;
use meshdb_rpc::proto::{GetNodeRequest, PutNodeRequest};
use meshdb_rpc::MeshService;
use meshdb_storage::{RocksDbStorageEngine as Store, StorageEngine};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tonic::Request;
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[derive(Default)]
struct CaptureLayer {
    spans: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        // Capture both the span name itself and any `rpc` field value so
        // tests can match on either. The instrumented handlers all set
        // `rpc = "<name>"` as a span field.
        let mut visitor = RpcFieldVisitor::default();
        attrs.record(&mut visitor);
        let mut sink = self.spans.lock().unwrap();
        sink.push(attrs.metadata().name().to_string());
        if let Some(rpc) = visitor.rpc {
            sink.push(format!("rpc={rpc}"));
        }
    }
}

#[derive(Default)]
struct RpcFieldVisitor {
    rpc: Option<String>,
}

impl tracing::field::Visit for RpcFieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "rpc" {
            self.rpc = Some(value.to_string());
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "rpc" {
            self.rpc = Some(format!("{value:?}").trim_matches('"').to_string());
        }
    }
}

#[tokio::test]
async fn put_node_handler_emits_instrumented_span() {
    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        spans: captured.clone(),
    };
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let dir = TempDir::new().unwrap();
    let store: Arc<dyn StorageEngine> = Arc::new(Store::open(dir.path()).unwrap());
    let svc = MeshService::new(store);

    let node = Node::new()
        .with_label("Person")
        .with_property("name", "Ada");
    let id = node.id;

    svc.put_node(Request::new(PutNodeRequest {
        node: Some(node_to_proto(&node).unwrap()),
        local_only: false,
    }))
    .await
    .unwrap();

    svc.get_node(Request::new(GetNodeRequest {
        id: Some(meshdb_rpc::convert::uuid_to_proto(id.as_uuid())),
        local_only: false,
    }))
    .await
    .unwrap();

    let names = captured.lock().unwrap();
    assert!(
        names.iter().any(|n| n == "rpc=put_node"),
        "expected put_node span, captured: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "rpc=get_node"),
        "expected get_node span, captured: {names:?}"
    );
}
