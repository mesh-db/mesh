//! End-to-end Bolt protocol tests. Spins up a mesh-server with a
//! local-only `MeshService` and a Bolt listener on an ephemeral port,
//! then drives it with a raw TCP client that uses `mesh_bolt` for
//! framing / encoding. Validates the full pipeline: handshake, HELLO,
//! RUN (CREATE + MATCH), PULL, RECORD decoding, and GOODBYE.

use mesh_bolt::{
    perform_client_handshake, read_message, version_bytes, write_message, BoltMessage, BoltValue,
    BOLT_4_4,
};
use mesh_rpc::MeshService;
use mesh_server::bolt::run_listener;
use mesh_storage::Store;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};

/// Spawn a single-node mesh service + Bolt listener on an ephemeral
/// port. Returns the bound address and a guard directory that deletes
/// the RocksDB store on drop.
async fn spawn_bolt_server() -> (String, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let service = Arc::new(MeshService::new(store));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = run_listener(listener, service).await;
    });

    // Give the listener a moment to enter its accept loop.
    tokio::time::sleep(Duration::from_millis(25)).await;

    (addr.to_string(), dir)
}

/// Connect, negotiate Bolt 4.4, and send a HELLO — leaves the socket
/// in the Ready phase.
async fn connect_and_hello(addr: &str) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.unwrap();

    let preferences = [BOLT_4_4, [0; 4], [0; 4], [0; 4]];
    let agreed = perform_client_handshake(&mut sock, &preferences)
        .await
        .unwrap();
    assert_eq!(agreed, BOLT_4_4);

    let hello = BoltMessage::Hello {
        extra: BoltValue::map([
            ("user_agent", BoltValue::String("mesh-test/0.1".into())),
            ("scheme", BoltValue::String("none".into())),
        ]),
    };
    write_message(&mut sock, &hello.encode()).await.unwrap();

    let reply_bytes = read_message(&mut sock).await.unwrap();
    let reply = BoltMessage::decode(&reply_bytes).unwrap();
    match reply {
        BoltMessage::Success { .. } => {}
        other => panic!("expected HELLO SUCCESS, got {:?}", other),
    }
    sock
}

/// Issue a RUN + PULL round-trip and return the decoded RECORD rows
/// plus the trailing PULL SUCCESS metadata.
async fn run_and_pull(sock: &mut TcpStream, query: &str) -> (Vec<Vec<BoltValue>>, BoltValue) {
    run_and_pull_with_params(sock, query, BoltValue::Map(vec![])).await
}

/// Same as `run_and_pull` but binds `params` on the RUN message — used
/// by the parameter end-to-end tests below to exercise the full
/// pipeline from BoltValue → ParamMap → executor → result.
async fn run_and_pull_with_params(
    sock: &mut TcpStream,
    query: &str,
    params: BoltValue,
) -> (Vec<Vec<BoltValue>>, BoltValue) {
    let run = BoltMessage::Run {
        query: query.to_string(),
        params,
        extra: BoltValue::Map(vec![]),
    };
    write_message(sock, &run.encode()).await.unwrap();

    let run_reply = BoltMessage::decode(&read_message(sock).await.unwrap()).unwrap();
    let _run_meta = match run_reply {
        BoltMessage::Success { metadata } => metadata,
        other => panic!("expected RUN SUCCESS, got {:?}", other),
    };

    let pull = BoltMessage::Pull {
        extra: BoltValue::map([("n", BoltValue::Int(-1))]),
    };
    write_message(sock, &pull.encode()).await.unwrap();

    let mut records: Vec<Vec<BoltValue>> = Vec::new();
    let pull_meta;
    loop {
        let raw = read_message(sock).await.unwrap();
        let msg = BoltMessage::decode(&raw).unwrap();
        match msg {
            BoltMessage::Record { fields } => records.push(fields),
            BoltMessage::Success { metadata } => {
                pull_meta = metadata;
                break;
            }
            other => panic!("unexpected message during PULL: {:?}", other),
        }
    }
    (records, pull_meta)
}

async fn goodbye(mut sock: TcpStream) {
    write_message(&mut sock, &BoltMessage::Goodbye.encode())
        .await
        .unwrap();
}

#[tokio::test]
async fn bolt_handshake_and_hello() {
    let (addr, _dir) = spawn_bolt_server().await;
    let sock = connect_and_hello(&addr).await;
    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_create_then_match_round_trips_records() {
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    // Write phase: CREATE three Person nodes. A bare CREATE without
    // RETURN still yields one row with the new node binding, so we
    // just drain the PULL response and don't inspect its shape.
    for i in 0..3 {
        let _ = run_and_pull(
            &mut sock,
            &format!("CREATE (n:Person {{name: 'p{}', age: {}}})", i, 20 + i * 10),
        )
        .await;
    }

    // Read phase: MATCH and project the properties.
    let (records, meta) = run_and_pull(
        &mut sock,
        "MATCH (n:Person) RETURN n.age AS age, n.name AS name ORDER BY age",
    )
    .await;

    assert_eq!(records.len(), 3, "expected exactly the three created nodes");

    // Fields are sorted alphabetically by field_names_from_rows, so
    // the RECORD column order is: age, name.
    let rows: Vec<(i64, String)> = records
        .iter()
        .map(|fields| {
            let age = fields[0].as_int().expect("age should be Int");
            let name = fields[1]
                .as_str()
                .expect("name should be String")
                .to_string();
            (age, name)
        })
        .collect();
    assert_eq!(
        rows,
        vec![(20, "p0".into()), (30, "p1".into()), (40, "p2".into()),]
    );

    // Trailing SUCCESS after PULL carries a type and record count.
    assert_eq!(meta.get("type").and_then(BoltValue::as_str), Some("r"));
    assert_eq!(
        meta.get("record_count").and_then(BoltValue::as_int),
        Some(3)
    );

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_returns_full_node_struct_with_labels_and_props() {
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    let (_, _) = run_and_pull(&mut sock, "CREATE (n:Widget {sku: 'abc-123', weight: 4.2})").await;

    let (records, _) = run_and_pull(&mut sock, "MATCH (n:Widget) RETURN n").await;
    assert_eq!(records.len(), 1);

    // The single field is the node itself, encoded as PackStream
    // Struct(tag=0x4E, [id, labels, props, element_id]).
    let node = &records[0][0];
    let (tag, fields) = match node {
        BoltValue::Struct { tag, fields } => (*tag, fields),
        other => panic!("expected Node struct, got {:?}", other),
    };
    assert_eq!(tag, mesh_bolt::TAG_NODE);
    assert_eq!(fields.len(), 4);

    // Field 0: id (Int — folded from the UUID).
    assert!(matches!(fields[0], BoltValue::Int(_)));

    // Field 1: labels — list of strings.
    let labels = fields[1].as_list().unwrap();
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].as_str(), Some("Widget"));

    // Field 2: properties map — keys sorted.
    let props = fields[2].as_map().unwrap();
    let keys: Vec<&str> = props.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["sku", "weight"]);
    assert_eq!(props[0].1.as_str(), Some("abc-123"));
    match props[1].1 {
        BoltValue::Float(f) => assert!((f - 4.2).abs() < 1e-9),
        ref other => panic!("expected Float for weight, got {:?}", other),
    }

    // Field 3: element_id — UUID string form, 36 chars with 4 dashes.
    let elem = fields[3].as_str().unwrap();
    assert_eq!(elem.len(), 36);
    assert_eq!(elem.chars().filter(|c| *c == '-').count(), 4);

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_reset_after_failure_returns_to_ready() {
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    // Send a syntactically-broken query so the server replies FAILURE.
    let run = BoltMessage::Run {
        query: "THIS IS NOT CYPHER".into(),
        params: BoltValue::Map(vec![]),
        extra: BoltValue::Map(vec![]),
    };
    write_message(&mut sock, &run.encode()).await.unwrap();
    let reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    match reply {
        BoltMessage::Failure { metadata } => {
            // Code should start with Mesh.ClientError.*
            let code = metadata.get("code").and_then(BoltValue::as_str).unwrap();
            assert!(code.starts_with("Mesh.ClientError."), "got code {code}");
        }
        other => panic!("expected FAILURE, got {:?}", other),
    }

    // Any further message before RESET is ignored — prove it.
    write_message(&mut sock, &BoltMessage::Goodbye.encode())
        .await
        .ok();
    // GOODBYE above closes from our side; re-connect for the RESET
    // flow since the server also tears down on GOODBYE.
    drop(sock);
    let mut sock = connect_and_hello(&addr).await;

    // Fresh session: send the same bad query, get FAILURE, then send
    // RESET and see SUCCESS + ability to issue new queries.
    write_message(&mut sock, &run.encode()).await.unwrap();
    let failure = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(matches!(failure, BoltMessage::Failure { .. }));

    write_message(&mut sock, &BoltMessage::Reset.encode())
        .await
        .unwrap();
    let reset_ack = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(matches!(reset_ack, BoltMessage::Success { .. }));

    // Session is now back in Ready — a fresh RUN should succeed.
    let (records, _) = run_and_pull(&mut sock, "MATCH (n) RETURN n").await;
    // Empty store → zero records, and no error.
    assert!(records.is_empty());

    goodbye(sock).await;
}

/// Send a BEGIN and assert SUCCESS — used by every explicit-tx test
/// that follows.
async fn begin_tx(sock: &mut TcpStream) {
    write_message(
        sock,
        &BoltMessage::Begin {
            extra: BoltValue::Map(vec![]),
        }
        .encode(),
    )
    .await
    .unwrap();
    let reply = BoltMessage::decode(&read_message(sock).await.unwrap()).unwrap();
    assert!(
        matches!(reply, BoltMessage::Success { .. }),
        "expected BEGIN SUCCESS, got {:?}",
        reply
    );
}

async fn commit_tx(sock: &mut TcpStream) -> BoltMessage {
    write_message(sock, &BoltMessage::Commit.encode())
        .await
        .unwrap();
    BoltMessage::decode(&read_message(sock).await.unwrap()).unwrap()
}

async fn rollback_tx(sock: &mut TcpStream) -> BoltMessage {
    write_message(sock, &BoltMessage::Rollback.encode())
        .await
        .unwrap();
    BoltMessage::decode(&read_message(sock).await.unwrap()).unwrap()
}

#[tokio::test]
async fn bolt_explicit_tx_commit_persists_buffered_writes() {
    // BEGIN; CREATE n; CREATE m; COMMIT; MATCH (...) — the two
    // creates accumulate into the connection's tx buffer and land
    // atomically when COMMIT dispatches them through
    // commit_buffered_commands. After COMMIT the nodes are visible.
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    begin_tx(&mut sock).await;
    let _ = run_and_pull(&mut sock, "CREATE (n:T {i: 1})").await;
    let _ = run_and_pull(&mut sock, "CREATE (n:T {i: 2})").await;
    let reply = commit_tx(&mut sock).await;
    assert!(
        matches!(reply, BoltMessage::Success { .. }),
        "expected COMMIT SUCCESS, got {:?}",
        reply
    );

    // Both nodes are visible after COMMIT.
    let (records, _) = run_and_pull(&mut sock, "MATCH (n:T) RETURN n.i AS i ORDER BY i").await;
    let xs: Vec<i64> = records
        .iter()
        .map(|fields| fields[0].as_int().unwrap())
        .collect();
    assert_eq!(xs, vec![1, 2]);

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_explicit_tx_rollback_drops_buffered_writes() {
    // BEGIN; CREATE; ROLLBACK leaves the store unchanged. A
    // post-ROLLBACK MATCH returns zero rows because the buffered
    // commands were dropped instead of committed.
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    begin_tx(&mut sock).await;
    let _ = run_and_pull(&mut sock, "CREATE (n:Discard)").await;
    let reply = rollback_tx(&mut sock).await;
    assert!(
        matches!(reply, BoltMessage::Success { .. }),
        "expected ROLLBACK SUCCESS, got {:?}",
        reply
    );

    let (records, _) = run_and_pull(&mut sock, "MATCH (n:Discard) RETURN n").await;
    assert!(records.is_empty(), "ROLLBACK must not persist writes");

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_explicit_tx_atomic_across_multiple_runs() {
    // Three CREATEs in one tx, COMMIT, verify we observe exactly the
    // committed set and nothing leaks from a possible partial commit.
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    begin_tx(&mut sock).await;
    for i in 0..5 {
        let _ = run_and_pull(&mut sock, &format!("CREATE (n:Batch {{i: {i}}})")).await;
    }
    let reply = commit_tx(&mut sock).await;
    assert!(matches!(reply, BoltMessage::Success { .. }));

    let (records, _) = run_and_pull(&mut sock, "MATCH (n:Batch) RETURN n.i AS i ORDER BY i").await;
    let xs: Vec<i64> = records
        .iter()
        .map(|fields| fields[0].as_int().unwrap())
        .collect();
    assert_eq!(xs, vec![0, 1, 2, 3, 4]);

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_explicit_tx_failed_run_invalidates_whole_tx() {
    // A CREATE that succeeds, then a malformed RUN that fails, then a
    // COMMIT attempt — the failed RUN must drop the whole buffer and
    // transition to the IGNORED-everything Failed state. RESET clears
    // and the previously-buffered CREATE must NOT show up afterwards.
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    begin_tx(&mut sock).await;
    let _ = run_and_pull(&mut sock, "CREATE (n:Half {step: 'first'})").await;

    // Send a parse-failing RUN.
    write_message(
        &mut sock,
        &BoltMessage::Run {
            query: "THIS IS NOT CYPHER".into(),
            params: BoltValue::Map(vec![]),
            extra: BoltValue::Map(vec![]),
        }
        .encode(),
    )
    .await
    .unwrap();
    let reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(matches!(reply, BoltMessage::Failure { .. }));

    // COMMIT in Failed state is IGNORED.
    let reply = commit_tx(&mut sock).await;
    assert!(matches!(reply, BoltMessage::Ignored));

    // RESET to recover.
    write_message(&mut sock, &BoltMessage::Reset.encode())
        .await
        .unwrap();
    let reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(matches!(reply, BoltMessage::Success { .. }));

    // The successful CREATE from inside the failed tx must NOT have
    // landed — failed RUNs invalidate the whole tx buffer.
    let (records, _) = run_and_pull(&mut sock, "MATCH (n:Half) RETURN n").await;
    assert!(
        records.is_empty(),
        "writes from a failed tx should not persist"
    );

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_nested_begin_rejected() {
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    begin_tx(&mut sock).await;

    // Second BEGIN inside the same tx → FAILURE with a Mesh.ClientError.* code.
    write_message(
        &mut sock,
        &BoltMessage::Begin {
            extra: BoltValue::Map(vec![]),
        }
        .encode(),
    )
    .await
    .unwrap();
    let reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    match reply {
        BoltMessage::Failure { metadata } => {
            let code = metadata.get("code").and_then(BoltValue::as_str).unwrap();
            assert!(
                code.starts_with("Mesh.ClientError."),
                "expected client-error code, got {code}"
            );
        }
        other => panic!("expected FAILURE, got {:?}", other),
    }

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_commit_outside_tx_is_protocol_error() {
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    // COMMIT from Ready (no preceding BEGIN) must FAILURE.
    let reply = commit_tx(&mut sock).await;
    match reply {
        BoltMessage::Failure { metadata } => {
            let msg = metadata
                .get("message")
                .and_then(BoltValue::as_str)
                .unwrap_or("");
            assert!(
                msg.contains("COMMIT") || msg.contains("transaction"),
                "expected protocol error message, got {msg}"
            );
        }
        other => panic!("expected FAILURE, got {:?}", other),
    }

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_rollback_after_failure_clears_via_reset() {
    // Documents the recovery path: after any failure inside a tx the
    // session is in Failed state and ROLLBACK gets IGNORED. RESET is
    // the canonical way out — equivalent to an implicit rollback.
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    begin_tx(&mut sock).await;

    // Fail the tx with a bad query.
    write_message(
        &mut sock,
        &BoltMessage::Run {
            query: "PARSE FAIL".into(),
            params: BoltValue::Map(vec![]),
            extra: BoltValue::Map(vec![]),
        }
        .encode(),
    )
    .await
    .unwrap();
    let _ = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();

    // ROLLBACK after failure is IGNORED.
    let reply = rollback_tx(&mut sock).await;
    assert!(matches!(reply, BoltMessage::Ignored));

    // RESET to recover and confirm we're back in Ready by issuing a
    // simple read.
    write_message(&mut sock, &BoltMessage::Reset.encode())
        .await
        .unwrap();
    let reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    assert!(matches!(reply, BoltMessage::Success { .. }));

    let (records, _) = run_and_pull(&mut sock, "MATCH (n) RETURN n").await;
    assert!(records.is_empty());

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_run_with_string_param_filters_match() {
    // End-to-end: drive a parameterized RUN through the Bolt handler,
    // exercising bolt_params_to_param_map → execute_cypher_local →
    // executor → row encoding back through PackStream.
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    let _ = run_and_pull(&mut sock, "CREATE (n:Person {name: 'Ada', age: 37})").await;
    let _ = run_and_pull(&mut sock, "CREATE (n:Person {name: 'Alan', age: 41})").await;

    let params = BoltValue::map([("name", BoltValue::String("Ada".into()))]);
    let (records, _) = run_and_pull_with_params(
        &mut sock,
        "MATCH (n:Person {name: $name}) RETURN n.age AS age",
        params,
    )
    .await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0][0].as_int(), Some(37));

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_unwind_with_param_list_returns_one_record_per_element() {
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    let params = BoltValue::map([(
        "items",
        BoltValue::List(vec![
            BoltValue::Int(10),
            BoltValue::Int(20),
            BoltValue::Int(30),
        ]),
    )]);
    let (records, meta) =
        run_and_pull_with_params(&mut sock, "UNWIND $items AS x RETURN x ORDER BY x", params).await;
    let xs: Vec<i64> = records
        .iter()
        .map(|fields| fields[0].as_int().unwrap())
        .collect();
    assert_eq!(xs, vec![10, 20, 30]);
    assert_eq!(
        meta.get("record_count").and_then(BoltValue::as_int),
        Some(3)
    );

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_unbound_parameter_surfaces_as_failure() {
    // RUN with an empty params map but a query that references $missing
    // — the executor returns `Error::UnboundParameter` which the Bolt
    // handler maps to a FAILURE. The session enters Failed and a
    // follow-up RESET clears it.
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = connect_and_hello(&addr).await;

    let run = BoltMessage::Run {
        query: "UNWIND $missing AS x RETURN x".into(),
        params: BoltValue::Map(vec![]),
        extra: BoltValue::Map(vec![]),
    };
    write_message(&mut sock, &run.encode()).await.unwrap();
    let reply = BoltMessage::decode(&read_message(&mut sock).await.unwrap()).unwrap();
    match reply {
        BoltMessage::Failure { metadata } => {
            let msg = metadata
                .get("message")
                .and_then(BoltValue::as_str)
                .unwrap_or("");
            assert!(
                msg.contains("unbound parameter") && msg.contains("missing"),
                "expected unbound-parameter failure, got: {msg}"
            );
        }
        other => panic!("expected FAILURE, got {:?}", other),
    }

    goodbye(sock).await;
}

#[tokio::test]
async fn bolt_rejects_bolt_5_only_clients() {
    let (addr, _dir) = spawn_bolt_server().await;
    let mut sock = TcpStream::connect(&addr).await.unwrap();
    let preferences = [version_bytes(5, 0, 0), [0; 4], [0; 4], [0; 4]];
    let err = perform_client_handshake(&mut sock, &preferences)
        .await
        .unwrap_err();
    // Server wrote 00000000 and closed; client surfaces NoCompatibleVersion.
    matches!(err, mesh_bolt::BoltError::NoCompatibleVersion(_));
}
