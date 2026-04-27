//! End-to-end Cypher benchmarks through `MeshService::execute_cypher_local`.
//!
//! Each bench seeds a single-node service (no routing, no Raft)
//! with 10k `:Person` nodes + 10k `KNOWS` edges + a
//! `(:Person, name)` property index, then runs a representative
//! Cypher query inside a `tokio::runtime::Runtime::block_on` call.
//! Purposely excludes cluster-setup noise: distributed-mode perf
//! needs a different methodology and goes in a separate suite.
//!
//! Run with:
//!     cargo bench -p meshdb-rpc --bench query_bench
//!     cargo bench -p meshdb-rpc --bench query_bench -- --quick
//!
//! Reports land in `target/criterion/<bench_name>/report/index.html`.

// Same recursion_limit bump as the lib — `MeshService::execute_cypher_local`
// nested async exceeds rustc's default 128.
#![recursion_limit = "256"]

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use meshdb_executor::{ParamMap, Value};
use meshdb_rpc::MeshService;
use meshdb_storage::{RocksDbStorageEngine, StorageEngine};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const SEED_NODES: usize = 10_000;
const SEED_EDGE_SOURCES: usize = 1_000;
const SEED_EDGES_PER_SOURCE: usize = 10;

/// Build a fresh RocksDB-backed `MeshService` and seed it by
/// issuing the same UNWIND CREATE the query benches would
/// measure. The bench caller keeps the returned `TempDir`
/// alive so the data dir survives the full run.
fn seed_service(rt: &Runtime) -> (TempDir, MeshService) {
    let dir = TempDir::new().expect("mk tempdir");
    let store: Arc<dyn StorageEngine> =
        Arc::new(RocksDbStorageEngine::open(dir.path()).expect("open store"));
    let service = MeshService::new(store.clone());

    // Seed nodes + index + edges via Cypher so the bench setup
    // exercises the same code path a user-facing load would.
    rt.block_on(async {
        service
            .execute_cypher_local(
                "CREATE INDEX FOR (p:Person) ON (p.name)".into(),
                HashMap::new(),
            )
            .await
            .expect("create index");

        // Seed in chunks of 500 to keep any single transaction
        // short. `range(start, end)` is inclusive on both sides
        // in Cypher.
        let chunk = 500usize;
        let mut start: usize = 0;
        while start < SEED_NODES {
            let end = (start + chunk).min(SEED_NODES) - 1;
            let q = format!(
                "UNWIND range({start}, {end}) AS i \
                 CREATE (:Person {{name: 'user-' + toString(i), age: i % 100}})"
            );
            service
                .execute_cypher_local(q, HashMap::new())
                .await
                .expect("seed nodes");
            start += chunk;
        }

        // Edges: sourced from the first SEED_EDGE_SOURCES, each
        // connecting to SEED_EDGES_PER_SOURCE forward neighbours.
        // Issue one statement per source — keeps each query small
        // and well under Cypher's reasonable size budget.
        for src_idx in 0..SEED_EDGE_SOURCES {
            let q = format!(
                "MATCH (a:Person {{name: 'user-{src_idx}'}}) \
                 WITH a \
                 UNWIND range(1, {SEED_EDGES_PER_SOURCE}) AS off \
                 MATCH (b:Person {{name: 'user-' + toString(({src_idx} + off) % {SEED_NODES})}}) \
                 CREATE (a)-[:KNOWS]->(b)"
            );
            service
                .execute_cypher_local(q, HashMap::new())
                .await
                .expect("seed edges");
        }
    });

    (dir, service)
}

fn bench_cypher_create_single_node(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_dir, service) = seed_service(&rt);
    c.bench_function("cypher_create_single_node", |b| {
        b.iter_batched(
            || {
                let mut p = ParamMap::new();
                // Fresh value every iteration so the writes don't
                // collide on any uniqueness constraint users might
                // later add to `:Benchmark`.
                p.insert(
                    "i".into(),
                    Value::Property(meshdb_core::Property::Int64(fastish_random())),
                );
                p
            },
            |params| {
                rt.block_on(
                    service.execute_cypher_local("CREATE (:Benchmark {n: $i})".into(), params),
                )
                .expect("create")
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_cypher_match_by_label(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_dir, service) = seed_service(&rt);
    c.bench_function("cypher_match_by_label", |b| {
        b.iter(|| {
            rt.block_on(
                service.execute_cypher_local(
                    "MATCH (n:Person) RETURN n LIMIT 1".into(),
                    ParamMap::new(),
                ),
            )
            .expect("label match")
        })
    });
}

fn bench_cypher_match_indexed_property(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_dir, service) = seed_service(&rt);
    let mut cursor: usize = 0;
    c.bench_function("cypher_match_indexed_property", |b| {
        b.iter(|| {
            let idx = cursor % SEED_NODES;
            cursor = cursor.wrapping_add(1);
            let mut params = ParamMap::new();
            params.insert(
                "name".into(),
                Value::Property(meshdb_core::Property::String(format!("user-{idx}"))),
            );
            rt.block_on(
                service
                    .execute_cypher_local("MATCH (n:Person {name: $name}) RETURN n".into(), params),
            )
            .expect("indexed match")
        })
    });
}

fn bench_cypher_match_one_hop(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_dir, service) = seed_service(&rt);
    c.bench_function("cypher_match_one_hop", |b| {
        b.iter(|| {
            rt.block_on(service.execute_cypher_local(
                "MATCH (a:Person)-[:KNOWS]->(b) RETURN b LIMIT 10".into(),
                ParamMap::new(),
            ))
            .expect("one-hop")
        })
    });
}

fn bench_cypher_count_by_label(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_dir, service) = seed_service(&rt);
    c.bench_function("cypher_count_by_label", |b| {
        b.iter(|| {
            rt.block_on(service.execute_cypher_local(
                "MATCH (n:Person) RETURN count(n) AS c".into(),
                ParamMap::new(),
            ))
            .expect("count")
        })
    });
}

/// Cheap non-crypto PRNG good enough to keep the CREATE bench's
/// `$i` param unique across iterations. Pulling in `rand` for
/// one call would be overkill.
fn fastish_random() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    nanos ^ (nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15u64 as i64))
}

criterion_group!(
    benches,
    bench_cypher_create_single_node,
    bench_cypher_match_by_label,
    bench_cypher_match_indexed_property,
    bench_cypher_match_one_hop,
    bench_cypher_count_by_label,
);
criterion_main!(benches);
