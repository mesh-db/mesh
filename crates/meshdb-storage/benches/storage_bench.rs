//! Storage-layer micro-benchmarks against a seeded RocksDB store.
//!
//! Six benches covering the hot paths a real workload hammers:
//! point lookup, single insert, adjacency scan, label index
//! iteration, indexed property seek, and bulk insert. Read benches
//! share one 10k-node seeded store for the whole run; write benches
//! use fresh ids per iteration so the store doesn't grow into a
//! pathological size across samples.
//!
//! Run with:
//!     cargo bench -p meshdb-storage --bench storage_bench
//!     cargo bench -p meshdb-storage --bench storage_bench -- --quick
//!
//! Reports land in `target/criterion/<bench_name>/report/index.html`.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use meshdb_core::{Edge, Node, NodeId, Property};
use meshdb_storage::RocksDbStorageEngine;
use tempfile::TempDir;

const SEED_NODES: usize = 10_000;
const SEED_EDGE_SOURCES: usize = 1_000;
const SEED_EDGES_PER_SOURCE: usize = 10;

/// Seed a fresh store with `SEED_NODES` `:Person` nodes (each with
/// `name` + `age`), a `(:Person, name)` property index, and
/// `SEED_EDGE_SOURCES * SEED_EDGES_PER_SOURCE` `KNOWS` edges sourced
/// from the first `SEED_EDGE_SOURCES` node ids.
///
/// Returns the `TempDir` alongside the store so the caller can keep
/// both alive for the duration of the benchmark — dropping the
/// TempDir while the store is still open would wipe the data dir
/// mid-bench.
fn seed_store() -> (TempDir, RocksDbStorageEngine, Vec<NodeId>) {
    let dir = TempDir::new().expect("mk tempdir");
    let store = RocksDbStorageEngine::open(dir.path()).expect("open store");

    // Property index up front so the `nodes_by_property` bench has
    // an index-backed path to measure. Created before inserts so
    // every node flows through the index-maintenance hot path too,
    // matching real-workload shape (index defined at schema time).
    store
        .create_property_index("Person", "name")
        .expect("create name index");

    let mut ids = Vec::with_capacity(SEED_NODES);
    for i in 0..SEED_NODES {
        let n = Node::new()
            .with_label("Person")
            .with_property("name", format!("user-{i}"))
            .with_property("age", (i % 100) as i64);
        ids.push(n.id);
        store.put_node(&n).expect("put seed node");
    }

    for src_idx in 0..SEED_EDGE_SOURCES {
        for dst_offset in 1..=SEED_EDGES_PER_SOURCE {
            let src = ids[src_idx];
            let dst = ids[(src_idx + dst_offset) % SEED_NODES];
            let e = Edge::new("KNOWS", src, dst);
            store.put_edge(&e).expect("put seed edge");
        }
    }

    (dir, store, ids)
}

fn bench_get_node_by_id(c: &mut Criterion) {
    let (_dir, store, ids) = seed_store();
    // Rotate through ids so the block cache doesn't trivially
    // short-circuit every lookup.
    let mut cursor: usize = 0;
    c.bench_function("get_node_by_id", |b| {
        b.iter(|| {
            let id = ids[cursor % ids.len()];
            cursor = cursor.wrapping_add(1);
            let node = store.get_node(id).expect("get_node");
            assert!(node.is_some());
            node
        })
    });
}

fn bench_put_node_single(c: &mut Criterion) {
    let (_dir, store, _ids) = seed_store();
    // Fresh node per iteration — `Node::new()` mints a new
    // `NodeId` internally, so successive inserts never collide.
    c.bench_function("put_node_single", |b| {
        b.iter_batched(
            || {
                Node::new()
                    .with_label("Benchmark")
                    .with_property("flag", true)
            },
            |n| store.put_node(&n).expect("put_node"),
            BatchSize::SmallInput,
        )
    });
}

fn bench_outgoing_neighbors(c: &mut Criterion) {
    let (_dir, store, ids) = seed_store();
    let mut cursor: usize = 0;
    c.bench_function("outgoing_neighbors", |b| {
        b.iter(|| {
            // Rotate across the SEED_EDGE_SOURCES sources so every
            // iteration hits a node with ~SEED_EDGES_PER_SOURCE
            // outgoing edges.
            let src = ids[cursor % SEED_EDGE_SOURCES];
            cursor = cursor.wrapping_add(1);
            store.outgoing(src).expect("outgoing")
        })
    });
}

fn bench_nodes_by_label_full_scan(c: &mut Criterion) {
    let (_dir, store, _ids) = seed_store();
    c.bench_function("nodes_by_label_full_scan", |b| {
        b.iter(|| store.nodes_by_label("Person").expect("label scan"))
    });
}

fn bench_nodes_by_property_indexed(c: &mut Criterion) {
    let (_dir, store, _ids) = seed_store();
    let mut cursor: usize = 0;
    c.bench_function("nodes_by_property_indexed", |b| {
        b.iter(|| {
            let name = format!("user-{}", cursor % SEED_NODES);
            cursor = cursor.wrapping_add(1);
            let hits = store
                .nodes_by_property("Person", "name", &Property::String(name))
                .expect("indexed lookup");
            assert_eq!(hits.len(), 1);
            hits
        })
    });
}

fn bench_put_node_batch_1000(c: &mut Criterion) {
    let (_dir, store, _ids) = seed_store();
    // Fewer samples — each iteration inserts 1_000 nodes.
    // Criterion's default (100 samples) would add ~100k extra
    // nodes to the store and slow subsequent iterations.
    let mut group = c.benchmark_group("put_node_batch_1000");
    group.sample_size(10);
    group.bench_function("put_node_batch_1000", |b| {
        b.iter_batched(
            || {
                (0..1_000)
                    .map(|i| {
                        Node::new()
                            .with_label("BenchBatch")
                            .with_property("idx", i as i64)
                    })
                    .collect::<Vec<_>>()
            },
            |batch| {
                for n in &batch {
                    store.put_node(n).expect("put_node");
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_get_node_by_id,
    bench_put_node_single,
    bench_outgoing_neighbors,
    bench_nodes_by_label_full_scan,
    bench_nodes_by_property_indexed,
    bench_put_node_batch_1000,
);
criterion_main!(benches);
