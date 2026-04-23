//! [`GraphStateMachine`] implementation that applies graph mutations to a
//! local [`meshdb_storage::StorageEngine`].
//!
//! Plugged into [`meshdb_cluster::raft::RaftCluster::new_with_applier`] so
//! every Raft replica's local store ends up with the same graph data after
//! a `MeshLogEntry::Graph` entry commits.
//!
//! ## Snapshot format
//!
//! Historically, snapshots serialized `Vec<Node>` + `Vec<Edge>` as one JSON
//! document — simple but scaling-hostile, because the serializer materializes
//! the entire graph in memory before any bytes hit the wire. The current
//! implementation instead:
//!
//! 1. Asks rocksdb for a point-in-time [`Checkpoint`] into a temp dir. This
//!    is effectively free on the same filesystem (hard links over SST files)
//!    and captures a consistent view without blocking writers for long.
//! 2. Walks the checkpoint directory and packs every file into a simple
//!    length-prefixed archive (see [`pack_directory`] / [`unpack_directory`]).
//!    The archive is the rocksdb native on-disk format, so restore on the
//!    other end opens it as a read-only `Store` and streams nodes/edges
//!    through `apply_batch` without ever holding a `Vec<Node>`-of-the-world.
//!
//! The archive format is intentionally minimal — a four-byte magic, a u32
//! file count, then `{ u16 name_len, name bytes, u64 content_len, content
//! bytes }` per file. No tar dep, no compression, just enough to round-trip
//! the checkpoint directory structure that rocksdb needs to re-open it.
//!
//! [`Checkpoint`]: meshdb_storage::StorageEngine::create_checkpoint

use meshdb_cluster::raft::GraphStateMachine;
use meshdb_cluster::{
    ConstraintKind as ClusterConstraintKind, ConstraintScope as ClusterConstraintScope,
    GraphCommand, PropertyType as ClusterPropertyType,
};
use meshdb_storage::{
    ConstraintScope as StorageConstraintScope, GraphMutation, PropertyConstraintKind,
    PropertyType as StoragePropertyType, RocksDbStorageEngine, StorageEngine,
};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

/// Magic bytes at the start of every meshdb-rpc snapshot archive. Different
/// from the on-disk Raft snapshot magic (there isn't one yet) so a stray
/// blob from another source fails the format check before we try to open
/// it as a rocksdb directory.
const ARCHIVE_MAGIC: &[u8; 4] = b"MSNP";
const ARCHIVE_VERSION: u32 = 1;

pub struct StoreGraphApplier {
    store: Arc<dyn StorageEngine>,
    /// Optional handle to the local in-memory trigger registry.
    /// Each peer's applier holds its own copy; install/drop
    /// commands replayed via Raft refresh the cache so the
    /// firing path on the leader sees the same set the
    /// followers' storage has.
    #[cfg(feature = "apoc-trigger")]
    trigger_registry: Option<meshdb_executor::apoc_trigger::TriggerRegistry>,
}

impl std::fmt::Debug for StoreGraphApplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreGraphApplier").finish_non_exhaustive()
    }
}

impl StoreGraphApplier {
    pub fn new(store: Arc<dyn StorageEngine>) -> Self {
        Self {
            store,
            #[cfg(feature = "apoc-trigger")]
            trigger_registry: None,
        }
    }

    /// Attach the local trigger registry so trigger DDL Raft
    /// log entries refresh the in-memory cache after applying.
    /// Called from `meshdb-server` startup with the same
    /// registry instance that's wired into `MeshService`'s
    /// procedure registry factory.
    #[cfg(feature = "apoc-trigger")]
    pub fn with_trigger_registry(
        mut self,
        registry: meshdb_executor::apoc_trigger::TriggerRegistry,
    ) -> Self {
        self.trigger_registry = Some(registry);
        self
    }

    /// Refresh the local trigger registry after a trigger DDL
    /// command has been applied to storage. Failures are logged
    /// but never propagated — a stale cache will self-correct on
    /// the next firing path that reads it (we re-load on every
    /// fire to keep the surface simple). When the apoc-trigger
    /// feature isn't compiled in, this is a no-op so the apply
    /// arms can call it unconditionally.
    fn notify_trigger_change(&self) {
        #[cfg(feature = "apoc-trigger")]
        if let Some(reg) = &self.trigger_registry {
            if let Err(e) = reg.refresh() {
                tracing::warn!(error = %e, "refreshing trigger registry after Raft apply failed");
            }
        }
    }
}

impl GraphStateMachine for StoreGraphApplier {
    fn apply(&self, command: &GraphCommand) -> Result<(), String> {
        match command {
            GraphCommand::PutNode(node) => self.store.put_node(node).map_err(|e| e.to_string()),
            GraphCommand::PutEdge(edge) => self.store.put_edge(edge).map_err(|e| e.to_string()),
            GraphCommand::DeleteEdge(id) => {
                // Idempotent: a redundant Raft replay shouldn't error.
                if self
                    .store
                    .get_edge(*id)
                    .map_err(|e| e.to_string())?
                    .is_some()
                {
                    self.store.delete_edge(*id).map_err(|e| e.to_string())?;
                }
                Ok(())
            }
            GraphCommand::DetachDeleteNode(id) => {
                // detach_delete_node is already idempotent for missing nodes.
                self.store
                    .detach_delete_node(*id)
                    .map_err(|e| e.to_string())
            }
            GraphCommand::Batch(cmds) => {
                // Translate to GraphMutation and apply atomically through
                // a single rocksdb WriteBatch — either every mutation in
                // the Cypher query lands or none does, even across a
                // process crash. Nested Batch variants are flattened
                // because `Store::apply_batch` only knows the leaf ops.
                //
                // DDL (CreateIndex / DropIndex) can't be expressed as a
                // GraphMutation because the backfill step needs to read
                // the live CF_NODES, which an uncommitted WriteBatch
                // isn't queryable against. So we apply any DDL
                // commands in the batch first (each in its own small
                // batch), then the leaf graph ops in one atomic
                // WriteBatch. Cypher never mixes DDL with graph
                // writes in a single query, so this ordering is only
                // exercised on path that don't actually interleave.
                let mut flat: Vec<GraphMutation> = Vec::with_capacity(cmds.len());
                apply_ddl_and_collect(cmds, self.store.as_ref(), &mut flat)?;
                if !flat.is_empty() {
                    self.store.apply_batch(&flat).map_err(|e| e.to_string())?;
                }
                // Cheap defensive refresh — covers Batch entries
                // that contain trigger DDL (rare in practice;
                // CALL apoc.trigger.* doesn't co-mingle with
                // graph writes in a single Cypher query, but a
                // future caller might).
                if batch_contains_trigger_ddl(cmds) {
                    self.notify_trigger_change();
                }
                Ok(())
            }
            GraphCommand::CreateIndex { label, properties } => self
                .store
                .create_property_index_composite(label, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::DropIndex { label, properties } => self
                .store
                .drop_property_index_composite(label, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::CreateEdgeIndex {
                edge_type,
                properties,
            } => self
                .store
                .create_edge_property_index_composite(edge_type, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::DropEdgeIndex {
                edge_type,
                properties,
            } => self
                .store
                .drop_edge_property_index_composite(edge_type, properties)
                .map_err(|e| e.to_string()),
            GraphCommand::CreatePointIndex { label, property } => self
                .store
                .create_point_index(label, property)
                .map_err(|e| e.to_string()),
            GraphCommand::DropPointIndex { label, property } => self
                .store
                .drop_point_index(label, property)
                .map_err(|e| e.to_string()),
            GraphCommand::CreateEdgePointIndex {
                edge_type,
                property,
            } => self
                .store
                .create_edge_point_index(edge_type, property)
                .map_err(|e| e.to_string()),
            GraphCommand::DropEdgePointIndex {
                edge_type,
                property,
            } => self
                .store
                .drop_edge_point_index(edge_type, property)
                .map_err(|e| e.to_string()),
            GraphCommand::CreateConstraint {
                name,
                scope,
                properties,
                kind,
                if_not_exists,
            } => self
                .store
                .create_property_constraint(
                    name.as_deref(),
                    &storage_scope(scope),
                    properties,
                    storage_kind(*kind),
                    *if_not_exists,
                )
                .map(|_| ())
                .map_err(|e| e.to_string()),
            GraphCommand::DropConstraint { name, if_exists } => self
                .store
                .drop_property_constraint(name, *if_exists)
                .map_err(|e| e.to_string()),
            GraphCommand::InstallTrigger { name, spec_blob } => {
                self.store
                    .put_trigger(name, spec_blob)
                    .map_err(|e| e.to_string())?;
                self.notify_trigger_change();
                Ok(())
            }
            GraphCommand::DropTrigger { name } => {
                self.store.delete_trigger(name).map_err(|e| e.to_string())?;
                self.notify_trigger_change();
                Ok(())
            }
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        // Point-in-time rocksdb checkpoint into a fresh temp dir.
        // `create_checkpoint` requires that the target NOT already
        // exist, so we use a parent TempDir and put the checkpoint in
        // a named subdirectory underneath it.
        let parent = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
        let cp_dir = parent.path().join("cp");
        self.store
            .create_checkpoint(&cp_dir)
            .map_err(|e| format!("rocksdb checkpoint: {e}"))?;
        pack_directory(&cp_dir).map_err(|e| format!("packing checkpoint: {e}"))
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        // Empty blob means the snapshot didn't include graph data
        // (e.g., a cluster-state-only test). Leave the local store
        // alone.
        if snapshot.is_empty() {
            return Ok(());
        }

        // Unpack the archive into a fresh temp dir, open it as a
        // read-only Store, and stream every node + edge into the
        // live store via apply_batch. The live store is wiped first
        // so a crash mid-restore still recovers from the leader's
        // snapshot on the next reconnect (the new Raft snapshot
        // install will replay).
        let parent = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
        let unpack_dir = parent.path().join("cp");
        std::fs::create_dir_all(&unpack_dir).map_err(|e| format!("creating unpack dir: {e}"))?;
        unpack_directory(snapshot, &unpack_dir).map_err(|e| format!("unpacking snapshot: {e}"))?;

        // Backend-bound by choice: the shipped snapshot archive is a
        // raw RocksDB checkpoint (SST files), so the unpacker opens it
        // as a concrete RocksDB engine regardless of what the live
        // engine is. When a second backend lands, the snapshot format
        // becomes backend-specific and this site needs branching.
        let shipped = RocksDbStorageEngine::open(&unpack_dir)
            .map_err(|e| format!("opening shipped store: {e}"))?;

        self.store.clear_all().map_err(|e| e.to_string())?;

        // Drain the shipped store into mutations and apply in one
        // WriteBatch. Using the chunked path keeps memory bounded
        // even when the shipped graph is large — we only hold
        // `CHUNK` items at a time rather than the whole dataset.
        const CHUNK: usize = 4096;
        let mut buf: Vec<GraphMutation> = Vec::with_capacity(CHUNK);

        for node in shipped
            .all_nodes()
            .map_err(|e| format!("reading shipped nodes: {e}"))?
        {
            buf.push(GraphMutation::PutNode(node));
            if buf.len() >= CHUNK {
                self.store
                    .apply_batch(&buf)
                    .map_err(|e| format!("applying node batch: {e}"))?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            self.store
                .apply_batch(&buf)
                .map_err(|e| format!("applying node tail: {e}"))?;
            buf.clear();
        }

        for edge in shipped
            .all_edges()
            .map_err(|e| format!("reading shipped edges: {e}"))?
        {
            buf.push(GraphMutation::PutEdge(edge));
            if buf.len() >= CHUNK {
                self.store
                    .apply_batch(&buf)
                    .map_err(|e| format!("applying edge batch: {e}"))?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            self.store
                .apply_batch(&buf)
                .map_err(|e| format!("applying edge tail: {e}"))?;
        }

        Ok(())
    }
}

/// Pack every regular file under `dir` (recursively) into a simple
/// length-prefixed archive. File names are stored as UTF-8 paths
/// relative to `dir`, with `/` as the separator regardless of
/// platform, so the resulting archive is portable across the nodes
/// of a heterogeneous cluster.
fn pack_directory(dir: &Path) -> std::io::Result<Vec<u8>> {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    collect_files(dir, dir, &mut files)?;

    // Sort by path so the serialized form is deterministic — makes
    // test assertions on snapshot bytes easier and stable across runs.
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = Vec::with_capacity(estimated_archive_size(&files));
    out.extend_from_slice(ARCHIVE_MAGIC);
    out.extend_from_slice(&ARCHIVE_VERSION.to_le_bytes());
    out.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for (name, content) in files {
        let name_bytes = name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("file name too long for archive: {}", name.len()),
            ));
        }
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(&(content.len() as u64).to_le_bytes());
        out.extend_from_slice(&content);
    }
    Ok(out)
}

/// Recursively walk `dir`, appending every regular file as a
/// `(relative_path, content)` pair into `out`.
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let path = entry.path();
        if ty.is_dir() {
            collect_files(root, &path, out)?;
        } else if ty.is_file() {
            let rel = path
                .strip_prefix(root)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let name = rel
                .to_str()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "non-UTF-8 path in checkpoint",
                    )
                })?
                .replace(std::path::MAIN_SEPARATOR, "/");
            let mut content = Vec::new();
            std::fs::File::open(&path)?.read_to_end(&mut content)?;
            out.push((name, content));
        }
        // Skip symlinks and other special entries — rocksdb checkpoints
        // are regular files and directories only.
    }
    Ok(())
}

fn estimated_archive_size(files: &[(String, Vec<u8>)]) -> usize {
    // Header (magic + version + count) + per-file overhead + content.
    let overhead_per_file = 2 + 8; // name_len + content_len
    4 + 4
        + 4
        + files
            .iter()
            .map(|(n, c)| overhead_per_file + n.len() + c.len())
            .sum::<usize>()
}

/// Unpack an archive produced by [`pack_directory`] into the given
/// directory. The directory must exist and will be overwritten for any
/// file paths the archive carries.
fn unpack_directory(archive: &[u8], dir: &Path) -> std::io::Result<()> {
    let mut cursor = ArchiveReader::new(archive)?;
    let count = cursor.read_u32()?;
    for _ in 0..count {
        let name_len = cursor.read_u16()? as usize;
        let name_bytes = cursor.read_slice(name_len)?;
        let name = std::str::from_utf8(name_bytes).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid UTF-8 file name")
        })?;
        // Sanitize: reject any path that tries to escape `dir` via
        // parent components or absolute prefixes. RocksDB's own
        // checkpoint files don't use either, so the rejection is
        // safely conservative.
        for component in std::path::Path::new(name).components() {
            use std::path::Component;
            match component {
                Component::Normal(_) => {}
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unsafe path in archive: {name}"),
                    ));
                }
            }
        }
        let content_len = cursor.read_u64()? as usize;
        let content = cursor.read_slice(content_len)?;
        let dest = dir.join(name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(&dest)?;
        file.write_all(content)?;
    }
    Ok(())
}

/// Bounds-checked sequential reader over an archive's bytes. Keeps the
/// unpack logic readable without reaching for a byte-level parser crate.
struct ArchiveReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ArchiveReader<'a> {
    fn new(buf: &'a [u8]) -> std::io::Result<Self> {
        if buf.len() < 4 + 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "archive truncated at header",
            ));
        }
        if &buf[0..4] != ARCHIVE_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "archive magic mismatch",
            ));
        }
        let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        if version != ARCHIVE_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported archive version {version}"),
            ));
        }
        Ok(Self { buf, pos: 8 })
    }

    fn read_slice(&mut self, len: usize) -> std::io::Result<&'a [u8]> {
        if self.pos + len > self.buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "archive truncated",
            ));
        }
        let s = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(s)
    }

    fn read_u16(&mut self) -> std::io::Result<u16> {
        let bytes = self.read_slice(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> std::io::Result<u32> {
        let bytes = self.read_slice(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> std::io::Result<u64> {
        let bytes = self.read_slice(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

/// Walk `cmds` and apply any `CreateIndex`/`DropIndex` entries
/// immediately to `store` (each one gets its own small rocksdb
/// WriteBatch inside the `Store` methods). Non-DDL leaves are
/// collected into `out` so the caller can commit them as one
/// atomic `apply_batch`. Nested `Batch` variants are recursed
/// into, mirroring `flatten_into`.
///
/// Splitting DDL out of the main batch is necessary because the
/// backfill step reads the live `CF_NODES`, and an uncommitted
/// `WriteBatch` isn't queryable — a DDL entry interleaved with
/// `PutNode` entries in the same batch would produce an index
/// that misses the new nodes.
fn apply_ddl_and_collect(
    cmds: &[GraphCommand],
    store: &dyn StorageEngine,
    out: &mut Vec<GraphMutation>,
) -> Result<(), String> {
    for cmd in cmds {
        match cmd {
            GraphCommand::PutNode(n) => out.push(GraphMutation::PutNode(n.clone())),
            GraphCommand::PutEdge(e) => out.push(GraphMutation::PutEdge(e.clone())),
            GraphCommand::DeleteEdge(id) => out.push(GraphMutation::DeleteEdge(*id)),
            GraphCommand::DetachDeleteNode(id) => out.push(GraphMutation::DetachDeleteNode(*id)),
            GraphCommand::Batch(inner) => apply_ddl_and_collect(inner, store, out)?,
            GraphCommand::CreateIndex { label, properties } => store
                .create_property_index_composite(label, properties)
                .map_err(|e| e.to_string())?,
            GraphCommand::DropIndex { label, properties } => store
                .drop_property_index_composite(label, properties)
                .map_err(|e| e.to_string())?,
            GraphCommand::CreateEdgeIndex {
                edge_type,
                properties,
            } => store
                .create_edge_property_index_composite(edge_type, properties)
                .map_err(|e| e.to_string())?,
            GraphCommand::DropEdgeIndex {
                edge_type,
                properties,
            } => store
                .drop_edge_property_index_composite(edge_type, properties)
                .map_err(|e| e.to_string())?,
            GraphCommand::CreatePointIndex { label, property } => store
                .create_point_index(label, property)
                .map_err(|e| e.to_string())?,
            GraphCommand::DropPointIndex { label, property } => store
                .drop_point_index(label, property)
                .map_err(|e| e.to_string())?,
            GraphCommand::CreateEdgePointIndex {
                edge_type,
                property,
            } => store
                .create_edge_point_index(edge_type, property)
                .map_err(|e| e.to_string())?,
            GraphCommand::DropEdgePointIndex {
                edge_type,
                property,
            } => store
                .drop_edge_point_index(edge_type, property)
                .map_err(|e| e.to_string())?,
            GraphCommand::CreateConstraint {
                name,
                scope,
                properties,
                kind,
                if_not_exists,
            } => {
                store
                    .create_property_constraint(
                        name.as_deref(),
                        &storage_scope(scope),
                        properties,
                        storage_kind(*kind),
                        *if_not_exists,
                    )
                    .map_err(|e| e.to_string())?;
            }
            GraphCommand::DropConstraint { name, if_exists } => store
                .drop_property_constraint(name, *if_exists)
                .map_err(|e| e.to_string())?,
            GraphCommand::InstallTrigger { name, spec_blob } => store
                .put_trigger(name, spec_blob)
                .map_err(|e| e.to_string())?,
            GraphCommand::DropTrigger { name } => {
                store.delete_trigger(name).map_err(|e| e.to_string())?
            }
        }
    }
    Ok(())
}

/// `true` if any command in the recursive batch tree mutates
/// the trigger registry. Used by the Raft applier's Batch arm
/// to decide whether to re-read the trigger meta CF after
/// applying — keeps the refresh cost off batches that don't
/// touch triggers (the common case).
fn batch_contains_trigger_ddl(cmds: &[GraphCommand]) -> bool {
    cmds.iter().any(|c| match c {
        GraphCommand::InstallTrigger { .. } | GraphCommand::DropTrigger { .. } => true,
        GraphCommand::Batch(inner) => batch_contains_trigger_ddl(inner),
        _ => false,
    })
}

/// Bridge the cluster-crate [`ClusterConstraintKind`] into the
/// storage-crate [`PropertyConstraintKind`]. Lives here because the
/// cluster crate intentionally doesn't depend on `meshdb-storage` —
/// the Raft log entry stays storage-agnostic, and every applier that
/// terminates against a concrete store does the mapping locally.
pub(crate) fn storage_kind(kind: ClusterConstraintKind) -> PropertyConstraintKind {
    match kind {
        ClusterConstraintKind::Unique => PropertyConstraintKind::Unique,
        ClusterConstraintKind::NotNull => PropertyConstraintKind::NotNull,
        ClusterConstraintKind::PropertyType(t) => {
            PropertyConstraintKind::PropertyType(storage_property_type(t))
        }
        ClusterConstraintKind::NodeKey => PropertyConstraintKind::NodeKey,
    }
}

/// Bridge the cluster-crate `ConstraintScope` into the storage-crate
/// enum. Same shape on both sides; the separation preserves the
/// dependency direction (cluster doesn't know about storage).
pub(crate) fn storage_scope(scope: &ClusterConstraintScope) -> StorageConstraintScope {
    match scope {
        ClusterConstraintScope::Node(l) => StorageConstraintScope::Node(l.clone()),
        ClusterConstraintScope::Relationship(t) => StorageConstraintScope::Relationship(t.clone()),
    }
}

/// Bridge for the inner `PropertyType`. Symmetric to `storage_kind`
/// above — every `PropertyType` variant maps 1:1 to its storage
/// counterpart.
pub(crate) fn storage_property_type(t: ClusterPropertyType) -> StoragePropertyType {
    match t {
        ClusterPropertyType::String => StoragePropertyType::String,
        ClusterPropertyType::Integer => StoragePropertyType::Integer,
        ClusterPropertyType::Float => StoragePropertyType::Float,
        ClusterPropertyType::Boolean => StoragePropertyType::Boolean,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshdb_core::{Edge, Node};

    #[test]
    fn archive_round_trip_preserves_directory_tree() {
        // Pack a directory containing a nested subdirectory, unpack
        // into a fresh temp dir, and verify every file comes back
        // with byte-identical contents at the expected relative path.
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
        std::fs::write(src.path().join("b.bin"), b"\x00\x01\x02\xff").unwrap();
        std::fs::create_dir_all(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("sub/nested.dat"), b"more").unwrap();

        let archive = pack_directory(src.path()).unwrap();
        assert_eq!(&archive[0..4], ARCHIVE_MAGIC);

        let dest = tempfile::tempdir().unwrap();
        unpack_directory(&archive, dest.path()).unwrap();

        assert_eq!(std::fs::read(dest.path().join("a.txt")).unwrap(), b"hello");
        assert_eq!(
            std::fs::read(dest.path().join("b.bin")).unwrap(),
            b"\x00\x01\x02\xff"
        );
        assert_eq!(
            std::fs::read(dest.path().join("sub/nested.dat")).unwrap(),
            b"more"
        );
    }

    #[test]
    fn archive_rejects_bad_magic() {
        let dest = tempfile::tempdir().unwrap();
        let bogus = [0u8; 16];
        let err = unpack_directory(&bogus, dest.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn archive_rejects_path_traversal() {
        // Hand-craft an archive whose file name escapes the target
        // directory. The sanitizer should reject it instead of writing
        // `/tmp/a` or similar.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(ARCHIVE_MAGIC);
        bytes.extend_from_slice(&ARCHIVE_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        let evil = "../evil".as_bytes();
        bytes.extend_from_slice(&(evil.len() as u16).to_le_bytes());
        bytes.extend_from_slice(evil);
        bytes.extend_from_slice(&0u64.to_le_bytes());

        let dest = tempfile::tempdir().unwrap();
        let err = unpack_directory(&bytes, dest.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn snapshot_restore_round_trips_nodes_and_edges() {
        // Full round-trip through the state machine: populate a
        // source store, snapshot it, restore into a fresh store,
        // assert every node and edge made it across.
        let src_dir = tempfile::tempdir().unwrap();
        let src: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(src_dir.path()).unwrap());

        // Put ~500 nodes and a smattering of edges between them. Enough
        // to exercise the chunked apply_batch path (CHUNK = 4096) once
        // at least, and to surface any off-by-one in name-length or
        // content-length framing.
        let mut node_ids = Vec::new();
        for i in 0..500 {
            let n = Node::new().with_label("Scale").with_property("i", i as i64);
            node_ids.push(n.id);
            src.put_node(&n).unwrap();
        }
        for window in node_ids.windows(2).take(250) {
            let e = Edge::new("NEXT", window[0], window[1]);
            src.put_edge(&e).unwrap();
        }

        let src_applier = StoreGraphApplier::new(src.clone());
        let snapshot = src_applier.snapshot().expect("snapshotting source");
        assert!(
            !snapshot.is_empty(),
            "snapshot should be non-empty for a populated store"
        );

        // Restore into a brand-new store and verify contents.
        let dest_dir = tempfile::tempdir().unwrap();
        let dest: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(dest_dir.path()).unwrap());
        let dest_applier = StoreGraphApplier::new(dest.clone());
        dest_applier
            .restore(&snapshot)
            .expect("restoring into fresh store");

        let restored_nodes = dest.all_nodes().unwrap();
        assert_eq!(restored_nodes.len(), 500);
        let restored_edges = dest.all_edges().unwrap();
        assert_eq!(restored_edges.len(), 250);

        // Spot-check one node's property survives the round-trip.
        let any_id = node_ids[42];
        let got = dest.get_node(any_id).unwrap().unwrap();
        assert_eq!(got.labels, vec!["Scale"]);
        assert_eq!(
            got.properties.get("i"),
            Some(&meshdb_core::Property::Int64(42))
        );
    }

    #[test]
    fn empty_snapshot_restore_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(dir.path()).unwrap());
        let applier = StoreGraphApplier::new(store.clone());
        // Empty blob should not error and must leave the store alone.
        applier.restore(&[]).unwrap();
        assert!(store.all_nodes().unwrap().is_empty());
    }

    #[test]
    fn snapshot_of_empty_store_round_trips_to_empty_store() {
        let src_dir = tempfile::tempdir().unwrap();
        let src: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(src_dir.path()).unwrap());
        let src_applier = StoreGraphApplier::new(src);

        let snapshot = src_applier.snapshot().unwrap();
        // An empty store still produces a real archive (the checkpoint
        // contains the rocksdb MANIFEST and friends), not a zero-byte
        // blob. The restore path must handle that correctly.
        assert!(!snapshot.is_empty());

        let dest_dir = tempfile::tempdir().unwrap();
        let dest: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(dest_dir.path()).unwrap());
        let dest_applier = StoreGraphApplier::new(dest.clone());
        dest_applier.restore(&snapshot).unwrap();

        assert!(dest.all_nodes().unwrap().is_empty());
        assert!(dest.all_edges().unwrap().is_empty());
    }

    #[test]
    fn snapshot_replace_on_non_empty_destination() {
        // Restore should replace the destination's existing data with
        // the snapshot's — old nodes that aren't in the snapshot must
        // NOT survive, otherwise a follower catching up via snapshot
        // could end up with stale state from its previous term.
        let src_dir = tempfile::tempdir().unwrap();
        let src: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(src_dir.path()).unwrap());
        let snapshot_node = Node::new().with_label("FromSnapshot");
        let snapshot_node_id = snapshot_node.id;
        src.put_node(&snapshot_node).unwrap();
        let src_applier = StoreGraphApplier::new(src);
        let snapshot = src_applier.snapshot().unwrap();

        let dest_dir = tempfile::tempdir().unwrap();
        let dest: Arc<dyn StorageEngine> =
            Arc::new(RocksDbStorageEngine::open(dest_dir.path()).unwrap());
        let stale = Node::new().with_label("Stale");
        let stale_id = stale.id;
        dest.put_node(&stale).unwrap();

        let dest_applier = StoreGraphApplier::new(dest.clone());
        dest_applier.restore(&snapshot).unwrap();

        // Snapshot node present, stale node gone.
        assert!(dest.get_node(snapshot_node_id).unwrap().is_some());
        assert!(dest.get_node(stale_id).unwrap().is_none());
    }
}
