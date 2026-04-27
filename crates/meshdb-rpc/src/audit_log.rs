//! Append-only audit log for admin operations.
//!
//! Compliance regimes (SOC2, HIPAA, etc.) generally require a
//! durable record of every privileged operation: who did it,
//! when, what arguments, what the result was. `tracing` to
//! stdout is fine for debugging but evaporates on log rotation
//! and isn't append-only-fsync-durable.
//!
//! This module gives MeshService + MultiRaftCluster a shared
//! handle they call from each admin code path. One JSONL line
//! per record, fsync'd before the call returns. Mirrors the
//! `CoordinatorLog` design — simple file, no rotation, no
//! compression — because this volume is tiny (one line per
//! `drain`, `take_cluster_backup`, replica change, etc.; not
//! per Cypher RUN).
//!
//! Records cover:
//!
//! * `drain_leadership` — peer asked to step down from every
//!   partition it leads (called from the SIGTERM handler).
//! * `take_cluster_backup` — operator triggered a cluster-wide
//!   backup; manifest entry count + error count recorded.
//! * `add_partition_replica` / `remove_partition_replica` —
//!   per-partition voter membership change.
//! * `drain_peer` — every partition's replica set has the named
//!   peer removed.
//! * `add_partition_learner` / `remove_partition_learner` —
//!   read-replica membership change.
//! * `demote_partition_voter_to_learner` — voter steps down to
//!   a non-voting replica.
//!
//! Each record carries the calling peer's id (the initiator),
//! the wall-clock timestamp, the operation name, structured
//! arguments, and the result (success or the error message).

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One JSONL line in the audit log.
///
/// `args` is `serde_json::Value` so each operation can shape its
/// own argument record without bloating this enum with one
/// variant per call site.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// RFC3339 timestamp at the moment `record` was called.
    pub timestamp: String,
    /// `self_id` of the peer that initiated the operation.
    /// Useful when correlating across a multi-peer audit archive.
    pub peer_id: u64,
    /// Stable string identifier for the operation, e.g.
    /// `"drain_leadership"`, `"take_cluster_backup"`.
    pub operation: String,
    /// Structured arguments — operation-specific. Helpers below
    /// build common shapes.
    pub args: serde_json::Value,
    /// True when the operation succeeded; false when it returned
    /// an error.
    pub success: bool,
    /// On failure, the error message. None on success.
    pub error: Option<String>,
}

/// Append-only audit log on disk. One instance per server
/// process; the open file handle lives inside a `Mutex` so
/// concurrent admin calls serialize their appends without
/// blocking the tokio runtime longer than the fsync takes.
pub struct AuditLog {
    path: PathBuf,
    file: Mutex<File>,
}

impl std::fmt::Debug for AuditLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLog")
            .field("path", &self.path)
            .finish()
    }
}

impl AuditLog {
    /// Open (or create) the log at `path`. Appends only — no
    /// rotation or compression. Caller is responsible for
    /// ensuring the parent directory exists.
    pub fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// Append one entry, fsync, return. Errors propagate to the
    /// caller — the admin operation should still complete (the
    /// audit log is for compliance, not correctness), so callers
    /// typically log the error rather than failing the operation.
    pub fn record(&self, entry: &AuditEntry) -> std::io::Result<()> {
        let mut line = serde_json::to_vec(entry).map_err(std::io::Error::other)?;
        line.push(b'\n');
        let mut f = self.file.lock().expect("audit-log mutex poisoned");
        f.write_all(&line)?;
        f.sync_all()?;
        Ok(())
    }

    /// Build an entry with `success = true` and no error, then
    /// record it.
    pub fn record_success(
        &self,
        peer_id: u64,
        operation: &str,
        args: serde_json::Value,
    ) -> std::io::Result<()> {
        self.record(&AuditEntry {
            timestamp: now_rfc3339(),
            peer_id,
            operation: operation.to_string(),
            args,
            success: true,
            error: None,
        })
    }

    /// Build an entry with `success = false` and the given
    /// error message, then record it.
    pub fn record_failure(
        &self,
        peer_id: u64,
        operation: &str,
        args: serde_json::Value,
        error: &str,
    ) -> std::io::Result<()> {
        self.record(&AuditEntry {
            timestamp: now_rfc3339(),
            peer_id,
            operation: operation.to_string(),
            args,
            success: false,
            error: Some(error.to_string()),
        })
    }

    /// Returns the path of the underlying file. Tests use this
    /// to read entries back.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Current time as an RFC3339 / ISO 8601 string. Uses chrono
/// since the workspace already depends on it.
fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_one_entry_through_jsonl() {
        let dir = tempfile::TempDir::new().unwrap();
        let log = AuditLog::open(dir.path().join("audit.jsonl")).unwrap();
        log.record_success(
            7,
            "drain_leadership",
            serde_json::json!({"timeout_seconds": 30}),
        )
        .unwrap();
        log.record_failure(
            7,
            "take_cluster_backup",
            serde_json::json!({"errors": 1}),
            "snapshot trigger failed on partition 2",
        )
        .unwrap();

        let raw = std::fs::read_to_string(log.path()).unwrap();
        let mut lines = raw.lines();
        let first: AuditEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(first.peer_id, 7);
        assert_eq!(first.operation, "drain_leadership");
        assert!(first.success);
        assert!(first.error.is_none());

        let second: AuditEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(second.operation, "take_cluster_backup");
        assert!(!second.success);
        assert_eq!(
            second.error.as_deref(),
            Some("snapshot trigger failed on partition 2")
        );

        assert!(lines.next().is_none(), "exactly two entries written");
    }
}
