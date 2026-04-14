//! Durable append-only log used by the 2PC coordinator to recover from
//! crashes between PREPARE and COMMIT.
//!
//! Without this log, a coordinator crash mid-transaction leaves the
//! staged batches on every participant in limbo: the original client
//! may have already received a success for a tx that never finished,
//! and the participant peers hold in-memory staging entries that
//! nothing resolves. The log records the coordinator's progress
//! through the 2PC phases so that a restart can look at any unfinished
//! txid and push it forward to whichever decision it was moving toward
//! — or, if no decision was ever made, roll it back.
//!
//! ## Log format
//!
//! One JSON object per line, appended and `fsync`ed before the
//! coordinator takes the next protocol step. On read, a corrupt tail
//! (partial or malformed line) terminates parsing — we stop at the
//! last fully-written entry and treat everything beyond it as never
//! having happened. That matches the durability invariant: an entry
//! is only "committed to the log" once its line has been fsync'd end
//! to end.
//!
//! Entry order for a successful commit:
//! 1. `Prepared { txid, groups }` — intent to commit, one map entry
//!    per participating peer with the commands that peer will run.
//!    Written **before** the coordinator sends `PREPARE` to anyone.
//! 2. `CommitDecision { txid }` — the point of no return. Written
//!    **after** every `PREPARE` ack'd, **before** any `COMMIT` is
//!    sent. A crash here means "on restart, finish the commit."
//! 3. `Completed { txid }` — written after every peer ack'd its
//!    `COMMIT` (or `ABORT`). Marks the tx as out-of-scope for
//!    recovery and lets `compact` drop it on the next rewrite.
//!
//! A rollback replaces step 2 with `AbortDecision { txid }` and the
//! post-ABORT step 3 is the same.

use mesh_cluster::{GraphCommand, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One line in the coordinator log. `serde(tag = "type")` tags each
/// variant with a `type` field so the JSON-lines output is easy to
/// inspect by hand during debugging.
///
/// The `Prepared` variant carries the per-peer command groups as a
/// `Vec<(PeerId, ...)>` rather than a `HashMap` because serde_json
/// can't serialize non-string map keys — `PeerId` is a newtype over
/// `u64`. The vec is trivially serializable and converts back to a
/// map at recovery time via [`prepared_groups_to_map`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TxLogEntry {
    /// The coordinator has grouped a transaction's commands per peer
    /// and is about to start the PREPARE phase. On recovery, a txid
    /// whose log history stops here is rolled back.
    Prepared {
        txid: String,
        groups: Vec<(PeerId, Vec<GraphCommand>)>,
    },
    /// Every PREPARE ack'd. The coordinator has chosen to commit and
    /// is about to send COMMIT to each peer. On recovery, a txid that
    /// reaches this entry is pushed forward to completion.
    CommitDecision { txid: String },
    /// A PREPARE failed (or the coordinator chose to abort for
    /// another reason). The coordinator will send ABORT to each peer
    /// that saw a PREPARE. On recovery, a txid that reaches this
    /// entry is rolled back idempotently.
    AbortDecision { txid: String },
    /// The coordinator finished sending the post-decision RPCs to
    /// every peer. On recovery, completed txids are skipped.
    Completed { txid: String },
}

impl TxLogEntry {
    pub fn txid(&self) -> &str {
        match self {
            TxLogEntry::Prepared { txid, .. }
            | TxLogEntry::CommitDecision { txid }
            | TxLogEntry::AbortDecision { txid }
            | TxLogEntry::Completed { txid } => txid.as_str(),
        }
    }
}

/// Append-only log on disk. One instance per coordinator process;
/// the open file handle lives inside a `Mutex` so concurrent tx
/// coordinator calls serialize their log appends without a
/// round-trip through the tokio runtime.
pub struct CoordinatorLog {
    path: PathBuf,
    file: Mutex<File>,
}

impl std::fmt::Debug for CoordinatorLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorLog")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl CoordinatorLog {
    /// Open (or create) the log file at `path`. Any missing parent
    /// directories are created too so callers can pass a path nested
    /// under the data dir without a pre-creation step.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// Append one entry and `fsync` before returning. A successful
    /// return means the entry has reached durable storage and a
    /// subsequent `read_all` — even across a process crash — will
    /// observe it.
    pub fn append(&self, entry: &TxLogEntry) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec(entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        bytes.push(b'\n');
        let mut f = self.file.lock().unwrap();
        f.write_all(&bytes)?;
        // `sync_data` is enough — we don't care about metadata updates
        // like mtime, only the file contents being durable.
        f.sync_data()?;
        Ok(())
    }

    /// Read every complete entry in the log in insertion order. On
    /// parse failure (partial or malformed tail), stop reading and
    /// return what was parsed so far. This is the recovery invariant:
    /// a half-written line at crash time is treated as "never written."
    pub fn read_all(&self) -> std::io::Result<Vec<TxLogEntry>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<TxLogEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "stopping coordinator log parse at malformed line",
                    );
                    break;
                }
            }
        }
        Ok(entries)
    }

    /// Rewrite the log in place so only entries whose txid is in
    /// `keep_txids` survive. Called after recovery to drop entries
    /// for completed transactions that no longer need a log trail.
    ///
    /// Uses a sibling temp file + atomic rename so a crash mid-compact
    /// leaves the original log intact.
    pub fn compact(&self, keep_txids: &HashSet<String>) -> std::io::Result<()> {
        let entries = self.read_all()?;
        let tmp_path = self.path.with_extension("jsonl.tmp");

        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            for entry in entries {
                if keep_txids.contains(entry.txid()) {
                    let mut bytes = serde_json::to_vec(&entry)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    bytes.push(b'\n');
                    tmp.write_all(&bytes)?;
                }
            }
            tmp.sync_data()?;
        }

        // Atomic rename — and then swap the live file handle so future
        // appends hit the compacted file.
        std::fs::rename(&tmp_path, &self.path)?;
        let new_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;
        let mut slot = self.file.lock().unwrap();
        *slot = new_handle;
        Ok(())
    }
}

/// Per-txid aggregate derived from the raw log entries. Built by
/// `reconstruct_state`; recovery walks each `TxState` and decides what
/// to do.
#[derive(Debug, Clone)]
pub struct TxState {
    pub txid: String,
    pub groups: HashMap<PeerId, Vec<GraphCommand>>,
    pub decision: Option<TxDecision>,
    pub completed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxDecision {
    Commit,
    Abort,
}

/// Convert the log's `Vec<(PeerId, ...)>` groups representation to
/// the `HashMap` shape the recovery logic wants.
pub fn prepared_groups_to_map(
    groups: &[(PeerId, Vec<GraphCommand>)],
) -> HashMap<PeerId, Vec<GraphCommand>> {
    groups.iter().map(|(p, cmds)| (*p, cmds.clone())).collect()
}

/// Fold a flat sequence of log entries into a per-txid map. Later
/// entries override earlier ones for the same txid (e.g. a
/// `CommitDecision` for `T1` following a `Prepared` for `T1` leaves
/// the prepared groups intact while setting `decision = Commit`).
pub fn reconstruct_state(entries: &[TxLogEntry]) -> HashMap<String, TxState> {
    let mut map: HashMap<String, TxState> = HashMap::new();
    for entry in entries {
        match entry {
            TxLogEntry::Prepared { txid, groups } => {
                let groups_map = prepared_groups_to_map(groups);
                map.entry(txid.clone())
                    .and_modify(|s| s.groups = groups_map.clone())
                    .or_insert_with(|| TxState {
                        txid: txid.clone(),
                        groups: groups_map,
                        decision: None,
                        completed: false,
                    });
            }
            TxLogEntry::CommitDecision { txid } => {
                if let Some(s) = map.get_mut(txid) {
                    s.decision = Some(TxDecision::Commit);
                }
            }
            TxLogEntry::AbortDecision { txid } => {
                if let Some(s) = map.get_mut(txid) {
                    s.decision = Some(TxDecision::Abort);
                }
            }
            TxLogEntry::Completed { txid } => {
                if let Some(s) = map.get_mut(txid) {
                    s.completed = true;
                }
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_core::Node;
    use tempfile::TempDir;

    fn dummy_groups() -> Vec<(PeerId, Vec<GraphCommand>)> {
        vec![
            (
                PeerId(1),
                vec![GraphCommand::PutNode(Node::new().with_label("A"))],
            ),
            (
                PeerId(2),
                vec![GraphCommand::PutNode(Node::new().with_label("B"))],
            ),
        ]
    }

    #[test]
    fn roundtrips_prepared_commit_completed() {
        let dir = TempDir::new().unwrap();
        let log = CoordinatorLog::open(dir.path().join("log.jsonl")).unwrap();

        log.append(&TxLogEntry::Prepared {
            txid: "t1".into(),
            groups: dummy_groups(),
        })
        .unwrap();
        log.append(&TxLogEntry::CommitDecision { txid: "t1".into() })
            .unwrap();
        log.append(&TxLogEntry::Completed { txid: "t1".into() })
            .unwrap();

        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert!(matches!(entries[0], TxLogEntry::Prepared { .. }));
        assert!(matches!(entries[1], TxLogEntry::CommitDecision { .. }));
        assert!(matches!(entries[2], TxLogEntry::Completed { .. }));
    }

    #[test]
    fn reconstruct_state_merges_entries_by_txid() {
        let entries = vec![
            TxLogEntry::Prepared {
                txid: "t1".into(),
                groups: dummy_groups(),
            },
            TxLogEntry::Prepared {
                txid: "t2".into(),
                groups: dummy_groups(),
            },
            TxLogEntry::CommitDecision { txid: "t1".into() },
            TxLogEntry::Completed { txid: "t1".into() },
        ];
        let state = reconstruct_state(&entries);
        assert_eq!(state.len(), 2);
        let t1 = &state["t1"];
        assert_eq!(t1.decision, Some(TxDecision::Commit));
        assert!(t1.completed);
        let t2 = &state["t2"];
        assert_eq!(t2.decision, None);
        assert!(!t2.completed);
    }

    #[test]
    fn compact_drops_entries_for_completed_txids() {
        let dir = TempDir::new().unwrap();
        let log = CoordinatorLog::open(dir.path().join("log.jsonl")).unwrap();
        // Two transactions: t_done is fully completed, t_live is still
        // prepared with no decision yet.
        log.append(&TxLogEntry::Prepared {
            txid: "t_done".into(),
            groups: dummy_groups(),
        })
        .unwrap();
        log.append(&TxLogEntry::CommitDecision {
            txid: "t_done".into(),
        })
        .unwrap();
        log.append(&TxLogEntry::Completed {
            txid: "t_done".into(),
        })
        .unwrap();
        log.append(&TxLogEntry::Prepared {
            txid: "t_live".into(),
            groups: dummy_groups(),
        })
        .unwrap();

        let keep: HashSet<String> = ["t_live".to_string()].into_iter().collect();
        log.compact(&keep).unwrap();

        // Subsequent appends hit the compacted file and co-exist with
        // the entries `compact` kept.
        log.append(&TxLogEntry::AbortDecision {
            txid: "t_live".into(),
        })
        .unwrap();

        let after = log.read_all().unwrap();
        assert_eq!(after.len(), 2);
        assert!(after.iter().all(|e| e.txid() == "t_live"));
    }

    #[test]
    fn corrupt_tail_stops_parsing_without_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.jsonl");
        let log = CoordinatorLog::open(&path).unwrap();
        log.append(&TxLogEntry::Prepared {
            txid: "t1".into(),
            groups: dummy_groups(),
        })
        .unwrap();

        // Simulate a crash mid-write by appending a half line.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"{\"type\":\"Prepared\",\"txid\":\"t2\",\"gro")
                .unwrap();
        }

        // Fresh reader stops at the last complete entry and does not
        // surface an error.
        let log2 = CoordinatorLog::open(&path).unwrap();
        let entries = log2.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].txid(), "t1");
    }
}
