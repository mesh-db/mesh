//! Durable append-only log used by a 2PC participant peer to preserve
//! staged `BatchWrite(Prepare)` payloads across process restarts.
//!
//! Without this log, a peer that ACKs a PREPARE and then crashes loses
//! the staged commands — they live only in
//! [`crate::ParticipantStaging`], which is an in-memory map. On
//! restart the peer has no way to honour a later COMMIT, so it
//! surfaces `failed_precondition` and forces the coordinator to
//! re-PREPARE from its own log. That's correct but wasteful, and it
//! breaks the implicit 2PC contract that a peer which said "yes" to
//! PREPARE can always be driven to a decision the coordinator writes
//! into its log. This log closes the loop: every entry mirrors a
//! state transition on the participant side, fsync'd before the RPC
//! ACKs.
//!
//! ## Log format
//!
//! One JSON object per line. Tail-corruption semantics are the same
//! as the coordinator log (see [`crate::CoordinatorLog`]): a partial
//! or malformed trailing line stops the replay and is treated as
//! "never happened" — only fully-fsync'd lines count as durable.
//!
//! Entry order for a successful commit on this peer:
//! 1. `Prepared { txid, commands }` — written **before** the
//!    PREPARE RPC returns, so a crash after the ACK leaves the
//!    staged commands recoverable.
//! 2. `Committed { txid }` — written **after**
//!    `apply_prepared_batch` lands, so a crash between apply and the
//!    ACK is safe (the apply is already durable through RocksDB; the
//!    log entry just records the outcome so a duplicate COMMIT can
//!    short-circuit to success).
//!
//! A rollback replaces step 2 with `Aborted { txid }`. An ABORT for
//! a txid that was never staged still logs `Aborted`; that keeps the
//! idempotence story simple at the cost of one entry per spurious
//! ABORT.

use meshdb_cluster::GraphCommand;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One line in the participant log. `serde(tag = "type")` tags each
/// variant with a `type` field so the JSONL output is easy to inspect
/// by hand during incident response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ParticipantLogEntry {
    /// The peer staged a batch under `txid` and is about to ACK the
    /// PREPARE RPC. On restart, a txid whose last entry is `Prepared`
    /// gets re-staged so a late COMMIT still finds its payload.
    Prepared {
        txid: String,
        commands: Vec<GraphCommand>,
    },
    /// The peer applied the staged batch and is about to ACK the
    /// COMMIT RPC. A duplicate COMMIT that arrives after this entry
    /// returns success idempotently instead of `failed_precondition`.
    Committed { txid: String },
    /// The peer dropped the staged batch (or confirmed there was
    /// nothing to drop) and is about to ACK the ABORT RPC. A
    /// subsequent COMMIT for the same txid must fail —
    /// [`replay_outcomes`] flags it so the handler can short-circuit.
    Aborted { txid: String },
}

impl ParticipantLogEntry {
    pub fn txid(&self) -> &str {
        match self {
            ParticipantLogEntry::Prepared { txid, .. }
            | ParticipantLogEntry::Committed { txid }
            | ParticipantLogEntry::Aborted { txid } => txid.as_str(),
        }
    }
}

/// Append-only log on disk. One instance per peer process; mutating
/// methods take `&self` so the handle is `Arc`-shareable across the
/// gRPC workers that handle `BatchWrite`.
pub struct ParticipantLog {
    path: PathBuf,
    file: Mutex<File>,
}

impl std::fmt::Debug for ParticipantLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParticipantLog")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl ParticipantLog {
    /// Open (or create) the log at `path`. Any missing parent
    /// directories are created so callers can pass a path nested
    /// under `data_dir` without a pre-creation step.
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
    /// return means the entry has reached durable storage — a
    /// subsequent [`Self::read_all`] after a crash observes it.
    pub fn append(&self, entry: &ParticipantLogEntry) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec(entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        bytes.push(b'\n');
        let mut f = self.file.lock().unwrap();
        f.write_all(&bytes)?;
        f.sync_data()?;
        Ok(())
    }

    /// Read every complete entry in insertion order. A malformed tail
    /// stops the parse — matches the durability invariant that a
    /// half-written line at crash time is treated as never written.
    pub fn read_all(&self) -> std::io::Result<Vec<ParticipantLogEntry>> {
        read_all_from_path(&self.path)
    }

    /// Rewrite the log so only entries with txids in `keep` survive.
    /// Sibling temp file + atomic rename, with the write mutex held
    /// across the whole operation so concurrent `append`s can't race
    /// the rename.
    pub fn compact(&self, keep: &std::collections::HashSet<String>) -> std::io::Result<()> {
        let mut slot = self.file.lock().unwrap();
        let entries = read_all_from_path(&self.path)?;
        let tmp_path = self.path.with_extension("jsonl.tmp");
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            for entry in entries {
                if keep.contains(entry.txid()) {
                    let mut bytes = serde_json::to_vec(&entry)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    bytes.push(b'\n');
                    tmp.write_all(&bytes)?;
                }
            }
            tmp.sync_data()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;
        let new_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;
        *slot = new_handle;
        Ok(())
    }

    /// Walk the log, identify txids that reached a terminal outcome
    /// (Committed or Aborted), and compact them out if at least
    /// `min_terminal` have accumulated. Returns the number dropped.
    pub fn compact_terminals(&self, min_terminal: usize) -> std::io::Result<usize> {
        let entries = self.read_all()?;
        let outcomes = replay_outcomes(&entries);
        let terminal_count = outcomes
            .values()
            .filter(|o| !matches!(o, ParticipantOutcome::Prepared))
            .count();
        if terminal_count < min_terminal {
            return Ok(0);
        }
        let keep: std::collections::HashSet<String> = outcomes
            .iter()
            .filter_map(|(txid, outcome)| match outcome {
                ParticipantOutcome::Prepared => Some(txid.clone()),
                _ => None,
            })
            .collect();
        self.compact(&keep)?;
        Ok(terminal_count)
    }

    /// Spawn a background task that calls [`Self::compact_terminals`]
    /// every `interval`. The returned handle should be aborted on
    /// shutdown.
    pub fn spawn_rotator(
        self: std::sync::Arc<Self>,
        interval: std::time::Duration,
        min_terminal: usize,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match self.compact_terminals(min_terminal) {
                    Ok(0) => {}
                    Ok(dropped) => {
                        tracing::info!(dropped, "compacted participant log");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "participant log compaction failed");
                    }
                }
            }
        })
    }
}

/// Default interval between background log rotations. Matches the
/// coordinator log's cadence so operators see both maintenance tasks
/// fire on similar schedules.
pub const PARTICIPANT_LOG_ROTATION_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Default threshold of terminal txids before compaction runs.
pub const PARTICIPANT_LOG_MIN_TERMINAL: usize = 100;

fn read_all_from_path(path: &Path) -> std::io::Result<Vec<ParticipantLogEntry>> {
    let file = match File::open(path) {
        Ok(f) => f,
        // A brand-new log file is often opened-for-append before any
        // entries are written; a missing file at read time is the
        // same "empty log" case.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<ParticipantLogEntry>(&line) {
            Ok(entry) => entries.push(entry),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "stopping participant log parse at malformed line",
                );
                break;
            }
        }
    }
    Ok(entries)
}

/// Per-txid state derived from the raw log. Callers use this to drive
/// the replay: `Prepared` txids need re-staging, `Committed` /
/// `Aborted` txids short-circuit duplicate RPCs.
#[derive(Debug, Clone, PartialEq)]
pub enum ParticipantOutcome {
    /// The peer logged a PREPARE and never reached a terminal
    /// outcome. The recovery path must rehydrate the staged commands
    /// into memory so a late COMMIT still finds its payload.
    Prepared,
    /// The peer logged a successful apply. A duplicate COMMIT returns
    /// success; a late ABORT is a bug on the coordinator side.
    Committed,
    /// The peer logged an abort. A duplicate ABORT returns success; a
    /// late COMMIT returns `failed_precondition`.
    Aborted,
}

/// Fold a flat sequence of log entries into a per-txid outcome map.
/// Later entries override earlier ones — a `Prepared` followed by a
/// `Committed` ends at `Committed`.
pub fn replay_outcomes(entries: &[ParticipantLogEntry]) -> HashMap<String, ParticipantOutcome> {
    let mut map: HashMap<String, ParticipantOutcome> = HashMap::new();
    for entry in entries {
        match entry {
            ParticipantLogEntry::Prepared { txid, .. } => {
                map.insert(txid.clone(), ParticipantOutcome::Prepared);
            }
            ParticipantLogEntry::Committed { txid } => {
                map.insert(txid.clone(), ParticipantOutcome::Committed);
            }
            ParticipantLogEntry::Aborted { txid } => {
                map.insert(txid.clone(), ParticipantOutcome::Aborted);
            }
        }
    }
    map
}

/// Extract the commands staged for every txid whose last recorded
/// outcome is `Prepared` — i.e. the set of in-doubt transactions that
/// need rehydrating into [`crate::ParticipantStaging`] at startup.
/// Preserves the commands from the most recent `Prepared` entry for
/// each txid (later `Prepared` entries with the same txid are an
/// invariant violation, but if they occur the last one wins).
pub fn replay_in_doubt_commands(
    entries: &[ParticipantLogEntry],
) -> HashMap<String, Vec<GraphCommand>> {
    let outcomes = replay_outcomes(entries);
    let mut last_prepared: HashMap<String, Vec<GraphCommand>> = HashMap::new();
    for entry in entries {
        if let ParticipantLogEntry::Prepared { txid, commands } = entry {
            last_prepared.insert(txid.clone(), commands.clone());
        }
    }
    last_prepared
        .into_iter()
        .filter(|(txid, _)| outcomes.get(txid) == Some(&ParticipantOutcome::Prepared))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshdb_core::Node;
    use tempfile::TempDir;

    fn node_cmd(label: &str) -> GraphCommand {
        GraphCommand::PutNode(Node::new().with_label(label))
    }

    #[test]
    fn append_and_read_round_trips_entries() {
        let dir = TempDir::new().unwrap();
        let log = ParticipantLog::open(dir.path().join("log.jsonl")).unwrap();
        log.append(&ParticipantLogEntry::Prepared {
            txid: "t1".into(),
            commands: vec![node_cmd("A")],
        })
        .unwrap();
        log.append(&ParticipantLogEntry::Committed { txid: "t1".into() })
            .unwrap();
        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(matches!(entries[0], ParticipantLogEntry::Prepared { .. }));
        assert!(matches!(entries[1], ParticipantLogEntry::Committed { .. }));
    }

    #[test]
    fn missing_log_file_reads_as_empty() {
        let dir = TempDir::new().unwrap();
        let log = ParticipantLog::open(dir.path().join("log.jsonl")).unwrap();
        // Drop the handle without writing anything, then reopen —
        // read_all on the empty file must not error.
        drop(log);
        let log = ParticipantLog::open(dir.path().join("log.jsonl")).unwrap();
        assert!(log.read_all().unwrap().is_empty());
    }

    #[test]
    fn replay_outcomes_picks_last_state_per_txid() {
        let entries = vec![
            ParticipantLogEntry::Prepared {
                txid: "a".into(),
                commands: vec![node_cmd("A")],
            },
            ParticipantLogEntry::Prepared {
                txid: "b".into(),
                commands: vec![node_cmd("B")],
            },
            ParticipantLogEntry::Committed { txid: "a".into() },
            ParticipantLogEntry::Aborted { txid: "b".into() },
            ParticipantLogEntry::Prepared {
                txid: "c".into(),
                commands: vec![node_cmd("C")],
            },
        ];
        let outcomes = replay_outcomes(&entries);
        assert_eq!(outcomes["a"], ParticipantOutcome::Committed);
        assert_eq!(outcomes["b"], ParticipantOutcome::Aborted);
        assert_eq!(outcomes["c"], ParticipantOutcome::Prepared);
    }

    #[test]
    fn replay_in_doubt_commands_returns_only_prepared_txids() {
        let entries = vec![
            ParticipantLogEntry::Prepared {
                txid: "committed".into(),
                commands: vec![node_cmd("A")],
            },
            ParticipantLogEntry::Committed {
                txid: "committed".into(),
            },
            ParticipantLogEntry::Prepared {
                txid: "aborted".into(),
                commands: vec![node_cmd("B")],
            },
            ParticipantLogEntry::Aborted {
                txid: "aborted".into(),
            },
            ParticipantLogEntry::Prepared {
                txid: "in-doubt".into(),
                commands: vec![node_cmd("C")],
            },
        ];
        let in_doubt = replay_in_doubt_commands(&entries);
        assert_eq!(in_doubt.len(), 1);
        assert!(in_doubt.contains_key("in-doubt"));
    }

    #[test]
    fn corrupt_tail_stops_parse_without_erroring() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.jsonl");
        let log = ParticipantLog::open(&path).unwrap();
        log.append(&ParticipantLogEntry::Prepared {
            txid: "good".into(),
            commands: vec![node_cmd("A")],
        })
        .unwrap();
        // Hand-append garbage to simulate a torn write.
        drop(log);
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"{not json").unwrap();
        }
        let log = ParticipantLog::open(&path).unwrap();
        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 1, "corrupt tail line must be skipped");
    }

    #[test]
    fn compact_terminals_drops_only_terminal_txids() {
        let dir = TempDir::new().unwrap();
        let log = ParticipantLog::open(dir.path().join("log.jsonl")).unwrap();
        log.append(&ParticipantLogEntry::Prepared {
            txid: "stays".into(),
            commands: vec![node_cmd("A")],
        })
        .unwrap();
        log.append(&ParticipantLogEntry::Prepared {
            txid: "done".into(),
            commands: vec![node_cmd("B")],
        })
        .unwrap();
        log.append(&ParticipantLogEntry::Committed {
            txid: "done".into(),
        })
        .unwrap();
        let dropped = log.compact_terminals(1).unwrap();
        assert_eq!(dropped, 1);
        let entries = log.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].txid(), "stays");
    }

    #[test]
    fn compact_terminals_below_threshold_is_noop() {
        let dir = TempDir::new().unwrap();
        let log = ParticipantLog::open(dir.path().join("log.jsonl")).unwrap();
        log.append(&ParticipantLogEntry::Prepared {
            txid: "done".into(),
            commands: vec![node_cmd("A")],
        })
        .unwrap();
        log.append(&ParticipantLogEntry::Committed {
            txid: "done".into(),
        })
        .unwrap();
        let dropped = log.compact_terminals(5).unwrap();
        assert_eq!(dropped, 0);
        assert_eq!(log.read_all().unwrap().len(), 2);
    }
}
