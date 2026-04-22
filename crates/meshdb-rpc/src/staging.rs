//! In-memory 2PC participant staging with TTL-based garbage
//! collection.
//!
//! Every peer running routing mode holds `BatchWrite(Prepare)` payloads
//! in a per-service map keyed by `txid`. `Commit` drains the entry and
//! applies it; `Abort` drops it. Without a TTL, a coordinator that
//! crashes *without* restarting (or loses the connection long enough
//! to forget the tx) would leave staging sitting in memory forever —
//! the peer has no way to know the original coordinator is never
//! coming back.
//!
//! The coordinator recovery log closes the case where the coordinator
//! crashes and then restarts, but it can't help with a coordinator
//! that just disappears. `ParticipantStaging` closes the gap by
//! time-bounding every staged entry: a background sweeper task walks
//! the map on a fixed interval and drops entries older than the
//! configured TTL. If a late `Commit` arrives after the sweep, the
//! handler surfaces `FailedPrecondition` ("not prepared") and the
//! coordinator's recovery path re-prepares from its own log.
//!
//! Defaults: 60s TTL, 10s sweep interval. Both are configurable via
//! the constructor so tests can run with millisecond-scale TTLs and
//! still be deterministic.

use meshdb_cluster::GraphCommand;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Sensible default TTL for production: 60 seconds. Long enough to
/// cover a normal 2PC round trip plus reasonable network slack; short
/// enough that a coordinator that vanishes doesn't leak staging
/// indefinitely.
pub const DEFAULT_STAGING_TTL: Duration = Duration::from_secs(60);

/// Sensible default sweep interval: 10 seconds. Bounds the worst-case
/// lag between expiry and actual collection to `ttl + interval`.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(10);

/// One staged-but-not-yet-committed `BatchWrite(Prepare)` payload.
/// Keyed by txid in the [`ParticipantStaging`] map.
#[derive(Debug)]
struct StagedEntry {
    commands: Vec<GraphCommand>,
    staged_at: Instant,
}

/// Terminal outcome of a 2PC transaction on this peer. Cached for a
/// bounded window after COMMIT / ABORT so a duplicate RPC can be
/// answered idempotently — committed-then-committed returns OK,
/// aborted-then-committed returns `failed_precondition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalOutcome {
    Committed,
    Aborted,
}

#[derive(Debug)]
struct OutcomeEntry {
    outcome: TerminalOutcome,
    at: Instant,
}

/// Per-service in-memory 2PC participant staging. Safe to share across
/// tasks via `Arc`; all mutating methods take `&self`.
///
/// Holds two maps:
/// - `entries`: staged PREPARE payloads waiting for a COMMIT / ABORT.
/// - `outcomes`: the terminal outcome of recently-finalized txids,
///   consulted when a duplicate COMMIT / ABORT arrives after the
///   staged entry has already been drained. Both share the same TTL
///   so the retention window is consistent across pre- and
///   post-decision state.
#[derive(Debug)]
pub struct ParticipantStaging {
    entries: Mutex<HashMap<String, StagedEntry>>,
    outcomes: Mutex<HashMap<String, OutcomeEntry>>,
    ttl: Duration,
}

impl ParticipantStaging {
    /// Build an empty staging map with the given TTL. A TTL of zero
    /// means "sweep everything on every sweep" — useful for tests
    /// that want to force expiry without real waits.
    pub fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            outcomes: Mutex::new(HashMap::new()),
            ttl,
        })
    }

    /// Build an empty staging with the production default TTL
    /// ([`DEFAULT_STAGING_TTL`]). Used by the `MeshService`
    /// constructors when no custom TTL is requested.
    pub fn with_default_ttl() -> Arc<Self> {
        Self::new(DEFAULT_STAGING_TTL)
    }

    /// Insert a new staged entry for `txid`. Returns `Err(())` if the
    /// txid is already staged — the handler translates that into
    /// `AlreadyExists`.
    pub fn try_insert(&self, txid: String, commands: Vec<GraphCommand>) -> Result<(), ()> {
        let mut map = self.entries.lock().unwrap();
        if map.contains_key(&txid) {
            return Err(());
        }
        map.insert(
            txid,
            StagedEntry {
                commands,
                staged_at: Instant::now(),
            },
        );
        crate::metrics::PENDING_2PC_STAGED.set(map.len() as i64);
        Ok(())
    }

    /// Remove and return the staged commands for `txid`. `None` means
    /// "not prepared" — either the coordinator never sent PREPARE, or
    /// the TTL sweeper has already dropped the entry.
    pub fn take(&self, txid: &str) -> Option<Vec<GraphCommand>> {
        let mut map = self.entries.lock().unwrap();
        let result = map.remove(txid).map(|e| e.commands);
        crate::metrics::PENDING_2PC_STAGED.set(map.len() as i64);
        result
    }

    /// True iff `txid` currently has a staged entry. Used only by
    /// tests — production code flows through `try_insert` + `take`.
    pub fn contains(&self, txid: &str) -> bool {
        self.entries.lock().unwrap().contains_key(txid)
    }

    /// Number of currently-staged entries. Test-only.
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Rehydrate a staged entry at startup replay, ignoring the
    /// duplicate-insert check that `try_insert` enforces during
    /// normal RPC handling. Used by
    /// [`crate::MeshService::recover_participant_staging`] to
    /// populate staging from the participant log before any gRPC
    /// handlers run. Overwrites an existing entry rather than
    /// erroring — at startup only one source is writing.
    pub fn rehydrate(&self, txid: String, commands: Vec<GraphCommand>) {
        let mut map = self.entries.lock().unwrap();
        map.insert(
            txid,
            StagedEntry {
                commands,
                staged_at: Instant::now(),
            },
        );
        crate::metrics::PENDING_2PC_STAGED.set(map.len() as i64);
    }

    /// Record a terminal outcome for `txid`. Called after the
    /// handler's apply (for COMMIT) or drop (for ABORT) so a
    /// subsequent duplicate RPC finds a cached answer and doesn't
    /// mistake "already finalized" for "never prepared".
    pub fn finalize(&self, txid: String, outcome: TerminalOutcome) {
        let mut map = self.outcomes.lock().unwrap();
        map.insert(
            txid,
            OutcomeEntry {
                outcome,
                at: Instant::now(),
            },
        );
    }

    /// Look up the terminal outcome for `txid`. `Some(...)` means the
    /// txid reached a decision within the retention window;
    /// a duplicate RPC can short-circuit. `None` means either no
    /// decision was made or the retention TTL has elapsed.
    pub fn terminal_outcome(&self, txid: &str) -> Option<TerminalOutcome> {
        self.outcomes.lock().unwrap().get(txid).map(|e| e.outcome)
    }

    /// Walk both the staging and outcomes maps, dropping every entry
    /// whose age exceeds the configured TTL. Returns the number of
    /// staging entries dropped — a non-zero count means a prepared
    /// transaction expired without a decision and is the signal an
    /// operator cares about. Outcome-map expiry is routine cleanup
    /// and doesn't surface in the return value.
    pub fn sweep_expired(&self) -> usize {
        let now = Instant::now();
        let ttl = self.ttl;
        let before = {
            let mut map = self.entries.lock().unwrap();
            let before = map.len();
            map.retain(|_, entry| now.saturating_duration_since(entry.staged_at) < ttl);
            crate::metrics::PENDING_2PC_STAGED.set(map.len() as i64);
            before - map.len()
        };
        {
            let mut map = self.outcomes.lock().unwrap();
            map.retain(|_, entry| now.saturating_duration_since(entry.at) < ttl);
        }
        before
    }

    /// Spawn a tokio task that calls [`Self::sweep_expired`] every
    /// `interval`. The returned handle should be aborted on server
    /// shutdown so the task doesn't outlive the service.
    ///
    /// The first sweep fires one `interval` after this call returns —
    /// never immediately — so a very fresh staging entry inserted just
    /// after startup can't race the first sweep.
    pub fn spawn_sweeper(self: Arc<Self>, interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The default tokio Interval fires immediately on the
            // first tick; absorb that so "just started" entries
            // aren't candidates for collection.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let dropped = self.sweep_expired();
                if dropped > 0 {
                    tracing::warn!(
                        dropped,
                        ttl_secs = self.ttl.as_secs(),
                        "expired 2PC participant staging entries",
                    );
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshdb_core::Node;

    fn node_cmd(label: &str) -> GraphCommand {
        GraphCommand::PutNode(Node::new().with_label(label))
    }

    #[test]
    fn insert_then_take_round_trips_commands() {
        let staging = ParticipantStaging::new(Duration::from_secs(60));
        staging
            .try_insert("t1".into(), vec![node_cmd("A"), node_cmd("B")])
            .unwrap();
        assert!(staging.contains("t1"));
        assert_eq!(staging.len(), 1);

        let taken = staging.take("t1").unwrap();
        assert_eq!(taken.len(), 2);
        assert!(!staging.contains("t1"));
        assert_eq!(staging.len(), 0);
    }

    #[test]
    fn duplicate_insert_is_rejected() {
        let staging = ParticipantStaging::new(Duration::from_secs(60));
        staging.try_insert("t1".into(), vec![]).unwrap();
        assert!(staging.try_insert("t1".into(), vec![]).is_err());
    }

    #[test]
    fn take_unknown_txid_returns_none() {
        let staging = ParticipantStaging::new(Duration::from_secs(60));
        assert!(staging.take("never-staged").is_none());
    }

    #[test]
    fn sweep_drops_entries_older_than_ttl() {
        let staging = ParticipantStaging::new(Duration::from_millis(10));
        staging.try_insert("old".into(), vec![]).unwrap();

        std::thread::sleep(Duration::from_millis(30));
        let dropped = staging.sweep_expired();
        assert_eq!(dropped, 1);
        assert!(!staging.contains("old"));
    }

    #[test]
    fn sweep_preserves_fresh_entries() {
        let staging = ParticipantStaging::new(Duration::from_secs(60));
        staging.try_insert("fresh".into(), vec![]).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let dropped = staging.sweep_expired();
        assert_eq!(dropped, 0);
        assert!(staging.contains("fresh"));
    }

    #[test]
    fn sweep_handles_mixed_ages() {
        // Stage one entry, wait past TTL, stage a second, sweep —
        // the first should be dropped and the second preserved.
        let staging = ParticipantStaging::new(Duration::from_millis(20));
        staging
            .try_insert("old".into(), vec![node_cmd("X")])
            .unwrap();
        std::thread::sleep(Duration::from_millis(40));
        staging
            .try_insert("new".into(), vec![node_cmd("Y")])
            .unwrap();

        let dropped = staging.sweep_expired();
        assert_eq!(dropped, 1);
        assert!(!staging.contains("old"));
        assert!(staging.contains("new"));
    }

    #[tokio::test]
    async fn spawn_sweeper_runs_in_background_and_drops_stale_entries() {
        let staging = ParticipantStaging::new(Duration::from_millis(20));
        staging.try_insert("tx".into(), vec![]).unwrap();

        // Sweep interval 15ms: first tick absorbed, second tick fires
        // at ~30ms, by which point the 20ms TTL has elapsed.
        let handle = staging.clone().spawn_sweeper(Duration::from_millis(15));

        // Two cycles' worth of wall-clock time.
        tokio::time::sleep(Duration::from_millis(80)).await;
        handle.abort();

        assert!(
            !staging.contains("tx"),
            "sweeper should have dropped the stale entry"
        );
    }
}
