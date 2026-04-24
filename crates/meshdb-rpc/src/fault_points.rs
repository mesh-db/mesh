//! Test-only fault-injection points for the 2PC coordinator and
//! participant paths. Entire module is gated behind
//! `#[cfg(any(test, feature = "fault-inject"))]` so none of this
//! lands in release builds.
//!
//! Flags are set by an integration test before driving a write;
//! the coordinator and participant consult them at deterministic
//! checkpoints so restart-recovery paths can be exercised without
//! relying on timing races.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32};

/// Deterministic crash / error points the test harness can enable
/// before issuing a write. Each field is inspected at a single
/// checkpoint in the coordinator or participant code, identified by
/// the field name.
///
/// Do not derive `Default` — `AtomicI32::default()` is 0, which is a
/// live trigger for `crash_after_kth_commit_rpc`. Callers use
/// [`FaultPoints::new`] so every counter / flag starts disabled.
#[derive(Debug)]
pub struct FaultPoints {
    /// Coordinator: after the `Prepared` log entry fsyncs, return
    /// `Status::internal("injected fault")` instead of sending the
    /// first PREPARE RPC. Simulates a coordinator crash pre-decision.
    pub crash_after_prepare_log: AtomicBool,

    /// Coordinator: after the `CommitDecision` log entry fsyncs,
    /// return the injected error instead of sending any COMMIT.
    /// Simulates a coordinator crash post-decision, before fanout.
    pub crash_after_commit_decision_log: AtomicBool,

    /// Coordinator: fire N COMMIT RPCs, then on the (N+1)th return the
    /// injected error. `-1` disables; `0` fires on the very first
    /// COMMIT send. Used to simulate a coordinator crash mid-fanout
    /// after some participants have already applied.
    pub crash_after_kth_commit_rpc: AtomicI32,

    /// Participant: in the `BatchPhase::Prepare` handler, refuse
    /// before staging or logging anything. The coordinator treats
    /// this as a normal PREPARE failure and drives its rollback
    /// path — log `AbortDecision`, send ABORT to every peer that
    /// did ack PREPARE, log `Completed`, return the original error.
    pub reject_prepare: AtomicBool,

    /// Participant: in the `BatchPhase::Commit` handler, return
    /// `Status::internal("injected fault")` before applying the
    /// staged batch. Leaves the participant's `Prepared` log entry
    /// on disk with no `Committed` / `Aborted` terminal, which is
    /// the exact state recovery is supposed to handle.
    pub reject_commit_before_apply: AtomicBool,

    /// Observability counter: bumped once per `ResolveTransaction`
    /// RPC served. Tests read this to assert that recovery actually
    /// polled peers, not just rehydrated staging from the log.
    pub resolve_transaction_call_count: AtomicU32,
}

impl FaultPoints {
    pub fn new() -> Self {
        Self {
            crash_after_prepare_log: AtomicBool::new(false),
            crash_after_commit_decision_log: AtomicBool::new(false),
            crash_after_kth_commit_rpc: AtomicI32::new(-1),
            reject_prepare: AtomicBool::new(false),
            reject_commit_before_apply: AtomicBool::new(false),
            resolve_transaction_call_count: AtomicU32::new(0),
        }
    }
}
