//! Implementation of the `apoc.trigger.*` procedure family.
//!
//! Triggers are Cypher snippets that fire automatically after a
//! commit succeeds. Each trigger receives the transaction's
//! diff as a set of well-known parameters:
//!
//! * `$createdNodes` — list of `Node` values created in the tx
//! * `$createdRelationships` — list of `Edge` values created
//! * `$deletedNodes` — list of `Node` values deleted
//! * `$deletedRelationships` — list of `Edge` values deleted
//!
//! The Cypher body can reference these parameters like any
//! other parameter. Triggers run in their own transaction —
//! their writes commit independently of the triggering write,
//! matching Neo4j APOC's `phase: 'after'` semantics.
//!
//! # V1 scope (this release)
//!
//! Procedures: `install`, `drop`, `list`. Phase: `after` only.
//! Storage: per-node (no cluster replication). Recursion: a
//! thread-local "in trigger" flag suppresses re-firing while a
//! trigger's own writes commit.
//!
//! # Deferred (documented in CLAUDE.md)
//!
//! * `before` / `afterAsync` / `rollback` phases
//! * `pause` / `resume` (via `apoc.trigger.start` / `stop`)
//! * Property-change params (`$assignedNodeProperties` etc)
//! * `apoc.trigger.show(databaseName)`
//! * Cluster replication of trigger registration — Raft and
//!   routing modes currently install per-peer, which means
//!   different peers could disagree about the trigger set
//!   under partial failure. Operators running clusters should
//!   manually keep registrations in sync.
//! * Multi-database support (`databaseName` arg accepted but
//!   ignored — Mesh is single-db today).

use crate::error::{Error, Result};
use crate::procedures::{ProcRow, ProcedureRegistry};
use crate::reader::GraphReader;
use crate::value::{ParamMap, Value};
use crate::writer::GraphWriter;
use meshdb_core::{Edge, Node, Property};
use meshdb_storage::StorageEngine;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Persisted trigger spec. Roundtrips through `serde_json` for
/// storage in the `trigger_meta` column family. The `name` field
/// duplicates the storage key — kept on the value too so list /
/// show output is self-describing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerSpec {
    pub name: String,
    pub query: String,
    /// `phase`. Always `"after"` in this release.
    pub phase: String,
    /// Pre-evaluated extra params from the install-time `config`
    /// map's `params` entry. Captured at install time to keep
    /// the firing path purely local.
    pub extra_params: HashMap<String, Property>,
    /// Wall-clock millis since UNIX epoch when the trigger was
    /// registered.
    pub installed_at_ms: i64,
}

/// In-memory cache + persistent backing of the trigger
/// registry. Mutating operations (install / drop) write
/// through to storage; reads served from the cache. The
/// `Arc<dyn StorageEngine>` is held internally so the
/// procedure call surface only needs the registry handle —
/// not a separate store reference — to mutate the persistent
/// state.
#[derive(Clone)]
pub struct TriggerRegistry {
    inner: Arc<RwLock<HashMap<String, TriggerSpec>>>,
    store: Arc<dyn StorageEngine>,
}

impl std::fmt::Debug for TriggerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.inner.read().map(|g| g.len()).unwrap_or(0);
        f.debug_struct("TriggerRegistry")
            .field("triggers", &count)
            .finish()
    }
}

impl TriggerRegistry {
    /// Hydrate from the storage backend on startup. Skips any
    /// blob that fails to deserialise — a corrupted entry should
    /// surface a warning but not refuse to start the server.
    pub fn from_storage(store: Arc<dyn StorageEngine>) -> Result<Self> {
        let registry = Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            store,
        };
        let entries = registry
            .store
            .list_triggers()
            .map_err(|e| Error::Procedure(format!("loading triggers from storage: {e}")))?;
        let mut guard = registry.inner.write().expect("trigger registry lock");
        for (name, blob) in entries {
            match serde_json::from_slice::<TriggerSpec>(&blob) {
                Ok(spec) => {
                    guard.insert(name, spec);
                }
                Err(e) => {
                    tracing::warn!(trigger = %name, error = %e, "skipping corrupt trigger spec");
                }
            }
        }
        drop(guard);
        Ok(registry)
    }

    /// Persist a new trigger and add it to the cache. Returns
    /// the previous spec under the same name, if any (so the
    /// caller can report install-replaced-existing).
    pub fn install(&self, spec: TriggerSpec) -> Result<Option<TriggerSpec>> {
        let blob = serde_json::to_vec(&spec)
            .map_err(|e| Error::Procedure(format!("encoding trigger spec: {e}")))?;
        self.store
            .put_trigger(&spec.name, &blob)
            .map_err(|e| Error::Procedure(format!("persisting trigger: {e}")))?;
        let mut guard = self.inner.write().expect("trigger registry lock");
        Ok(guard.insert(spec.name.clone(), spec))
    }

    /// Drop by name. Returns the previous spec when one
    /// existed; `Ok(None)` is the no-op case.
    pub fn drop(&self, name: &str) -> Result<Option<TriggerSpec>> {
        self.store
            .delete_trigger(name)
            .map_err(|e| Error::Procedure(format!("removing trigger: {e}")))?;
        let mut guard = self.inner.write().expect("trigger registry lock");
        Ok(guard.remove(name))
    }

    /// Snapshot every registered trigger in deterministic
    /// (name-sorted) order.
    pub fn list(&self) -> Vec<TriggerSpec> {
        let guard = self.inner.read().expect("trigger registry lock");
        let mut specs: Vec<TriggerSpec> = guard.values().cloned().collect();
        specs.sort_by(|a, b| a.name.cmp(&b.name));
        specs
    }

    /// `true` when at least one trigger is registered. Lets the
    /// commit hook short-circuit the diff computation when no
    /// trigger could possibly fire.
    pub fn is_empty(&self) -> bool {
        self.inner.read().expect("trigger registry lock").is_empty()
    }
}

thread_local! {
    /// Reentrancy guard. Set to `true` for the duration of a
    /// trigger's body execution; the commit hook checks it and
    /// skips trigger firing while the flag is on. Stops the
    /// classic "trigger writes a node, which fires the trigger
    /// again, which writes another node, ..." infinite loop.
    /// Local to the firing thread because the executor runs
    /// synchronously inside `spawn_blocking`.
    static SUPPRESSING: Cell<bool> = const { Cell::new(false) };
}

/// Run `body` with the suppression flag set. The flag resets to
/// the prior value on drop — even on panic — so a panicking
/// trigger doesn't leave the calling thread permanently
/// suppressed.
pub fn with_suppression<R, F: FnOnce() -> R>(body: F) -> R {
    struct Guard(bool);
    impl Drop for Guard {
        fn drop(&mut self) {
            SUPPRESSING.with(|s| s.set(self.0));
        }
    }
    let prev = SUPPRESSING.with(|s| s.replace(true));
    let _guard = Guard(prev);
    body()
}

/// Whether the calling thread is currently inside a trigger
/// invocation. The commit hook calls this before firing.
pub fn is_suppressed() -> bool {
    SUPPRESSING.with(|s| s.get())
}

/// Per-tx diff handed to a trigger as four `$created*` /
/// `$deleted*` params. The two pairs are independent — a trigger
/// can choose to inspect any subset. Computed in `meshdb-rpc`
/// (where `GraphCommand` lives) and passed in to
/// [`fire_after_triggers`].
#[derive(Debug, Default)]
pub struct TriggerDiff {
    pub created_nodes: Vec<Node>,
    pub created_relationships: Vec<Edge>,
    pub deleted_nodes: Vec<Node>,
    pub deleted_relationships: Vec<Edge>,
}

impl TriggerDiff {
    /// Convert the diff into a [`ParamMap`] suitable for
    /// passing to `execute_with_reader_and_procs`. The four
    /// keys match what Neo4j APOC's trigger system exposes.
    pub fn into_param_map(self, extra: &HashMap<String, Property>) -> ParamMap {
        let mut params: ParamMap = HashMap::new();
        params.insert(
            "createdNodes".into(),
            Value::List(self.created_nodes.into_iter().map(Value::Node).collect()),
        );
        params.insert(
            "createdRelationships".into(),
            Value::List(
                self.created_relationships
                    .into_iter()
                    .map(Value::Edge)
                    .collect(),
            ),
        );
        params.insert(
            "deletedNodes".into(),
            Value::List(self.deleted_nodes.into_iter().map(Value::Node).collect()),
        );
        params.insert(
            "deletedRelationships".into(),
            Value::List(
                self.deleted_relationships
                    .into_iter()
                    .map(Value::Edge)
                    .collect(),
            ),
        );
        for (k, p) in extra {
            params.insert(k.clone(), Value::Property(p.clone()));
        }
        params
    }
}

/// Fire every registered trigger against the given diff. Errors
/// from individual triggers are logged but don't propagate —
/// the original tx has already committed, and aborting other
/// triggers because one failed isn't useful.
pub fn fire_after_triggers(
    registry: &TriggerRegistry,
    diff: TriggerDiff,
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    procedures: &ProcedureRegistry,
) {
    if registry.is_empty() {
        return;
    }
    if is_suppressed() {
        return;
    }
    let triggers = registry.list();
    let extras: Vec<HashMap<String, Property>> =
        triggers.iter().map(|t| t.extra_params.clone()).collect();
    // Move out of `diff` once: each trigger needs its own clone.
    let diff_clones: Vec<TriggerDiff> = (0..triggers.len())
        .map(|_| TriggerDiff {
            created_nodes: diff.created_nodes.clone(),
            created_relationships: diff.created_relationships.clone(),
            deleted_nodes: diff.deleted_nodes.clone(),
            deleted_relationships: diff.deleted_relationships.clone(),
        })
        .collect();
    with_suppression(|| {
        for ((trigger, extra), diff) in triggers
            .iter()
            .zip(extras.iter())
            .zip(diff_clones.into_iter())
        {
            let params = diff.into_param_map(extra);
            let stmt = match meshdb_cypher::parse(&trigger.query) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        trigger = %trigger.name,
                        error = %e,
                        "trigger Cypher failed to parse — skipping"
                    );
                    continue;
                }
            };
            let plan = match meshdb_cypher::plan(&stmt) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        trigger = %trigger.name,
                        error = %e,
                        "trigger Cypher failed to plan — skipping"
                    );
                    continue;
                }
            };
            if let Err(e) = crate::ops::execute_with_reader_and_procs(
                &plan, reader, writer, &params, procedures,
            ) {
                tracing::warn!(
                    trigger = %trigger.name,
                    error = %e,
                    "trigger body returned an error — skipping"
                );
            }
        }
    });
}

/// Helper: wall-clock millis since the UNIX epoch. Used to
/// stamp `installed_at_ms` on a fresh trigger spec.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------- Procedure call entry points ----------------

/// `apoc.trigger.install(databaseName, name, statement, selector, config)`.
/// `databaseName` is accepted but ignored — Mesh is
/// single-database today. `selector` and `config` are inspected
/// for a `phase` (defaults to "after") and `params` (defaults to
/// empty); other keys are ignored for forward-compat.
pub fn install_call(registry: &TriggerRegistry, args: &[Value]) -> Result<Vec<ProcRow>> {
    if args.len() < 3 {
        return Err(Error::Procedure(
            "apoc.trigger.install: expects (databaseName, name, statement[, selector[, config]])"
                .into(),
        ));
    }
    let _db = expect_string(&args[0], "first argument (databaseName)")?;
    let name = expect_string(&args[1], "second argument (name)")?;
    let query = expect_string(&args[2], "third argument (statement)")?;
    let phase = if args.len() > 3 {
        match &args[3] {
            Value::Property(Property::Map(m)) => match m.get("phase") {
                Some(Property::String(s)) => s.clone(),
                Some(Property::Null) | None => "after".to_string(),
                Some(other) => {
                    return Err(Error::Procedure(format!(
                        "apoc.trigger.install: selector.phase must be a string, got {other:?}"
                    )));
                }
            },
            Value::Null | Value::Property(Property::Null) => "after".to_string(),
            other => {
                return Err(Error::Procedure(format!(
                    "apoc.trigger.install: selector must be a map or null, got {other:?}"
                )));
            }
        }
    } else {
        "after".to_string()
    };
    if phase != "after" {
        return Err(Error::Procedure(format!(
            "apoc.trigger.install: only the 'after' phase is supported in this release, got {phase:?}"
        )));
    }
    let extra_params: HashMap<String, Property> = if args.len() > 4 {
        match &args[4] {
            Value::Property(Property::Map(m)) => match m.get("params") {
                Some(Property::Map(p)) => p.clone(),
                Some(Property::Null) | None => HashMap::new(),
                Some(other) => {
                    return Err(Error::Procedure(format!(
                        "apoc.trigger.install: config.params must be a map, got {other:?}"
                    )));
                }
            },
            Value::Null | Value::Property(Property::Null) => HashMap::new(),
            other => {
                return Err(Error::Procedure(format!(
                    "apoc.trigger.install: config must be a map or null, got {other:?}"
                )));
            }
        }
    } else {
        HashMap::new()
    };
    // Validate the query parses + plans before persisting so
    // `install` rejects garbage at registration time rather than
    // silently failing on every commit afterwards.
    let stmt = meshdb_cypher::parse(&query)
        .map_err(|e| Error::Procedure(format!("apoc.trigger.install: Cypher parse error: {e}")))?;
    meshdb_cypher::plan(&stmt)
        .map_err(|e| Error::Procedure(format!("apoc.trigger.install: Cypher plan error: {e}")))?;
    let spec = TriggerSpec {
        name: name.clone(),
        query: query.clone(),
        phase,
        extra_params,
        installed_at_ms: now_ms(),
    };
    let prev = registry.install(spec.clone())?;
    let mut row: ProcRow = HashMap::new();
    row.insert("name".into(), Value::Property(Property::String(name)));
    row.insert("query".into(), Value::Property(Property::String(query)));
    row.insert("installed".into(), Value::Property(Property::Bool(true)));
    row.insert(
        "previous".into(),
        match prev {
            Some(p) => Value::Property(Property::String(p.query)),
            None => Value::Null,
        },
    );
    Ok(vec![row])
}

/// `apoc.trigger.drop(databaseName, name)`.
pub fn drop_call(registry: &TriggerRegistry, args: &[Value]) -> Result<Vec<ProcRow>> {
    if args.len() < 2 {
        return Err(Error::Procedure(
            "apoc.trigger.drop: expects (databaseName, name)".into(),
        ));
    }
    let _db = expect_string(&args[0], "first argument (databaseName)")?;
    let name = expect_string(&args[1], "second argument (name)")?;
    let prev = registry.drop(&name)?;
    let mut row: ProcRow = HashMap::new();
    row.insert("name".into(), Value::Property(Property::String(name)));
    row.insert(
        "removed".into(),
        Value::Property(Property::Bool(prev.is_some())),
    );
    Ok(vec![row])
}

/// `apoc.trigger.list()`.
pub fn list_call(registry: &TriggerRegistry) -> Result<Vec<ProcRow>> {
    Ok(registry
        .list()
        .into_iter()
        .map(|spec| {
            let mut row: ProcRow = HashMap::new();
            row.insert("name".into(), Value::Property(Property::String(spec.name)));
            row.insert(
                "query".into(),
                Value::Property(Property::String(spec.query)),
            );
            row.insert(
                "phase".into(),
                Value::Property(Property::String(spec.phase)),
            );
            row.insert(
                "installed_at".into(),
                Value::Property(Property::Int64(spec.installed_at_ms)),
            );
            row
        })
        .collect())
}

fn expect_string(v: &Value, position: &str) -> Result<String> {
    match v {
        Value::Property(Property::String(s)) => Ok(s.clone()),
        Value::Null | Value::Property(Property::Null) => Err(Error::Procedure(format!(
            "apoc.trigger.*: {position} must be a string, got null"
        ))),
        other => Err(Error::Procedure(format!(
            "apoc.trigger.*: {position} must be a string, got {other:?}"
        ))),
    }
}
