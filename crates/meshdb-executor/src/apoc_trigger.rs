//! Implementation of the `apoc.trigger.*` procedure family.
//!
//! Triggers are Cypher snippets that fire on commit boundaries.
//! Each trigger receives the transaction's diff as a set of
//! well-known parameters that mirror Neo4j APOC's surface:
//!
//! * `$createdNodes` / `$createdRelationships` — newly-created
//!   elements (id wasn't in the store pre-tx)
//! * `$deletedNodes` / `$deletedRelationships` — deleted
//!   elements, captured pre-tx so the trigger sees their final
//!   shape before deletion
//! * `$assignedNodeProperties` /
//!   `$assignedRelationshipProperties` — property changes on
//!   existing elements, list of `{key, old, new, node|relationship}`
//! * `$removedNodeProperties` /
//!   `$removedRelationshipProperties` — property removals,
//!   list of `{key, old, node|relationship}`
//! * `$assignedLabels` / `$removedLabels` — label changes on
//!   existing nodes, list of `{label, node}`
//!
//! All four phases work — `before` (pre-commit; errors abort
//! the tx; trigger writes commit atomically with the user's
//! writes), `after` (post-commit, sync; recurses through the
//! cluster commit path so trigger writes replicate),
//! `afterAsync` (post-commit, spawned in a background tokio
//! task so it doesn't block the response), and `rollback`
//! (fires when the commit fails, including before-trigger
//! aborts).
//!
//! # Procedure surface
//!
//! Five procedures: `install` / `drop` / `list` / `start` /
//! `stop`. Install/drop emit `GraphCommand::InstallTrigger` /
//! `DropTrigger` so the cluster commit machinery replicates
//! the registry across every peer (Raft log + routing-mode
//! DDL fan-out). Firing is leader/coordinator-only — exactly
//! one peer per logical commit fires.
//!
//! Recursion safety has two layers: the thread-local
//! `SUPPRESSING` flag catches the sync-call path, and the
//! `from_trigger: bool` parameter on `commit_buffered_commands`
//! catches the async-recursive cluster commit path. Either
//! alone is sufficient; both together close every observed
//! self-firing scenario.
//!
//! # Multi-db
//!
//! `databaseName` is accepted on every procedure but ignored —
//! Mesh is single-db today. Callers can pass any string.

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
    /// `phase`. One of `"before"` / `"after"` / `"afterAsync"` /
    /// `"rollback"`.
    pub phase: String,
    /// Pre-evaluated extra params from the install-time `config`
    /// map's `params` entry. Captured at install time to keep
    /// the firing path purely local.
    pub extra_params: HashMap<String, Property>,
    /// Wall-clock millis since UNIX epoch when the trigger was
    /// registered.
    pub installed_at_ms: i64,
    /// `true` when paused via `apoc.trigger.stop`. Paused
    /// triggers stay in the registry (and replicate) but are
    /// skipped on every commit until `apoc.trigger.start` flips
    /// the flag back. Defaults to `false` for backward
    /// compatibility with pre-pause stored specs.
    #[serde(default)]
    pub paused: bool,
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

    /// Reload the in-memory cache from storage. Called by the
    /// Raft applier and the routing-mode trigger fan-out after
    /// they apply an `InstallTrigger` / `DropTrigger` command,
    /// so a peer that wasn't the original install/drop site
    /// still sees the change immediately. Failures are surfaced
    /// to the caller — the applier logs them, since by that
    /// point the storage write has already succeeded.
    pub fn refresh(&self) -> Result<()> {
        let entries = self
            .store
            .list_triggers()
            .map_err(|e| Error::Procedure(format!("refreshing trigger registry: {e}")))?;
        let mut next: HashMap<String, TriggerSpec> = HashMap::new();
        for (name, blob) in entries {
            match serde_json::from_slice::<TriggerSpec>(&blob) {
                Ok(spec) => {
                    next.insert(name, spec);
                }
                Err(e) => {
                    tracing::warn!(
                        trigger = %name,
                        error = %e,
                        "skipping corrupt trigger spec on refresh"
                    );
                }
            }
        }
        let mut guard = self.inner.write().expect("trigger registry lock");
        *guard = next;
        Ok(())
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

/// One property-change record (assignment or removal) emitted
/// per (element, key) tuple touched by the tx. The `element`
/// field is a `Value::Node` or `Value::Edge` depending on
/// scope so the trigger body can inspect the wider shape.
#[derive(Debug, Clone)]
pub struct PropertyChange {
    pub key: String,
    pub old: Option<Property>,
    pub new: Option<Property>,
    pub element: Value,
}

/// One label-change record (assignment or removal). `element`
/// is always a `Value::Node` since labels are node-scope.
#[derive(Debug, Clone)]
pub struct LabelChange {
    pub label: String,
    pub element: Value,
}

/// Per-tx diff handed to a trigger as `$created*` / `$deleted*` /
/// `$assigned*` / `$removed*` params. Computed in `meshdb-rpc`
/// (where `GraphCommand` lives) and passed in to the firing
/// helpers. `into_param_map` shapes the lists into Neo4j APOC's
/// expected param surface.
#[derive(Debug, Default)]
pub struct TriggerDiff {
    pub created_nodes: Vec<Node>,
    pub created_relationships: Vec<Edge>,
    pub deleted_nodes: Vec<Node>,
    pub deleted_relationships: Vec<Edge>,
    pub assigned_node_properties: Vec<PropertyChange>,
    pub removed_node_properties: Vec<PropertyChange>,
    pub assigned_relationship_properties: Vec<PropertyChange>,
    pub removed_relationship_properties: Vec<PropertyChange>,
    pub assigned_labels: Vec<LabelChange>,
    pub removed_labels: Vec<LabelChange>,
}

impl TriggerDiff {
    /// Deep-clone the diff. Used by call sites that need to fan
    /// the same diff out to multiple firing phases (e.g.
    /// after-sync + afterAsync) since `into_param_map` consumes
    /// `self`.
    pub fn clone_diff(&self) -> Self {
        Self {
            created_nodes: self.created_nodes.clone(),
            created_relationships: self.created_relationships.clone(),
            deleted_nodes: self.deleted_nodes.clone(),
            deleted_relationships: self.deleted_relationships.clone(),
            assigned_node_properties: self.assigned_node_properties.clone(),
            removed_node_properties: self.removed_node_properties.clone(),
            assigned_relationship_properties: self.assigned_relationship_properties.clone(),
            removed_relationship_properties: self.removed_relationship_properties.clone(),
            assigned_labels: self.assigned_labels.clone(),
            removed_labels: self.removed_labels.clone(),
        }
    }

    /// Convert the diff into a [`ParamMap`] suitable for
    /// passing to `execute_with_reader_and_procs`. The keys
    /// match what Neo4j APOC's trigger system exposes:
    /// `createdNodes` / `createdRelationships` /
    /// `deletedNodes` / `deletedRelationships` (lists),
    /// `assignedNodeProperties` / `removedNodeProperties` /
    /// `assignedRelationshipProperties` /
    /// `removedRelationshipProperties` (lists of
    /// `{key, old, new, node|relationship}` maps), and
    /// `assignedLabels` / `removedLabels` (lists of
    /// `{label, node}` maps).
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
        params.insert(
            "assignedNodeProperties".into(),
            property_changes_to_value(self.assigned_node_properties, "node"),
        );
        params.insert(
            "removedNodeProperties".into(),
            property_changes_to_value(self.removed_node_properties, "node"),
        );
        params.insert(
            "assignedRelationshipProperties".into(),
            property_changes_to_value(self.assigned_relationship_properties, "relationship"),
        );
        params.insert(
            "removedRelationshipProperties".into(),
            property_changes_to_value(self.removed_relationship_properties, "relationship"),
        );
        params.insert(
            "assignedLabels".into(),
            label_changes_to_value(self.assigned_labels),
        );
        params.insert(
            "removedLabels".into(),
            label_changes_to_value(self.removed_labels),
        );
        for (k, p) in extra {
            params.insert(k.clone(), Value::Property(p.clone()));
        }
        params
    }
}

/// Lower a list of [`PropertyChange`] entries to a `Value::List`
/// of `Value::Map` records with the keys Neo4j APOC expects:
/// `key`, `old`, `new`, and the element under either `node` or
/// `relationship` depending on scope.
fn property_changes_to_value(changes: Vec<PropertyChange>, element_key: &str) -> Value {
    Value::List(
        changes
            .into_iter()
            .map(|c| {
                let mut entry: HashMap<String, Value> = HashMap::new();
                entry.insert("key".into(), Value::Property(Property::String(c.key)));
                entry.insert(
                    "old".into(),
                    c.old.map(Value::Property).unwrap_or(Value::Null),
                );
                entry.insert(
                    "new".into(),
                    c.new.map(Value::Property).unwrap_or(Value::Null),
                );
                entry.insert(element_key.to_string(), c.element);
                Value::Map(entry)
            })
            .collect(),
    )
}

/// Lower a list of [`LabelChange`] entries to a `Value::List`
/// of `Value::Map` records with `label` and `node` keys —
/// matching Neo4j APOC's `assignedLabels` / `removedLabels`
/// shape.
fn label_changes_to_value(changes: Vec<LabelChange>) -> Value {
    Value::List(
        changes
            .into_iter()
            .map(|c| {
                let mut entry: HashMap<String, Value> = HashMap::new();
                entry.insert("label".into(), Value::Property(Property::String(c.label)));
                entry.insert("node".into(), c.element);
                Value::Map(entry)
            })
            .collect(),
    )
}

/// Fire every registered before-phase trigger against the
/// pre-commit diff. Returns Ok if every trigger ran cleanly,
/// Err with the offending message if one failed — propagated
/// to abort the originator's commit. Trigger writes are
/// buffered into the supplied writer so the caller can merge
/// them into the prepared command batch.
///
/// Unlike after-firing, before-firing stops at the first error:
/// the user's commit hasn't happened yet, so a failing before-
/// trigger cleanly aborts everything.
pub fn fire_before_triggers(
    registry: &TriggerRegistry,
    diff: TriggerDiff,
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    procedures: &ProcedureRegistry,
) -> Result<()> {
    if registry.is_empty() {
        return Ok(());
    }
    if is_suppressed() {
        return Ok(());
    }
    let triggers: Vec<TriggerSpec> = registry
        .list()
        .into_iter()
        .filter(|t| !t.paused && t.phase == "before")
        .collect();
    if triggers.is_empty() {
        return Ok(());
    }
    let extras: Vec<HashMap<String, Property>> =
        triggers.iter().map(|t| t.extra_params.clone()).collect();
    let diff_clones: Vec<TriggerDiff> = (0..triggers.len()).map(|_| diff.clone_diff()).collect();
    let mut first_error: Option<Error> = None;
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
                    first_error = Some(Error::Procedure(format!(
                        "before-trigger '{}' parse error: {e}",
                        trigger.name
                    )));
                    return;
                }
            };
            let plan = match meshdb_cypher::plan(&stmt) {
                Ok(p) => p,
                Err(e) => {
                    first_error = Some(Error::Procedure(format!(
                        "before-trigger '{}' plan error: {e}",
                        trigger.name
                    )));
                    return;
                }
            };
            if let Err(e) = crate::ops::execute_with_reader_and_procs(
                &plan, reader, writer, &params, procedures,
            ) {
                first_error = Some(Error::Procedure(format!(
                    "before-trigger '{}' aborted commit: {e}",
                    trigger.name
                )));
                return;
            }
        }
    });
    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Fire every registered trigger of `phase` against the given
/// diff. Errors are logged and swallowed — the original tx has
/// already committed (for `after` / `afterAsync` / `rollback`)
/// or already aborted (for `rollback`), so a failing trigger
/// can't undo anything. Used as the workhorse for the three
/// non-blocking phases; `before` has its own entry point with
/// abort semantics.
pub fn fire_phase_triggers(
    registry: &TriggerRegistry,
    phase: &str,
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
    let triggers: Vec<TriggerSpec> = registry
        .list()
        .into_iter()
        .filter(|t| !t.paused && t.phase == phase)
        .collect();
    if triggers.is_empty() {
        return;
    }
    let extras: Vec<HashMap<String, Property>> =
        triggers.iter().map(|t| t.extra_params.clone()).collect();
    let diff_clones: Vec<TriggerDiff> = (0..triggers.len()).map(|_| diff.clone_diff()).collect();
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
                        phase = phase,
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
                        phase = phase,
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
                    phase = phase,
                    error = %e,
                    "trigger body returned an error — skipping"
                );
            }
        }
    });
}

/// Fire every registered after-phase trigger. Backwards-compat
/// shim for callers that haven't migrated to
/// [`fire_phase_triggers`] yet.
pub fn fire_after_triggers(
    registry: &TriggerRegistry,
    diff: TriggerDiff,
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    procedures: &ProcedureRegistry,
) {
    fire_phase_triggers(registry, "after", diff, reader, writer, procedures);
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
///
/// Emits the install through the writer (as a
/// [`GraphCommand::InstallTrigger`](meshdb_cluster::GraphCommand::InstallTrigger))
/// so the commit path replicates it across the cluster. The
/// in-memory registry refreshes on each peer when the storage
/// CF lands its updated entry.
pub fn install_call(writer: &dyn GraphWriter, args: &[Value]) -> Result<Vec<ProcRow>> {
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
    if !matches!(
        phase.as_str(),
        "before" | "after" | "afterAsync" | "rollback"
    ) {
        return Err(Error::Procedure(format!(
            "apoc.trigger.install: phase must be one of \
             'before' / 'after' / 'afterAsync' / 'rollback', got {phase:?}"
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
        paused: false,
    };
    let blob = serde_json::to_vec(&spec)
        .map_err(|e| Error::Procedure(format!("apoc.trigger.install: encoding spec: {e}")))?;
    writer.install_trigger(&name, &blob)?;
    let mut row: ProcRow = HashMap::new();
    row.insert("name".into(), Value::Property(Property::String(name)));
    row.insert("query".into(), Value::Property(Property::String(query)));
    row.insert("installed".into(), Value::Property(Property::Bool(true)));
    // V2 drops the synchronous `previous` column: the actual
    // apply happens through the cluster commit path, so we
    // can't synchronously report whether a prior trigger
    // existed under the same name. Callers that need the
    // before/after pair can `apoc.trigger.list` first.
    row.insert("previous".into(), Value::Null);
    Ok(vec![row])
}

/// `apoc.trigger.start(databaseName, name)` — un-pause a
/// previously stopped trigger. Looks up the existing spec
/// through the registry, sets `paused = false`, and re-emits
/// the spec via the writer so the cluster commit replicates
/// the unpause. Errors if the trigger doesn't exist.
pub fn start_call(
    registry: &TriggerRegistry,
    writer: &dyn GraphWriter,
    args: &[Value],
) -> Result<Vec<ProcRow>> {
    set_paused(registry, writer, args, false, "apoc.trigger.start")
}

/// `apoc.trigger.stop(databaseName, name)` — pause a registered
/// trigger. Mirrors `start_call`; sets `paused = true`.
pub fn stop_call(
    registry: &TriggerRegistry,
    writer: &dyn GraphWriter,
    args: &[Value],
) -> Result<Vec<ProcRow>> {
    set_paused(registry, writer, args, true, "apoc.trigger.stop")
}

/// Shared body for start_call / stop_call.
fn set_paused(
    registry: &TriggerRegistry,
    writer: &dyn GraphWriter,
    args: &[Value],
    paused: bool,
    proc_name: &'static str,
) -> Result<Vec<ProcRow>> {
    if args.len() < 2 {
        return Err(Error::Procedure(format!(
            "{proc_name}: expects (databaseName, name)"
        )));
    }
    let _db = expect_string(&args[0], "first argument (databaseName)")?;
    let name = expect_string(&args[1], "second argument (name)")?;
    let mut spec = registry
        .list()
        .into_iter()
        .find(|s| s.name == name)
        .ok_or_else(|| Error::Procedure(format!("{proc_name}: no trigger named '{name}'")))?;
    spec.paused = paused;
    let blob = serde_json::to_vec(&spec)
        .map_err(|e| Error::Procedure(format!("{proc_name}: encoding spec: {e}")))?;
    writer.install_trigger(&name, &blob)?;
    let mut row: ProcRow = HashMap::new();
    row.insert("name".into(), Value::Property(Property::String(name)));
    row.insert("paused".into(), Value::Property(Property::Bool(paused)));
    Ok(vec![row])
}

/// `apoc.trigger.drop(databaseName, name)`. Same write-path
/// shape as `install_call` — the drop rides through the cluster
/// commit so every peer's storage drops the entry. The yielded
/// `removed` column is always `true` in V2 because the actual
/// apply happens on the leader/applier path; the install/drop
/// procedure itself only buffers the command.
pub fn drop_call(writer: &dyn GraphWriter, args: &[Value]) -> Result<Vec<ProcRow>> {
    if args.len() < 2 {
        return Err(Error::Procedure(
            "apoc.trigger.drop: expects (databaseName, name)".into(),
        ));
    }
    let _db = expect_string(&args[0], "first argument (databaseName)")?;
    let name = expect_string(&args[1], "second argument (name)")?;
    writer.drop_trigger(&name)?;
    let mut row: ProcRow = HashMap::new();
    row.insert("name".into(), Value::Property(Property::String(name)));
    row.insert("removed".into(), Value::Property(Property::Bool(true)));
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
            row.insert(
                "paused".into(),
                Value::Property(Property::Bool(spec.paused)),
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
