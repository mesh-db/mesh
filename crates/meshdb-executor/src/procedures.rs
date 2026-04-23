#[cfg(any(
    feature = "apoc-create",
    feature = "apoc-refactor",
    feature = "apoc-cypher"
))]
use crate::error::Error;
use crate::error::Result;
use crate::reader::GraphReader;
use crate::value::Value;
#[cfg(any(
    feature = "apoc-create",
    feature = "apoc-refactor",
    feature = "apoc-cypher"
))]
use crate::writer::GraphWriter;
#[cfg(any(feature = "apoc-create", feature = "apoc-refactor"))]
use meshdb_core::Edge;
#[cfg(feature = "apoc-create")]
use meshdb_core::Node;
use meshdb_core::Property;
use std::collections::{BTreeSet, HashMap};

/// Declared argument / output type for a procedure signature.
/// Mirrors the openCypher type names the TCK uses (`STRING?`,
/// `INTEGER?`, `FLOAT?`, `NUMBER?`, `BOOLEAN?`, `ANY?`). Nullability
/// is not tracked separately — every TCK type in practice is nullable
/// (`?`) and the match logic treats nulls uniformly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcType {
    String,
    Integer,
    Float,
    Number,
    Boolean,
    Any,
}

impl ProcType {
    pub fn parse(s: &str) -> Self {
        let trimmed = s.trim().trim_end_matches('?').trim();
        match trimmed.to_ascii_uppercase().as_str() {
            "STRING" => ProcType::String,
            "INTEGER" | "INT" => ProcType::Integer,
            "FLOAT" => ProcType::Float,
            "NUMBER" | "NUMERIC" => ProcType::Number,
            "BOOLEAN" | "BOOL" => ProcType::Boolean,
            _ => ProcType::Any,
        }
    }

    /// True when `value` is acceptable as a procedure argument of
    /// this declared type. Follows Neo4j's assignable-type rules:
    /// `FLOAT` accepts integers (coerced), `NUMBER` accepts both
    /// numeric kinds, `ANY` accepts everything.
    pub fn accepts(&self, value: &Value) -> bool {
        if matches!(value, Value::Null) {
            return true;
        }
        match (self, value) {
            (ProcType::Any, _) => true,
            (ProcType::String, Value::Property(Property::String(_))) => true,
            (ProcType::Integer, Value::Property(Property::Int64(_))) => true,
            (ProcType::Float, Value::Property(Property::Float64(_))) => true,
            (ProcType::Float, Value::Property(Property::Int64(_))) => true,
            (ProcType::Number, Value::Property(Property::Int64(_))) => true,
            (ProcType::Number, Value::Property(Property::Float64(_))) => true,
            (ProcType::Boolean, Value::Property(Property::Bool(_))) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcArgSpec {
    pub name: String,
    pub ty: ProcType,
}

#[derive(Debug, Clone)]
pub struct ProcOutSpec {
    pub name: String,
    pub ty: ProcType,
}

/// A procedure registered with a [`ProcedureRegistry`]. The TCK
/// harness builds one per `And there exists a procedure ...` step by
/// collating the signature and the gherkin data table: each data row
/// contributes one entry to `rows` where the leading cells are the
/// input-column values (matched against call arguments) and the
/// trailing cells are the output-column values (projected by
/// YIELD).
///
/// Built-in procedures — `db.labels()` and friends — leave `rows`
/// empty and set `builtin` so the executor materialises the row set
/// live from the current graph via [`Procedure::resolve_rows`].
#[derive(Debug, Clone)]
pub struct Procedure {
    pub qualified_name: Vec<String>,
    pub inputs: Vec<ProcArgSpec>,
    pub outputs: Vec<ProcOutSpec>,
    pub rows: Vec<ProcRow>,
    pub builtin: Option<BuiltinProc>,
}

/// Identifies a procedure whose rows are derived from the live graph
/// at call time rather than pre-populated in [`Procedure::rows`].
/// Keeps the procedure surface uniform — the executor still iterates
/// `ProcRow`s; the only difference is who produced them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinProc {
    /// `db.labels()` — yields one row per distinct node label.
    DbLabels,
    /// `db.relationshipTypes()` — yields one row per distinct edge type.
    DbRelationshipTypes,
    /// `db.propertyKeys()` — yields one row per distinct property key
    /// observed on any node or edge.
    DbPropertyKeys,
    /// `db.constraints()` — yields one row per registered constraint,
    /// carrying `name`, `label`, `property`, and `type` columns.
    /// Mirrors the `SHOW CONSTRAINTS` surface.
    DbConstraints,
    /// `apoc.create.node(labels, props)` — write builtin that
    /// creates a node with the given labels and properties and
    /// yields it as `node`. The first builtin that mutates the
    /// store, so it goes through [`Procedure::resolve_write_rows`]
    /// rather than [`Procedure::resolve_rows`] — the args drive
    /// the row directly, no candidate-row filtering needed.
    #[cfg(feature = "apoc-create")]
    ApocCreateNode,
    /// `apoc.create.relationship(from, relType, props, to)` —
    /// write builtin that creates an edge between two existing
    /// nodes and yields it as `rel`. Argument order matches
    /// Neo4j's APOC (relType + props between the two endpoints,
    /// not at the end). Both endpoints must already exist; this
    /// procedure does not auto-create missing nodes.
    #[cfg(feature = "apoc-create")]
    ApocCreateRelationship,
    /// `apoc.create.addLabels(node|nodes, labels)` — adds the
    /// given labels to the supplied nodes (no-op for labels
    /// already present) and yields each updated node back. The
    /// first arg accepts either a single Node or a list of Nodes
    /// to match Neo4j APOC's variadic input convention.
    #[cfg(feature = "apoc-create")]
    ApocCreateAddLabels,
    /// `apoc.create.removeLabels(node|nodes, labels)` — removes
    /// the given labels from the supplied nodes (no-op for
    /// labels not present) and yields each updated node back.
    #[cfg(feature = "apoc-create")]
    ApocCreateRemoveLabels,
    /// `apoc.create.setLabels(node|nodes, labels)` — replaces
    /// the entire label set on each supplied node with the given
    /// list and yields the result back.
    #[cfg(feature = "apoc-create")]
    ApocCreateSetLabels,
    /// `apoc.create.setProperty(node|nodes, key, value)` — sets
    /// (or, if value is null, clears) a single property on each
    /// supplied node and yields the result. Mirrors Neo4j APOC's
    /// signature; for relationships use
    /// [`Self::ApocCreateSetRelProperty`].
    #[cfg(feature = "apoc-create")]
    ApocCreateSetProperty,
    /// `apoc.create.setRelProperty(rel|rels, key, value)` —
    /// relationship-scope counterpart to
    /// [`Self::ApocCreateSetProperty`].
    #[cfg(feature = "apoc-create")]
    ApocCreateSetRelProperty,
    /// `apoc.refactor.setType(rel, newType)` — change a
    /// relationship's type. Implementation deletes the old edge
    /// and creates a new one with the supplied type, preserving
    /// endpoints and properties; the new edge has a fresh
    /// EdgeId. Yields the new edge as `rel`.
    #[cfg(feature = "apoc-refactor")]
    ApocRefactorSetType,
    /// `apoc.meta.schema()` — read-only introspection that walks
    /// the live graph and produces a single-row Map keyed by
    /// label and relationship-type names. Each entry carries a
    /// `count`, a `type` discriminator (`"node"` or
    /// `"relationship"`), and a `properties` map describing each
    /// observed property's APOC type and (for nodes) whether
    /// it's covered by a property index. O(N) in the graph size,
    /// same complexity class as Neo4j APOC's equivalent.
    #[cfg(feature = "apoc-meta")]
    ApocMetaSchema,
    /// `apoc.path.expand(startNode, relationshipFilter,
    /// labelFilter, minLevel, maxLevel)` — BFS path traversal
    /// from `startNode`, yielding one `path` column per
    /// discovered path that satisfies the filter + level
    /// constraints. Streams rows via [`ProcRows::Streaming`] so
    /// a downstream `LIMIT` short-circuits enumeration.
    #[cfg(feature = "apoc-path")]
    ApocPathExpand,
    /// `apoc.path.expandConfig(startNode, config)` — config-map
    /// variant of [`Self::ApocPathExpand`]. Accepts `minLevel`,
    /// `maxLevel`, `relationshipFilter`, `labelFilter`,
    /// `uniqueness`, `filterStartNode`, `limit`, `endNodes`,
    /// `blacklistNodes`, `whitelistNodes`. Other Neo4j APOC
    /// config keys (`sequence`, `bfs`, `optional`,
    /// `terminatorNodes`) parse without error but aren't yet
    /// load-bearing — `bfs` is treated as always-true, the
    /// others as not-set.
    #[cfg(feature = "apoc-path")]
    ApocPathExpandConfig,
    /// `apoc.path.subgraphNodes(startNode, config)` — walks the
    /// reachable subgraph under NODE_GLOBAL uniqueness and
    /// yields one row per distinct node reached, under the
    /// `node` column. Same config surface as expandConfig, but
    /// `minLevel` is forced to 0 (start node is included) and
    /// `uniqueness` is forced to NODE_GLOBAL.
    #[cfg(feature = "apoc-path")]
    ApocPathSubgraphNodes,
    /// `apoc.path.subgraphAll(startNode, config)` — walks the
    /// reachable subgraph and yields a single row with two
    /// columns: `nodes` (list of distinct nodes) and
    /// `relationships` (list of distinct edges). Enumeration is
    /// eager internally since the output is a single aggregate.
    #[cfg(feature = "apoc-path")]
    ApocPathSubgraphAll,
    /// `apoc.path.spanningTree(startNode, config)` — walks under
    /// NODE_GLOBAL uniqueness and yields one row per reached
    /// node carrying its discovery path (the first BFS-order
    /// path that reached that node) under the `path` column.
    #[cfg(feature = "apoc-path")]
    ApocPathSpanningTree,
    /// `apoc.cypher.run(cypher, params)` — parse / plan / execute
    /// the supplied Cypher statement against the current reader
    /// and yield each result row as a single `value` column
    /// carrying a `Map<String, Value>`. Rejects plans containing
    /// write operators — callers who need writes route through
    /// [`Self::ApocCypherDoIt`]. Registered as a write builtin
    /// so the dispatch path has access to both reader and
    /// procedure registry; the read-only check fires after
    /// planning.
    #[cfg(feature = "apoc-cypher")]
    ApocCypherRun,
    /// `apoc.cypher.doIt(cypher, params)` — same shape as
    /// `apoc.cypher.run` but allows writes. The inner execution
    /// uses the outer query's writer, so mutations accumulate in
    /// the same buffer and commit atomically with the enclosing
    /// transaction.
    #[cfg(feature = "apoc-cypher")]
    ApocCypherDoIt,
    /// `apoc.load.json(urlOrPath [, path])` — stream the top-
    /// level elements of a JSON document fetched from a local
    /// file or an HTTP URL. Security gates live on the
    /// ImportConfig attached to the ProcedureRegistry; a
    /// missing / disabled config fails every call.
    #[cfg(feature = "apoc-load")]
    ApocLoadJson,
    /// `apoc.load.csv(urlOrPath [, config])` — stream CSV rows
    /// (lineNo, list, map) from the source. Same security
    /// model as [`Self::ApocLoadJson`].
    #[cfg(feature = "apoc-load")]
    ApocLoadCsv,
    /// `apoc.export.csv.all(file, config)` — serialise every
    /// node and relationship to a CSV file. Reuses
    /// ImportConfig's gates for file writes.
    #[cfg(feature = "apoc-export")]
    ApocExportCsvAll,
    /// `apoc.export.csv.query(query, file, config)` — execute
    /// the inner Cypher and serialise each result row as a CSV
    /// record. Inner query must be read-only.
    #[cfg(feature = "apoc-export")]
    ApocExportCsvQuery,
    /// `apoc.export.json.all(file, config)` — JSONL export of
    /// every node + relationship.
    #[cfg(feature = "apoc-export")]
    ApocExportJsonAll,
    /// `apoc.export.json.query(query, file, config)` — one
    /// JSONL object per result row.
    #[cfg(feature = "apoc-export")]
    ApocExportJsonQuery,
    /// `apoc.export.cypher.all(file, config)` — emit re-runnable
    /// Cypher reconstructing the graph.
    #[cfg(feature = "apoc-export")]
    ApocExportCypherAll,
    /// `apoc.export.cypher.query(query, file, config)` — one
    /// `RETURN {...} AS row;` statement per result row.
    #[cfg(feature = "apoc-export")]
    ApocExportCypherQuery,
    /// `apoc.trigger.install(databaseName, name, statement,
    /// selector, config)` — register an after-phase Cypher
    /// trigger persisted in the trigger CF.
    #[cfg(feature = "apoc-trigger")]
    ApocTriggerInstall,
    /// `apoc.trigger.drop(databaseName, name)` — remove a
    /// registered trigger.
    #[cfg(feature = "apoc-trigger")]
    ApocTriggerDrop,
    /// `apoc.trigger.list()` — yield one row per registered
    /// trigger.
    #[cfg(feature = "apoc-trigger")]
    ApocTriggerList,
    /// `apoc.trigger.start(databaseName, name)` — un-pause a
    /// previously stopped trigger. The change replicates
    /// across the cluster like install/drop.
    #[cfg(feature = "apoc-trigger")]
    ApocTriggerStart,
    /// `apoc.trigger.stop(databaseName, name)` — pause a
    /// registered trigger. Paused triggers stay in the
    /// registry (and replicate) but are skipped on every commit.
    #[cfg(feature = "apoc-trigger")]
    ApocTriggerStop,
}

/// One data-table row. Columns are keyed by declared column name
/// so the registry can look up either the input side (for arg
/// matching) or the output side (for YIELD projection) without
/// recomputing offsets.
pub type ProcRow = HashMap<String, Value>;

/// A read-procedure row source. Most built-ins materialize a small
/// finite result up front (`Eager`). Path-traversal procedures with
/// potentially-unbounded output return a `Streaming` cursor so the
/// executor pulls one row at a time — a downstream `LIMIT` stops
/// enumeration without materializing the full path set.
pub enum ProcRows {
    Eager(Vec<ProcRow>),
    Streaming(Box<dyn ProcCursor>),
}

impl std::fmt::Debug for ProcRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcRows::Eager(rows) => f
                .debug_tuple("Eager")
                .field(&format_args!("{} rows", rows.len()))
                .finish(),
            ProcRows::Streaming(_) => f.debug_struct("Streaming").finish(),
        }
    }
}

/// Lazy row producer used by streaming procedures. Each `advance`
/// call receives a fresh `GraphReader` reference and pulls the next
/// output row, returning `None` when exhausted. State that needs to
/// live across calls (visited sets, frontier queues, the currently-
/// expanding path) is owned by the cursor itself; the reader is
/// borrowed afresh on each call so the executor retains the freedom
/// to switch reader implementations mid-query (e.g. for the
/// in-transaction overlay reader).
pub trait ProcCursor {
    fn advance(&mut self, reader: &dyn GraphReader) -> Result<Option<ProcRow>>;
}

impl Procedure {
    /// True when the call arguments match this row's input columns.
    /// Applied per row during execution — rows whose input cells
    /// differ from the supplied arg values are filtered out.
    /// Argument-type coercion (`FLOAT` accepts an integer, etc.) is
    /// handled by the caller converting the call arg to the declared
    /// type before comparing here.
    pub fn row_matches(&self, row: &ProcRow, args: &[Value]) -> bool {
        for (spec, arg) in self.inputs.iter().zip(args.iter()) {
            let cell = row.get(&spec.name).unwrap_or(&Value::Null);
            if !values_equal_for_procedure(cell, arg) {
                return false;
            }
        }
        true
    }

    /// True when this procedure mutates the store and therefore
    /// needs to be dispatched through [`Self::resolve_write_rows`]
    /// (which receives the writer and the already-evaluated args)
    /// rather than [`Self::resolve_rows`]. Read-only built-ins and
    /// pre-populated rows return `false`.
    pub fn is_write_builtin(&self) -> bool {
        match self.builtin {
            #[cfg(feature = "apoc-create")]
            Some(
                BuiltinProc::ApocCreateNode
                | BuiltinProc::ApocCreateRelationship
                | BuiltinProc::ApocCreateAddLabels
                | BuiltinProc::ApocCreateRemoveLabels
                | BuiltinProc::ApocCreateSetLabels
                | BuiltinProc::ApocCreateSetProperty
                | BuiltinProc::ApocCreateSetRelProperty,
            ) => true,
            #[cfg(feature = "apoc-refactor")]
            Some(BuiltinProc::ApocRefactorSetType) => true,
            // Both cypher.run and cypher.doIt go through the
            // write path — run uses it only for the plan-level
            // read-only check it performs *after* planning, doIt
            // uses it because it actually writes.
            #[cfg(feature = "apoc-cypher")]
            Some(BuiltinProc::ApocCypherRun | BuiltinProc::ApocCypherDoIt) => true,
            #[cfg(feature = "apoc-trigger")]
            Some(
                BuiltinProc::ApocTriggerInstall
                | BuiltinProc::ApocTriggerDrop
                | BuiltinProc::ApocTriggerStart
                | BuiltinProc::ApocTriggerStop,
            ) => true,
            _ => false,
        }
    }

    /// Produce the row source the executor should iterate. Static
    /// procedures simply hand back their pre-populated `rows` as
    /// `Eager`; built-ins with bounded output derive their rows
    /// from the live graph (still eager, but live). Streaming
    /// built-ins — path traversals, subgraph walks, `apoc.load.*`
    /// — hand back a cursor so the executor can pull lazily and
    /// short-circuit on downstream `LIMIT`. `args` carries the
    /// call arguments after type coercion; today's eager
    /// builtins ignore them (their inputs are empty), but
    /// streaming cursors use them to seed traversal state. The
    /// procedure registry is threaded through so load cursors
    /// can read their `ImportConfig` at construction time; most
    /// built-ins ignore it.
    pub fn resolve_rows(
        &self,
        reader: &dyn GraphReader,
        args: &[Value],
        procedures: &ProcedureRegistry,
    ) -> Result<ProcRows> {
        let _ = args;
        let _ = procedures;
        match self.builtin {
            None => Ok(ProcRows::Eager(self.rows.clone())),
            Some(BuiltinProc::DbLabels) => builtin_db_labels(reader).map(ProcRows::Eager),
            Some(BuiltinProc::DbRelationshipTypes) => {
                builtin_db_relationship_types(reader).map(ProcRows::Eager)
            }
            Some(BuiltinProc::DbPropertyKeys) => {
                builtin_db_property_keys(reader).map(ProcRows::Eager)
            }
            Some(BuiltinProc::DbConstraints) => builtin_db_constraints(reader).map(ProcRows::Eager),
            #[cfg(feature = "apoc-meta")]
            Some(BuiltinProc::ApocMetaSchema) => {
                builtin_apoc_meta_schema(reader).map(ProcRows::Eager)
            }
            #[cfg(feature = "apoc-path")]
            Some(BuiltinProc::ApocPathExpand) => {
                let cfg = crate::apoc_path::config_from_expand_args(args)?;
                Ok(ProcRows::Streaming(Box::new(
                    crate::apoc_path::ExpandCursor::new(cfg),
                )))
            }
            #[cfg(feature = "apoc-path")]
            Some(BuiltinProc::ApocPathExpandConfig) => {
                let cfg = crate::apoc_path::config_from_expand_config_args(args)?;
                Ok(ProcRows::Streaming(Box::new(
                    crate::apoc_path::ExpandCursor::new(cfg),
                )))
            }
            #[cfg(feature = "apoc-path")]
            Some(BuiltinProc::ApocPathSubgraphNodes) => {
                let cfg = crate::apoc_path::config_from_expand_config_args(args)?;
                Ok(ProcRows::Streaming(Box::new(
                    crate::apoc_path::SubgraphNodesCursor::new(cfg),
                )))
            }
            #[cfg(feature = "apoc-path")]
            Some(BuiltinProc::ApocPathSubgraphAll) => {
                let cfg = crate::apoc_path::config_from_expand_config_args(args)?;
                Ok(ProcRows::Streaming(Box::new(
                    crate::apoc_path::SubgraphAllCursor::new(cfg),
                )))
            }
            #[cfg(feature = "apoc-path")]
            Some(BuiltinProc::ApocPathSpanningTree) => {
                let cfg = crate::apoc_path::config_from_expand_config_args(args)?;
                Ok(ProcRows::Streaming(Box::new(
                    crate::apoc_path::SpanningTreeCursor::new(cfg),
                )))
            }
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateNode) => Err(Error::Procedure(
                "apoc.create.node is a write procedure — call via resolve_write_rows".into(),
            )),
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateRelationship) => Err(Error::Procedure(
                "apoc.create.relationship is a write procedure — call via resolve_write_rows"
                    .into(),
            )),
            #[cfg(feature = "apoc-create")]
            Some(
                BuiltinProc::ApocCreateAddLabels
                | BuiltinProc::ApocCreateRemoveLabels
                | BuiltinProc::ApocCreateSetLabels
                | BuiltinProc::ApocCreateSetProperty
                | BuiltinProc::ApocCreateSetRelProperty,
            ) => Err(Error::Procedure(
                "apoc.create.* mutator is a write procedure — call via resolve_write_rows".into(),
            )),
            #[cfg(feature = "apoc-refactor")]
            Some(BuiltinProc::ApocRefactorSetType) => Err(Error::Procedure(
                "apoc.refactor.setType is a write procedure — call via resolve_write_rows".into(),
            )),
            #[cfg(feature = "apoc-cypher")]
            Some(BuiltinProc::ApocCypherRun | BuiltinProc::ApocCypherDoIt) => {
                Err(Error::Procedure(
                    "apoc.cypher.* is dispatched through resolve_write_rows — caller bug".into(),
                ))
            }
            #[cfg(feature = "apoc-load")]
            Some(BuiltinProc::ApocLoadJson) => {
                let input =
                    crate::apoc_load::expect_source_arg(&args[0], "first argument (urlOrPath)")?;
                let pointer = if args.len() > 1 {
                    crate::apoc_load::expect_optional_string(&args[1], "second argument (path)")?
                } else {
                    None
                };
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                Ok(ProcRows::Streaming(Box::new(
                    crate::apoc_load::LoadJsonCursor::new(cfg, input, pointer),
                )))
            }
            #[cfg(feature = "apoc-load")]
            Some(BuiltinProc::ApocLoadCsv) => {
                let input =
                    crate::apoc_load::expect_source_arg(&args[0], "first argument (urlOrPath)")?;
                let csv_cfg = if args.len() > 1 {
                    crate::apoc_load::expect_optional_config_map(&args[1])?
                } else {
                    None
                };
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                Ok(ProcRows::Streaming(Box::new(
                    crate::apoc_load::LoadCsvCursor::new(cfg, input, csv_cfg.as_ref()),
                )))
            }
            #[cfg(feature = "apoc-export")]
            Some(BuiltinProc::ApocExportCsvAll) => {
                let file = crate::apoc_export::expect_all_args(args)?;
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                crate::apoc_export::export_csv_all(reader, &cfg, &file).map(ProcRows::Eager)
            }
            #[cfg(feature = "apoc-export")]
            Some(BuiltinProc::ApocExportCsvQuery) => {
                let (query, file) = crate::apoc_export::expect_query_args(args)?;
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                crate::apoc_export::export_csv_query(reader, &cfg, procedures, &query, &file)
                    .map(ProcRows::Eager)
            }
            #[cfg(feature = "apoc-export")]
            Some(BuiltinProc::ApocExportJsonAll) => {
                let file = crate::apoc_export::expect_all_args(args)?;
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                crate::apoc_export::export_json_all(reader, &cfg, &file).map(ProcRows::Eager)
            }
            #[cfg(feature = "apoc-export")]
            Some(BuiltinProc::ApocExportJsonQuery) => {
                let (query, file) = crate::apoc_export::expect_query_args(args)?;
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                crate::apoc_export::export_json_query(reader, &cfg, procedures, &query, &file)
                    .map(ProcRows::Eager)
            }
            #[cfg(feature = "apoc-export")]
            Some(BuiltinProc::ApocExportCypherAll) => {
                let file = crate::apoc_export::expect_all_args(args)?;
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                crate::apoc_export::export_cypher_all(reader, &cfg, &file).map(ProcRows::Eager)
            }
            #[cfg(feature = "apoc-export")]
            Some(BuiltinProc::ApocExportCypherQuery) => {
                let (query, file) = crate::apoc_export::expect_query_args(args)?;
                let cfg = crate::apoc_load::import_config_from_registry(procedures);
                crate::apoc_export::export_cypher_query(reader, &cfg, procedures, &query, &file)
                    .map(ProcRows::Eager)
            }
            #[cfg(feature = "apoc-trigger")]
            Some(
                BuiltinProc::ApocTriggerInstall
                | BuiltinProc::ApocTriggerDrop
                | BuiltinProc::ApocTriggerStart
                | BuiltinProc::ApocTriggerStop,
            ) => Err(Error::Procedure(
                "apoc.trigger.install/drop/start/stop are write builtins — must go through resolve_write_rows"
                    .into(),
            )),
            #[cfg(feature = "apoc-trigger")]
            Some(BuiltinProc::ApocTriggerList) => {
                let registry = procedures.trigger_registry().ok_or_else(|| {
                    Error::Procedure(
                        "apoc.trigger.* not available — server did not attach a trigger registry"
                            .into(),
                    )
                })?;
                crate::apoc_trigger::list_call(registry).map(ProcRows::Eager)
            }
        }
    }

    /// Write-procedure dispatch path. The args are already
    /// evaluated and type-checked; the row is produced as a side
    /// effect of the mutation (e.g. the newly-created node) so
    /// `row_matches` is skipped for these procedures. The
    /// procedure registry is threaded through for procedures
    /// (like `apoc.cypher.doIt`) that recurse into the executor
    /// and need to resolve nested `CALL` sites against the same
    /// set; most write built-ins ignore it.
    #[cfg(any(
        feature = "apoc-create",
        feature = "apoc-refactor",
        feature = "apoc-cypher"
    ))]
    pub fn resolve_write_rows(
        &self,
        reader: &dyn GraphReader,
        writer: &dyn GraphWriter,
        args: &[Value],
        procedures: &ProcedureRegistry,
    ) -> Result<Vec<ProcRow>> {
        let _ = procedures;
        match self.builtin {
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateNode) => apoc_create_node(writer, args),
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateRelationship) => apoc_create_relationship(writer, args),
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateAddLabels) => {
                apoc_label_mutator(reader, writer, args, LabelMode::Add)
            }
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateRemoveLabels) => {
                apoc_label_mutator(reader, writer, args, LabelMode::Remove)
            }
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateSetLabels) => {
                apoc_label_mutator(reader, writer, args, LabelMode::Set)
            }
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateSetProperty) => {
                apoc_set_node_property(reader, writer, args)
            }
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateSetRelProperty) => {
                apoc_set_rel_property(reader, writer, args)
            }
            #[cfg(feature = "apoc-refactor")]
            Some(BuiltinProc::ApocRefactorSetType) => apoc_refactor_set_type(reader, writer, args),
            #[cfg(feature = "apoc-cypher")]
            Some(BuiltinProc::ApocCypherRun) => {
                crate::apoc_cypher::run_cypher(reader, writer, args, procedures, false)
            }
            #[cfg(feature = "apoc-cypher")]
            Some(BuiltinProc::ApocCypherDoIt) => {
                crate::apoc_cypher::run_cypher(reader, writer, args, procedures, true)
            }
            #[cfg(feature = "apoc-trigger")]
            Some(BuiltinProc::ApocTriggerInstall) => {
                crate::apoc_trigger::install_call(writer, args)
            }
            #[cfg(feature = "apoc-trigger")]
            Some(BuiltinProc::ApocTriggerDrop) => crate::apoc_trigger::drop_call(writer, args),
            #[cfg(feature = "apoc-trigger")]
            Some(BuiltinProc::ApocTriggerStart) => {
                let registry = procedures.trigger_registry().ok_or_else(|| {
                    Error::Procedure("apoc.trigger.start: no trigger registry attached".into())
                })?;
                crate::apoc_trigger::start_call(registry, writer, args)
            }
            #[cfg(feature = "apoc-trigger")]
            Some(BuiltinProc::ApocTriggerStop) => {
                let registry = procedures.trigger_registry().ok_or_else(|| {
                    Error::Procedure("apoc.trigger.stop: no trigger registry attached".into())
                })?;
                crate::apoc_trigger::stop_call(registry, writer, args)
            }
            _ => Err(Error::Procedure("procedure is not a write builtin".into())),
        }
    }
}

fn str_row(column: &str, value: String) -> ProcRow {
    let mut row = HashMap::new();
    row.insert(column.to_string(), Value::Property(Property::String(value)));
    row
}

fn builtin_db_labels(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let mut labels: BTreeSet<String> = BTreeSet::new();
    for id in reader.all_node_ids()? {
        if let Some(n) = reader.get_node(id)? {
            for l in n.labels {
                labels.insert(l);
            }
        }
    }
    Ok(labels.into_iter().map(|l| str_row("label", l)).collect())
}

fn builtin_db_relationship_types(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let mut types: BTreeSet<String> = BTreeSet::new();
    for id in reader.all_node_ids()? {
        for (edge_id, _) in reader.outgoing(id)? {
            if let Some(e) = reader.get_edge(edge_id)? {
                types.insert(e.edge_type);
            }
        }
    }
    Ok(types
        .into_iter()
        .map(|t| str_row("relationshipType", t))
        .collect())
}

fn builtin_db_constraints(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let specs = reader.list_property_constraints()?;
    Ok(specs
        .into_iter()
        .map(|spec| {
            let mut row: ProcRow = HashMap::new();
            row.insert("name".into(), Value::Property(Property::String(spec.name)));
            let (scope_tag, target) = match spec.scope {
                meshdb_storage::ConstraintScope::Node(l) => ("NODE", l),
                meshdb_storage::ConstraintScope::Relationship(t) => ("RELATIONSHIP", t),
            };
            row.insert(
                "scope".into(),
                Value::Property(Property::String(scope_tag.into())),
            );
            row.insert("label".into(), Value::Property(Property::String(target)));
            let props: Vec<Property> = spec.properties.into_iter().map(Property::String).collect();
            row.insert("properties".into(), Value::Property(Property::List(props)));
            row.insert(
                "type".into(),
                Value::Property(Property::String(spec.kind.as_string())),
            );
            row
        })
        .collect())
}

fn builtin_db_property_keys(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for id in reader.all_node_ids()? {
        if let Some(n) = reader.get_node(id)? {
            for k in n.properties.keys() {
                keys.insert(k.clone());
            }
            for (edge_id, _) in reader.outgoing(id)? {
                if let Some(e) = reader.get_edge(edge_id)? {
                    for k in e.properties.keys() {
                        keys.insert(k.clone());
                    }
                }
            }
        }
    }
    Ok(keys
        .into_iter()
        .map(|k| str_row("propertyKey", k))
        .collect())
}

/// Implementation of `apoc.meta.schema()` — one full graph walk,
/// collecting per-label and per-relationship-type statistics and
/// merging them with the registered index set into a single Map
/// keyed by label / type name. Matches Neo4j's APOC shape closely
/// enough that ported dashboards rendering the `value` column
/// keep working:
///
/// ```text
/// {
///   <label>: {
///     type: "node",
///     count: <int>,
///     properties: {
///       <key>: { type: <APOC type name>, indexed: <bool> }
///     }
///   },
///   <edgeType>: {
///     type: "relationship",
///     count: <int>,
///     properties: {
///       <key>: { type: <APOC type name> }
///     }
///   }
/// }
/// ```
///
/// Emits a single row under the `value` column (standard APOC
/// convention). Unlabeled nodes are omitted — Neo4j APOC does the
/// same, since there's no natural key for that bucket.
#[cfg(feature = "apoc-meta")]
fn builtin_apoc_meta_schema(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    use std::collections::HashSet;
    // Per-label accumulator: count + observed (key → type set).
    let mut label_count: HashMap<String, i64> = HashMap::new();
    let mut label_props: HashMap<String, HashMap<String, HashSet<&'static str>>> = HashMap::new();
    let mut edge_count: HashMap<String, i64> = HashMap::new();
    let mut edge_props: HashMap<String, HashMap<String, HashSet<&'static str>>> = HashMap::new();

    for id in reader.all_node_ids()? {
        let node = match reader.get_node(id)? {
            Some(n) => n,
            None => continue,
        };
        for label in &node.labels {
            *label_count.entry(label.clone()).or_insert(0) += 1;
            let per_label = label_props.entry(label.clone()).or_default();
            for (k, v) in &node.properties {
                per_label
                    .entry(k.clone())
                    .or_default()
                    .insert(apoc_schema_type(v));
            }
        }
        for (edge_id, _) in reader.outgoing(id)? {
            let edge = match reader.get_edge(edge_id)? {
                Some(e) => e,
                None => continue,
            };
            *edge_count.entry(edge.edge_type.clone()).or_insert(0) += 1;
            let per_type = edge_props.entry(edge.edge_type.clone()).or_default();
            for (k, v) in &edge.properties {
                per_type
                    .entry(k.clone())
                    .or_default()
                    .insert(apoc_schema_type(v));
            }
        }
    }

    // Index lookup: a property is marked `indexed: true` if any
    // registered index mentions it (composite indexes count for
    // each of their columns). Only node-scope indexes feed the
    // per-label view; relationship property indexes follow the
    // same pattern but under the edge type.
    let mut node_indexed: HashMap<String, HashSet<String>> = HashMap::new();
    for (label, props) in reader.list_property_indexes()? {
        let entry = node_indexed.entry(label).or_default();
        for p in props {
            entry.insert(p);
        }
    }
    let mut edge_indexed: HashMap<String, HashSet<String>> = HashMap::new();
    for (edge_type, props) in reader.list_edge_property_indexes()? {
        let entry = edge_indexed.entry(edge_type).or_default();
        for p in props {
            entry.insert(p);
        }
    }

    let mut schema: HashMap<String, Property> = HashMap::new();
    for (label, count) in label_count {
        let props = label_props.remove(&label).unwrap_or_default();
        let indexed = node_indexed.remove(&label).unwrap_or_default();
        schema.insert(
            label,
            Property::Map(schema_entry(count, "node", props, Some(&indexed))),
        );
    }
    for (edge_type, count) in edge_count {
        let props = edge_props.remove(&edge_type).unwrap_or_default();
        let indexed = edge_indexed.remove(&edge_type).unwrap_or_default();
        schema.insert(
            edge_type,
            Property::Map(schema_entry(count, "relationship", props, Some(&indexed))),
        );
    }

    let mut row = HashMap::new();
    row.insert("value".to_string(), Value::Property(Property::Map(schema)));
    Ok(vec![row])
}

/// Build one label-or-type entry in the schema map. `type_tag`
/// is `"node"` or `"relationship"`. If a property was observed
/// with several value kinds across rows, its `type` cell collapses
/// those into a sorted `"STRING|INTEGER"` string — that's the
/// convention Neo4j APOC uses for heterogeneous columns.
#[cfg(feature = "apoc-meta")]
fn schema_entry(
    count: i64,
    type_tag: &str,
    props: HashMap<String, std::collections::HashSet<&'static str>>,
    indexed: Option<&std::collections::HashSet<String>>,
) -> HashMap<String, Property> {
    let mut out: HashMap<String, Property> = HashMap::new();
    out.insert("type".into(), Property::String(type_tag.into()));
    out.insert("count".into(), Property::Int64(count));
    let mut properties: HashMap<String, Property> = HashMap::new();
    for (k, types) in props {
        let mut sorted: Vec<&'static str> = types.into_iter().collect();
        sorted.sort_unstable();
        let ty = sorted.join("|");
        let mut info: HashMap<String, Property> = HashMap::new();
        info.insert("type".into(), Property::String(ty));
        if let Some(idx) = indexed {
            info.insert("indexed".into(), Property::Bool(idx.contains(&k)));
        }
        properties.insert(k, Property::Map(info));
    }
    out.insert("properties".into(), Property::Map(properties));
    out
}

/// Upper-snake-case type name for a [`Property`], matching the
/// spelling used by `apoc.meta.type` (e.g. `"STRING"`,
/// `"DATE_TIME"`). Duplicates the scalar-side table in
/// `meshdb_apoc::meta` because procedures.rs deliberately stays
/// decoupled from meshdb-apoc.
#[cfg(feature = "apoc-meta")]
fn apoc_schema_type(p: &Property) -> &'static str {
    match p {
        Property::Null => "NULL",
        Property::String(_) => "STRING",
        Property::Int64(_) => "INTEGER",
        Property::Float64(_) => "FLOAT",
        Property::Bool(_) => "BOOLEAN",
        Property::List(_) => "LIST",
        Property::Map(_) => "MAP",
        Property::DateTime { .. } => "DATE_TIME",
        Property::LocalDateTime(_) => "LOCAL_DATE_TIME",
        Property::Date(_) => "DATE",
        Property::Time { .. } => "TIME",
        Property::Duration(_) => "DURATION",
        Property::Point(_) => "POINT",
    }
}

/// Implementation of the `apoc.create.node(labels, props)` write
/// builtin. Constructs a [`Node`] from the args, persists it via
/// `writer.put_node`, and yields a single row with the new node
/// under the `node` column. Both args may be Null (Neo4j allows
/// `apoc.create.node(null, null)` — yields an empty unlabeled
/// node), but a non-null first arg must be a list of strings and
/// a non-null second arg must be a map.
#[cfg(feature = "apoc-create")]
fn apoc_create_node(writer: &dyn GraphWriter, args: &[Value]) -> Result<Vec<ProcRow>> {
    let labels = match &args[0] {
        Value::Null | Value::Property(Property::Null) => Vec::new(),
        Value::List(items) => items
            .iter()
            .map(|v| match v {
                Value::Property(Property::String(s)) => Ok(s.clone()),
                other => Err(Error::Procedure(format!(
                    "apoc.create.node: labels must be strings, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        Value::Property(Property::List(items)) => items
            .iter()
            .map(|p| match p {
                Property::String(s) => Ok(s.clone()),
                other => Err(Error::Procedure(format!(
                    "apoc.create.node: labels must be strings, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        other => {
            return Err(Error::Procedure(format!(
                "apoc.create.node: first argument must be a list of strings, got {other:?}"
            )));
        }
    };
    let props: HashMap<String, Property> = match &args[1] {
        Value::Null | Value::Property(Property::Null) => HashMap::new(),
        Value::Map(pairs) => pairs
            .iter()
            .map(|(k, v)| Ok((k.clone(), value_to_storable_property(v)?)))
            .collect::<Result<HashMap<_, _>>>()?,
        Value::Property(Property::Map(entries)) => entries
            .iter()
            .map(|(k, p)| (k.clone(), p.clone()))
            .collect(),
        other => {
            return Err(Error::Procedure(format!(
                "apoc.create.node: second argument must be a map, got {other:?}"
            )));
        }
    };
    let mut node = Node::new();
    node.labels = labels;
    // Skip null property values — matches the openCypher rule used
    // by `CREATE (n {k: null})`: the key is treated as absent.
    for (k, p) in props {
        if !matches!(p, Property::Null) {
            node.properties.insert(k, p);
        }
    }
    writer.put_node(&node)?;
    let mut row = HashMap::new();
    row.insert("node".to_string(), Value::Node(node));
    Ok(vec![row])
}

/// Implementation of the `apoc.create.relationship(from, type,
/// props, to)` write builtin. Argument order matches Neo4j's
/// APOC: from + type + props + to. Both endpoint args must
/// resolve to a [`Value::Node`]; the procedure does not
/// auto-create missing endpoints (use `apoc.create.node` first
/// or merge through CREATE).
#[cfg(feature = "apoc-create")]
fn apoc_create_relationship(writer: &dyn GraphWriter, args: &[Value]) -> Result<Vec<ProcRow>> {
    let from = expect_node_id(&args[0], "first argument (from)")?;
    let rel_type = match &args[1] {
        Value::Property(Property::String(s)) => s.clone(),
        Value::Null | Value::Property(Property::Null) => {
            return Err(Error::Procedure(
                "apoc.create.relationship: relationship type must not be null".into(),
            ));
        }
        other => {
            return Err(Error::Procedure(format!(
                "apoc.create.relationship: relationship type must be a string, got {other:?}"
            )));
        }
    };
    let props: HashMap<String, Property> = match &args[2] {
        Value::Null | Value::Property(Property::Null) => HashMap::new(),
        Value::Map(pairs) => pairs
            .iter()
            .map(|(k, v)| Ok((k.clone(), value_to_storable_property(v)?)))
            .collect::<Result<HashMap<_, _>>>()?,
        Value::Property(Property::Map(entries)) => entries
            .iter()
            .map(|(k, p)| (k.clone(), p.clone()))
            .collect(),
        other => {
            return Err(Error::Procedure(format!(
                "apoc.create.relationship: third argument must be a map, got {other:?}"
            )));
        }
    };
    let to = expect_node_id(&args[3], "fourth argument (to)")?;
    let mut edge = Edge::new(rel_type, from, to);
    for (k, p) in props {
        if !matches!(p, Property::Null) {
            edge.properties.insert(k, p);
        }
    }
    writer.put_edge(&edge)?;
    let mut row = HashMap::new();
    row.insert("rel".to_string(), Value::Edge(edge));
    Ok(vec![row])
}

/// Implementation of `apoc.refactor.setType(rel, newType)`.
/// Edges in Mesh carry their type as immutable storage state, so
/// changing it requires a delete + recreate. The new edge keeps
/// the source / target / properties of the old but receives a
/// fresh [`EdgeId`] — mirrors what Neo4j APOC does (the
/// procedure documents the ID change explicitly).
#[cfg(feature = "apoc-refactor")]
fn apoc_refactor_set_type(
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    args: &[Value],
) -> Result<Vec<ProcRow>> {
    let old_id = match &args[0] {
        Value::Edge(e) => e.id,
        Value::Null | Value::Property(Property::Null) => {
            return Err(Error::Procedure(
                "apoc.refactor.setType: relationship argument must not be null".into(),
            ));
        }
        other => {
            return Err(Error::Procedure(format!(
                "apoc.refactor.setType: first argument must be a relationship, got {other:?}"
            )));
        }
    };
    let new_type = match &args[1] {
        Value::Property(Property::String(s)) => s.clone(),
        Value::Null | Value::Property(Property::Null) => {
            return Err(Error::Procedure(
                "apoc.refactor.setType: new type must not be null".into(),
            ));
        }
        other => {
            return Err(Error::Procedure(format!(
                "apoc.refactor.setType: new type must be a string, got {other:?}"
            )));
        }
    };
    let old = reader
        .get_edge(old_id)?
        .ok_or_else(|| Error::Procedure(format!("edge {old_id:?} no longer exists")))?;
    // Same-type case is a no-op: hand the existing edge back
    // unchanged so the caller's EdgeId stays valid.
    if old.edge_type == new_type {
        let mut row = HashMap::new();
        row.insert("rel".to_string(), Value::Edge(old));
        return Ok(vec![row]);
    }
    let mut new_edge = Edge::new(new_type, old.source, old.target);
    new_edge.properties = old.properties.clone();
    writer.delete_edge(old_id)?;
    writer.put_edge(&new_edge)?;
    let mut row = HashMap::new();
    row.insert("rel".to_string(), Value::Edge(new_edge));
    Ok(vec![row])
}

/// Three-way switch shared by the label mutators
/// (`apoc.create.addLabels` / `removeLabels` / `setLabels`). The
/// per-mode logic is identical except for how the existing label
/// list is combined with the supplied list.
#[cfg(feature = "apoc-create")]
#[derive(Debug, Clone, Copy)]
enum LabelMode {
    Add,
    Remove,
    Set,
}

/// Implementation of `apoc.create.addLabels` / `removeLabels` /
/// `setLabels`. The first arg is either a single Node or a list
/// of Nodes (matching APOC's variadic input). The second arg is
/// a list of label strings. Each input node is reloaded fresh
/// from the reader so we apply the mutation to the latest state
/// (the Node value in the row may have been captured earlier in
/// the query). The updated node is written back via the writer
/// and yielded under the `node` column.
#[cfg(feature = "apoc-create")]
fn apoc_label_mutator(
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    args: &[Value],
    mode: LabelMode,
) -> Result<Vec<ProcRow>> {
    let node_ids = expect_node_or_node_list(&args[0], "first argument")?;
    let labels = expect_string_list(&args[1], "second argument (labels)")?;
    let mut out: Vec<ProcRow> = Vec::with_capacity(node_ids.len());
    for nid in node_ids {
        let mut node = reader
            .get_node(nid)?
            .ok_or_else(|| Error::Procedure(format!("node {nid:?} no longer exists")))?;
        match mode {
            LabelMode::Add => {
                for l in &labels {
                    if !node.labels.iter().any(|existing| existing == l) {
                        node.labels.push(l.clone());
                    }
                }
            }
            LabelMode::Remove => {
                node.labels
                    .retain(|existing| !labels.iter().any(|l| l == existing));
            }
            LabelMode::Set => {
                node.labels = labels.clone();
            }
        }
        writer.put_node(&node)?;
        let mut row = HashMap::new();
        row.insert("node".to_string(), Value::Node(node));
        out.push(row);
    }
    Ok(out)
}

/// Implementation of `apoc.create.setProperty(nodes, key,
/// value)`. Sets the property on each supplied node; passing a
/// null value clears the property (matches APOC, and matches the
/// `SET n.k = null` openCypher rule). The node is reloaded from
/// the reader before mutation so concurrent writes earlier in
/// the query are picked up.
#[cfg(feature = "apoc-create")]
fn apoc_set_node_property(
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    args: &[Value],
) -> Result<Vec<ProcRow>> {
    let node_ids = expect_node_or_node_list(&args[0], "first argument")?;
    let key = expect_string(&args[1], "second argument (key)")?;
    let value = value_to_storable_property(&args[2])?;
    let mut out: Vec<ProcRow> = Vec::with_capacity(node_ids.len());
    for nid in node_ids {
        let mut node = reader
            .get_node(nid)?
            .ok_or_else(|| Error::Procedure(format!("node {nid:?} no longer exists")))?;
        if matches!(value, Property::Null) {
            node.properties.remove(&key);
        } else {
            node.properties.insert(key.clone(), value.clone());
        }
        writer.put_node(&node)?;
        let mut row = HashMap::new();
        row.insert("node".to_string(), Value::Node(node));
        out.push(row);
    }
    Ok(out)
}

/// Relationship-scope counterpart to [`apoc_set_node_property`].
#[cfg(feature = "apoc-create")]
fn apoc_set_rel_property(
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    args: &[Value],
) -> Result<Vec<ProcRow>> {
    let edge_ids = expect_edge_or_edge_list(&args[0], "first argument")?;
    let key = expect_string(&args[1], "second argument (key)")?;
    let value = value_to_storable_property(&args[2])?;
    let mut out: Vec<ProcRow> = Vec::with_capacity(edge_ids.len());
    for eid in edge_ids {
        let mut edge = reader
            .get_edge(eid)?
            .ok_or_else(|| Error::Procedure(format!("edge {eid:?} no longer exists")))?;
        if matches!(value, Property::Null) {
            edge.properties.remove(&key);
        } else {
            edge.properties.insert(key.clone(), value.clone());
        }
        writer.put_edge(&edge)?;
        let mut row = HashMap::new();
        row.insert("rel".to_string(), Value::Edge(edge));
        out.push(row);
    }
    Ok(out)
}

/// Coerce the variadic first argument used by the label / set-
/// property mutators into a flat list of [`NodeId`]s. Accepts a
/// single Node or a (possibly nested) list of Nodes — anything
/// else is a type error.
#[cfg(feature = "apoc-create")]
fn expect_node_or_node_list(v: &Value, position: &str) -> Result<Vec<meshdb_core::NodeId>> {
    let mut ids: Vec<meshdb_core::NodeId> = Vec::new();
    collect_node_ids(v, position, &mut ids)?;
    if ids.is_empty() {
        return Err(Error::Procedure(format!(
            "apoc.create.*: {position} resolved to zero nodes"
        )));
    }
    Ok(ids)
}

#[cfg(feature = "apoc-create")]
fn collect_node_ids(v: &Value, position: &str, out: &mut Vec<meshdb_core::NodeId>) -> Result<()> {
    match v {
        Value::Node(n) => {
            out.push(n.id);
            Ok(())
        }
        Value::List(items) => {
            for item in items {
                collect_node_ids(item, position, out)?;
            }
            Ok(())
        }
        Value::Null | Value::Property(Property::Null) => Ok(()),
        other => Err(Error::Procedure(format!(
            "apoc.create.*: {position} must be a node or list of nodes, got {other:?}"
        ))),
    }
}

/// Edge-scope counterpart to [`expect_node_or_node_list`].
#[cfg(feature = "apoc-create")]
fn expect_edge_or_edge_list(v: &Value, position: &str) -> Result<Vec<meshdb_core::EdgeId>> {
    let mut ids: Vec<meshdb_core::EdgeId> = Vec::new();
    collect_edge_ids(v, position, &mut ids)?;
    if ids.is_empty() {
        return Err(Error::Procedure(format!(
            "apoc.create.*: {position} resolved to zero relationships"
        )));
    }
    Ok(ids)
}

#[cfg(feature = "apoc-create")]
fn collect_edge_ids(v: &Value, position: &str, out: &mut Vec<meshdb_core::EdgeId>) -> Result<()> {
    match v {
        Value::Edge(e) => {
            out.push(e.id);
            Ok(())
        }
        Value::List(items) => {
            for item in items {
                collect_edge_ids(item, position, out)?;
            }
            Ok(())
        }
        Value::Null | Value::Property(Property::Null) => Ok(()),
        other => Err(Error::Procedure(format!(
            "apoc.create.*: {position} must be a relationship or list of relationships, got {other:?}"
        ))),
    }
}

#[cfg(feature = "apoc-create")]
fn expect_string_list(v: &Value, position: &str) -> Result<Vec<String>> {
    match v {
        Value::Null | Value::Property(Property::Null) => Ok(Vec::new()),
        Value::List(items) => items
            .iter()
            .map(|item| match item {
                Value::Property(Property::String(s)) => Ok(s.clone()),
                other => Err(Error::Procedure(format!(
                    "apoc.create.*: {position} must contain strings, got {other:?}"
                ))),
            })
            .collect(),
        Value::Property(Property::List(items)) => items
            .iter()
            .map(|p| match p {
                Property::String(s) => Ok(s.clone()),
                other => Err(Error::Procedure(format!(
                    "apoc.create.*: {position} must contain strings, got {other:?}"
                ))),
            })
            .collect(),
        other => Err(Error::Procedure(format!(
            "apoc.create.*: {position} must be a list of strings, got {other:?}"
        ))),
    }
}

#[cfg(feature = "apoc-create")]
fn expect_string(v: &Value, position: &str) -> Result<String> {
    match v {
        Value::Property(Property::String(s)) => Ok(s.clone()),
        other => Err(Error::Procedure(format!(
            "apoc.create.*: {position} must be a string, got {other:?}"
        ))),
    }
}

/// Resolve an endpoint argument to its [`NodeId`]. Both
/// [`Value::Node`] (a fully-materialised node) and a node-id
/// integer are accepted; everything else (including null) is a
/// type error since a relationship needs concrete endpoints.
#[cfg(feature = "apoc-create")]
fn expect_node_id(v: &Value, position: &str) -> Result<meshdb_core::NodeId> {
    match v {
        Value::Node(n) => Ok(n.id),
        Value::Null | Value::Property(Property::Null) => Err(Error::Procedure(format!(
            "apoc.create.relationship: {position} must be a node, got null"
        ))),
        other => Err(Error::Procedure(format!(
            "apoc.create.relationship: {position} must be a node, got {other:?}"
        ))),
    }
}

/// Convert a [`Value`] supplied as a procedure arg into a
/// storable [`Property`]. Mirrors the `value_to_property` helper
/// in `ops.rs` but lives here so procedures.rs can stay
/// self-contained — graph elements (Node/Edge/Path) and
/// graph-aware Maps aren't valid as stored properties.
#[cfg(feature = "apoc-create")]
fn value_to_storable_property(v: &Value) -> Result<Property> {
    match v {
        Value::Property(p) => Ok(p.clone()),
        Value::Null => Ok(Property::Null),
        Value::List(items) => {
            let props = items
                .iter()
                .map(value_to_storable_property)
                .collect::<Result<Vec<_>>>()?;
            Ok(Property::List(props))
        }
        Value::Map(_) | Value::Node(_) | Value::Edge(_) | Value::Path { .. } => {
            Err(Error::Procedure(
                "apoc.create.node: property values can't be graph elements or graph-aware maps"
                    .into(),
            ))
        }
    }
}

fn values_equal_for_procedure(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Property(Property::Int64(x)), Value::Property(Property::Int64(y))) => x == y,
        (Value::Property(Property::Float64(x)), Value::Property(Property::Float64(y))) => x == y,
        (Value::Property(Property::Int64(i)), Value::Property(Property::Float64(f)))
        | (Value::Property(Property::Float64(f)), Value::Property(Property::Int64(i))) => {
            *f == (*i as f64)
        }
        (Value::Property(Property::String(x)), Value::Property(Property::String(y))) => x == y,
        (Value::Property(Property::Bool(x)), Value::Property(Property::Bool(y))) => x == y,
        _ => a == b,
    }
}

/// Lookup table for registered procedures, keyed by fully qualified
/// name (`test.my.proc`). The executor consults this at run time;
/// callers (TCK harness, server startup) build an instance and pass
/// it to [`crate::execute_with_reader_and_procs`]. An empty registry
/// is the default, meaning no procedures are known and any CALL
/// raises `ProcedureNotFound`.
#[derive(Debug, Clone, Default)]
pub struct ProcedureRegistry {
    procs: HashMap<String, Procedure>,
    /// Runtime security config for `apoc.load.*` / (future)
    /// `apoc.export.*`. `None` means "strict default" — every
    /// load call fails with a message pointing at the server
    /// config. Callers opt in via [`Self::set_import_config`].
    #[cfg(feature = "apoc-load")]
    import_config: Option<crate::apoc_load::ImportConfig>,
    /// Active trigger registry. `None` means triggers are
    /// effectively disabled — install / drop / list will all
    /// fail with a clear error pointing at the missing
    /// attachment. meshdb-server attaches one at startup.
    #[cfg(feature = "apoc-trigger")]
    trigger_registry: Option<crate::apoc_trigger::TriggerRegistry>,
}

impl ProcedureRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, proc: Procedure) {
        let key = proc.qualified_name.join(".");
        self.procs.insert(key, proc);
    }

    pub fn get(&self, qualified_name: &[String]) -> Option<&Procedure> {
        self.procs.get(&qualified_name.join("."))
    }

    /// Attach the server's [`ImportConfig`] so `apoc.load.*`
    /// knows which file paths / URLs are allowed. Without a
    /// call to this method every load call refuses with the
    /// "apoc.import.enabled" message.
    #[cfg(feature = "apoc-load")]
    pub fn set_import_config(&mut self, cfg: crate::apoc_load::ImportConfig) {
        self.import_config = Some(cfg);
    }

    /// The currently-attached import config, if any.
    #[cfg(feature = "apoc-load")]
    pub fn import_config(&self) -> Option<&crate::apoc_load::ImportConfig> {
        self.import_config.as_ref()
    }

    /// Attach the runtime trigger registry. Required before
    /// `apoc.trigger.install` / `drop` / `list` will succeed.
    #[cfg(feature = "apoc-trigger")]
    pub fn set_trigger_registry(&mut self, registry: crate::apoc_trigger::TriggerRegistry) {
        self.trigger_registry = Some(registry);
    }

    /// The currently-attached trigger registry, if any.
    #[cfg(feature = "apoc-trigger")]
    pub fn trigger_registry(&self) -> Option<&crate::apoc_trigger::TriggerRegistry> {
        self.trigger_registry.as_ref()
    }

    /// Register the built-in `db.labels`, `db.relationshipTypes`, and
    /// `db.propertyKeys` procedures. Call once at server startup —
    /// the executor materialises each call's row set from the live
    /// graph, so no data needs to be recomputed here when new
    /// labels / types / keys appear.
    pub fn register_defaults(&mut self) {
        self.register(Procedure {
            qualified_name: vec!["db".into(), "labels".into()],
            inputs: Vec::new(),
            outputs: vec![ProcOutSpec {
                name: "label".into(),
                ty: ProcType::String,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbLabels),
        });
        self.register(Procedure {
            qualified_name: vec!["db".into(), "relationshipTypes".into()],
            inputs: Vec::new(),
            outputs: vec![ProcOutSpec {
                name: "relationshipType".into(),
                ty: ProcType::String,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbRelationshipTypes),
        });
        self.register(Procedure {
            qualified_name: vec!["db".into(), "propertyKeys".into()],
            inputs: Vec::new(),
            outputs: vec![ProcOutSpec {
                name: "propertyKey".into(),
                ty: ProcType::String,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbPropertyKeys),
        });
        self.register(Procedure {
            qualified_name: vec!["db".into(), "constraints".into()],
            inputs: Vec::new(),
            outputs: vec![
                ProcOutSpec {
                    name: "name".into(),
                    ty: ProcType::String,
                },
                ProcOutSpec {
                    name: "scope".into(),
                    ty: ProcType::String,
                },
                ProcOutSpec {
                    name: "label".into(),
                    ty: ProcType::String,
                },
                ProcOutSpec {
                    name: "properties".into(),
                    // Returned as a list of strings; the type lattice
                    // doesn't have a dedicated `List<String>` variant
                    // so `ANY` is the closest fit — the executor
                    // preserves the actual List value at call time.
                    ty: ProcType::Any,
                },
                ProcOutSpec {
                    name: "type".into(),
                    ty: ProcType::String,
                },
            ],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbConstraints),
        });
        #[cfg(feature = "apoc-create")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "create".into(), "node".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "labels".into(),
                    // List<String> declared as ANY because the type
                    // lattice doesn't carry container parameters; the
                    // builtin validates structure at call time.
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "props".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "node".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocCreateNode),
        });
        #[cfg(feature = "apoc-create")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "create".into(), "relationship".into()],
            inputs: vec![
                // Argument order matches Neo4j's APOC: from, type,
                // props, to. Endpoints accept Value::Node directly;
                // ProcType::Any is the broadest match that lets a
                // node value flow through without coercion.
                ProcArgSpec {
                    name: "from".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "relType".into(),
                    ty: ProcType::String,
                },
                ProcArgSpec {
                    name: "props".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "to".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "rel".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocCreateRelationship),
        });
        #[cfg(feature = "apoc-create")]
        for (name, builtin) in [
            ("addLabels", BuiltinProc::ApocCreateAddLabels),
            ("removeLabels", BuiltinProc::ApocCreateRemoveLabels),
            ("setLabels", BuiltinProc::ApocCreateSetLabels),
        ] {
            self.register(Procedure {
                qualified_name: vec!["apoc".into(), "create".into(), name.into()],
                inputs: vec![
                    // First arg accepts a Node or a list of Nodes;
                    // ProcType::Any covers both without coercion.
                    ProcArgSpec {
                        name: "nodes".into(),
                        ty: ProcType::Any,
                    },
                    ProcArgSpec {
                        name: "labels".into(),
                        ty: ProcType::Any,
                    },
                ],
                outputs: vec![ProcOutSpec {
                    name: "node".into(),
                    ty: ProcType::Any,
                }],
                rows: Vec::new(),
                builtin: Some(builtin),
            });
        }
        #[cfg(feature = "apoc-create")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "create".into(), "setProperty".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "nodes".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "key".into(),
                    ty: ProcType::String,
                },
                ProcArgSpec {
                    name: "value".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "node".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocCreateSetProperty),
        });
        #[cfg(feature = "apoc-create")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "create".into(), "setRelProperty".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "rels".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "key".into(),
                    ty: ProcType::String,
                },
                ProcArgSpec {
                    name: "value".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "rel".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocCreateSetRelProperty),
        });
        #[cfg(feature = "apoc-meta")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "meta".into(), "schema".into()],
            inputs: Vec::new(),
            outputs: vec![ProcOutSpec {
                name: "value".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocMetaSchema),
        });
        #[cfg(feature = "apoc-refactor")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "refactor".into(), "setType".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "rel".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "newType".into(),
                    ty: ProcType::String,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "rel".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocRefactorSetType),
        });
        #[cfg(feature = "apoc-path")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "path".into(), "expand".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "startNode".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "relationshipFilter".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "labelFilter".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "minLevel".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "maxLevel".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "path".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocPathExpand),
        });
        #[cfg(feature = "apoc-path")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "path".into(), "expandConfig".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "startNode".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "config".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "path".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocPathExpandConfig),
        });
        #[cfg(feature = "apoc-path")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "path".into(), "subgraphNodes".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "startNode".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "config".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "node".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocPathSubgraphNodes),
        });
        #[cfg(feature = "apoc-path")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "path".into(), "subgraphAll".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "startNode".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "config".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![
                ProcOutSpec {
                    name: "nodes".into(),
                    ty: ProcType::Any,
                },
                ProcOutSpec {
                    name: "relationships".into(),
                    ty: ProcType::Any,
                },
            ],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocPathSubgraphAll),
        });
        #[cfg(feature = "apoc-path")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "path".into(), "spanningTree".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "startNode".into(),
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "config".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "path".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocPathSpanningTree),
        });
        #[cfg(feature = "apoc-cypher")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "cypher".into(), "run".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "cypher".into(),
                    ty: ProcType::String,
                },
                ProcArgSpec {
                    name: "params".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "value".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocCypherRun),
        });
        #[cfg(feature = "apoc-cypher")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "cypher".into(), "doIt".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "cypher".into(),
                    ty: ProcType::String,
                },
                ProcArgSpec {
                    name: "params".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "value".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocCypherDoIt),
        });
        #[cfg(feature = "apoc-load")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "load".into(), "json".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "urlOrPath".into(),
                    ty: ProcType::String,
                },
                ProcArgSpec {
                    name: "path".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "value".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocLoadJson),
        });
        #[cfg(feature = "apoc-load")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "load".into(), "csv".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "urlOrPath".into(),
                    ty: ProcType::String,
                },
                ProcArgSpec {
                    name: "config".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![
                ProcOutSpec {
                    name: "lineNo".into(),
                    ty: ProcType::Integer,
                },
                ProcOutSpec {
                    name: "list".into(),
                    ty: ProcType::Any,
                },
                ProcOutSpec {
                    name: "map".into(),
                    ty: ProcType::Any,
                },
            ],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocLoadCsv),
        });
        #[cfg(feature = "apoc-export")]
        {
            let all_inputs = || {
                vec![
                    ProcArgSpec {
                        name: "file".into(),
                        ty: ProcType::String,
                    },
                    ProcArgSpec {
                        name: "config".into(),
                        ty: ProcType::Any,
                    },
                ]
            };
            let query_inputs = || {
                vec![
                    ProcArgSpec {
                        name: "query".into(),
                        ty: ProcType::String,
                    },
                    ProcArgSpec {
                        name: "file".into(),
                        ty: ProcType::String,
                    },
                    ProcArgSpec {
                        name: "config".into(),
                        ty: ProcType::Any,
                    },
                ]
            };
            let stats_outputs = || {
                vec![
                    ProcOutSpec {
                        name: "file".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "source".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "format".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "nodes".into(),
                        ty: ProcType::Integer,
                    },
                    ProcOutSpec {
                        name: "relationships".into(),
                        ty: ProcType::Integer,
                    },
                    ProcOutSpec {
                        name: "properties".into(),
                        ty: ProcType::Integer,
                    },
                    ProcOutSpec {
                        name: "rows".into(),
                        ty: ProcType::Integer,
                    },
                    ProcOutSpec {
                        name: "time".into(),
                        ty: ProcType::Integer,
                    },
                ]
            };
            for (ns, fn_name, is_query, variant) in [
                ("csv", "all", false, BuiltinProc::ApocExportCsvAll),
                ("csv", "query", true, BuiltinProc::ApocExportCsvQuery),
                ("json", "all", false, BuiltinProc::ApocExportJsonAll),
                ("json", "query", true, BuiltinProc::ApocExportJsonQuery),
                ("cypher", "all", false, BuiltinProc::ApocExportCypherAll),
                ("cypher", "query", true, BuiltinProc::ApocExportCypherQuery),
            ] {
                self.register(Procedure {
                    qualified_name: vec!["apoc".into(), "export".into(), ns.into(), fn_name.into()],
                    inputs: if is_query {
                        query_inputs()
                    } else {
                        all_inputs()
                    },
                    outputs: stats_outputs(),
                    rows: Vec::new(),
                    builtin: Some(variant),
                });
            }
        }
        #[cfg(feature = "apoc-trigger")]
        {
            self.register(Procedure {
                qualified_name: vec!["apoc".into(), "trigger".into(), "install".into()],
                inputs: vec![
                    ProcArgSpec {
                        name: "databaseName".into(),
                        ty: ProcType::String,
                    },
                    ProcArgSpec {
                        name: "name".into(),
                        ty: ProcType::String,
                    },
                    ProcArgSpec {
                        name: "statement".into(),
                        ty: ProcType::String,
                    },
                    ProcArgSpec {
                        name: "selector".into(),
                        ty: ProcType::Any,
                    },
                    ProcArgSpec {
                        name: "config".into(),
                        ty: ProcType::Any,
                    },
                ],
                outputs: vec![
                    ProcOutSpec {
                        name: "name".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "query".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "installed".into(),
                        ty: ProcType::Boolean,
                    },
                    ProcOutSpec {
                        name: "previous".into(),
                        ty: ProcType::Any,
                    },
                ],
                rows: Vec::new(),
                builtin: Some(BuiltinProc::ApocTriggerInstall),
            });
            self.register(Procedure {
                qualified_name: vec!["apoc".into(), "trigger".into(), "drop".into()],
                inputs: vec![
                    ProcArgSpec {
                        name: "databaseName".into(),
                        ty: ProcType::String,
                    },
                    ProcArgSpec {
                        name: "name".into(),
                        ty: ProcType::String,
                    },
                ],
                outputs: vec![
                    ProcOutSpec {
                        name: "name".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "removed".into(),
                        ty: ProcType::Boolean,
                    },
                ],
                rows: Vec::new(),
                builtin: Some(BuiltinProc::ApocTriggerDrop),
            });
            self.register(Procedure {
                qualified_name: vec!["apoc".into(), "trigger".into(), "list".into()],
                inputs: Vec::new(),
                outputs: vec![
                    ProcOutSpec {
                        name: "name".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "query".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "phase".into(),
                        ty: ProcType::String,
                    },
                    ProcOutSpec {
                        name: "installed_at".into(),
                        ty: ProcType::Integer,
                    },
                    ProcOutSpec {
                        name: "paused".into(),
                        ty: ProcType::Boolean,
                    },
                ],
                rows: Vec::new(),
                builtin: Some(BuiltinProc::ApocTriggerList),
            });
            for (fn_name, variant) in [
                ("start", BuiltinProc::ApocTriggerStart),
                ("stop", BuiltinProc::ApocTriggerStop),
            ] {
                self.register(Procedure {
                    qualified_name: vec!["apoc".into(), "trigger".into(), fn_name.into()],
                    inputs: vec![
                        ProcArgSpec {
                            name: "databaseName".into(),
                            ty: ProcType::String,
                        },
                        ProcArgSpec {
                            name: "name".into(),
                            ty: ProcType::String,
                        },
                    ],
                    outputs: vec![
                        ProcOutSpec {
                            name: "name".into(),
                            ty: ProcType::String,
                        },
                        ProcOutSpec {
                            name: "paused".into(),
                            ty: ProcType::Boolean,
                        },
                    ],
                    rows: Vec::new(),
                    builtin: Some(variant),
                });
            }
        }
    }
}
