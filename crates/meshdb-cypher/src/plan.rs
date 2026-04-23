use crate::ast::{
    BinaryOp, CallArgs, CompareOp, ConstraintKind, ConstraintScope, CreateConstraintStmt,
    CreateStmt, Direction, DropConstraintStmt, Expr, IndexDdl, IndexScope, Literal, MatchStmt,
    NodePattern, Pattern, ReturnItem, ReturnStmt, ShortestKind, SortItem, Statement, UnaryOp,
    UnionStmt, UnwindStmt,
};
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VarType {
    Node,
    Edge,
    Path,
    /// Opaque value whose runtime type the planner couldn't narrow
    /// (function calls, parameters, CASE, property access...). A
    /// downstream pattern is allowed to *refine* a Scalar binding
    /// to Node/Edge/Path because at runtime it could genuinely be
    /// that kind of value (`WITH head([n]) AS x MATCH (x)-->(y)`).
    Scalar,
    /// Value whose literal form guarantees it can never be a graph
    /// element (`WITH 123 AS n / MATCH (n)`). Distinct from
    /// `Scalar` so the strict-check code path can flag
    /// `VariableTypeConflict` statically without also rejecting
    /// legitimate Scalar→Node refinement.
    NonNode,
}

/// What the planner needs to know about the current store beyond the
/// statement itself — right now just the registered property indexes,
/// which gate whether `plan_pattern` rewrites a label-scan-plus-filter
/// to an `IndexSeek`. Kept intentionally small: extend by adding
/// fields as new optimizations need catalog data.
#[derive(Debug, Default, Clone)]
pub struct PlannerContext {
    /// `(label, properties)` pairs corresponding to active property
    /// indexes in the backing store. `properties` is a `Vec<String>`
    /// so single-property and composite indexes share one shape.
    /// Stored as a flat Vec rather than a HashMap because N is small
    /// (single digits in practice) and iterating a Vec is faster than
    /// hashing for that size.
    pub indexes: Vec<(String, Vec<String>)>,
    /// `(edge_type, properties)` pairs corresponding to active
    /// relationship-scope property indexes. Relationship analogue of
    /// `indexes`. Consumed by the unbound-endpoints `EdgeSeek`
    /// rewrite. The rewrite currently treats edge indexes as
    /// single-property on the seek key; composite edge seeks are a
    /// follow-up.
    pub edge_indexes: Vec<(String, Vec<String>)>,
    /// `(label, property)` pairs corresponding to active point /
    /// spatial indexes. Consulted by the filter-chain rewrite that
    /// turns `WHERE point.withinbbox(var.prop, lo, hi)` into a
    /// [`LogicalPlan::PointIndexSeek`]. Single-property by
    /// construction — composite spatial indexes would need their
    /// own plan variant.
    pub point_indexes: Vec<(String, String)>,
    /// Relationship-scope analogue of [`Self::point_indexes`].
    /// Gates the `WHERE point.withinbbox(r.prop, ...)` rewrite that
    /// lowers unbound-endpoints edge patterns to
    /// [`LogicalPlan::EdgePointIndexSeek`].
    pub edge_point_indexes: Vec<(String, String)>,
    /// Variables bound in an enclosing query scope. Populated when
    /// planning the body of an `exists { ... }` / `count { ... }`
    /// subquery so that a first-clause `MATCH (n)-->(m)` where `n`
    /// references the outer row gets lowered as a re-bind (no
    /// fresh scan) rather than a fresh label-wide scan that
    /// ignores the correlation. Each entry is `(name, kind)`.
    pub outer_bindings: Vec<(String, OuterBindingKind)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OuterBindingKind {
    Node,
    Edge,
    Scalar,
}

/// Shape of the bbox a [`LogicalPlan::PointIndexSeek`] should drive
/// `nodes_in_bbox` with at open-time.
///
/// `Corners` carries the two corner expressions verbatim from a
/// `point.withinbbox(n.p, lo, hi)` predicate — the operator evals
/// them once, extracts SRIDs, and hands the reader a literal bbox.
///
/// `Radius` carries a center point + scalar radius from a
/// `point.distance(n.p, center) {<, <=} radius` predicate. The
/// operator derives the enclosing bbox at open-time, SRID-aware:
/// Cartesian gets a simple `center ± radius` square, geographic
/// (WGS-84) converts metres to lat/lon spans with a `cos(lat)`
/// factor. The planner keeps the distance predicate as a residual
/// `Filter` above the seek so the circle-vs-square overshoot gets
/// culled at emission time.
#[derive(Debug, Clone, PartialEq)]
pub enum PointSeekBounds {
    Corners { lo: Expr, hi: Expr },
    Radius { center: Expr, radius: Expr },
}

impl PlannerContext {
    /// True when a single-property index exists on `(label, property)`.
    /// Kept for callers that still dispatch on "there is *an* index on
    /// this single property". Composite-aware callers should use
    /// [`PlannerContext::best_index_prefix`] instead.
    pub fn has_index(&self, label: &str, property: &str) -> bool {
        self.indexes
            .iter()
            .any(|(l, props)| l == label && props.len() == 1 && props[0] == property)
    }

    /// True when a single-property edge index exists on
    /// `(edge_type, property)`. See [`PlannerContext::has_index`] for
    /// the composite-aware equivalent.
    pub fn has_edge_index(&self, edge_type: &str, property: &str) -> bool {
        self.edge_indexes
            .iter()
            .any(|(t, props)| t == edge_type && props.len() == 1 && props[0] == property)
    }

    /// True when a point index is registered on `(label, property)`.
    /// Gate for the `WHERE point.withinbbox` → [`LogicalPlan::PointIndexSeek`]
    /// rewrite — without a matching index the planner falls back to
    /// `NodeScanByLabels` + `Filter`.
    pub fn has_point_index(&self, label: &str, property: &str) -> bool {
        self.point_indexes
            .iter()
            .any(|(l, p)| l == label && p == property)
    }

    /// Relationship-scope analogue of [`Self::has_point_index`].
    /// Gates the edge-scope `point.withinbbox` / `point.distance`
    /// rewrites.
    pub fn has_edge_point_index(&self, edge_type: &str, property: &str) -> bool {
        self.edge_point_indexes
            .iter()
            .any(|(t, p)| t == edge_type && p == property)
    }

    /// Pick the best composite index on `label` covered by the equality
    /// set `available`. Returns the matched index's properties in order
    /// (a *prefix* of the index's declared property list), or `None`
    /// when no index has even a one-property prefix covered by
    /// `available`.
    ///
    /// Scoring: longest matched prefix wins; ties broken by total
    /// index length (prefer the more specific index so single-property
    /// calls keep using the dedicated single-property index). The
    /// returned prefix determines which properties the seek consumes;
    /// every remaining equality in `available` becomes a residual
    /// filter the caller wraps around the seek.
    pub fn best_index_prefix(&self, label: &str, available: &[String]) -> Option<Vec<String>> {
        let avail: std::collections::HashSet<&str> = available.iter().map(String::as_str).collect();
        let mut best: Option<Vec<String>> = None;
        let mut best_idx_len = 0;
        for (idx_label, idx_props) in &self.indexes {
            if idx_label != label {
                continue;
            }
            let mut prefix: Vec<String> = Vec::new();
            for p in idx_props {
                if avail.contains(p.as_str()) {
                    prefix.push(p.clone());
                } else {
                    break;
                }
            }
            if prefix.is_empty() {
                continue;
            }
            let cand_len = prefix.len();
            let update = match &best {
                None => true,
                Some(current) => {
                    cand_len > current.len()
                        || (cand_len == current.len() && idx_props.len() > best_idx_len)
                }
            };
            if update {
                best = Some(prefix);
                best_idx_len = idx_props.len();
            }
        }
        best
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    NodeScanAll {
        var: String,
    },
    NodeScanByLabels {
        var: String,
        labels: Vec<String>,
    },
    EdgeExpand {
        input: Box<LogicalPlan>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_labels: Vec<String>,
        /// Inline edge property filter — `[r:TYPE {name: 'monkey'}]`
        /// lowers into an equality check the executor runs per
        /// traversed edge. Same shape as node-pattern properties.
        edge_properties: Vec<(String, Expr)>,
        edge_types: Vec<String>,
        direction: Direction,
        /// Optional pre-bound edge constraint. Set when the hop's
        /// edge variable was already bound in a prior clause — the
        /// expansion then only matches that exact edge by id,
        /// turning the hop into an existence check on the outer
        /// edge rather than a fresh scan that would clobber the
        /// binding.
        edge_constraint_var: Option<String>,
    },
    /// Left-join expand. Behaves like `EdgeExpand` for input
    /// rows that have at least one matching neighbor, but for
    /// input rows with zero matches it yields a single row with
    /// `edge_var` / `dst_var` bound to `Null`. Emitted by
    /// `plan_match` for each `OPTIONAL MATCH` clause; v1 only
    /// supports single-hop patterns whose start variable was
    /// already bound by a prior MATCH.
    OptionalEdgeExpand {
        input: Box<LogicalPlan>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_labels: Vec<String>,
        dst_properties: Vec<(String, Expr)>,
        edge_types: Vec<String>,
        direction: Direction,
        /// Optional pre-bound endpoint constraint. When set, a
        /// traversed edge is only considered a match if its target
        /// node's id equals the node already bound at
        /// `dst_constraint_var` in the current row. Keeps the
        /// per-row null-fallback semantics intact — if no edge
        /// satisfies the constraint, the input row is forwarded
        /// with `edge_var` / `dst_var` bound to Null. Used by
        /// OPTIONAL MATCH when both endpoints are already bound
        /// (`MATCH (a), (b) OPTIONAL MATCH (a)-[r]->(b)`).
        dst_constraint_var: Option<String>,
        /// Optional pre-bound edge constraint. When set, only the
        /// edge whose id matches the edge bound at this row
        /// variable is considered a match — used for patterns like
        /// `OPTIONAL MATCH (a1)<-[r]-(b2)` where `r` came from an
        /// outer MATCH and the OPT should succeed iff the same
        /// edge would also be visitable in the reversed direction.
        edge_constraint_var: Option<String>,
    },
    VarLengthExpand {
        input: Box<LogicalPlan>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_labels: Vec<String>,
        edge_types: Vec<String>,
        /// Inline edge property filter (`[:T* {year: 1988}]`) —
        /// every edge in the walked path must equal these
        /// property values. Applied edge-by-edge during DFS so
        /// paths whose members fail the check are pruned before
        /// any row is emitted.
        edge_properties: Vec<(String, Expr)>,
        direction: Direction,
        min_hops: u64,
        max_hops: u64,
        path_var: Option<String>,
        /// When set, the expansion reads the edge sequence from the
        /// list already bound at this row variable rather than
        /// enumerating paths from the graph. Supports openCypher's
        /// "walk a pre-bound edge list" form:
        ///   `WITH [r1, r2] AS rs MATCH (first)-[rs*]->(second)`
        /// where `rs` is expected to be a `List<Edge>`. The op
        /// verifies each edge connects correctly in the requested
        /// direction and that the walk ends at the terminal node;
        /// a mismatched chain or wrong terminal yields no rows.
        bound_edge_list_var: Option<String>,
        /// Row variables whose bound edges (or edge lists) must
        /// be excluded from the walk — enforces openCypher's
        /// relationship-uniqueness rule across hops in the same
        /// MATCH pattern. Each listed var can hold a single
        /// `Value::Edge` (e.g. a prior fixed hop's synthesised
        /// edge var or an outer-scope bound edge) or a
        /// `List<Edge>` (a prior var-length hop's emitted list).
        /// The DFS skips any edge whose id appears in the union
        /// of those bindings.
        excluded_edge_vars: Vec<String>,
        /// When `true`, the expansion behaves as a per-input-row
        /// left-join: if no path of length `[min_hops, max_hops]`
        /// exists, the input row is forwarded once with `edge_var`
        /// / `dst_var` / `path_var` bound to Null. Set by
        /// `apply_optional_match` so `OPTIONAL MATCH p = (a)-[*]->(b)`
        /// preserves the outer row when no path is found instead of
        /// silently dropping it.
        optional: bool,
        /// Optional pre-bound endpoint constraint. When set, only
        /// paths whose terminal node matches the node bound at
        /// this variable are considered a match. Lets OPTIONAL
        /// MATCH with both endpoints bound (`(a)-[*]->(b)` with
        /// `b` already bound) filter the expansion's targets
        /// without having to rebind `b`; combined with `optional`,
        /// paths that never reach the constrained target trigger
        /// the null fallback.
        dst_constraint_var: Option<String>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        items: Vec<ReturnItem>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<ReturnItem>,
        aggregates: Vec<AggregateSpec>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    OrderBy {
        input: Box<LogicalPlan>,
        sort_items: Vec<SortItem>,
    },
    Skip {
        input: Box<LogicalPlan>,
        count: Expr,
    },
    Limit {
        input: Box<LogicalPlan>,
        count: Expr,
    },
    CreatePath {
        input: Option<Box<LogicalPlan>>,
        nodes: Vec<CreateNodeSpec>,
        edges: Vec<CreateEdgeSpec>,
    },
    CartesianProduct {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    Delete {
        input: Box<LogicalPlan>,
        detach: bool,
        /// Variable-only entries — preserved for the simple
        /// `DELETE a, b` case where `vars` is enough to drive
        /// the executor without having to re-parse.
        vars: Vec<String>,
        /// Full DELETE target expressions. Superset of `vars`:
        /// includes property access (`DELETE m.key`), index
        /// access (`DELETE list[0]`), etc.
        exprs: Vec<Expr>,
    },
    SetProperty {
        input: Box<LogicalPlan>,
        assignments: Vec<SetAssignment>,
    },
    Remove {
        input: Box<LogicalPlan>,
        items: Vec<RemoveSpec>,
    },
    CallSubquery {
        input: Box<LogicalPlan>,
        body: Box<LogicalPlan>,
    },
    /// `CALL { ... } IN TRANSACTIONS [OF n ROWS] [ON ERROR mode]`
    /// — same per-input-row body semantics as
    /// [`Self::CallSubquery`] but with transactional batching.
    /// The executor pulls `batch_size` input rows, runs the body
    /// for each (writing into a fresh per-batch buffer), commits
    /// the batch as its own transaction, and moves on.
    /// `error_mode` controls behaviour when a batch's body or
    /// commit fails (FAIL = propagate, CONTINUE = skip and move
    /// on, BREAK = stop and report success on what committed).
    /// Running this inside an explicit Bolt transaction is a
    /// runtime error — caught at the server-side dispatch layer.
    CallSubqueryInTransactions {
        input: Box<LogicalPlan>,
        body: Box<LogicalPlan>,
        batch_size: i64,
        error_mode: crate::ast::OnErrorMode,
        /// `REPORT STATUS AS <var>` — when set, the dispatcher
        /// emits one status-Map row per batch (bound to this
        /// variable name) instead of forwarding the body's
        /// emitted rows. The status map carries `started`,
        /// `committed`, `errorMessage`, and `transactionId`.
        report_status_as: Option<String>,
    },
    /// `apoc.periodic.iterate(iterateQuery, actionQuery,
    /// {batchSize, params})` — the Neo4j APOC procedure for
    /// batched migrations. Synthesised at planning time when a
    /// `CALL apoc.periodic.iterate(...)` is recognised: both
    /// query strings are parsed and planned eagerly, and the
    /// dispatcher runs the action once per row of the iterate
    /// query's output (with that row's RETURN columns injected
    /// as `$param` values) in `batch_size` batches with
    /// ON ERROR CONTINUE-style semantics. Always emits exactly
    /// one result row carrying the standard APOC columns
    /// (`batches`, `total`, `committedOperations`,
    /// `failedOperations`, `failedBatches`, `errorMessages`,
    /// `timeTaken`, `wasTerminated`).
    ApocPeriodicIterate {
        iterate: Box<LogicalPlan>,
        action: Box<LogicalPlan>,
        batch_size: i64,
        /// Extra params merged into the per-row `$param` map
        /// (the Neo4j `params` config option). The iterate
        /// row's bindings take precedence on key collision.
        extra_params: std::collections::HashMap<String, crate::ast::Expr>,
        /// Number of retries to attempt for a failed batch
        /// before recording it as failed. Default 0 (no
        /// retries). Maps to Neo4j's `retries` config key.
        retries: i64,
        /// Maximum number of failed-parameter snapshots
        /// captured per error message and surfaced in the
        /// `failedParams` result column. -1 means unlimited
        /// (default in Neo4j); 0 disables capture; positive
        /// values cap the per-error sample list length.
        failed_params_cap: i64,
        /// `iterateList` config: when true, the action runs
        /// ONCE per batch with the batch's input rows passed
        /// as a single list parameter `$_batch` (each element
        /// is a Map of the row's RETURN columns). The action
        /// is expected to `UNWIND $_batch AS row` to process.
        /// When false (default), the action runs once per
        /// input row with the row's columns as `$column`
        /// params. Defaults to false here for ergonomic
        /// reasons — `$column` substitution is the simpler
        /// mental model — but users following Neo4j-flavour
        /// migration scripts can flip it on.
        iterate_list: bool,
    },
    /// Per-input-row left-join wrapper used by `apply_optional_match`
    /// when the OPTIONAL MATCH body is a multi-hop pattern. Runs
    /// `body` as a fresh sub-plan seeded with each outer row, then:
    /// * if the body produces one or more rows, forwards each merged
    ///   with the outer bindings (inner-join style, like
    ///   `CallSubquery`);
    /// * if the body produces zero rows, emits the outer row once
    ///   with every variable in `null_vars` bound to `Value::Null`.
    ///
    /// Solves the "multi-hop OPTIONAL MATCH shouldn't null-fallback
    /// per hop" problem: using per-hop OptionalEdgeExpand leaks
    /// partial matches (b bound, c null) that openCypher forbids —
    /// either the full pattern matches or the whole thing becomes
    /// null.
    OptionalApply {
        input: Box<LogicalPlan>,
        body: Box<LogicalPlan>,
        null_vars: Vec<String>,
    },
    /// Invocation of a registered procedure — `CALL ns.name[(args)]
    /// [YIELD cols]`. Distinct from [`LogicalPlan::CallSubquery`]
    /// (which runs a Cypher query body), a procedure call pulls
    /// rows from an external registry looked up by the executor at
    /// run time.
    ///
    /// `input` is `None` for a standalone call (the entire query is
    /// this CALL); `Some(_)` when the call is embedded mid-query,
    /// in which case each upstream row cross-products with the
    /// procedure's matching rows.
    ///
    /// `args` is `None` for the implicit-args form (`CALL ns.name`)
    /// where argument values come from the per-query `$`-parameter
    /// map. `Some(exprs)` carries the explicit argument expressions.
    ///
    /// `yield_spec` is `None` when no YIELD keyword appeared (valid
    /// only standalone: projects every declared output column).
    /// `Some(YieldSpec::Star)` is `YIELD *` (also standalone only).
    /// `Some(YieldSpec::Items(...))` is the named projection form.
    ///
    /// `standalone` mirrors the parser's context detection so the
    /// executor can cheaply reject constructs that are only legal
    /// in one form or the other.
    ProcedureCall {
        input: Option<Box<LogicalPlan>>,
        qualified_name: Vec<String>,
        args: Option<Vec<Expr>>,
        yield_spec: Option<crate::ast::YieldSpec>,
        standalone: bool,
    },
    LoadCsv {
        input: Option<Box<LogicalPlan>>,
        path_expr: Expr,
        var: String,
        with_headers: bool,
    },
    Foreach {
        input: Box<LogicalPlan>,
        var: String,
        list_expr: Expr,
        set_assignments: Vec<SetAssignment>,
        remove_items: Vec<RemoveSpec>,
    },
    /// Placeholder leaf used as the seed producer inside CALL { }
    /// bodies when the first clause is WITH (importing outer
    /// bindings). At execution time, the `CallSubqueryOp` replaces
    /// this with a single-row operator carrying the outer row.
    SeedRow,
    /// Pass-through projection emitted for `RETURN *` and `WITH *`.
    /// Forwards every row from `input` unchanged.
    Identity {
        input: Box<LogicalPlan>,
    },
    /// Standalone OPTIONAL MATCH semantics: forward all rows from
    /// `input` if it produces at least one; otherwise emit a single
    /// row with `null_vars` bound to Value::Null. Wraps the fresh
    /// scan when OPTIONAL MATCH is the first clause or has no
    /// preceding row stream to left-join against.
    CoalesceNullRow {
        input: Box<LogicalPlan>,
        null_vars: Vec<String>,
    },
    /// Match-or-create a single node. If at least one node matches the
    /// `(labels, properties)` pattern, returns one row per match (binding
    /// the existing node to `var`). If none match, creates exactly one
    /// node with the given labels + properties and returns one row.
    /// Property values are `Expr` so they can carry parameters; the
    /// executor evaluates them once at the start of execution.
    ///
    /// When `input` is `None`, `MergeNode` is a top-level producer —
    /// the merge logic runs once and emits its rows directly.
    ///
    /// When `input` is `Some`, `MergeNode` is a mid-chain clause —
    /// the merge logic still runs *once* (scan + maybe-create), and
    /// then every input row from the inner operator cross-joins
    /// with the merged node(s). This matches openCypher semantics
    /// for `MATCH ... MERGE ... RETURN` where the MERGE side has
    /// no references to input variables: the node is created on
    /// the first iteration and reused for every subsequent input
    /// row. Running the merge exactly once also sidesteps the
    /// "newly-created node invisible to a re-scan in the same
    /// query" issue caused by the buffered writer pattern.
    MergeNode {
        input: Option<Box<LogicalPlan>>,
        var: String,
        labels: Vec<String>,
        properties: Vec<(String, Expr)>,
        /// `ON CREATE SET ...` assignments — applied to the
        /// freshly-created node when MERGE took the create path.
        /// Empty when the user wrote no `ON CREATE` clause.
        on_create: Vec<SetAssignment>,
        /// `ON MATCH SET ...` assignments — applied to every
        /// matched node when MERGE took the match path. Empty
        /// when the user wrote no `ON MATCH` clause.
        on_match: Vec<SetAssignment>,
    },
    /// Match-or-create a relationship between two bound
    /// endpoints. For each row from `input`, reads the
    /// `src_var` and `dst_var` node bindings, scans outgoing
    /// edges from src, and either binds an existing edge that
    /// targets dst with the matching type or creates a fresh
    /// one. `ON CREATE SET` applies only on creation, `ON
    /// MATCH SET` applies only when an existing edge was
    /// found. v1 requires both endpoints to be already in
    /// scope and the relationship to be a single directed
    /// hop with an explicit type.
    MergeEdge {
        input: Box<LogicalPlan>,
        edge_var: String,
        src_var: String,
        dst_var: String,
        edge_type: String,
        /// True when the pattern's arrow is undirected (`-[r]-`) —
        /// an existing edge in either direction counts as a match
        /// and the newly-created edge is emitted in the src→dst
        /// direction (matching Neo4j's tie-breaking behavior).
        undirected: bool,
        /// Inline edge property filter (`[r:T {year: 1988}]`) — an
        /// existing edge matches only if all of these properties
        /// equal the bound values. When the create branch fires,
        /// the newly-synthesized edge is stamped with this map so
        /// a later MATCH finds the same shape.
        properties: Vec<(String, Expr)>,
        on_create: Vec<SetAssignment>,
        on_match: Vec<SetAssignment>,
    },
    /// Evaluate `expr` once, cast it to a list, and emit one row per element
    /// binding the element to `var`. The expression is evaluated against an
    /// empty row — this is the top-level UNWIND producer, used when UNWIND
    /// is the query's first clause. Chained UNWINDs use [`LogicalPlan::UnwindChain`]
    /// instead so the expression can reference earlier bindings.
    Unwind {
        var: String,
        expr: Expr,
    },
    /// Per-row UNWIND: for each input row, evaluate `expr` against that
    /// row, expect a list, and emit one output row per element — each
    /// output row inherits every binding from the input row plus a new
    /// binding of `var` to the element. Empty / null lists drop the
    /// input row. Emitted when `UNWIND` appears as a mid-query reading
    /// clause after MATCH / WITH / MERGE / another UNWIND.
    UnwindChain {
        input: Box<LogicalPlan>,
        var: String,
        expr: Expr,
    },
    /// Equality lookup through a property index. The executor
    /// evaluates each `value` (literals or parameters) at run time,
    /// converts to concrete [`meshdb_core::Property`]s, then calls
    /// `reader.nodes_by_properties(label, properties, values)`.
    /// `properties` / `values` are parallel slices of equal length —
    /// length 1 is the classic single-property seek; length > 1 is a
    /// composite seek whose tuple must match the stored index's
    /// declared prefix in the same order. Emitted only when the
    /// planner context confirms a matching index exists; otherwise
    /// the planner falls back to `NodeScanByLabels` + `Filter`.
    IndexSeek {
        var: String,
        label: String,
        properties: Vec<String>,
        values: Vec<Expr>,
    },
    /// Axis-aligned bbox range query through a point / spatial
    /// index. Emitted when a WHERE conjunct matches a bbox or
    /// distance-radius predicate on an indexed `(label, property)`
    /// pair. The operator evaluates the bounds at open-time and
    /// drives `reader.nodes_in_bbox(label, property, srid, ...)`.
    /// The bbox the operator feeds to the reader is always a *super*
    /// set of the query shape — a circle maps to its enclosing
    /// square — so any predicate whose shape is tighter than the
    /// enclosing bbox (the distance-radius case) stays in a residual
    /// Filter above the seek to cull the overshoot. The `withinbbox`
    /// case doesn't need that Filter because the storage-layer bbox
    /// check is exact.
    PointIndexSeek {
        var: String,
        label: String,
        property: String,
        bounds: PointSeekBounds,
    },
    /// Equality lookup through an edge property index. Relationship
    /// analogue of [`LogicalPlan::IndexSeek`]. Emitted for
    /// `MATCH ()-[r:T {p: v}]-()` when both endpoints are unbound
    /// and a `(T, p)` edge index is registered. The executor calls
    /// `reader.edges_by_property(edge_type, property, value)`, then
    /// hydrates each matching edge and binds `edge_var`, `src_var`,
    /// `dst_var` on the output row.
    ///
    /// `direction` preserves the pattern's arrow so a directed
    /// pattern only yields edges where source/target alignment
    /// matches; `Direction::Both` accepts either orientation.
    /// `residual_properties` carries any non-indexed pattern
    /// property equalities that still need evaluating per edge —
    /// `MATCH ()-[r:T {indexed: 1, other: 'x'}]-()` seeks on
    /// `indexed` and filters the returned edges by `other`.
    EdgeSeek {
        edge_var: String,
        src_var: String,
        dst_var: String,
        edge_type: String,
        property: String,
        value: Expr,
        direction: Direction,
        residual_properties: Vec<(String, Expr)>,
    },
    /// Relationship-scope bbox / distance-radius spatial seek.
    /// Emitted when a WHERE conjunct matches `point.withinbbox(r.p,
    /// lo, hi)` or `point.distance(r.p, center) <[=] r` against an
    /// edge variable from an unbound-endpoints pattern, and an
    /// edge point index on `(edge_type, property)` is registered.
    /// Same `Corners` / `Radius` duality as [`LogicalPlan::PointIndexSeek`];
    /// the distance-variant caller keeps the distance predicate as
    /// a residual Filter so the enclosing-bbox overshoot gets culled.
    ///
    /// Direction preserves the pattern's arrow — a directed pattern
    /// only yields edges where source/target alignment matches;
    /// `Direction::Both` accepts either orientation (edges emit once
    /// per orientation, matching `EdgeSeek`).
    EdgePointIndexSeek {
        edge_var: String,
        src_var: String,
        dst_var: String,
        edge_type: String,
        property: String,
        direction: Direction,
        bounds: PointSeekBounds,
    },
    /// Schema DDL — declare a new node property index. Has no input
    /// and produces no rows. The executor short-circuits this before
    /// constructing the operator pipeline, dispatching to the
    /// composite-capable `Store::create_property_index_composite`
    /// call. `properties` carries the ordered list from the DDL
    /// surface; length 1 is the common case, length > 1 is a
    /// composite tuple index.
    CreatePropertyIndex {
        label: String,
        properties: Vec<String>,
    },
    /// Schema DDL — tear down a node property index. Same dispatch
    /// pattern as [`LogicalPlan::CreatePropertyIndex`].
    DropPropertyIndex {
        label: String,
        properties: Vec<String>,
    },
    /// Schema DDL — declare a new edge property index. Relationship-
    /// scope analogue of [`LogicalPlan::CreatePropertyIndex`].
    CreateEdgePropertyIndex {
        edge_type: String,
        properties: Vec<String>,
    },
    /// Schema DDL — tear down an edge property index. Mirror of
    /// [`LogicalPlan::DropPropertyIndex`].
    DropEdgePropertyIndex {
        edge_type: String,
        properties: Vec<String>,
    },
    /// Schema DDL — emit one row per registered property index
    /// describing `(scope, target, property)`. The executor merges
    /// rows from both the node and edge index registries so a single
    /// `SHOW INDEXES` surfaces everything.
    ShowPropertyIndexes,
    /// Schema DDL — declare a node point / spatial index on
    /// `(label, property)`. Single-property and node-scope only in
    /// v1; composite / relationship variants would land as new
    /// plan variants rather than widening this one.
    CreatePointIndex {
        label: String,
        property: String,
    },
    /// Schema DDL — tear down a point index. Mirror of
    /// [`LogicalPlan::CreatePointIndex`].
    DropPointIndex {
        label: String,
        property: String,
    },
    /// Schema DDL — declare an edge / relationship-scope point index
    /// on `(edge_type, property)`. Relationship-scope analogue of
    /// [`LogicalPlan::CreatePointIndex`]; lives in its own CF so
    /// node-scope and edge-scope spatial entries don't alias.
    CreateEdgePointIndex {
        edge_type: String,
        property: String,
    },
    /// Schema DDL — tear down an edge point index. Mirror of
    /// [`LogicalPlan::CreateEdgePointIndex`].
    DropEdgePointIndex {
        edge_type: String,
        property: String,
    },
    /// Schema DDL — emit one row per registered point index. Kept
    /// separate from [`LogicalPlan::ShowPropertyIndexes`] so `SHOW
    /// POINT INDEXES` and `SHOW INDEXES` produce independent row
    /// streams with scope-appropriate column sets. Merges node- and
    /// edge-scoped point indexes into one row stream, disambiguated
    /// by the `scope` column.
    ShowPointIndexes,
    /// Schema DDL — declare a new property constraint. Same short-
    /// circuit dispatch pattern as [`LogicalPlan::CreatePropertyIndex`]:
    /// the executor handles the mutation outside the operator pipeline.
    /// `name` is `None` when the source omitted it; the storage layer
    /// fills in a deterministic default.
    CreatePropertyConstraint {
        name: Option<String>,
        scope: ConstraintScope,
        properties: Vec<String>,
        kind: ConstraintKind,
        if_not_exists: bool,
    },
    /// Schema DDL — tear down a constraint by name. `if_exists`
    /// makes a missing constraint a no-op instead of an error.
    DropPropertyConstraint {
        name: String,
        if_exists: bool,
    },
    /// Schema DDL — emit one row per registered constraint describing
    /// `name`, `label`, `property`, `type`. Rows are built from
    /// `Store::list_property_constraints`.
    ShowPropertyConstraints,
    /// Concatenate row streams from multiple sub-plans, optionally
    /// deduping across the union. Each branch is planned
    /// independently and must project the same set of columns
    /// (name + order) as the first branch. `all = true` corresponds
    /// to `UNION ALL` and skips the dedup; `all = false` does a
    /// row-key dedup across the entire combined stream.
    Union {
        branches: Vec<LogicalPlan>,
        all: bool,
    },
    /// Assemble a `Value::Path` into the row stream from the
    /// sequence of node / edge variables a pattern binds. Emitted
    /// once per row by `plan_pattern` when the source `Pattern`
    /// carries a `path_var`. `node_vars` holds the ordered list
    /// of per-hop node bindings (start + one per hop, so
    /// `node_vars.len() == edge_vars.len() + 1`). `edge_vars` has
    /// one entry per hop, referencing the edge variable the
    /// corresponding `EdgeExpand` binds into the row.
    BindPath {
        input: Box<LogicalPlan>,
        path_var: String,
        node_vars: Vec<String>,
        edge_vars: Vec<String>,
    },
    /// `MATCH p = shortestPath((src)-[:R*..N]->(dst))`. BFS
    /// from the (already-bound) `src_var` node to the
    /// (already-bound) `dst_var` node, filtering edges by
    /// `edge_type` and walking up to `max_hops` steps. Emits
    /// a row per input binding with `path_var` set to a
    /// `Value::Path` carrying the traversed node/edge
    /// sequence. Input rows where BFS finds no path are
    /// dropped — matching Cypher's `MATCH` semantics for an
    /// unsatisfiable pattern.
    ///
    /// v1 restriction: both endpoints must be bound in the
    /// input plan, only one edge type (or none) is accepted,
    /// and `max_hops` is required (no unbounded `*`).
    ShortestPath {
        input: Box<LogicalPlan>,
        src_var: String,
        dst_var: String,
        path_var: String,
        edge_types: Vec<String>,
        direction: Direction,
        max_hops: u64,
        kind: ShortestKind,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateNodeSpec {
    New {
        var: Option<String>,
        labels: Vec<String>,
        /// Property expressions. The grammar restricts these to literals
        /// and parameters (see `property_value` in cypher.pest), so
        /// `CreatePathOp` evaluates them against an empty row plus the
        /// per-query param map.
        properties: Vec<(String, Expr)>,
    },
    Reference(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateEdgeSpec {
    pub var: Option<String>,
    pub edge_type: String,
    pub src_idx: usize,
    pub dst_idx: usize,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetAssignment {
    Property {
        var: String,
        key: String,
        value: Expr,
    },
    Labels {
        var: String,
        labels: Vec<String>,
    },
    Replace {
        var: String,
        properties: Vec<(String, Expr)>,
    },
    Merge {
        var: String,
        properties: Vec<(String, Expr)>,
    },
    /// `SET x = y` (`replace = true`) / `SET x += y` (`replace = false`)
    /// where `y` is a bound node/edge identifier or a parameter —
    /// copies `y`'s properties onto `x`. Evaluated at runtime; the
    /// executor extracts the property map from the value.
    ReplaceFromExpr {
        var: String,
        source: Expr,
        replace: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoveSpec {
    Property { var: String, key: String },
    Labels { var: String, labels: Vec<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateSpec {
    pub alias: String,
    pub function: AggregateFn,
    pub arg: AggregateArg,
    /// Second argument, used by aggregates that need a constant
    /// parameter alongside the per-row value. Right now this is just
    /// `percentileDisc` / `percentileCont` — both take the fraction
    /// in `[0.0, 1.0]` as their second argument.
    pub extra_arg: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
    StDev,
    StDevP,
    PercentileDisc,
    PercentileCont,
    /// `apoc.agg.first(expr)` — first non-null value seen.
    ApocFirst,
    /// `apoc.agg.last(expr)` — last non-null value seen.
    ApocLast,
    /// `apoc.agg.nth(expr, n)` — n-th (0-indexed) non-null value.
    /// `n` is a constant stashed in [`AggregateSpec::extra_arg`].
    ApocNth,
    /// `apoc.agg.median(expr)` — median of numeric values.
    ApocMedian,
    /// `apoc.agg.product(expr)` — multiplicative aggregate, like
    /// [`AggregateFn::Sum`] but returning the running product.
    /// Identity on an empty stream is 1 (Int64).
    ApocProduct,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateArg {
    Star,
    Expr(Expr),
    DistinctExpr(Expr),
}

fn aggregate_fn_from_name(name: &str) -> Option<AggregateFn> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggregateFn::Count),
        "sum" => Some(AggregateFn::Sum),
        "avg" => Some(AggregateFn::Avg),
        "min" => Some(AggregateFn::Min),
        "max" => Some(AggregateFn::Max),
        "collect" => Some(AggregateFn::Collect),
        "stdev" => Some(AggregateFn::StDev),
        "stdevp" => Some(AggregateFn::StDevP),
        "percentiledisc" => Some(AggregateFn::PercentileDisc),
        "percentilecont" => Some(AggregateFn::PercentileCont),
        "apoc.agg.first" => Some(AggregateFn::ApocFirst),
        "apoc.agg.last" => Some(AggregateFn::ApocLast),
        "apoc.agg.nth" => Some(AggregateFn::ApocNth),
        "apoc.agg.median" => Some(AggregateFn::ApocMedian),
        "apoc.agg.product" => Some(AggregateFn::ApocProduct),
        _ => None,
    }
}

/// Plan a statement with no catalog context — the executor will
/// never see an `IndexSeek` through this entry point. Use
/// [`plan_with_context`] when the caller wants index-aware planning.
pub fn plan(statement: &Statement) -> Result<LogicalPlan> {
    plan_with_context(statement, &PlannerContext::default())
}

/// Plan a statement with awareness of the backing store's registered
/// property indexes. `MATCH` rewrites pattern-property equality to
/// [`LogicalPlan::IndexSeek`] when a matching index exists in `ctx`;
/// `Statement::CreateIndex` / `DropIndex` / `ShowIndexes` lower
/// directly to the DDL plan variants.
pub fn plan_with_context(statement: &Statement, ctx: &PlannerContext) -> Result<LogicalPlan> {
    let plan = match statement {
        Statement::Create(c) => plan_create(c)?,
        Statement::Match(m) => plan_match(m, ctx)?,
        Statement::Unwind(u) => plan_unwind(u)?,
        Statement::Return(r) => plan_return_only(r)?,
        Statement::CreateIndex(IndexDdl { scope, properties }) => {
            if properties.is_empty() {
                return Err(Error::Plan(
                    "CREATE INDEX requires at least one property".into(),
                ));
            }
            match scope {
                IndexScope::Node(label) => LogicalPlan::CreatePropertyIndex {
                    label: label.clone(),
                    properties: properties.clone(),
                },
                IndexScope::Relationship(edge_type) => LogicalPlan::CreateEdgePropertyIndex {
                    edge_type: edge_type.clone(),
                    properties: properties.clone(),
                },
            }
        }
        Statement::DropIndex(IndexDdl { scope, properties }) => {
            if properties.is_empty() {
                return Err(Error::Plan(
                    "DROP INDEX requires at least one property".into(),
                ));
            }
            match scope {
                IndexScope::Node(label) => LogicalPlan::DropPropertyIndex {
                    label: label.clone(),
                    properties: properties.clone(),
                },
                IndexScope::Relationship(edge_type) => LogicalPlan::DropEdgePropertyIndex {
                    edge_type: edge_type.clone(),
                    properties: properties.clone(),
                },
            }
        }
        Statement::ShowIndexes => LogicalPlan::ShowPropertyIndexes,
        Statement::CreatePointIndex(pd) => match &pd.scope {
            IndexScope::Node(label) => LogicalPlan::CreatePointIndex {
                label: label.clone(),
                property: pd.property.clone(),
            },
            IndexScope::Relationship(edge_type) => LogicalPlan::CreateEdgePointIndex {
                edge_type: edge_type.clone(),
                property: pd.property.clone(),
            },
        },
        Statement::DropPointIndex(pd) => match &pd.scope {
            IndexScope::Node(label) => LogicalPlan::DropPointIndex {
                label: label.clone(),
                property: pd.property.clone(),
            },
            IndexScope::Relationship(edge_type) => LogicalPlan::DropEdgePointIndex {
                edge_type: edge_type.clone(),
                property: pd.property.clone(),
            },
        },
        Statement::ShowPointIndexes => LogicalPlan::ShowPointIndexes,
        Statement::CreateConstraint(CreateConstraintStmt {
            name,
            scope,
            properties,
            kind,
            if_not_exists,
        }) => LogicalPlan::CreatePropertyConstraint {
            name: name.clone(),
            scope: scope.clone(),
            properties: properties.clone(),
            kind: *kind,
            if_not_exists: *if_not_exists,
        },
        Statement::DropConstraint(DropConstraintStmt { name, if_exists }) => {
            LogicalPlan::DropPropertyConstraint {
                name: name.clone(),
                if_exists: *if_exists,
            }
        }
        Statement::ShowConstraints => LogicalPlan::ShowPropertyConstraints,
        Statement::Union(u) => plan_union(u, ctx)?,
        Statement::Explain(inner) | Statement::Profile(inner) => {
            return plan_with_context(inner, ctx)
        }
        Statement::CallProcedure(pc) => LogicalPlan::ProcedureCall {
            input: None,
            qualified_name: pc.qualified_name.clone(),
            args: pc.args.clone(),
            yield_spec: pc.yield_spec.clone(),
            standalone: true,
        },
    };
    validate_pattern_predicates(&plan)?;
    Ok(plan)
}

pub fn format_plan(plan: &LogicalPlan) -> String {
    let mut buf = String::new();
    format_plan_inner(plan, &mut buf, 0);
    buf
}

fn format_plan_inner(plan: &LogicalPlan, buf: &mut String, depth: usize) {
    let indent = "  ".repeat(depth);
    match plan {
        LogicalPlan::NodeScanAll { var } => {
            buf.push_str(&format!("{indent}NodeScanAll({var})\n"));
        }
        LogicalPlan::NodeScanByLabels { var, labels } => {
            buf.push_str(&format!(
                "{indent}NodeScanByLabels({var}:{labels})\n",
                labels = labels.join(":")
            ));
        }
        LogicalPlan::EdgeExpand {
            input,
            src_var,
            dst_var,
            edge_types,
            direction,
            ..
        } => {
            let dir = format_dir(direction);
            let et = if edge_types.is_empty() {
                "*".to_string()
            } else {
                edge_types.join("|")
            };
            buf.push_str(&format!(
                "{indent}EdgeExpand({src_var}){dir}[:{et}]{dir_end}({dst_var})\n",
                dir_end = if matches!(direction, Direction::Incoming) {
                    ""
                } else {
                    ">"
                }
            ));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::OptionalEdgeExpand {
            input,
            src_var,
            dst_var,
            edge_types,
            direction,
            ..
        } => {
            let et = if edge_types.is_empty() {
                "*".to_string()
            } else {
                edge_types.join("|")
            };
            buf.push_str(&format!(
                "{indent}OptionalEdgeExpand({src_var})-[:{et}]->({dst_var})\n"
            ));
            let _ = direction;
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::VarLengthExpand {
            input,
            src_var,
            dst_var,
            edge_types,
            min_hops,
            max_hops,
            ..
        } => {
            let et = if edge_types.is_empty() {
                "*".to_string()
            } else {
                edge_types.join("|")
            };
            buf.push_str(&format!(
                "{indent}VarLengthExpand({src_var})-[:{et}*{min_hops}..{max_hops}]->({dst_var})\n"
            ));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Filter { input, predicate } => {
            buf.push_str(&format!("{indent}Filter({predicate:?})\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Project { input, items } => {
            let cols: Vec<String> = items.iter().map(|i| render_expr_key(&i.expr)).collect();
            buf.push_str(&format!("{indent}Project({})\n", cols.join(", ")));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
            ..
        } => {
            let gk: Vec<String> = group_keys
                .iter()
                .map(|i| render_expr_key(&i.expr))
                .collect();
            buf.push_str(&format!(
                "{indent}Aggregate(keys=[{}], aggs={})\n",
                gk.join(", "),
                aggregates.len()
            ));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::OrderBy { input, sort_items } => {
            buf.push_str(&format!("{indent}OrderBy({} items)\n", sort_items.len()));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Skip { input, count } => {
            buf.push_str(&format!("{indent}Skip({})\n", render_expr_key(count)));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Limit { input, count } => {
            buf.push_str(&format!("{indent}Limit({})\n", render_expr_key(count)));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Distinct { input } => {
            buf.push_str(&format!("{indent}Distinct\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::CartesianProduct { left, right } => {
            buf.push_str(&format!("{indent}CartesianProduct\n"));
            format_plan_inner(left, buf, depth + 1);
            format_plan_inner(right, buf, depth + 1);
        }
        LogicalPlan::CreatePath { input, .. } => {
            buf.push_str(&format!("{indent}CreatePath\n"));
            if let Some(i) = input {
                format_plan_inner(i, buf, depth + 1);
            }
        }
        LogicalPlan::Delete {
            input,
            detach,
            vars,
            ..
        } => {
            let kind = if *detach { "DetachDelete" } else { "Delete" };
            buf.push_str(&format!("{indent}{kind}({})\n", vars.join(", ")));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::SetProperty { input, .. } => {
            buf.push_str(&format!("{indent}SetProperty\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Remove { input, .. } => {
            buf.push_str(&format!("{indent}Remove\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::LoadCsv {
            input,
            var,
            with_headers,
            ..
        } => {
            let h = if *with_headers { " WITH HEADERS" } else { "" };
            buf.push_str(&format!("{indent}LoadCsv({var}{h})\n"));
            if let Some(i) = input {
                format_plan_inner(i, buf, depth + 1);
            }
        }
        LogicalPlan::Foreach { input, var, .. } => {
            buf.push_str(&format!("{indent}Foreach({var})\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::CallSubquery { input, body } => {
            buf.push_str(&format!("{indent}CallSubquery\n"));
            buf.push_str(&format!("{indent}  body:\n"));
            format_plan_inner(body, buf, depth + 2);
            buf.push_str(&format!("{indent}  input:\n"));
            format_plan_inner(input, buf, depth + 2);
        }
        LogicalPlan::CallSubqueryInTransactions {
            input,
            body,
            batch_size,
            error_mode,
            report_status_as,
        } => {
            let report_str = report_status_as
                .as_deref()
                .map(|v| format!(", report_status_as={v}"))
                .unwrap_or_default();
            buf.push_str(&format!(
                "{indent}CallSubqueryInTransactions(batch_size={batch_size}, on_error={error_mode:?}{report_str})\n"
            ));
            buf.push_str(&format!("{indent}  body:\n"));
            format_plan_inner(body, buf, depth + 2);
            buf.push_str(&format!("{indent}  input:\n"));
            format_plan_inner(input, buf, depth + 2);
        }
        LogicalPlan::ApocPeriodicIterate {
            iterate,
            action,
            batch_size,
            ..
        } => {
            buf.push_str(&format!(
                "{indent}ApocPeriodicIterate(batch_size={batch_size})\n"
            ));
            buf.push_str(&format!("{indent}  iterate:\n"));
            format_plan_inner(iterate, buf, depth + 2);
            buf.push_str(&format!("{indent}  action:\n"));
            format_plan_inner(action, buf, depth + 2);
        }
        LogicalPlan::OptionalApply {
            input,
            body,
            null_vars,
        } => {
            buf.push_str(&format!(
                "{indent}OptionalApply(nulls=[{}])\n",
                null_vars.join(", ")
            ));
            buf.push_str(&format!("{indent}  body:\n"));
            format_plan_inner(body, buf, depth + 2);
            buf.push_str(&format!("{indent}  input:\n"));
            format_plan_inner(input, buf, depth + 2);
        }
        LogicalPlan::ProcedureCall {
            qualified_name,
            input,
            ..
        } => {
            buf.push_str(&format!(
                "{indent}ProcedureCall({})\n",
                qualified_name.join(".")
            ));
            if let Some(inp) = input {
                format_plan_inner(inp, buf, depth + 1);
            }
        }
        LogicalPlan::IndexSeek {
            var,
            label,
            properties,
            ..
        } => {
            buf.push_str(&format!(
                "{indent}IndexSeek({var}:{label}.{})\n",
                properties.join(",")
            ));
        }
        LogicalPlan::PointIndexSeek {
            var,
            label,
            property,
            bounds,
        } => {
            let shape = match bounds {
                PointSeekBounds::Corners { .. } => "bbox",
                PointSeekBounds::Radius { .. } => "radius",
            };
            buf.push_str(&format!(
                "{indent}PointIndexSeek({var}:{label}.{property}, {shape})\n"
            ));
        }
        LogicalPlan::EdgeSeek {
            edge_var,
            src_var,
            dst_var,
            edge_type,
            property,
            direction,
            ..
        } => {
            let dir = format_dir(direction);
            let dir_end = if matches!(direction, Direction::Incoming) {
                ""
            } else {
                ">"
            };
            buf.push_str(&format!(
                "{indent}EdgeSeek({src_var}){dir}[{edge_var}:{edge_type}.{property}]{dir_end}({dst_var})\n"
            ));
        }
        LogicalPlan::EdgePointIndexSeek {
            edge_var,
            src_var,
            dst_var,
            edge_type,
            property,
            direction,
            bounds,
        } => {
            let dir = format_dir(direction);
            let dir_end = if matches!(direction, Direction::Incoming) {
                ""
            } else {
                ">"
            };
            let shape = match bounds {
                PointSeekBounds::Corners { .. } => "bbox",
                PointSeekBounds::Radius { .. } => "radius",
            };
            buf.push_str(&format!(
                "{indent}EdgePointIndexSeek({src_var}){dir}[{edge_var}:{edge_type}.{property}, {shape}]{dir_end}({dst_var})\n"
            ));
        }
        LogicalPlan::SeedRow => {
            buf.push_str(&format!("{indent}SeedRow\n"));
        }
        LogicalPlan::Identity { input } => {
            buf.push_str(&format!("{indent}Identity(*)\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::CoalesceNullRow { input, null_vars } => {
            buf.push_str(&format!(
                "{indent}CoalesceNullRow({})\n",
                null_vars.join(", ")
            ));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Unwind { var, .. } => {
            buf.push_str(&format!("{indent}Unwind(AS {var})\n"));
        }
        LogicalPlan::UnwindChain { input, var, .. } => {
            buf.push_str(&format!("{indent}UnwindChain(AS {var})\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::Union { branches, all } => {
            let kind = if *all { "UnionAll" } else { "Union" };
            buf.push_str(&format!("{indent}{kind}({} branches)\n", branches.len()));
            for b in branches {
                format_plan_inner(b, buf, depth + 1);
            }
        }
        LogicalPlan::BindPath {
            input, path_var, ..
        } => {
            buf.push_str(&format!("{indent}BindPath({path_var})\n"));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::ShortestPath {
            input,
            src_var,
            dst_var,
            path_var,
            kind,
            max_hops,
            ..
        } => {
            let kind_str = match kind {
                ShortestKind::Shortest => "shortestPath",
                ShortestKind::AllShortest => "allShortestPaths",
            };
            buf.push_str(&format!(
                "{indent}{kind_str}({src_var})->({dst_var}) AS {path_var} [..{max_hops}]\n"
            ));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::MergeNode {
            input, var, labels, ..
        } => {
            buf.push_str(&format!(
                "{indent}MergeNode({var}:{labels})\n",
                labels = labels.join(":")
            ));
            if let Some(i) = input {
                format_plan_inner(i, buf, depth + 1);
            }
        }
        LogicalPlan::MergeEdge {
            input,
            src_var,
            dst_var,
            edge_type,
            ..
        } => {
            buf.push_str(&format!(
                "{indent}MergeEdge({src_var})-[:{edge_type}]->({dst_var})\n"
            ));
            format_plan_inner(input, buf, depth + 1);
        }
        LogicalPlan::CreatePropertyIndex { label, properties } => {
            buf.push_str(&format!(
                "{indent}CreatePropertyIndex({label}.{})\n",
                properties.join(",")
            ));
        }
        LogicalPlan::DropPropertyIndex { label, properties } => {
            buf.push_str(&format!(
                "{indent}DropPropertyIndex({label}.{})\n",
                properties.join(",")
            ));
        }
        LogicalPlan::CreateEdgePropertyIndex {
            edge_type,
            properties,
        } => {
            buf.push_str(&format!(
                "{indent}CreateEdgePropertyIndex(:{edge_type}.{})\n",
                properties.join(",")
            ));
        }
        LogicalPlan::DropEdgePropertyIndex {
            edge_type,
            properties,
        } => {
            buf.push_str(&format!(
                "{indent}DropEdgePropertyIndex(:{edge_type}.{})\n",
                properties.join(",")
            ));
        }
        LogicalPlan::ShowPropertyIndexes => {
            buf.push_str(&format!("{indent}ShowPropertyIndexes\n"));
        }
        LogicalPlan::CreatePointIndex { label, property } => {
            buf.push_str(&format!("{indent}CreatePointIndex({label}.{property})\n"));
        }
        LogicalPlan::DropPointIndex { label, property } => {
            buf.push_str(&format!("{indent}DropPointIndex({label}.{property})\n"));
        }
        LogicalPlan::CreateEdgePointIndex {
            edge_type,
            property,
        } => {
            buf.push_str(&format!(
                "{indent}CreateEdgePointIndex(:{edge_type}.{property})\n"
            ));
        }
        LogicalPlan::DropEdgePointIndex {
            edge_type,
            property,
        } => {
            buf.push_str(&format!(
                "{indent}DropEdgePointIndex(:{edge_type}.{property})\n"
            ));
        }
        LogicalPlan::ShowPointIndexes => {
            buf.push_str(&format!("{indent}ShowPointIndexes\n"));
        }
        LogicalPlan::CreatePropertyConstraint {
            name,
            scope,
            properties,
            kind,
            ..
        } => {
            let kind_str = match kind {
                ConstraintKind::Unique => "UNIQUE".to_string(),
                ConstraintKind::NotNull => "NOT NULL".to_string(),
                ConstraintKind::NodeKey => "NODE KEY".to_string(),
                ConstraintKind::PropertyType(t) => format!(
                    ":: {}",
                    match t {
                        crate::ast::PropertyType::String => "STRING",
                        crate::ast::PropertyType::Integer => "INTEGER",
                        crate::ast::PropertyType::Float => "FLOAT",
                        crate::ast::PropertyType::Boolean => "BOOLEAN",
                    }
                ),
            };
            let name_str = name.as_deref().unwrap_or("<auto>");
            let props = properties.join(", ");
            let scope_str = match scope {
                ConstraintScope::Node(l) => format!("Node({l})"),
                ConstraintScope::Relationship(t) => format!("Rel({t})"),
            };
            buf.push_str(&format!(
                "{indent}CreatePropertyConstraint({name_str}: {scope_str}.({props}) IS {kind_str})\n"
            ));
        }
        LogicalPlan::DropPropertyConstraint { name, if_exists } => {
            let suffix = if *if_exists { " IF EXISTS" } else { "" };
            buf.push_str(&format!("{indent}DropPropertyConstraint({name}){suffix}\n"));
        }
        LogicalPlan::ShowPropertyConstraints => {
            buf.push_str(&format!("{indent}ShowPropertyConstraints\n"));
        }
    }
}

fn format_dir(d: &Direction) -> &'static str {
    match d {
        Direction::Outgoing => "-",
        Direction::Incoming => "<-",
        Direction::Both => "-",
    }
}

/// Walk the lowered plan and check every `Expr::PatternExists`
/// subexpression against the v1 pattern-predicate restrictions:
///
/// - Start variable must be named (bound by the outer row).
/// - Pattern must carry at least one hop (the parser enforces
///   this too, but a plan-time belt-and-suspenders check
///   catches any future grammar relaxation).
/// - No `path_var` on the pattern.
/// - No variable-length hops.
///
/// Violations produce `Error::Plan` with a message pointing
/// at the specific restriction, so driver errors are
/// actionable rather than a generic "pattern predicate
/// rejected".
fn validate_pattern_predicates(plan: &LogicalPlan) -> Result<()> {
    walk_plan_exprs(plan, &mut |expr| {
        walk_expr(expr, &mut |e| {
            if let Expr::PatternExists(pattern) = e {
                validate_pattern_predicate(pattern)?;
            }
            if let Expr::ExistsSubquery { body }
            | Expr::CountSubquery { body }
            | Expr::CollectSubquery { body } = e
            {
                validate_subquery_body_is_read_only(body)?;
            }
            Ok(())
        })
    })
}

/// openCypher `InvalidClauseComposition`: the body of an
/// `exists { ... }` / `count { ... }` subquery may only read from
/// the graph — SET / CREATE / DELETE / REMOVE / MERGE / FOREACH
/// are forbidden. Walks the body statement's clauses and terminal
/// tail and rejects any mutation it finds.
fn validate_subquery_body_is_read_only(body: &Statement) -> Result<()> {
    let match_stmt = match body {
        Statement::Match(m) => m,
        // Non-match bodies (Unwind, Return, Union, etc.) can't
        // carry writes at the grammar level — nothing to check.
        _ => return Ok(()),
    };
    for c in &match_stmt.clauses {
        use crate::ast::ReadingClause;
        match c {
            ReadingClause::Create(_)
            | ReadingClause::Set(_)
            | ReadingClause::Delete(_)
            | ReadingClause::Remove(_)
            | ReadingClause::Foreach(_)
            | ReadingClause::Merge(_) => {
                return Err(Error::Plan(
                    "InvalidClauseComposition: updating clauses are not allowed inside an exists / count subquery".into(),
                ));
            }
            _ => {}
        }
    }
    let t = &match_stmt.terminal;
    if !t.create_patterns.is_empty()
        || !t.set_items.is_empty()
        || !t.remove_items.is_empty()
        || t.delete.is_some()
        || t.foreach.is_some()
    {
        return Err(Error::Plan(
            "InvalidClauseComposition: updating clauses are not allowed inside an exists / count subquery".into(),
        ));
    }
    Ok(())
}

fn validate_pattern_predicate(pattern: &Pattern) -> Result<()> {
    if pattern.path_var.is_some() {
        return Err(Error::Plan(
            "path variable binding is not allowed inside a pattern predicate".into(),
        ));
    }
    if pattern.hops.is_empty() {
        return Err(Error::Plan(
            "pattern predicates must have at least one relationship hop".into(),
        ));
    }
    if pattern.start.var.is_none() {
        return Err(Error::Plan(
            "pattern predicate's start node must reference a bound variable".into(),
        ));
    }
    Ok(())
}

/// Every named variable inside a pattern predicate must already be
/// bound in the outer row — pattern predicates can existentially
/// quantify over anonymous positions (`(n)-[]->()`) but they can't
/// *introduce* fresh named bindings. openCypher raises
/// `SyntaxError: UndefinedVariable` at compile time for
/// `WHERE (n)-[r]->(a)` when `r` / `a` weren't bound by an earlier
/// clause. Walk every `PatternExists` the expression carries and
/// check each named position (start, per-hop rel, per-hop target).
fn validate_pattern_predicate_vars_bound(
    expr: &Expr,
    bound: &HashMap<String, VarType>,
) -> Result<()> {
    walk_expr(expr, &mut |e| {
        if let Expr::PatternExists(pattern) = e {
            check_pattern_vars_bound(pattern, bound)?;
        }
        Ok(())
    })
}

fn check_pattern_vars_bound(pattern: &Pattern, bound: &HashMap<String, VarType>) -> Result<()> {
    if let Some(v) = &pattern.start.var {
        if !bound.contains_key(v) {
            return Err(Error::Plan(format!(
                "UndefinedVariable: pattern predicate references unbound variable `{v}`"
            )));
        }
    }
    for hop in &pattern.hops {
        if let Some(v) = &hop.rel.var {
            if !bound.contains_key(v) {
                return Err(Error::Plan(format!(
                    "UndefinedVariable: pattern predicate references unbound variable `{v}`"
                )));
            }
        }
        if let Some(v) = &hop.target.var {
            if !bound.contains_key(v) {
                return Err(Error::Plan(format!(
                    "UndefinedVariable: pattern predicate references unbound variable `{v}`"
                )));
            }
        }
    }
    Ok(())
}

/// `WHERE (n)` or `WHERE n` where `n` is a node / edge / path
/// binding raises `SyntaxError: InvalidArgumentType` in openCypher
/// — a WHERE predicate has to evaluate to a boolean, and a bare
/// graph-element reference doesn't. Reject the narrow shape at
/// plan time so we don't silently coerce and succeed.
fn reject_non_boolean_where_predicate(expr: &Expr, bound: &HashMap<String, VarType>) -> Result<()> {
    if let Expr::Identifier(name) = expr {
        if let Some(vtype) = bound.get(name) {
            if matches!(vtype, VarType::Node | VarType::Edge | VarType::Path) {
                return Err(Error::Plan(format!(
                    "InvalidArgumentType: WHERE predicate `{name}` is a {vtype:?}, not a boolean"
                )));
            }
        }
    }
    Ok(())
}

/// Pattern predicates (`(n)-[r]->(m)` as a boolean expression)
/// are only legal inside WHERE clauses and EXISTS subqueries.
/// RETURN / WITH projections, SET right-hand sides, ORDER BY
/// keys and the like have to use a pattern comprehension
/// (`[(n)-[r]->(m) | r]`) instead — using a bare pattern
/// predicate there raises `SyntaxError: UnexpectedSyntax`.
fn reject_pattern_predicate_in_projection(expr: &Expr, context: &str) -> Result<()> {
    walk_expr(expr, &mut |e| {
        if matches!(e, Expr::PatternExists(_)) {
            return Err(Error::Plan(format!(
                "UnexpectedSyntax: pattern predicate not allowed in {context}"
            )));
        }
        Ok(())
    })
}

/// Looser version of [`validate_pattern_predicate`] for
/// `EXISTS { ... }` subqueries. Relaxes two restrictions that
/// apply to pattern predicates but aren't required by EXISTS:
///
/// - **Start var may be unnamed or unbound.** Uncorrelated
///   `EXISTS { MATCH (:Person) }` is valid — the evaluator
///   enumerates start candidates via the graph reader.
/// - **Zero-hop patterns are allowed.** `EXISTS { MATCH (:Person) }`
///   reduces to "does any Person node exist?"; there's no hop
///   to walk.
///
/// Everything else (no path variables, no variable-length hops)
/// is still rejected — those are v1 evaluator limitations
/// rather than semantic restrictions.
/// Walk every `Expr` that appears in `plan` — predicates on
/// `Filter`, projection expressions on `Project` / `Aggregate`,
/// set-assignment values on `SetProperty`, etc. — and invoke
/// `visit` on each one. Used by `validate_pattern_predicates`
/// to find every place a pattern predicate could be hiding.
fn walk_plan_exprs<F>(plan: &LogicalPlan, visit: &mut F) -> Result<()>
where
    F: FnMut(&Expr) -> Result<()>,
{
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            visit(predicate)?;
            walk_plan_exprs(input, visit)
        }
        LogicalPlan::Project { input, items } => {
            for item in items {
                visit(&item.expr)?;
            }
            walk_plan_exprs(input, visit)
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
        } => {
            for item in group_keys {
                visit(&item.expr)?;
            }
            for agg in aggregates {
                match &agg.arg {
                    AggregateArg::Star => {}
                    AggregateArg::Expr(e) | AggregateArg::DistinctExpr(e) => visit(e)?,
                }
            }
            walk_plan_exprs(input, visit)
        }
        LogicalPlan::SetProperty { input, assignments } => {
            for a in assignments {
                match a {
                    SetAssignment::Property { value, .. } => visit(value)?,
                    SetAssignment::Merge { properties, .. }
                    | SetAssignment::Replace { properties, .. } => {
                        for (_, e) in properties {
                            visit(e)?;
                        }
                    }
                    SetAssignment::ReplaceFromExpr { source, .. } => visit(source)?,
                    SetAssignment::Labels { .. } => {}
                }
            }
            walk_plan_exprs(input, visit)
        }
        LogicalPlan::OrderBy { input, sort_items } => {
            for item in sort_items {
                visit(&item.expr)?;
            }
            walk_plan_exprs(input, visit)
        }
        LogicalPlan::IndexSeek { values, .. } => {
            for v in values {
                visit(v)?;
            }
            Ok(())
        }
        LogicalPlan::PointIndexSeek { bounds, .. } => {
            match bounds {
                PointSeekBounds::Corners { lo, hi } => {
                    visit(lo)?;
                    visit(hi)?;
                }
                PointSeekBounds::Radius { center, radius } => {
                    visit(center)?;
                    visit(radius)?;
                }
            }
            Ok(())
        }
        LogicalPlan::EdgeSeek {
            value,
            residual_properties,
            ..
        } => {
            visit(value)?;
            for (_, e) in residual_properties {
                visit(e)?;
            }
            Ok(())
        }
        LogicalPlan::EdgePointIndexSeek { bounds, .. } => {
            match bounds {
                PointSeekBounds::Corners { lo, hi } => {
                    visit(lo)?;
                    visit(hi)?;
                }
                PointSeekBounds::Radius { center, radius } => {
                    visit(center)?;
                    visit(radius)?;
                }
            }
            Ok(())
        }
        LogicalPlan::Unwind { expr, .. } => visit(expr),
        LogicalPlan::UnwindChain { input, expr, .. } => {
            visit(expr)?;
            walk_plan_exprs(input, visit)
        }
        LogicalPlan::EdgeExpand { input, .. }
        | LogicalPlan::OptionalEdgeExpand { input, .. }
        | LogicalPlan::VarLengthExpand { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Delete { input, .. }
        | LogicalPlan::Remove { input, .. }
        | LogicalPlan::MergeEdge { input, .. }
        | LogicalPlan::Foreach { input, .. }
        | LogicalPlan::CallSubquery { input, .. }
        | LogicalPlan::CallSubqueryInTransactions { input, .. }
        | LogicalPlan::OptionalApply { input, .. }
        | LogicalPlan::LoadCsv {
            input: Some(input), ..
        }
        | LogicalPlan::BindPath { input, .. }
        | LogicalPlan::ShortestPath { input, .. }
        | LogicalPlan::CoalesceNullRow { input, .. }
        | LogicalPlan::Identity { input } => walk_plan_exprs(input, visit),
        LogicalPlan::Skip { input, count } | LogicalPlan::Limit { input, count } => {
            visit(count)?;
            walk_plan_exprs(input, visit)
        }
        LogicalPlan::CartesianProduct { left, right } => {
            walk_plan_exprs(left, visit)?;
            walk_plan_exprs(right, visit)
        }
        LogicalPlan::Union { branches, .. } => {
            for b in branches {
                walk_plan_exprs(b, visit)?;
            }
            Ok(())
        }
        LogicalPlan::CreatePath { input, .. } => match input {
            Some(i) => walk_plan_exprs(i, visit),
            None => Ok(()),
        },
        LogicalPlan::MergeNode { input, .. } => match input {
            Some(i) => walk_plan_exprs(i, visit),
            None => Ok(()),
        },
        LogicalPlan::ProcedureCall { input, args, .. } => {
            if let Some(exprs) = args {
                for e in exprs {
                    visit(e)?;
                }
            }
            match input {
                Some(i) => walk_plan_exprs(i, visit),
                None => Ok(()),
            }
        }
        LogicalPlan::NodeScanAll { .. }
        | LogicalPlan::NodeScanByLabels { .. }
        | LogicalPlan::SeedRow
        | LogicalPlan::LoadCsv { input: None, .. }
        | LogicalPlan::CreatePropertyIndex { .. }
        | LogicalPlan::DropPropertyIndex { .. }
        | LogicalPlan::CreateEdgePropertyIndex { .. }
        | LogicalPlan::DropEdgePropertyIndex { .. }
        | LogicalPlan::ShowPropertyIndexes
        | LogicalPlan::CreatePointIndex { .. }
        | LogicalPlan::DropPointIndex { .. }
        | LogicalPlan::CreateEdgePointIndex { .. }
        | LogicalPlan::DropEdgePointIndex { .. }
        | LogicalPlan::ShowPointIndexes
        | LogicalPlan::CreatePropertyConstraint { .. }
        | LogicalPlan::DropPropertyConstraint { .. }
        | LogicalPlan::ShowPropertyConstraints => Ok(()),
        // ApocPeriodicIterate carries fully-planned subtrees;
        // it doesn't expose its own scoped expressions to the
        // outer scope, so the walker stops here. The dispatcher
        // walks the subtrees independently when it runs them.
        LogicalPlan::ApocPeriodicIterate { .. } => Ok(()),
    }
}

/// Walk every sub-expression of `expr` (recursively) invoking
/// `visit` on each. Used to find `Expr::PatternExists` nodes
/// buried inside boolean trees, CASE branches, list
/// comprehensions, reduce bodies, map literal values, etc.
fn walk_expr<F>(expr: &Expr, visit: &mut F) -> Result<()>
where
    F: FnMut(&Expr) -> Result<()>,
{
    visit(expr)?;
    match expr {
        Expr::Literal(_) | Expr::Identifier(_) | Expr::Parameter(_) | Expr::Property { .. } => {
            Ok(())
        }
        Expr::PropertyAccess { base, .. } => walk_expr(base, visit),
        Expr::HasLabels { expr, .. } => walk_expr(expr, visit),
        Expr::IndexAccess { base, index } => {
            walk_expr(base, visit)?;
            walk_expr(index, visit)
        }
        Expr::SliceAccess { base, start, end } => {
            walk_expr(base, visit)?;
            if let Some(s) = start {
                walk_expr(s, visit)?;
            }
            if let Some(e) = end {
                walk_expr(e, visit)?;
            }
            Ok(())
        }
        Expr::Not(e) => walk_expr(e, visit),
        Expr::And(a, b) | Expr::Or(a, b) | Expr::Xor(a, b) => {
            walk_expr(a, visit)?;
            walk_expr(b, visit)
        }
        Expr::Compare { left, right, .. } => {
            walk_expr(left, visit)?;
            walk_expr(right, visit)
        }
        Expr::IsNull { inner, .. } => walk_expr(inner, visit),
        Expr::InList { element, list } => {
            walk_expr(element, visit)?;
            walk_expr(list, visit)
        }
        Expr::ListPredicate {
            list, predicate, ..
        } => {
            walk_expr(list, visit)?;
            walk_expr(predicate, visit)
        }
        Expr::Call { args, .. } => match args {
            CallArgs::Star => Ok(()),
            CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) => {
                for e in es {
                    walk_expr(e, visit)?;
                }
                Ok(())
            }
        },
        Expr::List(items) => {
            for e in items {
                walk_expr(e, visit)?;
            }
            Ok(())
        }
        Expr::Map(entries) => {
            for (_, e) in entries {
                walk_expr(e, visit)?;
            }
            Ok(())
        }
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            if let Some(s) = scrutinee {
                walk_expr(s, visit)?;
            }
            for (cond, result) in branches {
                walk_expr(cond, visit)?;
                walk_expr(result, visit)?;
            }
            if let Some(e) = else_expr {
                walk_expr(e, visit)?;
            }
            Ok(())
        }
        Expr::ListComprehension {
            source,
            predicate,
            projection,
            ..
        } => {
            walk_expr(source, visit)?;
            if let Some(p) = predicate {
                walk_expr(p, visit)?;
            }
            if let Some(p) = projection {
                walk_expr(p, visit)?;
            }
            Ok(())
        }
        Expr::Reduce {
            acc_init,
            source,
            body,
            ..
        } => {
            walk_expr(acc_init, visit)?;
            walk_expr(source, visit)?;
            walk_expr(body, visit)
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, visit)?;
            walk_expr(right, visit)
        }
        Expr::UnaryOp { operand, .. } => walk_expr(operand, visit),
        // `Expr::PatternExists` itself is also walked into: the
        // outer visit already saw it via the caller, so there's
        // no sub-expressions inside the pattern worth inspecting
        // (pattern-property values are restricted to literals
        // and parameters, which the walker doesn't care about).
        Expr::PatternExists(_) => Ok(()),
        // `Expr::ExistsSubquery` has an inner WHERE expression
        // that can itself contain pattern predicates, nested
        // existence checks, etc. — recurse into it so those
        // inner expressions get validated too. The pattern's
        // shape is checked separately by the subquery
        // validation pass.
        Expr::ExistsSubquery { .. } | Expr::CountSubquery { .. } | Expr::CollectSubquery { .. } => {
            Ok(())
        }
        // Pattern comprehension: the pattern itself carries no
        // sub-expressions the walker cares about (same rule as
        // `PatternExists`), but the WHERE / projection are plain
        // expressions and need to be visited so any pattern
        // predicates, parameter uses, etc. inside them are
        // validated.
        Expr::PatternComprehension {
            predicate,
            projection,
            ..
        } => {
            if let Some(p) = predicate {
                walk_expr(p, visit)?;
            }
            walk_expr(projection, visit)
        }
    }
}

/// Plan a `UNION` / `UNION ALL`. Every branch is lowered
/// independently, then the planner validates that all branches
/// agree on the RETURN column list (name + order) — same
/// invariant Neo4j enforces. Branches that don't end in a
/// readable projection (e.g. a `MATCH ... DELETE` with no final
/// `RETURN`) are rejected because UNION has to produce rows, not
/// side effects.
fn plan_union(u: &UnionStmt, ctx: &PlannerContext) -> Result<LogicalPlan> {
    if u.branches.len() < 2 {
        return Err(Error::Plan("UNION requires at least two branches".into()));
    }

    // Plan all branches first — needed both for the final Union
    // node and for column validation (RETURN * can only derive
    // column names from a planned branch).
    let branches: Vec<LogicalPlan> = u
        .branches
        .iter()
        .map(|b| plan_with_context(b, ctx))
        .collect::<Result<_>>()?;

    // Validate column alignment. For branches with RETURN *,
    // fall back to the planned output. For explicit RETURN items,
    // use the AST-level column names.
    let expected_columns = union_branch_columns(&u.branches[0])?;
    for (i, branch) in u.branches.iter().enumerate().skip(1) {
        let cols = union_branch_columns(branch)?;
        if cols != expected_columns {
            return Err(Error::Plan(format!(
                "UNION branch {i} has columns {cols:?}, expected {expected_columns:?} \
                 (all branches must project the same columns in the same order)"
            )));
        }
    }

    Ok(LogicalPlan::Union {
        branches,
        all: u.all,
    })
}

/// Return the ordered list of output column names for a UNION
/// branch. Only accepts read-producing statements — any branch
/// that doesn't carry a `RETURN` (e.g. pure SET/DELETE) fails,
/// and DDL statements are rejected outright because UNION is a
/// row-stream construct.
fn union_branch_columns(stmt: &Statement) -> Result<Vec<String>> {
    match stmt {
        Statement::Match(m) => {
            if m.terminal.star {
                return Ok(vec!["*".to_string()]);
            }
            if m.terminal.return_items.is_empty() {
                return Err(Error::Plan(
                    "UNION branches must end with RETURN; a bare effectful \
                     MATCH tail has no projected columns"
                        .into(),
                ));
            }
            Ok(m.terminal
                .return_items
                .iter()
                .map(return_item_column_name)
                .collect())
        }
        Statement::Unwind(u) => Ok(u.return_items.iter().map(return_item_column_name).collect()),
        Statement::Return(r) => Ok(r.return_items.iter().map(return_item_column_name).collect()),
        Statement::Union(u) => {
            // Nested UNION is flattened by the parser, but in
            // principle a plan-mode construction could hand us
            // one — inherit the column list from the first
            // branch.
            u.branches
                .first()
                .map(union_branch_columns)
                .unwrap_or_else(|| Err(Error::Plan("empty nested UNION".into())))
        }
        Statement::Explain(inner) | Statement::Profile(inner) => union_branch_columns(inner),
        Statement::Create(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::ShowIndexes
        | Statement::CreatePointIndex(_)
        | Statement::DropPointIndex(_)
        | Statement::ShowPointIndexes
        | Statement::CreateConstraint(_)
        | Statement::DropConstraint(_)
        | Statement::ShowConstraints
        | Statement::CallProcedure(_) => Err(Error::Plan(
            "UNION branches must be read queries (MATCH / UNWIND / RETURN); \
             DDL and CREATE-only statements are not allowed"
                .into(),
        )),
    }
}

/// Columns a `CALL { ... }` subquery body publishes to the outer
/// scope. `Some(names)` is an explicit RETURN item list (in order).
/// `Some(vec![])` is a unit body with no RETURN (e.g.
/// `CALL { CREATE (n) }` — legal, but contributes no new bindings).
/// `None` means the body's projection is `RETURN *` or an untyped
/// CALL without YIELD — the outer scope extension is skipped because
/// the column set can't be derived from the AST alone.
/// Standard set of column names that
/// [`LogicalPlan::ApocPeriodicIterate`] yields. Mirrors the
/// Neo4j 5 surface so an apoc-aware client's YIELD list works
/// unchanged. The dispatcher always emits exactly these columns
/// in the result row.
pub const APOC_PERIODIC_ITERATE_COLUMNS: &[&str] = &[
    "batches",
    "total",
    "committedOperations",
    "failedOperations",
    "failedBatches",
    "errorMessages",
    "timeTaken",
    "wasTerminated",
    "retries",
    "failedParams",
    "batch",
    "operations",
    "updateStatistics",
];

/// Plan-time rewrite of a `CALL apoc.periodic.iterate(iterateQ,
/// actionQ, config)` procedure call into the dedicated
/// [`LogicalPlan::ApocPeriodicIterate`] variant. Validates the
/// arg shapes (3 args, both query args literal strings, config
/// is a literal map), parses + plans both query strings as
/// independent sub-statements, and extracts `batchSize` and
/// `params` from the config map.
fn build_apoc_periodic_iterate(
    pc: &crate::ast::ProcedureCall,
    ctx: &PlannerContext,
) -> Result<LogicalPlan> {
    use crate::ast::{Expr, Literal};
    let args = pc.args.as_ref().ok_or_else(|| {
        Error::Plan(
            "apoc.periodic.iterate requires (iterateQuery, actionQuery, config) args".into(),
        )
    })?;
    if args.len() != 3 {
        return Err(Error::Plan(format!(
            "apoc.periodic.iterate expects 3 arguments (iterateQuery, actionQuery, \
             config), got {}",
            args.len()
        )));
    }
    let iterate_str = expect_string_literal(&args[0], "iterateQuery (arg 1)")?;
    let action_str = expect_string_literal(&args[1], "actionQuery (arg 2)")?;
    let config_entries = expect_map_literal(&args[2], "config (arg 3)")?;

    let mut batch_size: i64 = 10000;
    let mut extra_params: std::collections::HashMap<String, Expr> =
        std::collections::HashMap::new();
    let mut retries: i64 = 0;
    let mut failed_params_cap: i64 = -1;
    let mut iterate_list: bool = false;
    for (key, value) in &config_entries {
        match key.as_str() {
            "batchSize" => {
                batch_size = match value {
                    Expr::Literal(Literal::Integer(n)) if *n > 0 => *n,
                    _ => {
                        return Err(Error::Plan(
                            "apoc.periodic.iterate config.batchSize must be a positive \
                             integer literal"
                                .into(),
                        ));
                    }
                };
            }
            "params" => {
                // The `params` config value is itself a map of
                // extra param-name → expression bindings. We
                // accept any literal map here; the dispatcher
                // evaluates each binding once at execution
                // start.
                let pairs = expect_map_literal(value, "config.params")?;
                for (k, v) in pairs {
                    extra_params.insert(k, v);
                }
            }
            "retries" => {
                retries = match value {
                    Expr::Literal(Literal::Integer(n)) if *n >= 0 => *n,
                    _ => {
                        return Err(Error::Plan(
                            "apoc.periodic.iterate config.retries must be a non-negative \
                             integer literal"
                                .into(),
                        ));
                    }
                };
            }
            "failedParams" => {
                failed_params_cap = match value {
                    // -1 = unlimited capture (Neo4j default);
                    // 0 = disabled; positive caps per-error
                    // sample list length.
                    Expr::Literal(Literal::Integer(n)) if *n >= -1 => *n,
                    _ => {
                        return Err(Error::Plan(
                            "apoc.periodic.iterate config.failedParams must be -1, 0, or a \
                             positive integer literal"
                                .into(),
                        ));
                    }
                };
            }
            "iterateList" => {
                iterate_list = match value {
                    Expr::Literal(Literal::Boolean(b)) => *b,
                    _ => {
                        return Err(Error::Plan(
                            "apoc.periodic.iterate config.iterateList must be a boolean \
                             literal"
                                .into(),
                        ));
                    }
                };
            }
            // Forward-compat accept for options Mesh doesn't
            // execute against — Mesh has no parallel-action
            // dispatch path.
            "parallel" | "concurrency" => {}
            other => {
                return Err(Error::Plan(format!(
                    "apoc.periodic.iterate: unknown config key {other:?}"
                )));
            }
        }
    }

    // Parse + plan the iterate query.
    let iterate_stmt = crate::parse(&iterate_str)
        .map_err(|e| Error::Plan(format!("apoc.periodic.iterate iterateQuery: {e}")))?;
    let iterate_plan = plan_with_context(&iterate_stmt, ctx)
        .map_err(|e| Error::Plan(format!("apoc.periodic.iterate iterateQuery: {e}")))?;
    // Parse + plan the action query. Action is run with a
    // fresh PlannerContext (no outer bindings); per-row
    // values flow in via `$param` substitution at execute time.
    let action_stmt = crate::parse(&action_str)
        .map_err(|e| Error::Plan(format!("apoc.periodic.iterate actionQuery: {e}")))?;
    let action_plan = plan_with_context(&action_stmt, ctx)
        .map_err(|e| Error::Plan(format!("apoc.periodic.iterate actionQuery: {e}")))?;

    // YIELD validation: every requested column must be in the
    // standard apoc result set, otherwise the executor would
    // emit Null silently.
    if let Some(crate::ast::YieldSpec::Items(items)) = &pc.yield_spec {
        for yi in items {
            if !APOC_PERIODIC_ITERATE_COLUMNS.contains(&yi.column.as_str()) {
                return Err(Error::Plan(format!(
                    "apoc.periodic.iterate has no output column {:?}; available: {:?}",
                    yi.column, APOC_PERIODIC_ITERATE_COLUMNS
                )));
            }
        }
    }

    Ok(LogicalPlan::ApocPeriodicIterate {
        iterate: Box::new(iterate_plan),
        action: Box::new(action_plan),
        batch_size,
        extra_params,
        retries,
        failed_params_cap,
        iterate_list,
    })
}

/// Helper: extract a literal-string expression's value or fail
/// with a precise error citing the arg position.
fn expect_string_literal(expr: &crate::ast::Expr, position: &str) -> Result<String> {
    use crate::ast::{Expr, Literal};
    match expr {
        Expr::Literal(Literal::String(s)) => Ok(s.clone()),
        _ => Err(Error::Plan(format!(
            "apoc.periodic.iterate: {position} must be a string literal"
        ))),
    }
}

/// Helper: extract a literal-map expression's entries.
fn expect_map_literal(
    expr: &crate::ast::Expr,
    position: &str,
) -> Result<Vec<(String, crate::ast::Expr)>> {
    use crate::ast::Expr;
    match expr {
        Expr::Map(entries) => Ok(entries.clone()),
        _ => Err(Error::Plan(format!(
            "apoc.periodic.iterate: {position} must be a map literal"
        ))),
    }
}

fn call_subquery_output_columns(stmt: &Statement) -> Result<Option<Vec<String>>> {
    match stmt {
        Statement::Match(m) => {
            if m.terminal.star {
                return Ok(None);
            }
            Ok(Some(
                m.terminal
                    .return_items
                    .iter()
                    .map(return_item_column_name)
                    .collect(),
            ))
        }
        Statement::Unwind(u) => {
            if u.star {
                return Ok(None);
            }
            Ok(Some(
                u.return_items.iter().map(return_item_column_name).collect(),
            ))
        }
        Statement::Return(r) => {
            if r.star {
                return Ok(None);
            }
            Ok(Some(
                r.return_items.iter().map(return_item_column_name).collect(),
            ))
        }
        Statement::Create(c) => {
            if c.star {
                return Ok(None);
            }
            Ok(Some(
                c.return_items.iter().map(return_item_column_name).collect(),
            ))
        }
        Statement::Union(u) => u
            .branches
            .first()
            .map(call_subquery_output_columns)
            .unwrap_or_else(|| Err(Error::Plan("empty nested UNION".into()))),
        Statement::Explain(inner) | Statement::Profile(inner) => {
            call_subquery_output_columns(inner)
        }
        Statement::CallProcedure(pc) => match &pc.yield_spec {
            Some(crate::ast::YieldSpec::Items(items)) => Ok(Some(
                items
                    .iter()
                    .map(|y| y.alias.clone().unwrap_or_else(|| y.column.clone()))
                    .collect(),
            )),
            // `YIELD *` or missing YIELD — columns come from the
            // procedure registry at execute time, not from the AST.
            Some(crate::ast::YieldSpec::Star) | None => Ok(None),
        },
        Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::ShowIndexes
        | Statement::CreatePointIndex(_)
        | Statement::DropPointIndex(_)
        | Statement::ShowPointIndexes
        | Statement::CreateConstraint(_)
        | Statement::DropConstraint(_)
        | Statement::ShowConstraints => Err(Error::Plan(
            "CALL { ... } body must be a read query; DDL is not allowed".into(),
        )),
    }
}

/// Canonical column name for a `RETURN` item. Uses the explicit
/// alias when present, otherwise falls back to a stable rendering
/// of the expression — matching what the executor's `ProjectOp`
/// uses as the row's key.
fn return_item_column_name(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    if let Some(raw) = &item.raw_text {
        return raw.clone();
    }
    render_expr_key(&item.expr)
}

/// Best-effort string rendering of an expression for use as a
/// column name when no `AS` alias is given. Mirrors the key
/// strings the executor builds in `ProjectOp` so UNION column
/// matching stays consistent end to end.
fn render_expr_key(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::Property { var, key } => format!("{var}.{key}"),
        Expr::PropertyAccess { base, key } => format!("{}.{key}", render_expr_key(base)),
        Expr::Parameter(name) => format!("${name}"),
        Expr::Literal(Literal::String(s)) => format!("'{s}'"),
        Expr::Literal(Literal::Integer(i)) => i.to_string(),
        Expr::Literal(Literal::Float(f)) => f.to_string(),
        Expr::Literal(Literal::Boolean(b)) => b.to_string(),
        Expr::Literal(Literal::Null) => "NULL".into(),
        Expr::Call { name, args } => {
            let arg_str = match args {
                CallArgs::Star => "*".into(),
                CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) => {
                    let prefix = if matches!(args, CallArgs::DistinctExprs(_)) {
                        "DISTINCT "
                    } else {
                        ""
                    };
                    format!(
                        "{}{}",
                        prefix,
                        es.iter()
                            .map(render_expr_key)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            };
            format!("{name}({arg_str})")
        }
        Expr::BinaryOp { op, left, right } => {
            let op_str = match op {
                BinaryOp::Add => " + ",
                BinaryOp::Sub => " - ",
                BinaryOp::Mul => " * ",
                BinaryOp::Div => " / ",
                BinaryOp::Mod => " % ",
                BinaryOp::Pow => " ^ ",
            };
            format!(
                "{}{op_str}{}",
                render_expr_key(left),
                render_expr_key(right)
            )
        }
        Expr::UnaryOp { op, operand } => {
            let op_str = match op {
                UnaryOp::Neg => "-",
            };
            format!("{op_str}{}", render_expr_key(operand))
        }
        Expr::Not(inner) => format!("NOT {}", render_expr_key(inner)),
        Expr::IsNull { negated, inner } => {
            if *negated {
                format!("{} IS NOT NULL", render_expr_key(inner))
            } else {
                format!("{} IS NULL", render_expr_key(inner))
            }
        }
        Expr::Compare { op, left, right } => {
            let op_str = match op {
                CompareOp::Eq => " = ",
                CompareOp::Ne => " <> ",
                CompareOp::Lt => " < ",
                CompareOp::Le => " <= ",
                CompareOp::Gt => " > ",
                CompareOp::Ge => " >= ",
                CompareOp::StartsWith => " STARTS WITH ",
                CompareOp::EndsWith => " ENDS WITH ",
                CompareOp::Contains => " CONTAINS ",
                CompareOp::RegexMatch => " =~ ",
            };
            format!(
                "{}{op_str}{}",
                render_expr_key(left),
                render_expr_key(right)
            )
        }
        Expr::List(items) => {
            let inner: Vec<String> = items.iter().map(render_expr_key).collect();
            format!("[{}]", inner.join(", "))
        }
        Expr::Map(entries) => {
            let inner: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{k}: {}", render_expr_key(v)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        Expr::IndexAccess { base, index } => {
            format!("{}[{}]", render_expr_key(base), render_expr_key(index))
        }
        Expr::InList { element, list } => {
            format!("{} IN {}", render_expr_key(element), render_expr_key(list))
        }
        Expr::HasLabels { expr, labels } => {
            // Match the Cypher source-text form: `(var:Label:Label)`.
            // TCK result headers use this shape so it has to round-trip.
            let mut s = format!("({}", render_expr_key(expr));
            for l in labels {
                s.push(':');
                s.push_str(l);
            }
            s.push(')');
            s
        }
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            let mut s = String::from("CASE");
            if let Some(sc) = scrutinee {
                s.push_str(&format!(" {}", render_expr_key(sc)));
            }
            for (when, then) in branches {
                s.push_str(&format!(
                    " WHEN {} THEN {}",
                    render_expr_key(when),
                    render_expr_key(then)
                ));
            }
            if let Some(el) = else_expr {
                s.push_str(&format!(" ELSE {}", render_expr_key(el)));
            }
            s.push_str(" END");
            s
        }
        _ => format!("{expr:?}"),
    }
}

/// Lower a bare `RETURN <items>` to a single-row producer plus the
/// usual return pipeline. Implemented as `UNWIND [0] AS __bare_return`
/// so the executor's existing `UnwindOp` produces exactly one row,
/// after which `Project` overwrites the bound variable with the
/// caller's projection (the placeholder column is dropped because
/// `Project` returns only the explicitly-named items).
///
/// The choice of `0` for the placeholder value is arbitrary —
/// projections can't reference `__bare_return` (the parser
/// guarantees there's no MATCH binding it), and the executor's
/// `UnwindOp` only cares that the source list has length 1.
fn plan_return_only(stmt: &ReturnStmt) -> Result<LogicalPlan> {
    let producer = LogicalPlan::Unwind {
        var: "__bare_return".to_string(),
        expr: Expr::List(vec![Expr::Literal(Literal::Integer(0))]),
    };
    apply_return_pipeline(
        producer,
        &stmt.return_items,
        stmt.star,
        stmt.distinct,
        &stmt.order_by,
        stmt.skip.clone(),
        stmt.limit.clone(),
    )
}

fn plan_unwind(stmt: &UnwindStmt) -> Result<LogicalPlan> {
    let mut plan = LogicalPlan::Unwind {
        var: stmt.alias.clone(),
        expr: stmt.expr.clone(),
    };

    if let Some(predicate) = &stmt.where_clause {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    apply_return_pipeline(
        plan,
        &stmt.return_items,
        stmt.star,
        stmt.distinct,
        &stmt.order_by,
        stmt.skip.clone(),
        stmt.limit.clone(),
    )
}

/// Walk a SET right-hand side and reject `Expr::Identifier` /
/// `Expr::Property` references to variables that aren't in the
/// current binding scope. openCypher raises `SyntaxError:
/// UndefinedVariable` at compile time for `SET a.name = missing`
/// and similar — we mirror that with a plan-time `Error::Plan`.
/// Comprehension / reduce / list-predicate binders extend the
/// scope for their own subtree so `[x IN xs | x]` doesn't trip
/// the check. Subquery / pattern predicate bodies are skipped;
/// they manage their own scope internally.
fn check_set_expr_scope(expr: &Expr, bound: &HashMap<String, VarType>) -> Result<()> {
    check_set_expr_scope_inner(expr, bound, &[])
}

/// Scope-check the target-var and value-expression of a single
/// `SetItem`. Used for MERGE's `ON CREATE` / `ON MATCH` clauses,
/// where the usual set-tail scope check doesn't run (the items
/// live on the MergeClause rather than as standalone clauses).
fn check_set_item_scope(
    item: &crate::ast::SetItem,
    bound: &HashMap<String, VarType>,
) -> Result<()> {
    use crate::ast::SetItem;
    let missing = |v: &str| {
        Err(Error::Plan(format!(
            "UndefinedVariable: variable `{v}` is not defined"
        )))
    };
    match item {
        SetItem::Property { var, value, .. } => {
            if !bound.contains_key(var) {
                return missing(var);
            }
            check_set_expr_scope(value, bound)
        }
        SetItem::Labels { var, .. } => {
            if !bound.contains_key(var) {
                return missing(var);
            }
            Ok(())
        }
        SetItem::Replace { var, properties } | SetItem::Merge { var, properties } => {
            if !bound.contains_key(var) {
                return missing(var);
            }
            for (_, expr) in properties {
                check_set_expr_scope(expr, bound)?;
            }
            Ok(())
        }
        SetItem::ReplaceFromExpr { var, source, .. } => {
            if !bound.contains_key(var) {
                return missing(var);
            }
            check_set_expr_scope(source, bound)
        }
    }
}

/// Scope-check every property-value expression in `pattern` (its
/// start node plus every hop's rel and target) against `bound`.
/// Pattern-property values may be full expressions — including
/// references to earlier bindings (`UNWIND r AS i CREATE (n {var: i})`)
/// — so grammar-level restriction is off the table; this catches the
/// unbound-identifier case at plan time instead of letting it flow
/// into a filter predicate that silently matches zero rows.
fn check_pattern_property_scope(
    pattern: &crate::ast::Pattern,
    bound: &HashMap<String, VarType>,
) -> Result<()> {
    for (_, expr) in &pattern.start.properties {
        check_set_expr_scope(expr, bound)?;
    }
    for hop in &pattern.hops {
        for (_, expr) in &hop.rel.properties {
            check_set_expr_scope(expr, bound)?;
        }
        for (_, expr) in &hop.target.properties {
            check_set_expr_scope(expr, bound)?;
        }
    }
    Ok(())
}

fn check_set_expr_scope_inner(
    expr: &Expr,
    bound: &HashMap<String, VarType>,
    locals: &[&str],
) -> Result<()> {
    let in_scope = |name: &str| bound.contains_key(name) || locals.iter().any(|l| *l == name);
    match expr {
        Expr::Identifier(name) => {
            if !in_scope(name) {
                return Err(Error::Plan(format!(
                    "UndefinedVariable: variable `{name}` is not defined"
                )));
            }
            Ok(())
        }
        Expr::Property { var, .. } => {
            if !in_scope(var) {
                return Err(Error::Plan(format!(
                    "UndefinedVariable: variable `{var}` is not defined"
                )));
            }
            Ok(())
        }
        Expr::Literal(_) | Expr::Parameter(_) => Ok(()),
        Expr::PropertyAccess { base, .. } => check_set_expr_scope_inner(base, bound, locals),
        Expr::HasLabels { expr, .. } => check_set_expr_scope_inner(expr, bound, locals),
        Expr::IndexAccess { base, index } => {
            check_set_expr_scope_inner(base, bound, locals)?;
            check_set_expr_scope_inner(index, bound, locals)
        }
        Expr::SliceAccess { base, start, end } => {
            check_set_expr_scope_inner(base, bound, locals)?;
            if let Some(s) = start {
                check_set_expr_scope_inner(s, bound, locals)?;
            }
            if let Some(e) = end {
                check_set_expr_scope_inner(e, bound, locals)?;
            }
            Ok(())
        }
        Expr::Not(e) => check_set_expr_scope_inner(e, bound, locals),
        Expr::And(a, b) | Expr::Or(a, b) | Expr::Xor(a, b) => {
            check_set_expr_scope_inner(a, bound, locals)?;
            check_set_expr_scope_inner(b, bound, locals)
        }
        Expr::Compare { left, right, .. } => {
            check_set_expr_scope_inner(left, bound, locals)?;
            check_set_expr_scope_inner(right, bound, locals)
        }
        Expr::IsNull { inner, .. } => check_set_expr_scope_inner(inner, bound, locals),
        Expr::InList { element, list } => {
            check_set_expr_scope_inner(element, bound, locals)?;
            check_set_expr_scope_inner(list, bound, locals)
        }
        Expr::Call { args, .. } => match args {
            CallArgs::Star => Ok(()),
            CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) => {
                for e in es {
                    check_set_expr_scope_inner(e, bound, locals)?;
                }
                Ok(())
            }
        },
        Expr::List(items) => {
            for e in items {
                check_set_expr_scope_inner(e, bound, locals)?;
            }
            Ok(())
        }
        Expr::Map(entries) => {
            for (_, e) in entries {
                check_set_expr_scope_inner(e, bound, locals)?;
            }
            Ok(())
        }
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            if let Some(s) = scrutinee {
                check_set_expr_scope_inner(s, bound, locals)?;
            }
            for (cond, result) in branches {
                check_set_expr_scope_inner(cond, bound, locals)?;
                check_set_expr_scope_inner(result, bound, locals)?;
            }
            if let Some(e) = else_expr {
                check_set_expr_scope_inner(e, bound, locals)?;
            }
            Ok(())
        }
        Expr::ListComprehension {
            var,
            source,
            predicate,
            projection,
        } => {
            check_set_expr_scope_inner(source, bound, locals)?;
            let mut next: Vec<&str> = locals.to_vec();
            next.push(var);
            if let Some(p) = predicate {
                check_set_expr_scope_inner(p, bound, &next)?;
            }
            if let Some(p) = projection {
                check_set_expr_scope_inner(p, bound, &next)?;
            }
            Ok(())
        }
        Expr::Reduce {
            acc_var,
            acc_init,
            elem_var,
            source,
            body,
        } => {
            check_set_expr_scope_inner(acc_init, bound, locals)?;
            check_set_expr_scope_inner(source, bound, locals)?;
            let mut next: Vec<&str> = locals.to_vec();
            next.push(acc_var);
            next.push(elem_var);
            check_set_expr_scope_inner(body, bound, &next)
        }
        Expr::ListPredicate {
            var,
            list,
            predicate,
            ..
        } => {
            check_set_expr_scope_inner(list, bound, locals)?;
            let mut next: Vec<&str> = locals.to_vec();
            next.push(var);
            check_set_expr_scope_inner(predicate, bound, &next)
        }
        Expr::BinaryOp { left, right, .. } => {
            check_set_expr_scope_inner(left, bound, locals)?;
            check_set_expr_scope_inner(right, bound, locals)
        }
        Expr::UnaryOp { operand, .. } => check_set_expr_scope_inner(operand, bound, locals),
        // Subquery / pattern predicate bodies manage their own scope; skip.
        Expr::PatternExists(_)
        | Expr::ExistsSubquery { .. }
        | Expr::CountSubquery { .. }
        | Expr::CollectSubquery { .. } => Ok(()),
        // Pattern comprehension binds the pattern's node / edge
        // variables locally, so WHERE / projection see them on
        // top of the outer scope. Skipping the pattern itself
        // (same as PatternExists) — its property values are
        // literal-only.
        Expr::PatternComprehension {
            pattern,
            predicate,
            projection,
        } => {
            let mut local_names: Vec<String> = Vec::new();
            if let Some(v) = &pattern.path_var {
                local_names.push(v.clone());
            }
            if let Some(v) = &pattern.start.var {
                local_names.push(v.clone());
            }
            for hop in &pattern.hops {
                if let Some(v) = &hop.rel.var {
                    local_names.push(v.clone());
                }
                if let Some(v) = &hop.target.var {
                    local_names.push(v.clone());
                }
            }
            let mut next: Vec<&str> = locals.to_vec();
            for n in &local_names {
                next.push(n.as_str());
            }
            if let Some(p) = predicate {
                check_set_expr_scope_inner(p, bound, &next)?;
            }
            check_set_expr_scope_inner(projection, bound, &next)
        }
    }
}

fn check_set_assignments_scope(
    assignments: &[SetAssignment],
    bound: &HashMap<String, VarType>,
) -> Result<()> {
    for a in assignments {
        match a {
            SetAssignment::Property { value, .. } => {
                reject_pattern_predicate_in_projection(value, "SET right-hand side")?;
                check_set_expr_scope(value, bound)?;
            }
            SetAssignment::Replace { properties, .. } | SetAssignment::Merge { properties, .. } => {
                for (_, e) in properties {
                    reject_pattern_predicate_in_projection(e, "SET right-hand side")?;
                    check_set_expr_scope(e, bound)?;
                }
            }
            SetAssignment::ReplaceFromExpr { source, .. } => {
                reject_pattern_predicate_in_projection(source, "SET right-hand side")?;
                check_set_expr_scope(source, bound)?;
            }
            SetAssignment::Labels { .. } => {}
        }
    }
    Ok(())
}

/// Lower an AST `SetItem` to an executor-side `SetAssignment`.
/// Shared by every site that converts SET items into plan
/// assignments — both top-level `MATCH ... SET` and the
/// MERGE-conditional `ON CREATE SET` / `ON MATCH SET` forms.
fn set_item_to_assignment(item: &crate::ast::SetItem) -> SetAssignment {
    match item {
        crate::ast::SetItem::Property { var, key, value } => SetAssignment::Property {
            var: var.clone(),
            key: key.clone(),
            value: value.clone(),
        },
        crate::ast::SetItem::Labels { var, labels } => SetAssignment::Labels {
            var: var.clone(),
            labels: labels.clone(),
        },
        crate::ast::SetItem::Replace { var, properties } => SetAssignment::Replace {
            var: var.clone(),
            properties: properties.clone(),
        },
        crate::ast::SetItem::Merge { var, properties } => SetAssignment::Merge {
            var: var.clone(),
            properties: properties.clone(),
        },
        crate::ast::SetItem::ReplaceFromExpr {
            var,
            source,
            replace,
        } => SetAssignment::ReplaceFromExpr {
            var: var.clone(),
            source: source.clone(),
            replace: *replace,
        },
    }
}

fn plan_create(stmt: &CreateStmt) -> Result<LogicalPlan> {
    let mut nodes: Vec<CreateNodeSpec> = Vec::new();
    let mut edges: Vec<CreateEdgeSpec> = Vec::new();
    let mut var_idx: HashMap<String, usize> = HashMap::new();
    let no_bindings: HashMap<String, VarType> = HashMap::new();

    // Every identifier referenced in a property expression must
    // be in scope (i.e. introduced by an earlier pattern in this
    // CREATE), else openCypher raises `UndefinedVariable` at
    // compile time. Vars accumulate left-to-right across
    // patterns so `CREATE (a {..}), (:B {num: a.id})` sees `a`.
    let mut scope_for_props: HashMap<String, VarType> = HashMap::new();
    for pattern in &stmt.patterns {
        for (_, expr) in &pattern.start.properties {
            check_set_expr_scope(expr, &scope_for_props)?;
        }
        if let Some(v) = &pattern.start.var {
            scope_for_props.insert(v.clone(), VarType::Node);
        }
        for hop in &pattern.hops {
            if let Some(v) = &hop.rel.var {
                scope_for_props.insert(v.clone(), VarType::Edge);
            }
            if let Some(v) = &hop.target.var {
                scope_for_props.insert(v.clone(), VarType::Node);
            }
            for (_, expr) in &hop.rel.properties {
                check_set_expr_scope(expr, &scope_for_props)?;
            }
            for (_, expr) in &hop.target.properties {
                check_set_expr_scope(expr, &scope_for_props)?;
            }
        }
    }

    for pattern in &stmt.patterns {
        build_create_pattern(pattern, &mut nodes, &mut edges, &mut var_idx, &no_bindings)?;
    }

    let plan = LogicalPlan::CreatePath {
        input: None,
        nodes,
        edges,
    };

    if stmt.return_items.is_empty() && !stmt.star {
        Ok(plan)
    } else {
        // Build scope from CREATE-introduced vars so return
        // validators (unknown function, size-on-path, undefined
        // variable) match the `plan_match` terminal RETURN path.
        let mut return_scope: HashMap<String, VarType> = HashMap::new();
        for pattern in &stmt.patterns {
            if let Some(var) = &pattern.start.var {
                return_scope.insert(var.clone(), VarType::Node);
            }
            for hop in &pattern.hops {
                if let Some(var) = &hop.rel.var {
                    return_scope.insert(var.clone(), VarType::Edge);
                }
                if let Some(var) = &hop.target.var {
                    return_scope.insert(var.clone(), VarType::Node);
                }
            }
        }

        if stmt.star && return_scope.is_empty() {
            return Err(Error::Plan("RETURN * has no variables in scope".into()));
        }
        for item in &stmt.return_items {
            reject_size_on_path(&item.expr, &return_scope)?;
        }
        for item in &stmt.return_items {
            reject_unknown_functions(&item.expr)?;
        }
        for item in &stmt.return_items {
            check_set_expr_scope(&item.expr, &return_scope)?;
        }

        apply_return_pipeline(
            plan,
            &stmt.return_items,
            stmt.star,
            stmt.distinct,
            &stmt.order_by,
            stmt.skip.clone(),
            stmt.limit.clone(),
        )
    }
}

fn build_create_pattern(
    pattern: &Pattern,
    nodes: &mut Vec<CreateNodeSpec>,
    edges: &mut Vec<CreateEdgeSpec>,
    var_idx: &mut HashMap<String, usize>,
    bound_vars: &HashMap<String, VarType>,
) -> Result<()> {
    let start_idx = add_create_node(nodes, var_idx, bound_vars, &pattern.start)?;
    let mut prev_idx = start_idx;

    for hop in &pattern.hops {
        // openCypher: CREATE requires each relationship to be a
        // single, directed, non-variable-length hop.
        if hop.rel.var_length.is_some() {
            return Err(Error::Plan(
                "CREATE does not support variable-length relationships".into(),
            ));
        }
        let target_idx = add_create_node(nodes, var_idx, bound_vars, &hop.target)?;

        if hop.rel.edge_types.len() != 1 {
            return Err(Error::Plan(
                "CREATE relationship must specify exactly one type (e.g. [:KNOWS])".into(),
            ));
        }
        let edge_type = hop.rel.edge_types[0].clone();

        let (src_idx, dst_idx) = match hop.rel.direction {
            Direction::Outgoing => (prev_idx, target_idx),
            Direction::Incoming => (target_idx, prev_idx),
            Direction::Both => {
                return Err(Error::Plan(
                    "CREATE requires a directed relationship (-> or <-)".into(),
                ))
            }
        };

        edges.push(CreateEdgeSpec {
            var: hop.rel.var.clone(),
            edge_type,
            src_idx,
            dst_idx,
            properties: hop.rel.properties.clone(),
        });
        prev_idx = target_idx;
    }
    Ok(())
}

fn add_create_node(
    nodes: &mut Vec<CreateNodeSpec>,
    var_idx: &mut HashMap<String, usize>,
    bound_vars: &HashMap<String, VarType>,
    pattern: &NodePattern,
) -> Result<usize> {
    if let Some(name) = &pattern.var {
        if let Some(&idx) = var_idx.get(name) {
            // Re-encountering the same variable within the same
            // CREATE clause: labels/properties on subsequent
            // occurrences would silently be dropped (or silently
            // conflict), so reject with VariableAlreadyBound.
            if !pattern.labels.is_empty() || !pattern.properties.is_empty() {
                return Err(Error::Plan(format!(
                    "variable '{}' already defined with a different type",
                    name
                )));
            }
            return Ok(idx);
        }
    }
    let idx = nodes.len();
    let spec = match &pattern.var {
        Some(name) if bound_vars.contains_key(name) => CreateNodeSpec::Reference(name.clone()),
        _ => CreateNodeSpec::New {
            var: pattern.var.clone(),
            labels: pattern.labels.clone(),
            properties: pattern.properties.clone(),
        },
    };
    nodes.push(spec);
    if let Some(name) = &pattern.var {
        var_idx.insert(name.clone(), idx);
    }
    Ok(idx)
}

fn plan_pattern(
    pattern: &Pattern,
    pattern_idx: usize,
    ctx: &PlannerContext,
) -> Result<LogicalPlan> {
    plan_pattern_with_bound_edges(pattern, pattern_idx, ctx, &HashSet::new(), &HashSet::new())
}

/// Try to lower a single-hop `MATCH` pattern to an [`LogicalPlan::EdgeSeek`]
/// when every precondition holds:
///
/// 1. Exactly one hop, no path variable, no `shortestPath`, no
///    variable-length relationship.
/// 2. Neither endpoint is bound in an outer scope. Endpoint labels
///    and pattern-property equalities are allowed — the rewrite
///    wraps the seek in residual `HasLabels` / property-equality
///    `Filter`s so the output still honours them.
/// 3. The relationship has exactly one edge type, a fresh edge
///    variable (not pre-bound in an outer clause), and at least one
///    pattern-property equality matching a registered `(edge_type,
///    property)` index in `ctx`.
///
/// `None` means "preconditions didn't match — fall back to the
/// general `NodeScan + EdgeExpand` path". `Some(plan)` is the rewritten
/// plan. Residual edge-property equalities (non-indexed) ride along
/// on the `EdgeSeek` as per-edge filters; endpoint residuals wrap
/// the seek as a standard `Filter` stack.
fn try_plan_edge_seek(
    pattern: &Pattern,
    pattern_idx: usize,
    ctx: &PlannerContext,
    outer_bound_nodes: &HashSet<String>,
    outer_bound_edges: &HashSet<String>,
) -> Result<Option<LogicalPlan>> {
    if pattern.hops.len() != 1 {
        return Ok(None);
    }
    if pattern.path_var.is_some() || pattern.shortest.is_some() {
        return Ok(None);
    }
    let hop = &pattern.hops[0];
    if hop.rel.var_length.is_some() {
        return Ok(None);
    }
    // Endpoints must not already be bound in an outer scope — the
    // rewrite skips the cross-product handshake that rebind would
    // normally go through. Labels and pattern properties are fine
    // (we wrap them as residuals below), but an outer binding
    // means the fresh-scan path needs to do more work than a bare
    // seek can express.
    if let Some(v) = &pattern.start.var {
        if outer_bound_nodes.contains(v) {
            return Ok(None);
        }
    }
    if let Some(v) = &hop.target.var {
        if outer_bound_nodes.contains(v) {
            return Ok(None);
        }
    }
    // Rel must have a single, fresh edge type and a fresh edge var.
    if hop.rel.edge_types.len() != 1 {
        return Ok(None);
    }
    if let Some(v) = &hop.rel.var {
        if outer_bound_edges.contains(v) {
            return Ok(None);
        }
    }
    let edge_type = &hop.rel.edge_types[0];
    // Pick the first pattern-property equality whose key is indexed
    // as the seek key; every other equality becomes a residual
    // filter evaluated per edge.
    let seek_idx = hop
        .rel
        .properties
        .iter()
        .position(|(k, _)| ctx.has_edge_index(edge_type, k));
    let Some(seek_idx) = seek_idx else {
        return Ok(None);
    };
    // Synthesize names for anonymous endpoints / edge var. Mirrors
    // the naming convention used elsewhere in the planner so a
    // downstream clause referencing a named endpoint finds the
    // same key either way.
    let src_var = pattern
        .start
        .var
        .clone()
        .unwrap_or_else(|| format!("__p{pattern_idx}_a0"));
    let dst_var = hop
        .target
        .var
        .clone()
        .unwrap_or_else(|| format!("__p{pattern_idx}_a1"));
    let edge_var = hop
        .rel
        .var
        .clone()
        .unwrap_or_else(|| format!("__p{pattern_idx}_e1"));
    let (seek_key, seek_value) = hop.rel.properties[seek_idx].clone();
    let residual_properties: Vec<(String, Expr)> = hop
        .rel
        .properties
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != seek_idx)
        .map(|(_, p)| p.clone())
        .collect();
    let mut plan = LogicalPlan::EdgeSeek {
        edge_var,
        src_var: src_var.clone(),
        dst_var: dst_var.clone(),
        edge_type: edge_type.clone(),
        property: seek_key,
        value: seek_value,
        direction: hop.rel.direction,
        residual_properties,
    };
    // Endpoint residuals. Apply labels first so a mis-labelled row
    // drops before the per-property comparisons run.
    plan = wrap_with_label_filter(plan, &src_var, &pattern.start.labels);
    plan = wrap_with_label_filter(plan, &dst_var, &hop.target.labels);
    plan = wrap_with_pattern_prop_filter(plan, &src_var, &pattern.start.properties);
    plan = wrap_with_pattern_prop_filter(plan, &dst_var, &hop.target.properties);
    Ok(Some(plan))
}

/// Wrap `plan` in a `Filter` that asserts the value bound to `var`
/// has every label in `labels`. No-op when `labels` is empty. Used
/// by the edge-seek rewrite to enforce `(a:Label)` endpoint
/// constraints as a residual filter over the seek's output.
fn wrap_with_label_filter(plan: LogicalPlan, var: &str, labels: &[String]) -> LogicalPlan {
    if labels.is_empty() {
        return plan;
    }
    LogicalPlan::Filter {
        input: Box::new(plan),
        predicate: Expr::HasLabels {
            expr: Box::new(Expr::Identifier(var.to_string())),
            labels: labels.to_vec(),
        },
    }
}

/// Same as [`plan_pattern`] but honours sets of edge / edge-list
/// variables already bound in an outer clause. Each hop that
/// reuses such a variable lowers with the appropriate constraint
/// (id-equality for edges, list replay for var-length) so a
/// fresh scan treats it as an existence / replay check on the
/// outer binding rather than rebinding it. Lets patterns like
/// `MATCH ()-[r:EDGE]-() MATCH (n)-[r]-(m)` reuse `r`, and
/// `WITH [r1, r2] AS rs MATCH (first)-[rs*]->(second)` walk the
/// pre-bound list.
fn plan_pattern_with_bound_edges(
    pattern: &Pattern,
    pattern_idx: usize,
    ctx: &PlannerContext,
    external_bound_edges: &HashSet<String>,
    external_bound_edge_lists: &HashSet<String>,
) -> Result<LogicalPlan> {
    if pattern.shortest.is_some() {
        return Err(Error::Plan(
            "shortestPath(...) requires both endpoints to be bound \
             by a preceding MATCH clause; standalone \
             `MATCH p = shortestPath((a)-[:R*..N]->(b))` without \
             prior bindings for a and b is not supported"
                .into(),
        ));
    }

    // Clone + pre-fill synthetic names for unnamed edges/targets
    // when the pattern has a path variable, so every hop binds a
    // named node and edge that `BindPath` can pull out of the row.
    // For path-less patterns the clone is untouched and behaves
    // identically to the original.
    let mut working = pattern.clone();
    if working.path_var.is_some() {
        ensure_path_bindings(&mut working, pattern_idx)?;
    }

    let start_var = working
        .start
        .var
        .clone()
        .unwrap_or_else(|| format!("__p{}_a0", pattern_idx));

    let (mut plan, remaining_props) = plan_start_node(
        &start_var,
        &working.start.labels,
        &working.start.properties,
        ctx,
    );
    // Any pattern properties not consumed by an IndexSeek still need a
    // filter so the executor actually enforces them.
    plan = wrap_with_pattern_prop_filter(plan, &start_var, &remaining_props);

    let plan = chain_hops_with_bound(
        plan,
        &working,
        &start_var,
        pattern_idx,
        &HashSet::new(),
        external_bound_edges,
        external_bound_edge_lists,
    )?;
    Ok(wrap_with_bind_path(plan, &working, &start_var, pattern_idx))
}

/// Lower a pattern that starts from an already-bound row-stream
/// variable. Used by `plan_match` when a chained MATCH references
/// a variable introduced by an earlier reading clause — the
/// pattern's start doesn't need a fresh scan because `input` is
/// already producing rows where the start var is bound.
///
/// v1 requires the start node to be a pure reference (no labels
/// and no properties). Labels would need a `HasLabel`-style
/// filter on top of the input, and properties would need an
/// equivalent property-equality filter; both are tractable but
/// deferred to keep the rebind path small and obviously correct.
/// The dispatcher in `plan_match` checks these preconditions
/// before calling here, so the function just asserts them.
/// Swap the start and target of a single-hop pattern and flip
/// the relationship direction so the walker proceeds from what
/// was previously the target. Only changes `start`, the single
/// hop's rel/target, and keeps everything else (labels,
/// properties, edge var, path var) intact. Used when OPTIONAL
/// MATCH's declared start is unbound but the target is already
/// bound — starting from the bound side is the only way to
/// preserve the caller's binding without cross-product
/// clobbering.
fn reverse_single_hop(pattern: &Pattern) -> Pattern {
    debug_assert_eq!(pattern.hops.len(), 1);
    let hop = &pattern.hops[0];
    let new_start = hop.target.clone();
    let new_target = pattern.start.clone();
    let new_direction = match hop.rel.direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    };
    let new_rel = crate::ast::RelPattern {
        direction: new_direction,
        ..hop.rel.clone()
    };
    Pattern {
        start: new_start,
        hops: vec![crate::ast::Hop {
            rel: new_rel,
            target: new_target,
        }],
        path_var: pattern.path_var.clone(),
        shortest: pattern.shortest,
    }
}

fn plan_pattern_from_bound(
    input: LogicalPlan,
    pattern: &Pattern,
    pattern_idx: usize,
    bound_vars: &HashMap<String, VarType>,
) -> Result<LogicalPlan> {
    // shortestPath wrapping is lowered to a dedicated operator
    // here because this is the only context where both
    // endpoints are guaranteed to be bound in the input plan.
    // `plan_pattern` (fresh-scan path) rejects the wrapping
    // up front.
    if let Some(kind) = pattern.shortest {
        return plan_shortest_path(input, pattern, kind);
    }
    let start_var = pattern
        .start
        .var
        .clone()
        .expect("plan_pattern_from_bound requires a named start variable");
    let mut working = pattern.clone();
    if working.path_var.is_some() {
        ensure_path_bindings(&mut working, pattern_idx)?;
    }
    // A rebind that tightens the start with extra labels or inline
    // properties (`MATCH (a1)-[r]->() WITH a1,r MATCH (a1:X)...`)
    // turns those assertions into a pre-filter on the bound input
    // row. The hop chain then runs against the filtered stream.
    let mut plan = input;
    if !working.start.labels.is_empty() {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: Expr::HasLabels {
                expr: Box::new(Expr::Identifier(start_var.clone())),
                labels: working.start.labels.clone(),
            },
        };
    }
    if !working.start.properties.is_empty() {
        plan = wrap_with_pattern_prop_filter(plan, &start_var, &working.start.properties);
    }
    // Any node (or scalar-but-could-be-a-node) variable from a
    // prior clause is treated as a prior binding — subsequent hops
    // that target it produce equality filters instead of fresh
    // expansions, which makes shared endpoints in multi-pattern
    // MATCH clauses (`(a)-->(x), (b)-->(x)`) and cycle patterns
    // (`(a)-[:A]->(b), (b)-[:B]->(a)`) constrain to the same node.
    let mut external_bound: HashSet<String> = HashSet::new();
    let mut external_bound_edges: HashSet<String> = HashSet::new();
    let mut external_bound_edge_lists: HashSet<String> = HashSet::new();
    for (k, v) in bound_vars {
        match v {
            VarType::Node | VarType::Scalar => {
                external_bound.insert(k.clone());
            }
            VarType::Edge => {
                external_bound_edges.insert(k.clone());
            }
            // `NonNode` is what `infer_expr_type` stamps on a WITH
            // projection of a list literal (and similar non-graph
            // expressions). That's exactly the shape a pre-bound
            // edge list arrives in, so thread those names into the
            // edge-list replay path. The executor verifies the
            // list's element types at runtime.
            VarType::NonNode => {
                external_bound_edge_lists.insert(k.clone());
            }
            _ => {}
        }
    }
    let plan = chain_hops_with_bound(
        plan,
        &working,
        &start_var,
        pattern_idx,
        &external_bound,
        &external_bound_edges,
        &external_bound_edge_lists,
    )?;
    Ok(wrap_with_bind_path(plan, &working, &start_var, pattern_idx))
}

/// Lower a `shortestPath((a)-[:R*..N]->(b))` pattern to a
/// `LogicalPlan::ShortestPath` operator. Enforces the v1
/// restrictions: the start must be a bound variable, the
/// pattern must have exactly one hop with a variable-length
/// spec (`[*..N]` or `[*M..N]`), that spec must carry an
/// upper bound, the target must be a bound variable, no
/// path-less hops (would be covered by a plain expand), and
/// `allShortestPaths` is rejected outright since v1 only
/// implements `shortestPath`.
fn plan_shortest_path(
    input: LogicalPlan,
    pattern: &Pattern,
    kind: ShortestKind,
) -> Result<LogicalPlan> {
    let path_var = pattern.path_var.clone().ok_or_else(|| {
        Error::Plan(
            "shortestPath(...) must bind a path variable (e.g. `p = shortestPath(...)`)".into(),
        )
    })?;
    let src_var = pattern.start.var.clone().ok_or_else(|| {
        Error::Plan(
            "shortestPath(...) requires the start node to be a bound variable reference".into(),
        )
    })?;
    if !pattern.start.labels.is_empty() || !pattern.start.properties.is_empty() {
        return Err(Error::Plan(
            "shortestPath(...) start node may not carry additional labels or \
             pattern properties; apply them in a preceding MATCH"
                .into(),
        ));
    }
    if pattern.hops.len() != 1 {
        return Err(Error::Plan(format!(
            "shortestPath(...) supports exactly one relationship specifier, got {}",
            pattern.hops.len()
        )));
    }
    let hop = &pattern.hops[0];
    let dst_var = hop.target.var.clone().ok_or_else(|| {
        Error::Plan(
            "shortestPath(...) requires the end node to be a bound variable reference".into(),
        )
    })?;
    if !hop.target.labels.is_empty() || !hop.target.properties.is_empty() {
        return Err(Error::Plan(
            "shortestPath(...) end node may not carry additional labels or \
             pattern properties; apply them in a preceding MATCH"
                .into(),
        ));
    }
    let var_length = hop.rel.var_length.ok_or_else(|| {
        Error::Plan(
            "shortestPath(...) requires a variable-length relationship \
             (e.g. `[:KNOWS*..5]`)"
                .into(),
        )
    })?;
    if var_length.max == u64::MAX {
        return Err(Error::Plan(
            "shortestPath(...) requires an explicit upper bound on hop count \
             to guard against unbounded BFS; use `[:R*..N]` form"
                .into(),
        ));
    }
    if var_length.min > var_length.max {
        return Err(Error::Plan(format!(
            "shortestPath(...) variable-length range min ({}) > max ({})",
            var_length.min, var_length.max
        )));
    }
    Ok(LogicalPlan::ShortestPath {
        input: Box::new(input),
        src_var,
        dst_var,
        path_var,
        edge_types: hop.rel.edge_types.clone(),
        direction: hop.rel.direction,
        kind,
        max_hops: var_length.max,
    })
}

/// Pre-fill synthetic names for unnamed edges and target nodes
/// in a path-bound pattern so every hop binds something the
/// `BindPath` operator can reference. Synthetic names match the
/// `__p{idx}_{e,a}{i}` scheme `chain_hops` uses for auto-named
/// targets, so the two halves stay consistent even if someone
/// mixes named and unnamed hops in the same pattern.
///
/// Rejects variable-length hops: `VarLengthExpand` doesn't track
/// intermediate nodes along the walk, so a `Value::Path` built
/// from its output would be missing data. Drivers that need
/// variable-length path binding should use a concrete depth
/// (`[*2..2]` → unrolled as two fixed hops) or wait for a
/// follow-up that extends `VarLengthExpand` to retain the walk.
fn ensure_path_bindings(pattern: &mut Pattern, pattern_idx: usize) -> Result<()> {
    for (i, hop) in pattern.hops.iter_mut().enumerate() {
        if hop.rel.var.is_none() {
            hop.rel.var = Some(format!("__p{}_e{}", pattern_idx, i + 1));
        }
        if hop.target.var.is_none() {
            hop.target.var = Some(format!("__p{}_a{}", pattern_idx, i + 1));
        }
    }
    Ok(())
}

/// Wrap `plan` in a `BindPath` operator when the pattern carries
/// a path variable. The collected `node_vars` / `edge_vars`
/// reference the per-hop bindings `ensure_path_bindings` filled
/// in, plus the pattern's start variable as the first node.
fn wrap_with_bind_path(
    plan: LogicalPlan,
    pattern: &Pattern,
    start_var: &str,
    pattern_idx: usize,
) -> LogicalPlan {
    let Some(path_var) = pattern.path_var.clone() else {
        return plan;
    };
    if pattern.hops.len() == 1 && pattern.hops[0].rel.var_length.is_some() {
        return plan;
    }
    let mut node_vars = Vec::with_capacity(pattern.hops.len() + 1);
    let mut edge_vars = Vec::with_capacity(pattern.hops.len());
    node_vars.push(start_var.to_string());
    for (i, hop) in pattern.hops.iter().enumerate() {
        if hop.rel.var_length.is_some() {
            // Var-length hop: the VarLengthExpand stores a sub-path
            // under this synthetic name. BindPathOp splices it.
            edge_vars.push(format!("__p{}_subpath{}", pattern_idx, i + 1));
        } else {
            edge_vars.push(
                hop.rel
                    .var
                    .clone()
                    .expect("ensure_path_bindings must have filled edge var"),
            );
        }
        node_vars.push(
            hop.target
                .var
                .clone()
                .expect("ensure_path_bindings must have filled target var"),
        );
    }
    LogicalPlan::BindPath {
        input: Box::new(plan),
        path_var,
        node_vars,
        edge_vars,
    }
}

/// Chain `EdgeExpand` / `VarLengthExpand` operators for every
/// hop in `pattern`, starting from whatever plan `plan` is
/// currently producing (with `start_var` bound in its output
/// rows). Each hop's target pattern properties lower to a
/// wrapping `Filter` via [`wrap_with_pattern_prop_filter`],
/// matching the pre-rebind behavior of `plan_pattern`.
///
/// Shared between the fresh-scan path (`plan_pattern`) and the
/// cross-stage rebind path (`plan_pattern_from_bound`). Neither
/// caller cares about where the start binding came from — the
/// expand operators pull `src_var` out of the row, so
/// "just-scanned" and "already in the row from a prior stage"
/// are semantically identical.
/// Chain `EdgeExpand` / `VarLengthExpand` operators for every
/// hop in `pattern`, starting from whatever plan `plan` is
/// currently producing (with `start_var` bound in its output
/// rows). Each hop's target pattern properties lower to a
/// wrapping `Filter` via [`wrap_with_pattern_prop_filter`],
/// matching the pre-rebind behavior of `plan_pattern`.
///
/// `external_bound_nodes` / `external_bound_edges` /
/// `external_bound_edge_lists` carry the already-bound variables
/// from the outer scope so multi-pattern MATCH clauses and
/// cross-stage rebinds hoist reuse into the appropriate constraint
/// (equality filter for nodes, edge-id check for edges, replay of
/// a pre-bound list for var-length) instead of rebinding and
/// clobbering the outer value.
fn chain_hops_with_bound(
    mut plan: LogicalPlan,
    pattern: &Pattern,
    start_var: &str,
    pattern_idx: usize,
    external_bound_nodes: &HashSet<String>,
    external_bound_edges: &HashSet<String>,
    external_bound_edge_lists: &HashSet<String>,
) -> Result<LogicalPlan> {
    // Pre-collect every hop's effective edge variable — declared
    // if named, synthesised if not — so each var-length hop can
    // exclude the others' edges and enforce relationship uniqueness
    // across the whole pattern (not just within the fixed-hop
    // chain). Synthesised names here match the formulas used
    // inside the loop below.
    let hop_edge_var_names: Vec<String> = (0..pattern.hops.len())
        .map(|i| {
            pattern.hops[i]
                .rel
                .var
                .clone()
                .unwrap_or_else(|| format!("__p{}_e{}", pattern_idx, i + 1))
        })
        .collect();
    // Track already-bound node variables so repeat uses
    // (`(n)-[r]->(n)`) become equality filters on the same
    // binding rather than a fresh expansion.
    let mut prior_node_vars: HashSet<String> = external_bound_nodes.clone();
    prior_node_vars.insert(start_var.to_string());
    // Collect every edge variable along the chain so we can
    // enforce relationship uniqueness: a single MATCH's
    // relationships must all be distinct.
    let mut chain_edge_vars: Vec<String> = Vec::new();
    let mut current_var = start_var.to_string();
    for (i, hop) in pattern.hops.iter().enumerate() {
        let declared_dst_var = hop
            .target
            .var
            .clone()
            .unwrap_or_else(|| format!("__p{}_a{}", pattern_idx, i + 1));
        // When the target reuses a prior binding (`(n)-[r]->(n)`),
        // expand into a synthetic name so EdgeExpand doesn't
        // clobber the original — then add an equality filter so
        // we only emit rows where the synthetic matches the prior
        // binding by id.
        let dst_is_reuse = prior_node_vars.contains(&declared_dst_var);
        let dst_var = if dst_is_reuse {
            format!("__p{}_rebind{}", pattern_idx, i + 1)
        } else {
            declared_dst_var.clone()
        };
        // Auto-generate an edge var when the pattern omitted one
        // so uniqueness filters below can reference it. The
        // executor ignores unused synthesised vars.
        let expand_edge_var = Some(
            hop.rel
                .var
                .clone()
                .unwrap_or_else(|| format!("__p{}_e{}", pattern_idx, i + 1)),
        );
        plan = if let Some(vl) = hop.rel.var_length {
            if vl.min > vl.max {
                // openCypher treats an inverted range (e.g.
                // `[:R*2..1]`) as a match with zero results rather
                // than a syntax error. Short-circuit the rest of
                // the chain with an always-false filter so every
                // row produced so far is dropped downstream. We
                // still bind the declared dst/edge vars so a later
                // clause that references them doesn't trip a
                // scope check — the Filter guarantees no row
                // actually carries values for them.
                return Ok(LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: Expr::Literal(Literal::Boolean(false)),
                });
            }
            // Var-length expansion does its own binding, so keep
            // the caller's edge_var (even if None) — uniqueness
            // filtering below can't cross into the subpath.
            // When the hop reuses a var that was already bound in a
            // prior clause with a non-graph-element shape (e.g.
            // `WITH [r1, r2] AS rs MATCH (first)-[rs*]->(second)`),
            // treat the var-length as a *replay* of that pre-bound
            // edge list rather than a fresh DFS. The executor
            // verifies each element is an edge and the walk stays
            // connected; no rows are emitted if the list can't be
            // followed in the required direction.
            let bound_edge_list_var = hop.rel.var.as_ref().and_then(|v| {
                if external_bound_edge_lists.contains(v) {
                    Some(v.clone())
                } else {
                    None
                }
            });
            // Relationship uniqueness across the whole pattern:
            // exclude every other hop's edge variable and every
            // outer-scope edge (bound_edges + bound_edge_lists)
            // from this var-length walk. Replay-mode hops are
            // self-consistent — the list they're replaying is the
            // pre-bound truth — so skip the exclusion there.
            let excluded_edge_vars: Vec<String> = if bound_edge_list_var.is_some() {
                Vec::new()
            } else {
                let mut out: Vec<String> = Vec::new();
                for (j, name) in hop_edge_var_names.iter().enumerate() {
                    if j != i {
                        out.push(name.clone());
                    }
                }
                for e in external_bound_edges {
                    if !out.contains(e) {
                        out.push(e.clone());
                    }
                }
                for el in external_bound_edge_lists {
                    if !out.contains(el) {
                        out.push(el.clone());
                    }
                }
                out
            };
            LogicalPlan::VarLengthExpand {
                input: Box::new(plan),
                src_var: current_var.clone(),
                edge_var: hop.rel.var.clone(),
                dst_var: dst_var.clone(),
                dst_labels: hop.target.labels.clone(),
                edge_types: hop.rel.edge_types.clone(),
                edge_properties: hop.rel.properties.clone(),
                direction: hop.rel.direction,
                min_hops: vl.min,
                max_hops: vl.max,
                path_var: if pattern.path_var.is_some() && pattern.hops.len() == 1 {
                    pattern.path_var.clone()
                } else if pattern.path_var.is_some() {
                    Some(format!("__p{}_subpath{}", pattern_idx, i + 1))
                } else {
                    None
                },
                optional: false,
                dst_constraint_var: None,
                bound_edge_list_var,
                excluded_edge_vars,
            }
        } else {
            // If the hop reuses an edge variable that was bound in
            // a prior clause, constrain the expansion to that
            // specific edge by id — matches how the planner
            // already handles reused node variables as prior_node
            // equality filters.
            let edge_constraint_var = hop.rel.var.as_ref().and_then(|v| {
                if external_bound_edges.contains(v) {
                    Some(v.clone())
                } else {
                    None
                }
            });
            LogicalPlan::EdgeExpand {
                input: Box::new(plan),
                src_var: current_var.clone(),
                edge_var: expand_edge_var.clone(),
                dst_var: dst_var.clone(),
                dst_labels: hop.target.labels.clone(),
                edge_properties: hop.rel.properties.clone(),
                edge_types: hop.rel.edge_types.clone(),
                direction: hop.rel.direction,
                edge_constraint_var,
            }
        };
        // Lower the target node's pattern properties to a Filter
        // wrapping the expand for the same reason as the start node.
        plan = wrap_with_pattern_prop_filter(plan, &dst_var, &hop.target.properties);
        // If the target reuses a prior binding, check the synthesised
        // dst var equals the declared one by id, then drop the
        // rename so downstream clauses still see `declared_dst_var`
        // (the executor keeps both bindings, which is fine — the
        // synthetic name is scoped to this hop).
        if dst_is_reuse {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: Expr::Compare {
                    op: CompareOp::Eq,
                    left: Box::new(Expr::Identifier(dst_var.clone())),
                    right: Box::new(Expr::Identifier(declared_dst_var.clone())),
                },
            };
        }
        // Relationship uniqueness: every previously-bound edge in
        // this MATCH must be a different edge from the one we
        // just traversed. Only applies to fixed-length hops (we
        // don't have a synthetic edge var for var-length
        // expansions).
        if hop.rel.var_length.is_none() {
            // User-declared edge var appearing in two hops is a
            // `RelationshipUniquenessViolation` — Cypher forbids
            // the same edge variable binding to two positions in
            // one pattern. Raise a plan-time error instead of
            // emitting an always-false `r != r` filter.
            if let Some(declared) = hop.rel.var.as_ref() {
                if chain_edge_vars.iter().any(|e| e == declared) {
                    return Err(Error::Plan(format!(
                        "relationship variable '{declared}' cannot be used twice \
                         in the same pattern"
                    )));
                }
            }
            if let Some(this_edge) = expand_edge_var.as_ref() {
                for prior_edge in &chain_edge_vars {
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate: Expr::Compare {
                            op: CompareOp::Ne,
                            left: Box::new(Expr::Identifier(this_edge.clone())),
                            right: Box::new(Expr::Identifier(prior_edge.clone())),
                        },
                    };
                }
                chain_edge_vars.push(this_edge.clone());
            }
        }
        prior_node_vars.insert(declared_dst_var.clone());
        // Continue the chain from the declared name if we didn't
        // rename, or the rebind synthesis otherwise. Downstream
        // hops should key off the declared form.
        current_var = if dst_is_reuse {
            declared_dst_var
        } else {
            dst_var
        };
    }
    Ok(plan)
}

/// Decide how to scan the start node of a pattern. When the node has
/// exactly one label and a registered index on that label covers a
/// prefix of the pattern's properties, emit [`LogicalPlan::IndexSeek`]
/// on that prefix. Everything left over becomes a residual filter
/// the caller wraps around the seek. Otherwise fall back to the
/// `NodeScanAll` / `NodeScanByLabels` path.
///
/// Prefix matching (not "any covered property") is what makes a
/// composite index `(a, b, c)` usable for patterns that bind `a`
/// alone or `(a, b)`; patterns that bind only `b` / `c` don't hit
/// this index. See [`PlannerContext::best_index_prefix`].
fn plan_start_node(
    var: &str,
    labels: &[String],
    properties: &[(String, Expr)],
    ctx: &PlannerContext,
) -> (LogicalPlan, Vec<(String, Expr)>) {
    if labels.len() == 1 && !properties.is_empty() {
        let label = &labels[0];
        let available: Vec<String> = properties.iter().map(|(k, _)| k.clone()).collect();
        if let Some(prefix) = ctx.best_index_prefix(label, &available) {
            // Materialize the seek keys / values in index order so the
            // tuple lines up with the stored key. Properties not in
            // the prefix ride along as residuals — wrapped as a
            // pattern-property filter by the caller.
            let mut seek_props: Vec<String> = Vec::with_capacity(prefix.len());
            let mut seek_values: Vec<Expr> = Vec::with_capacity(prefix.len());
            let mut residual: Vec<(String, Expr)> = Vec::new();
            let prefix_set: std::collections::HashSet<&str> =
                prefix.iter().map(String::as_str).collect();
            for (k, v) in properties {
                if prefix_set.contains(k.as_str()) {
                    // Push in prefix order below, not in pattern order.
                    continue;
                }
                residual.push((k.clone(), v.clone()));
            }
            for p in &prefix {
                let (_, value_expr) = properties
                    .iter()
                    .find(|(k, _)| k == p)
                    .expect("prefix came from the property list");
                seek_props.push(p.clone());
                seek_values.push(value_expr.clone());
            }
            let seek = LogicalPlan::IndexSeek {
                var: var.to_string(),
                label: label.clone(),
                properties: seek_props,
                values: seek_values,
            };
            return (seek, residual);
        }
    }
    let base = if labels.is_empty() {
        LogicalPlan::NodeScanAll {
            var: var.to_string(),
        }
    } else {
        LogicalPlan::NodeScanByLabels {
            var: var.to_string(),
            labels: labels.to_vec(),
        }
    };
    (base, properties.to_vec())
}

/// If `properties` is non-empty, wrap `plan` in a `Filter` whose
/// predicate is `var.k1 = v1 AND var.k2 = v2 AND ...` for every entry.
/// Used to lower MATCH-side pattern properties (which the executor
/// otherwise doesn't see) into a real predicate. Empty `properties`
/// leaves the plan unchanged.
fn wrap_with_pattern_prop_filter(
    plan: LogicalPlan,
    var: &str,
    properties: &[(String, Expr)],
) -> LogicalPlan {
    use crate::ast::CompareOp;
    if properties.is_empty() {
        return plan;
    }
    let mut acc: Option<Expr> = None;
    for (key, value_expr) in properties {
        let cmp = Expr::Compare {
            op: CompareOp::Eq,
            left: Box::new(Expr::Property {
                var: var.to_string(),
                key: key.clone(),
            }),
            right: Box::new(value_expr.clone()),
        };
        acc = Some(match acc {
            None => cmp,
            Some(prev) => Expr::And(Box::new(prev), Box::new(cmp)),
        });
    }
    LogicalPlan::Filter {
        input: Box::new(plan),
        predicate: acc.expect("non-empty properties yields Some"),
    }
}

/// Try to rewrite a `Filter(... NodeScanByLabels)` chain into an
/// `IndexSeek` plus a residual filter when one of the conjuncts is
/// an indexed equality on the scan's variable.
///
/// Walks down through any number of stacked `Filter` wrappers
/// (collecting all their conjuncts), inspects the leaf, and only
/// rewrites when the leaf is `NodeScanByLabels` with exactly one
/// label. If no covered equality is found, the original chain is
/// rebuilt unchanged. If a rewrite happens and no conjuncts are
/// left over, the residual `Filter` wrapper is dropped entirely.
///
/// Only fires at the top of the plan. Filters buried under
/// `EdgeExpand` / `CartesianProduct` / etc. don't get rewritten by
/// this pass — they belong to a downstream join scope and the
/// extra plumbing for cost-based seek selection there isn't
/// justified by the current workloads. The pattern-property rewrite
/// in `plan_start_node` already covers the most common buried case.
fn optimize_filter_chain_to_index_seek(plan: LogicalPlan, ctx: &PlannerContext) -> LogicalPlan {
    if !matches!(plan, LogicalPlan::Filter { .. }) {
        return plan;
    }

    let mut conjuncts: Vec<Expr> = Vec::new();
    let mut current = plan;
    while let LogicalPlan::Filter { input, predicate } = current {
        push_conjuncts(predicate, &mut conjuncts);
        current = *input;
    }

    let (scan_var, scan_label) = match &current {
        LogicalPlan::NodeScanByLabels { var, labels } if labels.len() == 1 => {
            (var.clone(), labels[0].clone())
        }
        _ => return rebuild_filter_chain(current, conjuncts),
    };

    // Classify conjuncts: those that are row-independent
    // `scan_var.prop = value` equalities go into `eq_map` keyed by
    // property; everything else (including duplicate equalities on
    // the same property — only the first one is usable as a seek key)
    // stays in `residual_idxs` as filter material.
    let mut eq_map: std::collections::HashMap<String, (usize, Expr)> =
        std::collections::HashMap::new();
    let mut residual_idxs: Vec<usize> = Vec::new();
    for (i, c) in conjuncts.iter().enumerate() {
        match extract_property_eq(c, &scan_var) {
            Some((key, value)) if !eq_map.contains_key(&key) => {
                eq_map.insert(key, (i, value));
            }
            _ => residual_idxs.push(i),
        }
    }

    let available: Vec<String> = eq_map.keys().cloned().collect();
    let Some(prefix) = ctx.best_index_prefix(&scan_label, &available) else {
        return rebuild_filter_chain(current, conjuncts);
    };

    // Pull seek keys + values in prefix order. Every prefix entry
    // was proven to be in `eq_map` by `best_index_prefix`.
    let mut seek_props: Vec<String> = Vec::with_capacity(prefix.len());
    let mut seek_values: Vec<Expr> = Vec::with_capacity(prefix.len());
    let mut consumed_idxs: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for p in &prefix {
        let (idx, value) = eq_map
            .remove(p)
            .expect("best_index_prefix returned a property not in eq_map");
        consumed_idxs.insert(idx);
        seek_props.push(p.clone());
        seek_values.push(value);
    }
    // Equalities on non-prefix properties go back into residual.
    for (_, (idx, _)) in eq_map {
        residual_idxs.push(idx);
    }
    residual_idxs.sort_unstable();

    let seek = LogicalPlan::IndexSeek {
        var: scan_var,
        label: scan_label,
        properties: seek_props,
        values: seek_values,
    };
    let residual: Vec<Expr> = conjuncts
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !consumed_idxs.contains(i))
        .map(|(_, c)| c)
        .collect();
    rebuild_filter_chain(seek, residual)
}

/// Complement of [`optimize_filter_chain_to_index_seek`] for point /
/// spatial indexes. Looks for a conjunct of the form
/// `point.withinbbox(scan_var.prop, lo, hi)` with both corners
/// row-independent, and — if a point index is registered on
/// `(label, prop)` — rewrites to [`LogicalPlan::PointIndexSeek`]
/// with the rest of the conjuncts preserved as a residual filter.
///
/// Only the first matching `withinbbox` is consumed. Multiple bbox
/// predicates on the same node would need an intersection-of-bboxes
/// optimization that isn't implemented yet; for now the second one
/// stays in the residual filter and the executor re-checks it on
/// each seeked row.
///
/// Chained after the equality-seek rewrite so both can fire in a
/// single query: `WHERE n.q = 1 AND point.withinbbox(n.p, ...)`
/// prefers the IndexSeek on `q` when that's available. The point
/// rewrite only fires against a leaf `NodeScanByLabels`, so if the
/// equality rewrite already consumed the scan, the point check bails
/// cleanly and the withinbbox stays in the residual.
fn optimize_filter_chain_to_point_index_seek(
    plan: LogicalPlan,
    ctx: &PlannerContext,
) -> LogicalPlan {
    if !matches!(plan, LogicalPlan::Filter { .. }) {
        return plan;
    }

    let mut conjuncts: Vec<Expr> = Vec::new();
    let mut current = plan;
    while let LogicalPlan::Filter { input, predicate } = current {
        push_conjuncts(predicate, &mut conjuncts);
        current = *input;
    }

    let (scan_var, scan_label) = match &current {
        LogicalPlan::NodeScanByLabels { var, labels } if labels.len() == 1 => {
            (var.clone(), labels[0].clone())
        }
        _ => return rebuild_filter_chain(current, conjuncts),
    };

    let mut consumed: Option<(usize, String, Expr, Expr)> = None;
    for (i, c) in conjuncts.iter().enumerate() {
        if let Some((prop, lo, hi)) = extract_point_withinbbox(c, &scan_var) {
            if ctx.has_point_index(&scan_label, &prop) {
                consumed = Some((i, prop, lo, hi));
                break;
            }
        }
    }

    if let Some((idx, property, lo, hi)) = consumed {
        let seek = LogicalPlan::PointIndexSeek {
            var: scan_var,
            label: scan_label,
            property,
            bounds: PointSeekBounds::Corners { lo, hi },
        };
        let residual: Vec<Expr> = conjuncts
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, c)| c)
            .collect();
        return rebuild_filter_chain(seek, residual);
    }

    // Distance-radius rewrite: `point.distance(scan_var.p, ref) {<,<=} r`
    // or the flipped form. The conjunct stays in the residual filter
    // — the operator's enclosing bbox is a superset of the circle,
    // so the distance predicate culls the corner overshoot.
    for c in &conjuncts {
        let Some((property, center, radius)) = extract_point_distance_bound(c, &scan_var) else {
            continue;
        };
        if !ctx.has_point_index(&scan_label, &property) {
            continue;
        }
        let seek = LogicalPlan::PointIndexSeek {
            var: scan_var,
            label: scan_label,
            property,
            bounds: PointSeekBounds::Radius { center, radius },
        };
        return rebuild_filter_chain(seek, conjuncts);
    }

    rebuild_filter_chain(current, conjuncts)
}

/// Relationship-scope analogue of
/// [`optimize_filter_chain_to_point_index_seek`]. Recognizes a
/// `Filter { ... EdgeExpand { NodeScanAll } }` leaf shape lowered
/// from `MATCH ()-[r:T]-()` with both endpoints anonymous, matches
/// `point.withinbbox(r.p, lo, hi)` or distance-radius predicates
/// against an edge point index on `(T, p)`, and rewrites to
/// [`LogicalPlan::EdgePointIndexSeek`] — bypassing the full-graph
/// edge expansion.
///
/// Gating conditions (deliberately conservative so the rewrite
/// doesn't change the semantics of any existing pattern):
/// - Exactly one `Filter` chain above one `EdgeExpand`.
/// - The `EdgeExpand` input is `NodeScanAll { var }` where `var`
///   matches the expand's `src_var`.
/// - The pattern names a single edge type, no inline edge
///   properties, no pre-bound edge constraint, no destination
///   label filter.
/// - The edge variable is bound (anonymous edges can't host a
///   WHERE predicate).
fn optimize_filter_chain_to_edge_point_index_seek(
    plan: LogicalPlan,
    ctx: &PlannerContext,
) -> LogicalPlan {
    if !matches!(plan, LogicalPlan::Filter { .. }) {
        return plan;
    }

    let mut conjuncts: Vec<Expr> = Vec::new();
    let mut current = plan;
    while let LogicalPlan::Filter { input, predicate } = current {
        push_conjuncts(predicate, &mut conjuncts);
        current = *input;
    }

    // The leaf must be an EdgeExpand over a NodeScanAll whose var
    // is the expand's src_var. Anything else falls through — the
    // rewrite is opt-in for this one shape.
    let (edge_var, src_var, dst_var, edge_type, direction) = match &current {
        LogicalPlan::EdgeExpand {
            input,
            src_var,
            edge_var: Some(edge_var),
            dst_var,
            dst_labels,
            edge_properties,
            edge_types,
            direction,
            edge_constraint_var,
        } if edge_types.len() == 1
            && edge_properties.is_empty()
            && dst_labels.is_empty()
            && edge_constraint_var.is_none() =>
        {
            match input.as_ref() {
                LogicalPlan::NodeScanAll { var } if var == src_var => (
                    edge_var.clone(),
                    src_var.clone(),
                    dst_var.clone(),
                    edge_types[0].clone(),
                    *direction,
                ),
                _ => return rebuild_filter_chain(current, conjuncts),
            }
        }
        _ => return rebuild_filter_chain(current, conjuncts),
    };

    // Strategy 1: withinbbox — consume the conjunct if a matching
    // edge point index is registered. The storage bbox filter is
    // exact, so no residual is needed from this predicate.
    let mut consumed: Option<(usize, String, Expr, Expr)> = None;
    for (i, c) in conjuncts.iter().enumerate() {
        if let Some((prop, lo, hi)) = extract_point_withinbbox(c, &edge_var) {
            if ctx.has_edge_point_index(&edge_type, &prop) {
                consumed = Some((i, prop, lo, hi));
                break;
            }
        }
    }
    if let Some((idx, property, lo, hi)) = consumed {
        let seek = LogicalPlan::EdgePointIndexSeek {
            edge_var,
            src_var,
            dst_var,
            edge_type,
            property,
            direction,
            bounds: PointSeekBounds::Corners { lo, hi },
        };
        let residual: Vec<Expr> = conjuncts
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, c)| c)
            .collect();
        return rebuild_filter_chain(seek, residual);
    }

    // Strategy 2: distance-radius. Keep the distance conjunct as a
    // residual Filter so the circle-vs-square overshoot gets culled.
    for c in &conjuncts {
        let Some((property, center, radius)) = extract_point_distance_bound(c, &edge_var) else {
            continue;
        };
        if !ctx.has_edge_point_index(&edge_type, &property) {
            continue;
        }
        let seek = LogicalPlan::EdgePointIndexSeek {
            edge_var,
            src_var,
            dst_var,
            edge_type,
            property,
            direction,
            bounds: PointSeekBounds::Radius { center, radius },
        };
        return rebuild_filter_chain(seek, conjuncts);
    }

    rebuild_filter_chain(current, conjuncts)
}

/// Recognize `point.withinbbox(scan_var.prop, lo, hi)` as a
/// candidate for `PointIndexSeek`. Returns the indexed property name
/// plus the two corner expressions, or `None` when:
///
/// - the call isn't `point.withinbbox` (case-insensitive),
/// - the first argument isn't `scan_var.<ident>`,
/// - either corner references row state.
///
/// The corner check accepts arbitrary row-independent expressions
/// (not just `Literal` / `Parameter` like the equality-seek rewrite
/// does), so the common `point({x: 1, y: 2})` constructor call form
/// is eligible. The operator evaluates corners once at open, so
/// pure-but-non-trivial expressions (map literals, scalar calls on
/// params) are cheap.
fn extract_point_withinbbox(c: &Expr, scan_var: &str) -> Option<(String, Expr, Expr)> {
    use crate::ast::CallArgs;
    let Expr::Call { name, args } = c else {
        return None;
    };
    if !name.eq_ignore_ascii_case("point.withinbbox") {
        return None;
    }
    let CallArgs::Exprs(args) = args else {
        return None;
    };
    if args.len() != 3 {
        return None;
    }
    let prop = match &args[0] {
        Expr::Property { var, key } if var == scan_var => key.clone(),
        _ => return None,
    };
    if expr_references_row_state(&args[1]) || expr_references_row_state(&args[2]) {
        return None;
    }
    Some((prop, args[1].clone(), args[2].clone()))
}

/// Recognize `point.distance(scan_var.prop, center) {<,<=} radius`
/// (or the flipped `radius {>,>=} point.distance(...)` form) as a
/// candidate for a radius-based `PointIndexSeek`. The shorthand
/// `distance(...)` alias is accepted. Returns
/// `(property_name, center_expr, radius_expr)` or `None`.
///
/// Strict `<` / `<=` semantics both reduce to the same bbox at the
/// planner level — the enclosing bbox is computed from the radius
/// alone, and the comparison-op's exact cutoff (open vs closed
/// interval) is preserved by the unchanged residual filter. Caller
/// keeps the original conjunct in the plan so the filter still
/// enforces the right open/closed boundary.
fn extract_point_distance_bound(c: &Expr, scan_var: &str) -> Option<(String, Expr, Expr)> {
    use crate::ast::{CallArgs, CompareOp};
    let Expr::Compare { op, left, right } = c else {
        return None;
    };
    let (distance_call, radius) = match op {
        CompareOp::Lt | CompareOp::Le => (left.as_ref(), right.as_ref()),
        CompareOp::Gt | CompareOp::Ge => (right.as_ref(), left.as_ref()),
        _ => return None,
    };
    let Expr::Call { name, args } = distance_call else {
        return None;
    };
    if !name.eq_ignore_ascii_case("point.distance") && !name.eq_ignore_ascii_case("distance") {
        return None;
    }
    let CallArgs::Exprs(call_args) = args else {
        return None;
    };
    if call_args.len() != 2 {
        return None;
    }
    // Either arg may be `scan_var.prop`; the other is the reference
    // point. Both orientations show up in real queries — Neo4j's
    // `point.distance` is symmetric.
    let (prop, reference) = match (&call_args[0], &call_args[1]) {
        (Expr::Property { var, key }, other) if var == scan_var => (key.clone(), other.clone()),
        (other, Expr::Property { var, key }) if var == scan_var => (key.clone(), other.clone()),
        _ => return None,
    };
    if expr_references_row_state(&reference) || expr_references_row_state(radius) {
        return None;
    }
    Some((prop, reference, radius.clone()))
}

/// `true` when `e` could read values from the currently-streaming
/// row. Looser than [`expr_is_row_independent`]: map/list/call/etc.
/// forms are allowed as long as their children are also
/// row-independent. Used by the point-index rewrite so
/// `point({x: 0, y: 0})` literal corners can be lifted out of the
/// filter. Unknown shapes return `true` to stay on the safe side —
/// a conservative "no" on rewrite, rather than a wrong plan.
fn expr_references_row_state(e: &Expr) -> bool {
    use crate::ast::CallArgs;
    match e {
        Expr::Identifier(_) | Expr::Property { .. } | Expr::PropertyAccess { .. } => true,
        Expr::Literal(_) | Expr::Parameter(_) => false,
        Expr::Not(a) => expr_references_row_state(a),
        Expr::IsNull { inner, .. } => expr_references_row_state(inner),
        Expr::And(a, b) | Expr::Or(a, b) | Expr::Xor(a, b) => {
            expr_references_row_state(a) || expr_references_row_state(b)
        }
        Expr::Compare { left, right, .. } => {
            expr_references_row_state(left) || expr_references_row_state(right)
        }
        Expr::IndexAccess { base, index } => {
            expr_references_row_state(base) || expr_references_row_state(index)
        }
        Expr::SliceAccess { base, start, end } => {
            expr_references_row_state(base)
                || start.as_deref().map_or(false, expr_references_row_state)
                || end.as_deref().map_or(false, expr_references_row_state)
        }
        Expr::HasLabels { expr, .. } => expr_references_row_state(expr),
        Expr::InList { element, list } => {
            expr_references_row_state(element) || expr_references_row_state(list)
        }
        Expr::Call { args, .. } => match args {
            CallArgs::Star => false,
            CallArgs::Exprs(a) | CallArgs::DistinctExprs(a) => {
                a.iter().any(expr_references_row_state)
            }
        },
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            scrutinee
                .as_deref()
                .map_or(false, expr_references_row_state)
                || branches
                    .iter()
                    .any(|(c, r)| expr_references_row_state(c) || expr_references_row_state(r))
                || else_expr
                    .as_deref()
                    .map_or(false, expr_references_row_state)
        }
        Expr::List(items) => items.iter().any(expr_references_row_state),
        Expr::Map(pairs) => pairs.iter().any(|(_, v)| expr_references_row_state(v)),
        // Unhandled shapes (comprehensions, reduce, pattern predicates,
        // subquery exprs): treat as row-referencing. Widening this set
        // is safe but unnecessary — the point corners in real queries
        // are literals, parameters, or the `point({...})` constructor.
        _ => true,
    }
}

/// Extract `(key, value)` from a conjunct that's a row-independent
/// equality on `scan_var.key`, in either argument order. `None` for
/// anything else — that includes equalities whose value side isn't
/// row-independent (e.g. references another variable) and non-`Eq`
/// comparisons. Index lookup is equality-only.
fn extract_property_eq(c: &Expr, scan_var: &str) -> Option<(String, Expr)> {
    use crate::ast::CompareOp;
    let Expr::Compare { op, left, right } = c else {
        return None;
    };
    if *op != CompareOp::Eq {
        return None;
    }
    if let Some((key, value)) = match_property_eq(left, right, scan_var) {
        if expr_is_row_independent(&value) {
            return Some((key, value));
        }
    }
    if let Some((key, value)) = match_property_eq(right, left, scan_var) {
        if expr_is_row_independent(&value) {
            return Some((key, value));
        }
    }
    None
}

/// Decompose `e` into a flat list of conjuncts. `Expr::And` is
/// recursively split; everything else becomes a single entry.
/// Used by the WHERE-rewrite pass to find an indexed equality
/// hidden inside an arbitrary `AND` chain.
fn push_conjuncts(e: Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::And(l, r) => {
            push_conjuncts(*l, out);
            push_conjuncts(*r, out);
        }
        other => out.push(other),
    }
}

/// Reassemble `conjuncts` into a single `And`-chain wrapped around
/// `leaf` as a `Filter`. An empty conjunct list returns `leaf` bare.
/// Inverse of `push_conjuncts`.
fn rebuild_filter_chain(leaf: LogicalPlan, mut conjuncts: Vec<Expr>) -> LogicalPlan {
    if conjuncts.is_empty() {
        return leaf;
    }
    if conjuncts.len() == 1 {
        return LogicalPlan::Filter {
            input: Box::new(leaf),
            predicate: conjuncts.pop().unwrap(),
        };
    }
    let mut iter = conjuncts.into_iter();
    let first = iter.next().unwrap();
    let predicate = iter.fold(first, |acc, c| Expr::And(Box::new(acc), Box::new(c)));
    LogicalPlan::Filter {
        input: Box::new(leaf),
        predicate,
    }
}

/// Try to interpret `c` as an indexed equality predicate on
/// `scan_var.scan_label`. Returns the property key and the
/// row-independent value expression so the caller can build an
/// `IndexSeek`.
///
/// Accepts both `Property == value` and `value == Property` forms.
/// "Row-independent" means the value side is a literal or a
/// parameter — anything else (e.g., `n.name = m.name`) can't be
/// hoisted into a seek because the value isn't known when the
/// scan starts.
fn match_property_eq(maybe_prop: &Expr, value: &Expr, scan_var: &str) -> Option<(String, Expr)> {
    if let Expr::Property { var, key } = maybe_prop {
        if var == scan_var {
            return Some((key.clone(), value.clone()));
        }
    }
    None
}

/// True when `e` can be evaluated against an empty row — i.e., when
/// the executor can resolve it once at scan time without binding
/// any pattern variables. Only literals and parameters qualify in
/// v1; this is enough for the cases drivers actually emit.
fn expr_is_row_independent(e: &Expr) -> bool {
    matches!(e, Expr::Literal(_) | Expr::Parameter(_))
}

fn collect_pattern_vars(pattern: &Pattern, out: &mut HashSet<String>) {
    if let Some(var) = &pattern.start.var {
        out.insert(var.clone());
    }
    // `OPTIONAL MATCH p = (a)-->(b) RETURN p` needs `p` to bind to
    // null when the pattern doesn't match. Include the path var in
    // the set the null-row fallback populates.
    if let Some(pv) = &pattern.path_var {
        out.insert(pv.clone());
    }
    for hop in &pattern.hops {
        if let Some(var) = &hop.rel.var {
            out.insert(var.clone());
        }
        if let Some(var) = &hop.target.var {
            out.insert(var.clone());
        }
    }
}

fn collect_pattern_vars_typed(pattern: &Pattern, out: &mut HashMap<String, VarType>) -> Result<()> {
    if let Some(var) = &pattern.start.var {
        check_var_type_conflict_allow_scalar(var, VarType::Node, out)?;
        out.insert(var.clone(), VarType::Node);
    }
    if let Some(pv) = &pattern.path_var {
        check_var_type_conflict_allow_scalar(pv, VarType::Path, out)?;
        out.insert(pv.clone(), VarType::Path);
    }
    for hop in &pattern.hops {
        if let Some(var) = &hop.rel.var {
            // Var-length rel vars that reuse an outer `NonNode`
            // binding are the edge-list replay form
            // (`WITH [r1, r2] AS rs MATCH (a)-[rs*]->(b)`). Leave
            // the outer type in place; the executor pulls the
            // list out of the row at runtime. The inline check in
            // `plan_match` already validated that *cross-scope*
            // transition; this path just preserves it so a later
            // same-pattern reuse still conflicts.
            let is_edge_list_replay =
                hop.rel.var_length.is_some() && matches!(out.get(var), Some(VarType::NonNode));
            if !is_edge_list_replay {
                check_var_type_conflict_allow_scalar(var, VarType::Edge, out)?;
                out.insert(var.clone(), VarType::Edge);
            }
        }
        if let Some(var) = &hop.target.var {
            check_var_type_conflict_allow_scalar(var, VarType::Node, out)?;
            out.insert(var.clone(), VarType::Node);
        }
    }
    Ok(())
}

/// Like [`check_var_type_conflict`] but tolerates a pre-existing
/// `Scalar` binding. Used by `collect_pattern_vars_typed` to
/// refine vars introduced by a WITH projection — e.g.
/// `WITH coalesce(b, c) AS x` initially marks `x` as Scalar (the
/// planner can't narrow a `Call`), then a subsequent
/// `MATCH (x)-->(d)` should be allowed to pin `x` to Node. The
/// strict check is kept for the CREATE / pattern-predicate
/// scope-check paths that still want Node ≠ Edge to be hard
/// errors.
/// Top-level shape check for an expression supplied to `DELETE`.
/// Only accepts shapes that can plausibly evaluate to a graph
/// element (Node / Edge / Path) or Null at runtime — everything
/// else produces an openCypher `InvalidArgumentType`. Also
/// enforces the scope rule on identifiers: a DELETE reference
/// to an unbound name is a compile-time `UndefinedVariable`.
fn validate_delete_expr(expr: &Expr, bound_vars: &HashMap<String, VarType>) -> Result<()> {
    match expr {
        // Identifier: must be bound. The type isn't checked here
        // (a Scalar / NonNode binding might be wrong at runtime),
        // but catching the unbound case is the common failure.
        Expr::Identifier(name) => {
            if !bound_vars.contains_key(name) {
                return Err(Error::Plan(format!(
                    "DELETE references undefined variable '{name}'"
                )));
            }
        }
        // Accessors that can land on graph-element values at
        // runtime: property access (map.key), index / slice
        // access (list[0], map[key]), parameters, CASE, calls
        // (e.g. `head([n])`), and null literals.
        Expr::Property { var, .. } => {
            if !bound_vars.contains_key(var) {
                return Err(Error::Plan(format!(
                    "DELETE references undefined variable '{var}'"
                )));
            }
        }
        Expr::PropertyAccess { base, .. }
        | Expr::IndexAccess { base, .. }
        | Expr::SliceAccess { base, .. } => {
            validate_delete_expr_identifiers(base, bound_vars)?;
        }
        Expr::Parameter(_) => {}
        Expr::Case { .. } => {
            // CASE branches might produce graph elements; identifier
            // checks inside the branches happen at eval time.
        }
        Expr::Call { args, .. } => {
            // Function result could be anything — leave the shape
            // check to the executor, but still verify referenced
            // identifiers are in scope.
            match args {
                CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) => {
                    for e in es {
                        validate_delete_expr_identifiers(e, bound_vars)?;
                    }
                }
                CallArgs::Star => {}
            }
        }
        Expr::Literal(Literal::Null) => {}
        _ => {
            return Err(Error::Plan(
                "DELETE argument must be a node, edge, path, or null \
                 (got a scalar expression)"
                    .into(),
            ));
        }
    }
    Ok(())
}

/// Walk a nested expression and validate every identifier is
/// bound — used as a sub-routine by `validate_delete_expr` on
/// argument sub-expressions where the result shape is
/// unknowable at compile time but name resolution still matters.
fn validate_delete_expr_identifiers(
    expr: &Expr,
    bound_vars: &HashMap<String, VarType>,
) -> Result<()> {
    let mut err: Option<Error> = None;
    walk_expr(expr, &mut |e| {
        if let Expr::Identifier(name) = e {
            if !bound_vars.contains_key(name) {
                err = Some(Error::Plan(format!(
                    "DELETE references undefined variable '{name}'"
                )));
            }
        }
        Ok(())
    })?;
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn check_var_type_conflict_allow_scalar(
    var: &str,
    new_type: VarType,
    bound_vars: &HashMap<String, VarType>,
) -> Result<()> {
    if let Some(&existing) = bound_vars.get(var) {
        if existing != new_type && existing != VarType::Scalar {
            return Err(Error::Plan(format!(
                "variable '{}' already defined with a different type",
                var
            )));
        }
    }
    Ok(())
}

fn infer_expr_type(expr: &Expr, bound_vars: &HashMap<String, VarType>) -> VarType {
    match expr {
        Expr::Identifier(name) => bound_vars.get(name).copied().unwrap_or(VarType::Node),
        // Literals / list+map constructors / arithmetic & boolean
        // results are *definitively* not graph elements — marking
        // them `NonNode` lets the scope check statically reject
        // `WITH 123 AS n / MATCH (n)` while still allowing the
        // broader Scalar→Node refinement for opaque values.
        Expr::Literal(_)
        | Expr::List(_)
        | Expr::Map(_)
        | Expr::Compare { .. }
        | Expr::And(_, _)
        | Expr::Or(_, _)
        | Expr::Xor(_, _)
        | Expr::Not(_)
        | Expr::IsNull { .. }
        | Expr::HasLabels { .. }
        | Expr::InList { .. }
        | Expr::ListPredicate { .. }
        | Expr::PatternExists(_)
        | Expr::ExistsSubquery { .. }
        | Expr::CountSubquery { .. }
        | Expr::CollectSubquery { .. }
        | Expr::ListComprehension { .. }
        | Expr::PatternComprehension { .. }
        | Expr::Reduce { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. } => VarType::NonNode,
        // Parameters, property access, function calls, CASE,
        // index/slice access — could evaluate to anything at
        // runtime (including a node via a user-defined function
        // or `head([n])`), so stay as `Scalar` and let the
        // relaxed scope check admit a later pattern's role
        // refinement.
        Expr::Parameter(_)
        | Expr::Property { .. }
        | Expr::PropertyAccess { .. }
        | Expr::IndexAccess { .. }
        | Expr::SliceAccess { .. }
        | Expr::Case { .. }
        | Expr::Call { .. } => VarType::Scalar,
    }
}

fn plan_match(stmt: &MatchStmt, ctx: &PlannerContext) -> Result<LogicalPlan> {
    use crate::ast::ReadingClause;

    // Walk the reading clauses in order. Each clause extends
    // the current plan tree by its own rules:
    //
    //   * `Match`  — plan each pattern fresh, cartesian-join
    //                with the current row stream, apply any
    //                WHERE, then run the index-seek rewrite.
    //   * `OptionalMatch` — reuse the single-hop left-join
    //                operator; the tracker feeds its "start var
    //                must be already bound" restriction.
    //   * `With`   — project / aggregate / filter / order /
    //                skip / limit; new bindings shadow the
    //                old ones downstream.
    //
    // `bound_vars` accumulates across the whole loop so
    // downstream clauses can enforce variable-scoping rules
    // against every earlier stage.
    let mut plan: Option<LogicalPlan> = None;
    let mut bound_vars: HashMap<String, VarType> = HashMap::new();
    // Seed with any enclosing-scope bindings the caller passed
    // through PlannerContext. Only used today for correlated
    // exists/count subqueries, but the wiring is general enough
    // to support any nested-plan lowering that needs outer
    // variables in scope. When the caller supplies outer bindings
    // we also plant a `SeedRow` as the plan's producer so the
    // first MATCH on a pre-bound variable can lower as a rebind
    // (no fresh scan) — the executor wires the outer row into
    // `SeedRow` via `build_op_inner`'s `seed` parameter.
    for (name, kind) in &ctx.outer_bindings {
        let vt = match kind {
            OuterBindingKind::Node => VarType::Node,
            OuterBindingKind::Edge => VarType::Edge,
            OuterBindingKind::Scalar => VarType::Scalar,
        };
        bound_vars.insert(name.clone(), vt);
    }
    if !ctx.outer_bindings.is_empty() {
        plan = Some(LogicalPlan::SeedRow);
    }
    let mut stage_pattern_offset: usize = 0;

    for clause in &stmt.clauses {
        match clause {
            ReadingClause::Match(m) => {
                // Reject patterns that would bind a variable
                // that's already in scope — with one carve-out
                // for the cross-stage rebind case: a pattern's
                // *start* variable is allowed to reference an
                // already-bound name from an earlier reading
                // clause, as long as the start node is a pure
                // reference (no labels, no properties). That
                // turns `MATCH (a) WITH a MATCH (a)-[:X]->(b)`
                // into an expand from the existing row stream
                // instead of a fresh scan + cartesian join.
                //
                // Everything else — rebinding a hop destination,
                // rebinding inside the same comma-separated
                // MATCH, or rebinding with a labelled start node
                // — still surfaces as a clear plan-time error.
                let mut this_clause_vars: HashSet<String> = HashSet::new();
                for pattern in &m.patterns {
                    // `shortestPath(...)` patterns are validated
                    // entirely inside `plan_shortest_path` at
                    // lowering time — both endpoints are
                    // required to be pre-bound, and the
                    // cross-stage rebind check here would reject
                    // the valid shape because the target var is
                    // also in `bound_vars`. Skip this pattern's
                    // per-var validation and let the lowering
                    // path produce an actionable error.
                    if pattern.shortest.is_some() {
                        collect_pattern_vars(pattern, &mut this_clause_vars);
                        continue;
                    }
                    let start_var_name = pattern.start.var.as_deref();

                    let mut this_pattern_vars: HashSet<String> = HashSet::new();
                    collect_pattern_vars(pattern, &mut this_pattern_vars);
                    for var in &this_pattern_vars {
                        let is_bound_start =
                            start_var_name == Some(var.as_str()) && bound_vars.contains_key(var);
                        // Variables from earlier clauses can be
                        // re-referenced if the type matches. Only
                        // reject actual type conflicts.
                        if let Some(&existing_type) = bound_vars.get(var) {
                            let is_var_length_edge = pattern.hops.iter().any(|h| {
                                h.rel.var.as_deref() == Some(var.as_str())
                                    && h.rel.var_length.is_some()
                            });
                            let new_type = if start_var_name == Some(var.as_str()) {
                                VarType::Node
                            } else if pattern
                                .hops
                                .iter()
                                .any(|h| h.rel.var.as_deref() == Some(var.as_str()))
                            {
                                VarType::Edge
                            } else {
                                VarType::Node
                            };
                            // `VarType::Scalar` is the planner's
                            // catch-all when it couldn't statically
                            // decide the type (e.g. WITH projecting
                            // `coalesce(b, c)` where the inputs are
                            // nodes). Allow any pattern role to
                            // bind over a Scalar — the executor
                            // checks the actual runtime value.
                            //
                            // Var-length edge vars also accept a
                            // `NonNode` existing binding, because
                            // the replay form (`MATCH (a)-[rs*]->(b)`
                            // with `rs = [e1, e2]`) treats the
                            // list-typed outer value as the walk.
                            let allow_existing = existing_type == new_type
                                || existing_type == VarType::Scalar
                                || (is_var_length_edge
                                    && matches!(existing_type, VarType::NonNode));
                            if !allow_existing {
                                return Err(Error::Plan(format!(
                                    "variable '{}' already defined with a different type",
                                    var
                                )));
                            }
                        }
                        if !is_bound_start {
                            this_clause_vars.insert(var.clone());
                        }
                    }
                }

                // Pattern-property values are expressions, so a bare
                // identifier like `{name: foo}` must resolve to a
                // binding from an earlier clause or from this clause's
                // own patterns. Catch the unbound case here rather
                // than letting it flow into a `Filter` predicate that
                // would silently return zero rows at runtime.
                let scope_for_props: HashMap<String, VarType> = bound_vars
                    .iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .chain(
                        this_clause_vars
                            .iter()
                            .map(|v| (v.clone(), VarType::Scalar)),
                    )
                    .collect();
                for pattern in &m.patterns {
                    check_pattern_property_scope(pattern, &scope_for_props)?;
                }

                // Lower each pattern. A pattern whose start var
                // is already bound by an earlier clause is
                // lowered *directly onto the current plan* via
                // `plan_pattern_from_bound` — the hops become
                // `EdgeExpand` nodes rooted at the existing row
                // stream, with no fresh scan and no
                // `CartesianProduct`. Every other pattern goes
                // through the original fresh-scan path and gets
                // cartesian-joined with the current plan.
                for (i, pattern) in m.patterns.iter().enumerate() {
                    let pattern_offset = stage_pattern_offset + i;
                    let start_var_name = pattern.start.var.as_deref();
                    let is_rebind = start_var_name
                        .map(|v| bound_vars.contains_key(v))
                        .unwrap_or(false);

                    plan = if is_rebind {
                        let current = plan
                            .take()
                            .expect("is_rebind implies an earlier clause populated the plan");
                        Some(plan_pattern_from_bound(
                            current,
                            pattern,
                            pattern_offset,
                            &bound_vars,
                        )?)
                    } else {
                        // Fresh-scan patterns lower as the right
                        // side of a `CartesianProduct`. Expand
                        // operators on that side can still reach
                        // outer-scope bindings via
                        // `CartesianProductOp`'s outer_rows stack,
                        // so plumb outer-bound edge / edge-list
                        // variables as constraints here. Reusing
                        // `r` across clauses (`MATCH ()-[r]-()
                        // MATCH (n)-[r]-(m)`) becomes an edge-id
                        // check; reusing `rs` as a pre-bound edge
                        // list becomes a list replay.
                        let mut outer_edges: HashSet<String> = HashSet::new();
                        let mut outer_edge_lists: HashSet<String> = HashSet::new();
                        let mut outer_nodes: HashSet<String> = HashSet::new();
                        for (k, v) in &bound_vars {
                            match v {
                                VarType::Edge => {
                                    outer_edges.insert(k.clone());
                                }
                                VarType::NonNode => {
                                    outer_edge_lists.insert(k.clone());
                                }
                                VarType::Node => {
                                    outer_nodes.insert(k.clone());
                                }
                                _ => {}
                            }
                        }
                        // Try to lower `MATCH ()-[r:T {p: v}]-()` with
                        // both endpoints unbound + an indexed edge
                        // property to an `EdgeSeek` that skips the
                        // start-node scan entirely. Falls through to
                        // the general `plan_pattern_with_bound_edges`
                        // path when preconditions don't hold.
                        let rhs = if let Some(seek) = try_plan_edge_seek(
                            pattern,
                            pattern_offset,
                            ctx,
                            &outer_nodes,
                            &outer_edges,
                        )? {
                            seek
                        } else {
                            plan_pattern_with_bound_edges(
                                pattern,
                                pattern_offset,
                                ctx,
                                &outer_edges,
                                &outer_edge_lists,
                            )?
                        };
                        Some(match plan.take() {
                            None => rhs,
                            Some(lhs) => LogicalPlan::CartesianProduct {
                                left: Box::new(lhs),
                                right: Box::new(rhs),
                            },
                        })
                    };
                    collect_pattern_vars_typed(pattern, &mut bound_vars)?;
                }
                stage_pattern_offset += m.patterns.len();

                if let Some(predicate) = &m.where_clause {
                    validate_pattern_predicate_vars_bound(predicate, &bound_vars)?;
                    check_set_expr_scope(predicate, &bound_vars)?;
                    reject_non_boolean_where_predicate(predicate, &bound_vars)?;
                    // WHERE can't contain aggregates — openCypher
                    // raises `InvalidAggregation`. Grouping only
                    // happens in projections (RETURN / WITH).
                    if contains_aggregate(predicate) {
                        return Err(Error::Plan(
                            "aggregate functions are not allowed in WHERE".into(),
                        ));
                    }
                    // Property access on a statically-known Path
                    // variable is `InvalidArgumentType` —
                    // properties belong to nodes and edges.
                    reject_property_access_on_path(predicate, &bound_vars)?;
                    let current = plan.expect("MATCH populated plan above");
                    plan = Some(LogicalPlan::Filter {
                        input: Box::new(current),
                        predicate: predicate.clone(),
                    });
                }

                // Index-aware WHERE rewrite — runs per-stage so
                // a later MATCH with a WHERE on an indexed
                // property still gets the IndexSeek lowering.
                let current = plan.expect("MATCH produced a plan");
                let current = optimize_filter_chain_to_index_seek(current, ctx);
                // Point-index rewrite runs second so it sees the
                // post-IndexSeek plan; if the equality seek already
                // consumed the leaf NodeScanByLabels, the withinbbox
                // stays in the residual filter above the seek.
                let current = optimize_filter_chain_to_point_index_seek(current, ctx);
                // Edge-scope point-index rewrite — fires against the
                // `Filter { EdgeExpand { NodeScanAll } }` shape that
                // an unbound-endpoints single-hop pattern lowers to.
                // Order doesn't matter relative to the node-scope
                // rewrite above: the two target different leaves.
                plan = Some(optimize_filter_chain_to_edge_point_index_seek(current, ctx));
            }
            ReadingClause::OptionalMatch(o) => {
                let current = match plan.take() {
                    Some(p) => p,
                    None => LogicalPlan::SeedRow,
                };
                plan = Some(apply_optional_match(current, o, &mut bound_vars, ctx)?);
            }
            ReadingClause::With(w) => {
                let current = match plan.take() {
                    Some(p) => p,
                    None => LogicalPlan::SeedRow,
                };
                plan = Some(apply_with_clause(current, w)?);
                // A WITH clause introduces a fresh scope: only the
                // projected aliases (or the wildcard `*` pass-through
                // of all current bindings) survive into subsequent
                // clauses. Recompute `bound_vars` from the WITH
                // projection so later clauses can't see names that
                // were only bound before this scope boundary.
                let mut new_bound: HashMap<String, VarType> = HashMap::new();
                if w.star {
                    new_bound = bound_vars.clone();
                }
                for item in &w.items {
                    let alias = return_item_column_name(item);
                    let vtype = infer_expr_type(&item.expr, &bound_vars);
                    new_bound.insert(alias, vtype);
                }
                bound_vars = new_bound;
            }
            ReadingClause::Merge(mc) => {
                // Lower a MERGE clause. Dispatch on whether
                // the pattern has hops: zero-hop is a node
                // merge (the existing `MergeNode` variant);
                // non-zero is an edge merge (`MergeEdge`,
                // v1-restricted to a single directed hop with
                // both endpoints already bound).
                //
                // When `plan` is already populated, the
                // resulting plan chains via `input`. When it's
                // None (MERGE is the first clause of the
                // query), the merge runs top-level as a
                // producer with `input = None`.

                // openCypher rejects rebinding a variable with new
                // predicates inside a MERGE — `MATCH (a) MERGE (a)`
                // and `CREATE (a:Foo) MERGE (a)-[r:K]->(a:Bar)` both
                // raise `VariableAlreadyBound`. Scan every node
                // position in the pattern: if its var is already in
                // `bound_vars` and this occurrence adds labels or
                // properties, reject. The bare-rebind case (no hops,
                // no labels, no properties) is also rejected — it has
                // no side effect but the spec still forbids it.
                {
                    let start = &mc.pattern.start;
                    if let Some(v) = &start.var {
                        if bound_vars.contains_key(v)
                            && (!start.labels.is_empty() || !start.properties.is_empty())
                        {
                            return Err(Error::Plan(format!(
                                "VariableAlreadyBound: MERGE cannot impose new predicates on `{v}`"
                            )));
                        }
                    }
                    for hop in &mc.pattern.hops {
                        if let Some(v) = &hop.target.var {
                            if bound_vars.contains_key(v)
                                && (!hop.target.labels.is_empty()
                                    || !hop.target.properties.is_empty())
                            {
                                return Err(Error::Plan(format!(
                                    "VariableAlreadyBound: MERGE cannot impose new predicates on `{v}`"
                                )));
                            }
                        }
                    }
                    // Bare-node MERGE of an already-bound var is a
                    // no-op that openCypher still flags.
                    if mc.pattern.hops.is_empty() {
                        if let Some(v) = &start.var {
                            if bound_vars.contains_key(v)
                                && start.labels.is_empty()
                                && start.properties.is_empty()
                            {
                                return Err(Error::Plan(format!(
                                    "VariableAlreadyBound: MERGE on already-bound `{v}`"
                                )));
                            }
                        }
                    }
                }

                // `MERGE (n {k: null})` is a semantic error —
                // null can't participate in the equality match
                // that backs MERGE, so openCypher rejects it
                // (MergeReadOwnWrites). Catch top-level null
                // literals in any pattern property value.
                {
                    let reject_null = |props: &[(String, Expr)]| -> Result<()> {
                        for (_, v) in props {
                            if matches!(v, Expr::Literal(Literal::Null)) {
                                return Err(Error::Plan(
                                    "MergeReadOwnWrites: null property in MERGE pattern".into(),
                                ));
                            }
                        }
                        Ok(())
                    };
                    reject_null(&mc.pattern.start.properties)?;
                    for hop in &mc.pattern.hops {
                        reject_null(&hop.rel.properties)?;
                        reject_null(&hop.target.properties)?;
                    }
                }

                // Scope-check every SET expression in ON CREATE /
                // ON MATCH. The scope includes pre-existing bindings
                // plus anything the MERGE pattern will bind. Without
                // this, `MERGE (n) ON MATCH SET x.num = 1` silently
                // plans (and then executes, mutating nothing useful)
                // instead of raising UndefinedVariable.
                {
                    let mut merge_scope = bound_vars.clone();
                    if let Some(v) = &mc.pattern.start.var {
                        merge_scope.insert(v.clone(), VarType::Node);
                    }
                    for hop in &mc.pattern.hops {
                        if let Some(v) = &hop.rel.var {
                            merge_scope.insert(v.clone(), VarType::Edge);
                        }
                        if let Some(v) = &hop.target.var {
                            merge_scope.insert(v.clone(), VarType::Node);
                        }
                    }
                    for item in mc.on_create.iter().chain(mc.on_match.iter()) {
                        check_set_item_scope(item, &merge_scope)?;
                    }
                }

                let on_create = mc
                    .on_create
                    .iter()
                    .map(set_item_to_assignment)
                    .collect::<Vec<_>>();
                let on_match = mc
                    .on_match
                    .iter()
                    .map(set_item_to_assignment)
                    .collect::<Vec<_>>();

                if mc.pattern.hops.is_empty() {
                    // --- node merge ---
                    let var = mc
                        .pattern
                        .start
                        .var
                        .clone()
                        .unwrap_or_else(|| format!("__merge_stage{}", stage_pattern_offset));
                    plan = Some(LogicalPlan::MergeNode {
                        input: plan.take().map(Box::new),
                        var: var.clone(),
                        labels: mc.pattern.start.labels.clone(),
                        properties: mc.pattern.start.properties.clone(),
                        on_create,
                        on_match,
                    });
                    bound_vars.insert(var.clone(), VarType::Node);
                    // `MERGE p = (a)` — bind a zero-length path.
                    if let Some(pv) = &mc.pattern.path_var {
                        plan = Some(LogicalPlan::BindPath {
                            input: Box::new(plan.take().unwrap()),
                            path_var: pv.clone(),
                            node_vars: vec![var],
                            edge_vars: Vec::new(),
                        });
                        bound_vars.insert(pv.clone(), VarType::Path);
                    }
                } else {
                    // --- edge merge (single or multi-hop) ---
                    // Decompose: first MergeNode for each intermediate
                    // and target node that carries labels/properties,
                    // then MergeEdge for each hop.

                    // Phase 1: MergeNode for the start node if it has
                    // labels/properties and isn't already bound.
                    let start_var = mc
                        .pattern
                        .start
                        .var
                        .clone()
                        .unwrap_or_else(|| format!("__merge_s{}", stage_pattern_offset));
                    if !bound_vars.contains_key(&start_var) {
                        if !mc.pattern.start.labels.is_empty()
                            || !mc.pattern.start.properties.is_empty()
                        {
                            plan = Some(LogicalPlan::MergeNode {
                                input: plan.take().map(Box::new),
                                var: start_var.clone(),
                                labels: mc.pattern.start.labels.clone(),
                                properties: mc.pattern.start.properties.clone(),
                                on_create: on_create.clone(),
                                on_match: on_match.clone(),
                            });
                            bound_vars.insert(start_var.clone(), VarType::Node);
                        }
                    }

                    // Phase 2: MergeNode for each hop's target that
                    // has labels/properties and isn't bound.
                    for (hi, hop) in mc.pattern.hops.iter().enumerate() {
                        let target_var =
                            hop.target.var.clone().unwrap_or_else(|| {
                                format!("__merge_t{}_{}", stage_pattern_offset, hi)
                            });
                        if !bound_vars.contains_key(&target_var)
                            && (!hop.target.labels.is_empty() || !hop.target.properties.is_empty())
                        {
                            plan = Some(LogicalPlan::MergeNode {
                                input: plan.take().map(Box::new),
                                var: target_var.clone(),
                                labels: hop.target.labels.clone(),
                                properties: hop.target.properties.clone(),
                                on_create: on_create.clone(),
                                on_match: on_match.clone(),
                            });
                            bound_vars.insert(target_var, VarType::Node);
                        }
                    }

                    // Phase 3: MergeEdge for each hop. Track the
                    // node/edge sequence so we can bind a path var
                    // at the end if the pattern declared one.
                    let mut current_src = start_var.clone();
                    let mut path_nodes = vec![start_var.clone()];
                    let mut path_edges: Vec<String> = Vec::new();
                    for (hi, hop) in mc.pattern.hops.iter().enumerate() {
                        if hop.rel.var_length.is_some() {
                            return Err(Error::Plan(
                                "MERGE does not support variable-length relationships".into(),
                            ));
                        }
                        if hop.rel.edge_types.len() != 1 {
                            return Err(Error::Plan(
                                "MERGE edge pattern requires exactly one relationship type".into(),
                            ));
                        }
                        let edge_type = hop.rel.edge_types[0].clone();
                        let dst_var =
                            hop.target.var.clone().unwrap_or_else(|| {
                                format!("__merge_t{}_{}", stage_pattern_offset, hi)
                            });
                        let edge_var =
                            hop.rel.var.clone().unwrap_or_else(|| {
                                format!("__merge_e{}_{}", stage_pattern_offset, hi)
                            });
                        // For incoming edges, swap src and dst
                        let (merge_src, merge_dst) =
                            if matches!(hop.rel.direction, Direction::Incoming) {
                                (dst_var.clone(), current_src.clone())
                            } else {
                                (current_src.clone(), dst_var.clone())
                            };
                        let current = plan.take().ok_or_else(|| {
                            Error::Plan("MERGE on an edge pattern requires bound endpoints".into())
                        })?;
                        plan = Some(LogicalPlan::MergeEdge {
                            input: Box::new(current),
                            edge_var: edge_var.clone(),
                            src_var: merge_src,
                            dst_var: merge_dst,
                            edge_type,
                            undirected: matches!(hop.rel.direction, Direction::Both),
                            properties: hop.rel.properties.clone(),
                            on_create: on_create.clone(),
                            on_match: on_match.clone(),
                        });
                        path_edges.push(edge_var.clone());
                        path_nodes.push(dst_var.clone());
                        bound_vars.insert(edge_var, VarType::Edge);
                        bound_vars.insert(dst_var.clone(), VarType::Node);
                        current_src = dst_var;
                    }
                    if let Some(pv) = &mc.pattern.path_var {
                        plan = Some(LogicalPlan::BindPath {
                            input: Box::new(plan.take().unwrap()),
                            path_var: pv.clone(),
                            node_vars: path_nodes,
                            edge_vars: path_edges,
                        });
                        bound_vars.insert(pv.clone(), VarType::Path);
                    }
                }
                stage_pattern_offset += 1;
            }
            ReadingClause::Unwind(u) => {
                // A mid-query UNWIND wraps the current row stream.
                // When it's the first clause (plan is None), the
                // top-level producer form is used so the expression
                // evaluates against an empty row — identical to a
                // standalone `UNWIND ... RETURN` query. In the
                // chained case, each input row cross-products with
                // the list the expression produces, binding `alias`
                // per element.
                plan = Some(match plan.take() {
                    None => LogicalPlan::Unwind {
                        var: u.alias.clone(),
                        expr: u.expr.clone(),
                    },
                    Some(current) => LogicalPlan::UnwindChain {
                        input: Box::new(current),
                        var: u.alias.clone(),
                        expr: u.expr.clone(),
                    },
                });
                bound_vars.insert(u.alias.clone(), VarType::Scalar);
            }
            ReadingClause::Call(body_stmt) => {
                let body_plan = plan_with_context(body_stmt, ctx)?;
                let current = match plan.take() {
                    Some(p) => p,
                    None => LogicalPlan::Unwind {
                        var: "__call_seed".to_string(),
                        expr: Expr::List(vec![Expr::Literal(Literal::Integer(0))]),
                    },
                };
                plan = Some(LogicalPlan::CallSubquery {
                    input: Box::new(current),
                    body: Box::new(body_plan),
                });
                // Publish the body's RETURN columns into the outer
                // scope so a subsequent clause (or the terminal
                // RETURN) can reference them. `None` means the body
                // projected `RETURN *` — we don't derive names
                // statically in that case, so the outer scope stays
                // unchanged.
                if let Some(cols) = call_subquery_output_columns(body_stmt)? {
                    for name in cols {
                        if bound_vars.contains_key(&name) {
                            return Err(Error::Plan(format!("variable '{name}' already defined")));
                        }
                        bound_vars.insert(name, VarType::Scalar);
                    }
                }
            }
            ReadingClause::CallInTransactions(body_stmt, cfg) => {
                let body_plan = plan_with_context(body_stmt, ctx)?;
                let current = match plan.take() {
                    Some(p) => p,
                    None => LogicalPlan::Unwind {
                        var: "__call_seed".to_string(),
                        expr: Expr::List(vec![Expr::Literal(Literal::Integer(0))]),
                    },
                };
                plan = Some(LogicalPlan::CallSubqueryInTransactions {
                    input: Box::new(current),
                    body: Box::new(body_plan),
                    batch_size: cfg.batch_size,
                    error_mode: cfg.error_mode,
                    report_status_as: cfg.report_status_as.clone(),
                });
                // When `REPORT STATUS AS var` is present, publish
                // `var` into the outer scope so a downstream
                // RETURN / WITH can reference `var.committed` etc.
                if let Some(name) = &cfg.report_status_as {
                    if bound_vars.contains_key(name) {
                        return Err(Error::Plan(format!("variable '{name}' already defined")));
                    }
                    bound_vars.insert(name.clone(), VarType::Scalar);
                }
                // Same outer-scope projection as the non-batched
                // variant — the body's RETURN columns become
                // bindings the caller can reference downstream.
                if let Some(cols) = call_subquery_output_columns(body_stmt)? {
                    for name in cols {
                        if bound_vars.contains_key(&name) {
                            return Err(Error::Plan(format!("variable '{name}' already defined")));
                        }
                        bound_vars.insert(name, VarType::Scalar);
                    }
                }
            }
            ReadingClause::CallProcedure(pc) => {
                // Embedded CALL — reaches this arm only when the
                // enclosing MatchStmt has either more than one
                // reading clause or a terminal_tail; the standalone
                // variant is promoted to `Statement::CallProcedure`
                // back in the parser.
                if pc.args.is_none() {
                    // Implicit-args form (`CALL ns.name`) isn't
                    // allowed mid-query; openCypher rejects it with
                    // InvalidArgumentPassingMode.
                    return Err(Error::Plan(
                        "in-query CALL requires explicit argument list".into(),
                    ));
                }
                if let Some(crate::ast::YieldSpec::Star) = &pc.yield_spec {
                    return Err(Error::Plan(
                        "YIELD * is only allowed on standalone CALL".into(),
                    ));
                }
                // Reject aggregate expressions passed as args — TCK
                // Scenario 16 covers this. Aggregates aren't legal
                // outside of an Aggregate operator (RETURN / WITH),
                // and CALL args are evaluated per-row.
                if let Some(exprs) = &pc.args {
                    for e in exprs {
                        if contains_aggregate(e) {
                            return Err(Error::Plan(
                                "aggregate functions are not allowed in CALL arguments".into(),
                            ));
                        }
                    }
                }
                match &pc.yield_spec {
                    Some(crate::ast::YieldSpec::Items(items)) => {
                        for yi in items {
                            let bind_name = yi.alias.as_ref().unwrap_or(&yi.column).clone();
                            if bound_vars.contains_key(&bind_name) {
                                return Err(Error::Plan(format!(
                                    "variable '{bind_name}' already defined"
                                )));
                            }
                            bound_vars.insert(bind_name, VarType::Scalar);
                        }
                    }
                    Some(crate::ast::YieldSpec::Star) => {
                        // Already rejected above.
                    }
                    None => {
                        // No YIELD in an in-query CALL — the TCK
                        // flags this as UndefinedVariable the moment
                        // a later clause tries to reference an
                        // output. We mark it as "unyieldable" so
                        // the terminal tail's RETURN / WITH
                        // scope-check raises at plan time instead of
                        // silently projecting nulls.
                        //
                        // Since we don't know the procedure's
                        // outputs here (registry lookup is an
                        // executor concern), we can't selectively
                        // allow `CALL test.doNothing()` — but we
                        // also can't reject every YIELD-less
                        // in-query CALL. Defer the check to the
                        // executor, where the registry is in scope.
                    }
                }
                // Apoc.periodic.iterate gets rewritten at plan
                // time into a custom variant the dispatcher
                // handles directly. Validates here so the
                // ill-formed call surfaces as a plan error
                // instead of a runtime registry miss.
                if pc.qualified_name == ["apoc", "periodic", "iterate"] {
                    let current_input = plan.take();
                    let aip = build_apoc_periodic_iterate(pc, ctx)?;
                    // The procedure call sits at the seam: any
                    // upstream `current_input` is discarded — the
                    // procedure is its own row producer and emits
                    // exactly one summary row.
                    drop(current_input);
                    plan = Some(aip);
                    continue;
                }
                plan = Some(LogicalPlan::ProcedureCall {
                    input: plan.take().map(Box::new),
                    qualified_name: pc.qualified_name.clone(),
                    args: pc.args.clone(),
                    yield_spec: pc.yield_spec.clone(),
                    standalone: false,
                });
            }
            ReadingClause::LoadCsv(lc) => {
                plan = Some(LogicalPlan::LoadCsv {
                    input: plan.take().map(Box::new),
                    path_expr: lc.path_expr.clone(),
                    var: lc.alias.clone(),
                    with_headers: lc.with_headers,
                });
                bound_vars.insert(lc.alias.clone(), VarType::Scalar);
            }
            ReadingClause::Create(patterns) => {
                // Reject CREATE on already-bound nodes that attempt
                // to re-declare labels, properties, or emit a bare
                // node pattern — openCypher's VariableAlreadyBound
                // rule. Pure references (as an endpoint of a new
                // edge with no extra decoration) are still allowed
                // because that's the canonical way to attach new
                // edges to an existing node.
                // Every identifier referenced in a CREATE
                // property expression must be in scope, else
                // openCypher raises `UndefinedVariable` at
                // compile time. Walk left-to-right across
                // patterns and pattern elements so a value
                // references only earlier elements:
                // `CREATE (a {..}), (:B {k: a.id})` is OK,
                // `CREATE (:B {k: a.id}), (a {..})` is not.
                let mut scope_for_props = bound_vars.clone();
                for pattern in patterns {
                    if let Some(var) = &pattern.start.var {
                        if bound_vars.contains_key(var)
                            && (!pattern.start.labels.is_empty()
                                || pattern.start.has_property_clause
                                || pattern.hops.is_empty())
                        {
                            return Err(Error::Plan(format!(
                                "variable '{}' already defined with a different type",
                                var
                            )));
                        }
                    }
                    for hop in &pattern.hops {
                        if let Some(var) = &hop.target.var {
                            if bound_vars.contains_key(var)
                                && (!hop.target.labels.is_empty() || hop.target.has_property_clause)
                            {
                                return Err(Error::Plan(format!(
                                    "variable '{}' already defined with a different type",
                                    var
                                )));
                            }
                        }
                    }
                    for (_, expr) in &pattern.start.properties {
                        check_set_expr_scope(expr, &scope_for_props)?;
                    }
                    if let Some(v) = &pattern.start.var {
                        scope_for_props.insert(v.clone(), VarType::Node);
                    }
                    for hop in &pattern.hops {
                        if let Some(v) = &hop.rel.var {
                            scope_for_props.insert(v.clone(), VarType::Edge);
                        }
                        if let Some(v) = &hop.target.var {
                            scope_for_props.insert(v.clone(), VarType::Node);
                        }
                        for (_, expr) in &hop.rel.properties {
                            check_set_expr_scope(expr, &scope_for_props)?;
                        }
                        for (_, expr) in &hop.target.properties {
                            check_set_expr_scope(expr, &scope_for_props)?;
                        }
                    }
                }
                let mut nodes: Vec<CreateNodeSpec> = Vec::new();
                let mut edges: Vec<CreateEdgeSpec> = Vec::new();
                let mut var_idx: HashMap<String, usize> = HashMap::new();
                for pattern in patterns {
                    build_create_pattern(
                        pattern,
                        &mut nodes,
                        &mut edges,
                        &mut var_idx,
                        &bound_vars,
                    )?;
                }
                // Register newly created variables as bound
                for n in &nodes {
                    if let CreateNodeSpec::New { var: Some(v), .. } = n {
                        bound_vars.insert(v.clone(), VarType::Node);
                    }
                }
                for e in &edges {
                    if let Some(v) = &e.var {
                        bound_vars.insert(v.clone(), VarType::Edge);
                    }
                }
                plan = Some(LogicalPlan::CreatePath {
                    input: plan.take().map(Box::new),
                    nodes,
                    edges,
                });
            }
            ReadingClause::Set(items) => {
                let assignments: Vec<SetAssignment> =
                    items.iter().map(set_item_to_assignment).collect();
                check_set_assignments_scope(&assignments, &bound_vars)?;
                let current = plan
                    .take()
                    .ok_or_else(|| Error::Plan("SET requires a preceding clause".into()))?;
                plan = Some(LogicalPlan::SetProperty {
                    input: Box::new(current),
                    assignments,
                });
            }
            ReadingClause::Delete(dc) => {
                // Scope check: every identifier mentioned in a
                // DELETE expression must be in scope. Catches
                // `MATCH (a) DELETE x` at compile time instead of
                // waiting for a runtime `UnboundVariable`.
                // Additionally reject shapes whose result can
                // never be a graph element (arithmetic, boolean
                // ops, bare literals) — openCypher raises
                // `InvalidArgumentType` for `DELETE 1 + 1`.
                for expr in &dc.exprs {
                    validate_delete_expr(expr, &bound_vars)?;
                }
                let current = plan
                    .take()
                    .ok_or_else(|| Error::Plan("DELETE requires a preceding clause".into()))?;
                plan = Some(LogicalPlan::Delete {
                    input: Box::new(current),
                    detach: dc.detach,
                    vars: dc.vars.clone(),
                    exprs: dc.exprs.clone(),
                });
            }
            ReadingClause::Remove(items) => {
                let specs = items
                    .iter()
                    .map(|ri| match ri {
                        crate::ast::RemoveItem::Property { var, key } => RemoveSpec::Property {
                            var: var.clone(),
                            key: key.clone(),
                        },
                        crate::ast::RemoveItem::Labels { var, labels } => RemoveSpec::Labels {
                            var: var.clone(),
                            labels: labels.clone(),
                        },
                    })
                    .collect();
                let current = plan
                    .take()
                    .ok_or_else(|| Error::Plan("REMOVE requires a preceding clause".into()))?;
                plan = Some(LogicalPlan::Remove {
                    input: Box::new(current),
                    items: specs,
                });
            }
            ReadingClause::Foreach(fe) => {
                let set_assignments = fe.set_items.iter().map(set_item_to_assignment).collect();
                let remove_specs = fe
                    .remove_items
                    .iter()
                    .map(|ri| match ri {
                        crate::ast::RemoveItem::Property { var, key } => RemoveSpec::Property {
                            var: var.clone(),
                            key: key.clone(),
                        },
                        crate::ast::RemoveItem::Labels { var, labels } => RemoveSpec::Labels {
                            var: var.clone(),
                            labels: labels.clone(),
                        },
                    })
                    .collect();
                let current = plan
                    .take()
                    .ok_or_else(|| Error::Plan("FOREACH requires a preceding clause".into()))?;
                plan = Some(LogicalPlan::Foreach {
                    input: Box::new(current),
                    var: fe.var.clone(),
                    list_expr: fe.list_expr.clone(),
                    set_assignments,
                    remove_items: remove_specs,
                });
            }
        }
    }

    let mut plan = plan.ok_or_else(|| {
        Error::Plan("match_stmt must contain at least one producer clause".into())
    })?;

    let terminal = &stmt.terminal;

    // Multiple mutation clauses can appear in sequence:
    // CREATE (n) SET n.name = 'Ada' REMOVE n:Temp RETURN n
    if !terminal.create_patterns.is_empty() {
        for pattern in &terminal.create_patterns {
            // Reject CREATE on already-bound nodes that try to
            // CREATE new labels/properties (VariableAlreadyBound).
            // Pure references (no labels, no properties) are fine.
            if let Some(var) = &pattern.start.var {
                if bound_vars.contains_key(var)
                    && (!pattern.start.labels.is_empty()
                        || !pattern.start.properties.is_empty()
                        || pattern.hops.is_empty())
                {
                    return Err(Error::Plan(format!(
                        "variable '{}' already defined with a different type",
                        var
                    )));
                }
            }
        }
        let mut nodes: Vec<CreateNodeSpec> = Vec::new();
        let mut edges: Vec<CreateEdgeSpec> = Vec::new();
        let mut var_idx: HashMap<String, usize> = HashMap::new();
        for pattern in &terminal.create_patterns {
            build_create_pattern(pattern, &mut nodes, &mut edges, &mut var_idx, &bound_vars)?;
        }
        plan = LogicalPlan::CreatePath {
            input: Some(Box::new(plan)),
            nodes,
            edges,
        };
    }
    if let Some(delete_clause) = &terminal.delete {
        plan = LogicalPlan::Delete {
            input: Box::new(plan),
            detach: delete_clause.detach,
            vars: delete_clause.vars.clone(),
            exprs: delete_clause.exprs.clone(),
        };
    }
    if !terminal.set_items.is_empty() {
        let assignments: Vec<SetAssignment> = terminal
            .set_items
            .iter()
            .map(set_item_to_assignment)
            .collect();
        check_set_assignments_scope(&assignments, &bound_vars)?;
        plan = LogicalPlan::SetProperty {
            input: Box::new(plan),
            assignments,
        };
    }
    if !terminal.remove_items.is_empty() {
        let items = terminal
            .remove_items
            .iter()
            .map(|ri| match ri {
                crate::ast::RemoveItem::Property { var, key } => RemoveSpec::Property {
                    var: var.clone(),
                    key: key.clone(),
                },
                crate::ast::RemoveItem::Labels { var, labels } => RemoveSpec::Labels {
                    var: var.clone(),
                    labels: labels.clone(),
                },
            })
            .collect();
        plan = LogicalPlan::Remove {
            input: Box::new(plan),
            items,
        };
    }

    if let Some(fe) = &terminal.foreach {
        let set_assignments = fe.set_items.iter().map(set_item_to_assignment).collect();
        let remove_specs = fe
            .remove_items
            .iter()
            .map(|ri| match ri {
                crate::ast::RemoveItem::Property { var, key } => RemoveSpec::Property {
                    var: var.clone(),
                    key: key.clone(),
                },
                crate::ast::RemoveItem::Labels { var, labels } => RemoveSpec::Labels {
                    var: var.clone(),
                    labels: labels.clone(),
                },
            })
            .collect();
        plan = LogicalPlan::Foreach {
            input: Box::new(plan),
            var: fe.var.clone(),
            list_expr: fe.list_expr.clone(),
            set_assignments,
            remove_items: remove_specs,
        };
    }

    let has_mutation = terminal.delete.is_some()
        || !terminal.set_items.is_empty()
        || !terminal.create_patterns.is_empty()
        || !terminal.remove_items.is_empty()
        || terminal.foreach.is_some();

    // MERGE clauses count as side-effectful even without a
    // trailing RETURN, so `MERGE (x)` is a valid complete
    // query. MATCH-only (with no terminal and no writing
    // clause) is still an error. Also check for inline
    // mutation clauses (CREATE/SET/DELETE/REMOVE in reading position).
    let has_merge_clause = stmt
        .clauses
        .iter()
        .any(|c| matches!(c, crate::ast::ReadingClause::Merge(_)));
    let has_inline_mutation = stmt.clauses.iter().any(|c| {
        matches!(
            c,
            crate::ast::ReadingClause::Create(_)
                | crate::ast::ReadingClause::Set(_)
                | crate::ast::ReadingClause::Delete(_)
                | crate::ast::ReadingClause::Remove(_)
                | crate::ast::ReadingClause::Foreach(_)
                // CALL { ... } IN TRANSACTIONS is a write-bearing
                // clause in its own right — its body batches and
                // commits writes per chunk, so the statement
                // doesn't need a separate trailing mutation/
                // RETURN to be valid.
                | crate::ast::ReadingClause::CallInTransactions(_, _)
        )
    });

    // Scope visible to RETURN: MATCH-derived bindings plus any
    // variables introduced by the terminal-tail's CREATE
    // patterns (`CREATE (a)-[r:T]->(b) RETURN r`). CREATE runs
    // before RETURN at execution time, so the vars are in scope
    // by the time the projection fires.
    let mut return_scope: HashMap<String, VarType> = bound_vars.clone();
    for pattern in &terminal.create_patterns {
        if let Some(var) = &pattern.start.var {
            return_scope.insert(var.clone(), VarType::Node);
        }
        if let Some(pv) = &pattern.path_var {
            return_scope.insert(pv.clone(), VarType::Path);
        }
        for hop in &pattern.hops {
            if let Some(var) = &hop.rel.var {
                return_scope.insert(var.clone(), VarType::Edge);
            }
            if let Some(var) = &hop.target.var {
                return_scope.insert(var.clone(), VarType::Node);
            }
        }
    }

    if terminal.star {
        // `RETURN *` demands at least one variable in scope —
        // openCypher calls out `NoVariablesInScope` explicitly.
        // Anonymous MATCH patterns (`MATCH ()`) don't introduce
        // any, and silently returning empty masks the mistake.
        if return_scope.is_empty() {
            return Err(Error::Plan("RETURN * has no variables in scope".into()));
        }
    }
    if terminal.star || !terminal.return_items.is_empty() {
        // `size()` is for strings and lists only — calling it on
        // a Path is an openCypher `InvalidArgumentType`. Catch
        // the common case where the argument is a known-Path
        // identifier at plan time (runtime would only see a raw
        // Value::Path without the original variable role).
        for item in &terminal.return_items {
            reject_size_on_path(&item.expr, &return_scope)?;
        }
        for item in &terminal.return_items {
            reject_unknown_functions(&item.expr)?;
        }
        // Every identifier referenced in a RETURN item must be
        // in scope. Without this check, `MATCH () RETURN foo`
        // against an empty graph silently returns zero rows
        // because the pipeline never evaluates `foo`, where
        // openCypher mandates a compile-time
        // `UndefinedVariable`. `check_set_expr_scope` already
        // walks with local-binder awareness (list comp / reduce
        // / pattern comp), so reuse it.
        for item in &terminal.return_items {
            check_set_expr_scope(&item.expr, &return_scope)?;
        }
        plan = apply_return_pipeline(
            plan,
            &terminal.return_items,
            terminal.star,
            terminal.distinct,
            &terminal.order_by,
            terminal.skip.clone(),
            terminal.limit.clone(),
        )?;
    } else if !has_mutation && !has_merge_clause && !has_inline_mutation {
        return Err(Error::Plan(
            "query must be followed by RETURN, SET, DELETE, CREATE, or end with a MERGE".into(),
        ));
    }

    Ok(plan)
}

fn apply_return_pipeline(
    mut plan: LogicalPlan,
    return_items: &[ReturnItem],
    star: bool,
    distinct: bool,
    order_by: &[SortItem],
    skip: Option<Expr>,
    limit: Option<Expr>,
) -> Result<LogicalPlan> {
    for item in return_items {
        reject_pattern_predicate_in_projection(&item.expr, "RETURN projection")?;
    }
    // Duplicate output-column names — explicit `AS` aliases or
    // bare variable references — are a `ColumnNameConflict` in
    // openCypher. Flag the first repeat so a typo like
    // `RETURN 1 AS a, 2 AS a` fails at compile time instead of
    // silently keeping whichever the HashMap-backed Row decided
    // to retain.
    {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for item in return_items {
            let name = item
                .alias
                .clone()
                .unwrap_or_else(|| render_expr_key(&item.expr));
            if !seen.insert(name.clone()) {
                return Err(Error::Plan(format!(
                    "RETURN has multiple columns with the same name `{name}`"
                )));
            }
        }
    }
    for s in order_by {
        reject_pattern_predicate_in_projection(&s.expr, "ORDER BY")?;
    }

    // Rewrite ORDER BY expressions that refer to an already-projected
    // aggregate so they reference the output column instead of
    // re-evaluating the aggregate in a post-aggregation (scalar)
    // context. Built from the RETURN items' aliases/expressions, so
    // `RETURN max(n.age) ORDER BY max(n.age)` sorts on the aggregate
    // output rather than trying to call `max` as a scalar.
    let order_by_rewritten: Vec<SortItem> = if !star {
        // After Aggregate, the row only has columns for the
        // projected items — raw bindings like `n` are gone.
        // Rewrite ORDER BY expressions that match a RETURN item
        // into references to the projected column so
        // `RETURN n.num, count(*) ORDER BY n.num` can sort on
        // the projected `n.num` column instead of dereferencing
        // `n` on a squashed row.
        let mut alias_map: Vec<(Expr, String)> = Vec::new();
        for item in return_items {
            let col = item
                .alias
                .clone()
                .unwrap_or_else(|| render_expr_key(&item.expr));
            alias_map.push((item.expr.clone(), col));
        }
        order_by
            .iter()
            .map(|s| SortItem {
                expr: rewrite_aggregate_refs(s.expr.clone(), &alias_map),
                descending: s.descending,
            })
            .collect()
    } else {
        order_by.to_vec()
    };

    let (group_keys, aggregates, post_items) = if star {
        (Vec::new(), Vec::new(), Vec::new())
    } else {
        classify_return_items(return_items)?
    };
    let has_aggregates = !aggregates.is_empty();

    // InvalidAggregation: a non-aggregating RETURN can't grow an
    // aggregate out of its ORDER BY — there's no group-by scope for
    // the aggregate to fold over. `MATCH (n) RETURN n.num1 ORDER BY
    // max(n.num2)` is the canonical TCK example.
    if !has_aggregates && !star {
        for s in order_by {
            if contains_aggregate(&s.expr) {
                return Err(Error::Plan(
                    "InvalidAggregation: ORDER BY on a non-aggregating RETURN cannot reference an aggregate".into(),
                ));
            }
        }
    }

    // AmbiguousAggregationExpression: when the RETURN is aggregating
    // and ORDER BY contains (but isn't itself) an aggregate, the
    // same leaf-level rule as RETURN items applies — non-aggregate
    // parts must be simple references to a projected item or its
    // alias. Catches `RETURN me.age + you.age, count(*) AS cnt
    // ORDER BY me.age + you.age + count(*)`.
    if has_aggregates {
        let sort_items_as_return: Vec<ReturnItem> = order_by
            .iter()
            .map(|s| ReturnItem {
                expr: s.expr.clone(),
                alias: None,
                raw_text: None,
            })
            .collect();
        let mut top_refs: Vec<ReturnItem> = return_items
            .iter()
            .filter(|it| !contains_aggregate(&it.expr))
            .cloned()
            .collect();
        for it in return_items {
            if let Some(alias) = &it.alias {
                top_refs.push(ReturnItem {
                    expr: Expr::Identifier(alias.clone()),
                    alias: None,
                    raw_text: None,
                });
            }
        }
        let scope: Vec<ReturnItem> = top_refs
            .into_iter()
            .chain(sort_items_as_return.iter().cloned())
            .collect();
        check_ambiguous_aggregation(&scope)?;
    }

    // openCypher lets ORDER BY reference either projected
    // columns (`RETURN n.num AS x ORDER BY x`) or pre-RETURN
    // bindings that weren't projected (`WITH n, 0 AS r RETURN n
    // ORDER BY r`). We sort in the post-project position when
    // every ORDER BY identifier resolves to a projected column,
    // and in the pre-project position otherwise. DISTINCT and
    // aggregation both collapse rows so they force the
    // post-project path — any ORDER BY referencing a dropped
    // binding in that case is already a user error the earlier
    // scope check flagged.
    let projected_cols: std::collections::HashSet<String> = if star {
        std::collections::HashSet::new()
    } else {
        return_items
            .iter()
            .map(|i| i.alias.clone().unwrap_or_else(|| render_expr_key(&i.expr)))
            .collect()
    };
    let order_before_project = !star
        && !has_aggregates
        && !distinct
        && !order_by.is_empty()
        && !order_by
            .iter()
            .all(|s| all_refs_in(&s.expr, &projected_cols));

    if order_before_project {
        plan = LogicalPlan::OrderBy {
            input: Box::new(plan),
            sort_items: order_by.to_vec(),
        };
    }

    if star {
        plan = LogicalPlan::Identity {
            input: Box::new(plan),
        };
    } else {
        plan = if has_aggregates {
            let mut agg = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_keys,
                aggregates,
            };
            if !post_items.is_empty() {
                agg = LogicalPlan::Project {
                    input: Box::new(agg),
                    items: post_items,
                };
            }
            agg
        } else {
            LogicalPlan::Project {
                input: Box::new(plan),
                items: return_items.to_vec(),
            }
        };
    }

    if distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    if !order_by_rewritten.is_empty() && !order_before_project {
        plan = LogicalPlan::OrderBy {
            input: Box::new(plan),
            sort_items: order_by_rewritten,
        };
    }

    if let Some(n) = skip {
        plan = LogicalPlan::Skip {
            input: Box::new(plan),
            count: n,
        };
    }

    if let Some(n) = limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count: n,
        };
    }

    Ok(plan)
}

/// Lower one `OPTIONAL MATCH` clause onto `plan`, producing an
/// `OptionalEdgeExpand` node that left-joins the optional
/// pattern's adjacency step onto the current row stream.
///
/// v1 restrictions, enforced here so the executor can stay
/// simple:
///   * exactly one pattern in the clause
///   * the pattern must have exactly one hop (no bare node,
///     no multi-hop chains, no variable-length)
///   * the pattern's start node variable must already be in
///     `bound_vars` (i.e. bound by a prior MATCH clause), so
///     the left-join runs against existing row bindings
///
/// The clause's optional WHERE filter runs *after* the join —
/// matching Neo4j, it drops rows rather than Null-ing them.
fn apply_optional_match(
    mut plan: LogicalPlan,
    clause: &crate::ast::OptionalMatchClause,
    bound_vars: &mut HashMap<String, VarType>,
    ctx: &PlannerContext,
) -> Result<LogicalPlan> {
    let mut where_consumed = false;
    for pattern in &clause.patterns {
        if pattern.hops.is_empty() {
            // Bare-node OPTIONAL MATCH: scan for matching nodes.
            // If none match, wrap with CoalesceNullRow so the null
            // row semantics propagate.
            let fresh = plan_pattern(pattern, 0, ctx)?;
            let mut pattern_vars: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            collect_pattern_vars(pattern, &mut pattern_vars);
            let null_vars: Vec<String> = pattern_vars.into_iter().collect();
            let optional = LogicalPlan::CoalesceNullRow {
                input: Box::new(fresh),
                null_vars,
            };
            collect_pattern_vars_typed(pattern, bound_vars)?;
            plan = if matches!(plan, LogicalPlan::SeedRow) {
                optional
            } else {
                LogicalPlan::CartesianProduct {
                    left: Box::new(plan),
                    right: Box::new(optional),
                }
            };
            continue;
        }
        let start_var = pattern
            .start
            .var
            .clone()
            .unwrap_or_else(|| format!("__opt_start_{}", bound_vars.len()));
        // Special case: single-hop pattern with an unbound start
        // but a bound target. Reverse the hop so the bound side
        // becomes the starting point — otherwise the fresh-plan
        // branch below would re-bind the target, clobbering the
        // caller's value and losing the shared-endpoint
        // constraint. Handled here rather than generally because
        // multi-hop patterns with one bound-middle node need
        // richer rewriting than the current pattern shape tracks.
        let rewritten = if pattern.hops.len() == 1
            && !bound_vars.contains_key(&start_var)
            && pattern
                .hops
                .first()
                .and_then(|h| h.target.var.as_ref())
                .map(|v| bound_vars.contains_key(v))
                .unwrap_or(false)
        {
            Some(reverse_single_hop(pattern))
        } else {
            None
        };
        let pattern_ref: &Pattern = rewritten.as_ref().unwrap_or(pattern);
        let start_var = pattern_ref
            .start
            .var
            .clone()
            .unwrap_or_else(|| format!("__opt_start_{}", bound_vars.len()));
        if !bound_vars.contains_key(&start_var) {
            // Start var isn't bound by an earlier clause — the
            // optional pattern has to do a fresh scan. With no
            // outer row and no WHERE, `CoalesceNullRow` over a
            // plain pattern scan is enough. When there's an
            // outer plan to preserve (`MATCH ... WITH ...
            // OPTIONAL MATCH`) or a WHERE that references outer
            // bindings, wrap the fresh scan in `OptionalApply`
            // so each outer row gets its own left-join attempt
            // and the outer bindings stay in scope for the
            // WHERE filter.
            let fresh = plan_pattern(pattern_ref, 0, ctx)?;
            let mut pattern_vars: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            collect_pattern_vars(pattern_ref, &mut pattern_vars);
            // Drop vars already bound by an earlier clause (e.g.
            // a pre-bound edge reused as a pattern element) —
            // nulling them on the left-join fallback would
            // clobber the outer value.
            for k in bound_vars.keys() {
                pattern_vars.remove(k);
            }
            let null_vars: Vec<String> = pattern_vars.into_iter().collect();
            let has_outer = !matches!(plan, LogicalPlan::SeedRow);
            collect_pattern_vars_typed(pattern_ref, bound_vars)?;
            if has_outer || clause.where_clause.is_some() {
                let mut body = fresh;
                if let Some(predicate) = &clause.where_clause {
                    body = LogicalPlan::Filter {
                        input: Box::new(body),
                        predicate: predicate.clone(),
                    };
                    where_consumed = true;
                }
                plan = LogicalPlan::OptionalApply {
                    input: Box::new(plan),
                    body: Box::new(body),
                    null_vars,
                };
            } else {
                plan = LogicalPlan::CoalesceNullRow {
                    input: Box::new(fresh),
                    null_vars,
                };
            }
            continue;
        }

        // `OPTIONAL MATCH p = ...` needs every hop to bind both an
        // edge var and a target var so BindPath (below) can extract
        // the sequence from each row. Synthesise names when the
        // source pattern omitted them.
        let mut working = pattern_ref.clone();
        if working.path_var.is_some() {
            ensure_path_bindings(&mut working, bound_vars.len())?;
        }

        // Collect every pattern-introduced variable up front —
        // these are the ones a multi-hop failure needs to null out.
        let mut new_pattern_vars: Vec<String> = Vec::new();
        for hop in &working.hops {
            if let Some(v) = &hop.rel.var {
                if !bound_vars.contains_key(v) && !new_pattern_vars.contains(v) {
                    new_pattern_vars.push(v.clone());
                }
            }
            if let Some(v) = &hop.target.var {
                if !bound_vars.contains_key(v) && !new_pattern_vars.contains(v) {
                    new_pattern_vars.push(v.clone());
                }
            }
        }
        if let Some(pv) = &working.path_var {
            if !bound_vars.contains_key(pv) && !new_pattern_vars.contains(pv) {
                new_pattern_vars.push(pv.clone());
            }
        }

        // Multi-hop patterns, or anywhere the OPTIONAL clause has a
        // WHERE, can't use per-hop OptionalEdgeExpand safely: each
        // hop's null-fallback fires independently (leaking partial
        // matches with only a prefix bound) and a downstream filter
        // would also drop the fallback row (so an outer row whose
        // only match fails the WHERE vanishes instead of producing
        // the expected all-null left-join row). Build the whole
        // pattern as a non-optional chain seeded from the outer row,
        // fold the WHERE into the same sub-plan, and wrap in
        // `OptionalApply` so the null fallback only fires when the
        // *entire* pattern+WHERE produced zero rows for that input.
        let use_apply = working.hops.len() > 1 || clause.where_clause.is_some();
        if use_apply {
            // Seed the body with the outer row so the executor's
            // per-row apply operator replays it; `SeedRow` is
            // substituted for the actual row by `build_op_inner`'s
            // `seed` parameter.
            let body_seed: LogicalPlan = LogicalPlan::SeedRow;
            let mut body = plan_pattern_from_bound(body_seed, &working, 0, bound_vars)?;
            if let Some(predicate) = &clause.where_clause {
                body = LogicalPlan::Filter {
                    input: Box::new(body),
                    predicate: predicate.clone(),
                };
                where_consumed = true;
            }
            for hop in &working.hops {
                if let Some(v) = &hop.rel.var {
                    bound_vars.insert(v.clone(), VarType::Edge);
                }
                if let Some(v) = &hop.target.var {
                    bound_vars.insert(v.clone(), VarType::Node);
                }
            }
            if let Some(pv) = &working.path_var {
                bound_vars.insert(pv.clone(), VarType::Path);
            }
            plan = LogicalPlan::OptionalApply {
                input: Box::new(plan),
                body: Box::new(body),
                null_vars: new_pattern_vars.clone(),
            };
            continue;
        }

        let mut current_var = start_var;

        for (i, hop) in working.hops.iter().enumerate() {
            let declared_dst_var = hop
                .target
                .var
                .clone()
                .unwrap_or_else(|| format!("__opt_dst_{}_{}", bound_vars.len(), i));
            // If the target var is already bound by an earlier
            // clause, expand into a synthetic name and emit an
            // equality filter so the traversal only keeps rows
            // whose walked endpoint matches the existing binding
            // — the same approach `plan_pattern_from_bound`
            // takes for regular MATCH.
            let dst_is_reuse = bound_vars.contains_key(&declared_dst_var);
            let dst_var = if dst_is_reuse {
                format!("__opt_rebind_{}_{}", bound_vars.len(), i)
            } else {
                declared_dst_var.clone()
            };

            if let Some(vl) = hop.rel.var_length {
                if vl.min > vl.max {
                    return Err(Error::Plan(format!(
                        "variable-length path min ({}) > max ({})",
                        vl.min, vl.max
                    )));
                }
                let vl_constraint = if dst_is_reuse {
                    Some(declared_dst_var.clone())
                } else {
                    None
                };
                // For a single-hop var-length OPTIONAL MATCH with
                // a declared path variable (`OPTIONAL MATCH p =
                // (a)-[*]->(b)`), plumb the outer path_var
                // directly into the op — `wrap_with_bind_path`
                // short-circuits single-hop var-length patterns
                // and would otherwise leave `p` unbound. Multi-hop
                // patterns still synthesise a subpath name and
                // let BindPath splice it.
                let vl_path_var = if working.path_var.is_some() && working.hops.len() == 1 {
                    working.path_var.clone()
                } else if working.path_var.is_some() {
                    Some(format!("__opt_subpath_{}_{}", bound_vars.len(), i))
                } else {
                    None
                };
                plan = LogicalPlan::VarLengthExpand {
                    input: Box::new(plan),
                    src_var: current_var.clone(),
                    edge_var: hop.rel.var.clone(),
                    dst_var: dst_var.clone(),
                    dst_labels: hop.target.labels.clone(),
                    edge_types: hop.rel.edge_types.clone(),
                    edge_properties: hop.rel.properties.clone(),
                    direction: hop.rel.direction,
                    min_hops: vl.min,
                    max_hops: vl.max,
                    path_var: vl_path_var,
                    optional: true,
                    dst_constraint_var: vl_constraint,
                    bound_edge_list_var: None,
                    excluded_edge_vars: Vec::new(),
                };
            } else {
                // When the target is already bound, push the
                // endpoint constraint into the op itself so that
                // edges leading anywhere *other* than the bound
                // target count as "no match" and trigger the
                // per-row null-fallback — a post-filter can't do
                // that because it runs after the fallback
                // decision is made.
                let constraint_var = if dst_is_reuse {
                    Some(declared_dst_var.clone())
                } else {
                    None
                };
                // If the edge variable was already bound in a
                // prior clause (e.g. `WITH r ... OPTIONAL MATCH
                // (a)<-[r]-(b)`), pass it as an edge constraint
                // so only the pre-bound edge counts as a match.
                let edge_constraint_var = hop.rel.var.as_ref().and_then(|v| {
                    if bound_vars.contains_key(v) {
                        Some(v.clone())
                    } else {
                        None
                    }
                });
                plan = LogicalPlan::OptionalEdgeExpand {
                    input: Box::new(plan),
                    src_var: current_var.clone(),
                    edge_var: hop.rel.var.clone(),
                    dst_var: dst_var.clone(),
                    dst_labels: hop.target.labels.clone(),
                    dst_properties: hop.target.properties.clone(),
                    edge_types: hop.rel.edge_types.clone(),
                    direction: hop.rel.direction,
                    dst_constraint_var: constraint_var,
                    edge_constraint_var,
                };
            }

            if !dst_is_reuse {
                bound_vars.insert(declared_dst_var.clone(), VarType::Node);
            }
            if let Some(ev) = &hop.rel.var {
                bound_vars.insert(ev.clone(), VarType::Edge);
            }
            current_var = if dst_is_reuse {
                declared_dst_var
            } else {
                dst_var
            };
        }

        // Bind the path variable after all hops have run. BindPath
        // handles the optional case: when any referenced node/edge
        // var is null (because the left-join fired its fallback),
        // `path_var` comes out null, which is exactly what
        // `OPTIONAL MATCH p = ...` should produce.
        if working.path_var.is_some() {
            let start_var_for_bind = working
                .start
                .var
                .clone()
                .unwrap_or_else(|| format!("__opt_start_{}", bound_vars.len()));
            plan = wrap_with_bind_path(plan, &working, &start_var_for_bind, 0);
            if let Some(pv) = &working.path_var {
                bound_vars.insert(pv.clone(), VarType::Path);
            }
        }
    }

    if let Some(predicate) = &clause.where_clause {
        if !where_consumed {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: predicate.clone(),
            };
        }
    }

    Ok(plan)
}

/// Lower an intermediate `WITH` clause onto `plan`. The order of
/// operations matches openCypher: projection/aggregation runs
/// first, then DISTINCT, then the post-projection WHERE (which
/// references the newly-bound aliases), then ORDER BY, SKIP, and
/// LIMIT. Downstream clauses (RETURN, another MATCH) then see
/// only the names introduced by this WITH's items.
fn apply_with_clause(mut plan: LogicalPlan, w: &crate::ast::WithClause) -> Result<LogicalPlan> {
    for item in &w.items {
        reject_pattern_predicate_in_projection(&item.expr, "WITH projection")?;
    }

    // openCypher requires every WITH projection item that is not
    // a bare variable to have an explicit alias
    // (`NoExpressionAlias`). `WITH a, count(*)` fails — the
    // aggregate has no visible name downstream.
    if !w.star {
        for item in &w.items {
            if item.alias.is_none() && !matches!(item.expr, Expr::Identifier(_)) {
                return Err(Error::Plan(
                    "WITH projection item must have an AS alias".into(),
                ));
            }
        }
    }

    // Duplicate output-column names across WITH items are a
    // `ColumnNameConflict`. Same rule the RETURN pipeline uses.
    if !w.star {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for item in &w.items {
            let name = item
                .alias
                .clone()
                .unwrap_or_else(|| render_expr_key(&item.expr));
            if !seen.insert(name.clone()) {
                return Err(Error::Plan(format!(
                    "WITH has multiple columns with the same name `{name}`"
                )));
            }
        }
    }

    let has_aggregates = !w.star && w.items.iter().any(|it| contains_aggregate(&it.expr));

    // InvalidAggregation: ORDER BY on a non-aggregating WITH can't
    // introduce aggregates — the projection defines the row set and
    // aggregates would have nothing to group over.
    if !has_aggregates {
        for sort in &w.order_by {
            if contains_aggregate(&sort.expr) {
                return Err(Error::Plan(
                    "ORDER BY on a non-aggregating WITH cannot reference an aggregate".into(),
                ));
            }
        }
    }

    // AmbiguousAggregationExpression: when ORDER BY on an aggregating
    // WITH contains (but isn't itself) an aggregate, its non-aggregate
    // sub-parts must be simple references to projected items — same
    // rule classify_return_items enforces for RETURN items.
    // `WITH me.age + you.age AS ages, count(*) AS cnt ORDER BY
    // me.age + you.age + count(*)` fails because `me.age` / `you.age`
    // aren't simple projections even though their compound is.
    if has_aggregates {
        let sort_items_as_return: Vec<ReturnItem> = w
            .order_by
            .iter()
            .map(|s| ReturnItem {
                expr: s.expr.clone(),
                alias: None,
                raw_text: None,
            })
            .collect();
        // Treat the WITH projection as the "sibling" context the
        // ORDER BY can reference: non-aggregate WITH items and their
        // aliases (as bare identifiers) are both valid targets.
        let mut top_refs: Vec<ReturnItem> = w
            .items
            .iter()
            .filter(|it| !contains_aggregate(&it.expr))
            .cloned()
            .collect();
        for it in &w.items {
            if let Some(alias) = &it.alias {
                top_refs.push(ReturnItem {
                    expr: Expr::Identifier(alias.clone()),
                    alias: None,
                    raw_text: None,
                });
            }
        }
        // Classify ORDER BY items against that composite scope.
        let scope: Vec<ReturnItem> = top_refs
            .into_iter()
            .chain(sort_items_as_return.iter().cloned())
            .collect();
        check_ambiguous_aggregation(&scope)?;
    }

    // openCypher: `WHERE` attached to `WITH` can reference BOTH the
    // variables bound before the WITH and the aliases introduced by
    // it. When the projection is non-aggregating, push the filter
    // *before* the projection — substituting any alias reference
    // with its defining expression — so the pre-WITH vars stay in
    // scope. Aggregating WITH keeps the filter after so HAVING-style
    // predicates on aggregate results work correctly.
    let alias_map: std::collections::HashMap<String, Expr> = w
        .items
        .iter()
        .filter_map(|it| it.alias.as_ref().map(|a| (a.clone(), it.expr.clone())))
        .collect();

    let (pre_filter, post_filter) = match (&w.where_clause, has_aggregates, w.star) {
        (Some(pred), false, false) => (Some(substitute_aliases(pred.clone(), &alias_map)), None),
        (Some(pred), _, _) => (None, Some(pred.clone())),
        (None, _, _) => (None, None),
    };

    // ORDER BY on WITH sees pre-projection variables too: sort by
    // `a.name` after `WITH a.name AS name` is legal. For
    // non-aggregating WITH we can satisfy that by pushing the sort
    // *before* the projection, substituting alias references so
    // they map back onto the pre-projection bindings they stand
    // for. Aggregating WITH leaves ORDER BY after the aggregate so
    // it can still reach the aggregate result columns; the
    // explicit `a.name → name` back-substitution for that case
    // happens further below.
    let pre_order_by: Vec<SortItem> = if !has_aggregates && !w.star {
        w.order_by
            .iter()
            .map(|s| SortItem {
                expr: substitute_aliases(s.expr.clone(), &alias_map),
                descending: s.descending,
            })
            .collect()
    } else {
        Vec::new()
    };

    if let Some(predicate) = pre_filter {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    if !pre_order_by.is_empty() {
        plan = LogicalPlan::OrderBy {
            input: Box::new(plan),
            sort_items: pre_order_by,
        };
    }

    if w.star {
        plan = LogicalPlan::Identity {
            input: Box::new(plan),
        };
    } else {
        let (group_keys, aggregates, post_items) = classify_return_items(&w.items)?;
        plan = if !aggregates.is_empty() {
            let mut agg = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_keys,
                aggregates,
            };
            if !post_items.is_empty() {
                agg = LogicalPlan::Project {
                    input: Box::new(agg),
                    items: post_items,
                };
            }
            agg
        } else {
            LogicalPlan::Project {
                input: Box::new(plan),
                items: w.items.clone(),
            }
        };
    }

    if w.distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    if let Some(predicate) = post_filter {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // ORDER BY on an aggregating WITH goes here (after the aggregate
    // output is materialized). For non-aggregating WITH we already
    // emitted the sort before the projection above, so this branch
    // only applies when `has_aggregates` or `star`. Aggregating
    // WITH still accepts ORDER BY expressions that reference the
    // pre-WITH variables whose values are carried through via an
    // alias — e.g. `WITH a.name AS name, count(*) AS cnt ORDER BY
    // a.name` — so back-substitute any sub-expression that equals
    // an alias definition with an `Identifier(alias)`.
    if !w.order_by.is_empty() && (has_aggregates || w.star) {
        let back_map: Vec<(Expr, String)> = w
            .items
            .iter()
            .filter_map(|it| it.alias.as_ref().map(|a| (it.expr.clone(), a.clone())))
            .collect();
        let rewritten: Vec<SortItem> = w
            .order_by
            .iter()
            .map(|s| SortItem {
                expr: rewrite_aggregate_refs(s.expr.clone(), &back_map),
                descending: s.descending,
            })
            .collect();
        plan = LogicalPlan::OrderBy {
            input: Box::new(plan),
            sort_items: rewritten,
        };
    }
    if let Some(n) = &w.skip {
        plan = LogicalPlan::Skip {
            input: Box::new(plan),
            count: n.clone(),
        };
    }
    if let Some(n) = &w.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count: n.clone(),
        };
    }

    Ok(plan)
}

/// openCypher's `AmbiguousAggregationExpression` rule: in a
/// projection item that contains (but isn't itself) an aggregate,
/// every bare identifier / property reference outside the aggregate
/// subtrees must correspond to another top-level return item.
/// That's what lets `RETURN me.age, me.age + count(you.age)` work
/// while rejecting `RETURN me.age + count(you.age)` (me.age is
/// dangling) and `RETURN me.age + you.age, me.age + you.age + count(*)`
/// (the compound-as-group-key isn't legal even though it's
/// structurally returned — the leaves `me.age` / `you.age` themselves
/// aren't top-level items).
fn check_ambiguous_aggregation(items: &[ReturnItem]) -> Result<()> {
    // Group-key candidates: top-level return items that don't
    // contain an aggregate anywhere. These are the only items
    // an aggregate-containing sibling may reference by name.
    // `count(*)` alone is an aggregate — excluded. `me.age +
    // count(*)` contains an aggregate — also excluded (it's
    // not a legal thing to reference from another aggregate-
    // containing item).
    let top_level_refs: Vec<&Expr> = items
        .iter()
        .filter(|it| !contains_aggregate(&it.expr))
        .map(|it| &it.expr)
        .collect();
    for item in items {
        // Only items that contain (but aren't themselves) an
        // aggregate need checking — a standalone `count(*)` is
        // fine, and a pure non-aggregate is the group key itself.
        let top_is_agg = matches!(
            &item.expr,
            Expr::Call { name, .. } if aggregate_fn_from_name(name).is_some()
        );
        if top_is_agg || !contains_aggregate(&item.expr) {
            continue;
        }
        verify_non_agg_refs(&item.expr, &top_level_refs)?;
    }
    Ok(())
}

/// Walk `expr`, skipping aggregate subtrees entirely, and fail if
/// we find a bare `Identifier` / `Property` leaf that doesn't equal
/// any of `top_level_refs`. Intermediate operator nodes just recurse
/// into their children without any check of their own — the rule is
/// leaf-level.
fn verify_non_agg_refs(expr: &Expr, top_level_refs: &[&Expr]) -> Result<()> {
    // Aggregate subtrees are opaque placeholders. Don't descend.
    if let Expr::Call { name, .. } = expr {
        if aggregate_fn_from_name(name).is_some() {
            return Ok(());
        }
    }
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => Ok(()),
        // Only leaf references can match a top-level item — per
        // openCypher, compound expressions must not appear next to
        // aggregates even if the same compound is itself a return
        // item (that's what makes scenario 21 a SyntaxError). So we
        // check the leaves here and keep recursing through compound
        // shapes below without a matching shortcut.
        Expr::Identifier(_) | Expr::Property { .. } => {
            if top_level_refs.iter().any(|t| *t == expr) {
                Ok(())
            } else {
                Err(Error::Plan(format!(
                    "AmbiguousAggregationExpression: `{}` appears outside an aggregate but isn't a top-level return item",
                    render_expr_key(expr)
                )))
            }
        }
        Expr::PropertyAccess { base, .. } => verify_non_agg_refs(base, top_level_refs),
        Expr::HasLabels { expr, .. } => verify_non_agg_refs(expr, top_level_refs),
        Expr::IndexAccess { base, index } => {
            verify_non_agg_refs(base, top_level_refs)?;
            verify_non_agg_refs(index, top_level_refs)
        }
        Expr::SliceAccess { base, start, end } => {
            verify_non_agg_refs(base, top_level_refs)?;
            if let Some(s) = start {
                verify_non_agg_refs(s, top_level_refs)?;
            }
            if let Some(e) = end {
                verify_non_agg_refs(e, top_level_refs)?;
            }
            Ok(())
        }
        Expr::Not(e) => verify_non_agg_refs(e, top_level_refs),
        Expr::And(a, b) | Expr::Or(a, b) | Expr::Xor(a, b) => {
            verify_non_agg_refs(a, top_level_refs)?;
            verify_non_agg_refs(b, top_level_refs)
        }
        Expr::Compare { left, right, .. } => {
            verify_non_agg_refs(left, top_level_refs)?;
            verify_non_agg_refs(right, top_level_refs)
        }
        Expr::IsNull { inner, .. } => verify_non_agg_refs(inner, top_level_refs),
        Expr::BinaryOp { left, right, .. } => {
            verify_non_agg_refs(left, top_level_refs)?;
            verify_non_agg_refs(right, top_level_refs)
        }
        Expr::UnaryOp { operand, .. } => verify_non_agg_refs(operand, top_level_refs),
        Expr::InList { element, list } => {
            verify_non_agg_refs(element, top_level_refs)?;
            verify_non_agg_refs(list, top_level_refs)
        }
        Expr::List(items) => {
            for it in items {
                verify_non_agg_refs(it, top_level_refs)?;
            }
            Ok(())
        }
        Expr::Map(entries) => {
            for (_, v) in entries {
                verify_non_agg_refs(v, top_level_refs)?;
            }
            Ok(())
        }
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            if let Some(s) = scrutinee {
                verify_non_agg_refs(s, top_level_refs)?;
            }
            for (cond, then) in branches {
                verify_non_agg_refs(cond, top_level_refs)?;
                verify_non_agg_refs(then, top_level_refs)?;
            }
            if let Some(e) = else_expr {
                verify_non_agg_refs(e, top_level_refs)?;
            }
            Ok(())
        }
        // Non-aggregate function calls recurse through their args.
        Expr::Call { args, .. } => match args {
            CallArgs::Star => Ok(()),
            CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) => {
                for e in es {
                    verify_non_agg_refs(e, top_level_refs)?;
                }
                Ok(())
            }
        },
        // Subqueries, comprehensions, list predicates, reduce,
        // pattern exists/predicate — these introduce their own
        // scopes; don't descend for the purposes of this check.
        Expr::ExistsSubquery { .. }
        | Expr::CountSubquery { .. }
        | Expr::CollectSubquery { .. }
        | Expr::PatternExists(_)
        | Expr::PatternComprehension { .. }
        | Expr::ListComprehension { .. }
        | Expr::ListPredicate { .. }
        | Expr::Reduce { .. } => Ok(()),
    }
}

fn classify_return_items(
    items: &[ReturnItem],
) -> Result<(Vec<ReturnItem>, Vec<AggregateSpec>, Vec<ReturnItem>)> {
    // openCypher forbids aggregate calls inside list
    // comprehensions, list predicates, reduce, and similar
    // scoping constructs — each introduces its own row
    // iteration scope that has nothing to aggregate over.
    // Catch them at plan time before the nested-aggregate
    // extractor lifts them out.
    for item in items {
        reject_aggregate_inside_nested_scope(&item.expr)?;
    }
    check_ambiguous_aggregation(items)?;
    let mut group_keys: Vec<ReturnItem> = Vec::new();
    let mut aggregates: Vec<AggregateSpec> = Vec::new();
    let mut post_items: Vec<ReturnItem> = Vec::new();
    let mut synth_idx = 0usize;
    for item in items.iter() {
        let is_top_aggregate = matches!(
            &item.expr,
            Expr::Call { name, .. } if aggregate_fn_from_name(name).is_some()
        );
        if is_top_aggregate {
            let Expr::Call { name, args } = &item.expr else {
                unreachable!()
            };
            let func = aggregate_fn_from_name(name).unwrap();
            // openCypher requires aggregate arguments to be
            // deterministic — `count(rand())` is flagged as
            // `NonConstantExpression` at compile time. The same
            // rule covers `timestamp()` and any other side-effect
            // or non-deterministic scalar, so check against a
            // dedicated predicate rather than hard-coding `rand`.
            let reject_nondet = |e: &Expr| -> Result<()> {
                let mut err: Option<Error> = None;
                walk_expr(e, &mut |inner| {
                    if let Expr::Call { name, .. } = inner {
                        if is_non_deterministic_function(name) {
                            err = Some(Error::Plan(format!(
                                "NonConstantExpression: `{name}()` is not allowed inside an aggregate argument"
                            )));
                        }
                    }
                    Ok(())
                })?;
                if let Some(e) = err {
                    return Err(e);
                }
                Ok(())
            };
            if let CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) = args {
                for e in es {
                    reject_nondet(e)?;
                }
            }
            let mut percentile_extra: Option<Expr> = None;
            let agg_arg = match args {
                CallArgs::Star => {
                    if !matches!(func, AggregateFn::Count) {
                        return Err(Error::Plan("only count(*) accepts a star argument".into()));
                    }
                    AggregateArg::Star
                }
                CallArgs::Exprs(es) if es.len() == 1 => AggregateArg::Expr(es[0].clone()),
                // percentileDisc/percentileCont take 2 args: the
                // collected value, then the percentile in [0.0, 1.0].
                // We stash the second arg on the AggregateSpec so the
                // operator can resolve the percentile at finalize time.
                CallArgs::Exprs(es)
                    if es.len() == 2
                        && matches!(
                            func,
                            AggregateFn::PercentileDisc
                                | AggregateFn::PercentileCont
                                | AggregateFn::ApocNth
                        ) =>
                {
                    percentile_extra = Some(es[1].clone());
                    AggregateArg::Expr(es[0].clone())
                }
                CallArgs::Exprs(_) => {
                    return Err(Error::Plan(format!("{} takes exactly one argument", name)))
                }
                CallArgs::DistinctExprs(es) if es.len() == 1 => {
                    AggregateArg::DistinctExpr(es[0].clone())
                }
                CallArgs::DistinctExprs(_) => {
                    return Err(Error::Plan(format!(
                        "{}(DISTINCT ...) takes exactly one argument",
                        name
                    )))
                }
            };
            // Prefer verbatim source text for the output column —
            // the TCK treats `count(*)` and `count( * )` as
            // different column names. When no raw_text is available
            // (synthesized items from AST rewrites) fall back to
            // `render_expr_key`.
            let alias = item.alias.clone().unwrap_or_else(|| {
                item.raw_text
                    .clone()
                    .unwrap_or_else(|| render_expr_key(&item.expr))
            });
            aggregates.push(AggregateSpec {
                alias: alias.clone(),
                function: func,
                arg: agg_arg,
                extra_arg: percentile_extra,
            });
            // Keep the aggregate in `post_items` too, pointing
            // at its own alias. Otherwise a RETURN / WITH that
            // mixes a top-level aggregate with a nested-aggregate
            // sibling would construct a Project over the Aggregate
            // that only forwards the nested-aggregate's rewritten
            // expr and drop the top-level aggregate's column.
            // The Project stays a no-op-through when post_items
            // ends up containing only identifiers — cheap and
            // simpler than branching on "any nested".
            post_items.push(ReturnItem {
                expr: Expr::Identifier(alias),
                alias: item.alias.clone(),
                raw_text: item.raw_text.clone(),
            });
        } else {
            if contains_aggregate(&item.expr) {
                let mut rewrites: Vec<(String, AggregateFn, AggregateArg, Option<Expr>)> =
                    Vec::new();
                let rewritten =
                    extract_nested_aggregates(&item.expr, &mut rewrites, &mut synth_idx);
                for (alias, func, arg, extra_arg) in rewrites {
                    aggregates.push(AggregateSpec {
                        alias,
                        function: func,
                        arg,
                        extra_arg,
                    });
                }
                // Preserve the user-visible column name: if the
                // item already had `AS <name>` use that, otherwise
                // default to the source expression's canonical
                // rendering (`count(a) + 3`), not a synthetic
                // `__nested_agg_N`. The synthetic names are only
                // for the lifted aggregate aliases upstream.
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| render_expr_key(&item.expr));
                post_items.push(ReturnItem {
                    expr: rewritten,
                    alias: Some(alias),
                    raw_text: item.raw_text.clone(),
                });
            } else {
                group_keys.push(item.clone());
                // Mirror non-aggregate group keys into post_items
                // so the Project stays row-shape-complete when it
                // has to run (because of sibling nested aggregates).
                post_items.push(ReturnItem {
                    expr: Expr::Identifier(
                        item.alias
                            .clone()
                            .unwrap_or_else(|| render_expr_key(&item.expr)),
                    ),
                    alias: item.alias.clone(),
                    raw_text: item.raw_text.clone(),
                });
            }
        }
    }
    // If every item was a simple group key or top-level aggregate
    // (no nested aggregates anywhere), collapse `post_items` back
    // to empty so the existing "skip Project when nothing to
    // rewrite" fast path still triggers. The identifiers we added
    // would have been redundant there.
    let has_real_rewrite = post_items
        .iter()
        .any(|it| !matches!(it.expr, Expr::Identifier(_)));
    if !has_real_rewrite {
        post_items.clear();
    }
    Ok((group_keys, aggregates, post_items))
}

fn extract_nested_aggregates(
    expr: &Expr,
    out: &mut Vec<(String, AggregateFn, AggregateArg, Option<Expr>)>,
    idx: &mut usize,
) -> Expr {
    match expr {
        Expr::Call { name, args } if aggregate_fn_from_name(name).is_some() => {
            let func = aggregate_fn_from_name(name).unwrap();
            let mut extra: Option<Expr> = None;
            let agg_arg = match args {
                CallArgs::Star => AggregateArg::Star,
                CallArgs::Exprs(es) if es.len() == 1 => AggregateArg::Expr(es[0].clone()),
                CallArgs::Exprs(es) if es.is_empty() => AggregateArg::Star,
                CallArgs::Exprs(es)
                    if es.len() == 2
                        && matches!(
                            func,
                            AggregateFn::PercentileDisc
                                | AggregateFn::PercentileCont
                                | AggregateFn::ApocNth
                        ) =>
                {
                    extra = Some(es[1].clone());
                    AggregateArg::Expr(es[0].clone())
                }
                CallArgs::DistinctExprs(es) if es.len() == 1 => {
                    AggregateArg::DistinctExpr(es[0].clone())
                }
                _ => AggregateArg::Star,
            };
            let alias = format!("__agg_{}_{}", name.to_lowercase(), *idx);
            *idx += 1;
            out.push((alias.clone(), func, agg_arg, extra));
            Expr::Identifier(alias)
        }
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op: *op,
            left: Box::new(extract_nested_aggregates(left, out, idx)),
            right: Box::new(extract_nested_aggregates(right, out, idx)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op: *op,
            operand: Box::new(extract_nested_aggregates(operand, out, idx)),
        },
        Expr::Call { name, args } => {
            let new_args = match args {
                CallArgs::Star => CallArgs::Star,
                CallArgs::Exprs(es) => CallArgs::Exprs(
                    es.iter()
                        .map(|e| extract_nested_aggregates(e, out, idx))
                        .collect(),
                ),
                CallArgs::DistinctExprs(es) => CallArgs::DistinctExprs(
                    es.iter()
                        .map(|e| extract_nested_aggregates(e, out, idx))
                        .collect(),
                ),
            };
            Expr::Call {
                name: name.clone(),
                args: new_args,
            }
        }
        Expr::Not(inner) => Expr::Not(Box::new(extract_nested_aggregates(inner, out, idx))),
        Expr::And(a, b) => Expr::And(
            Box::new(extract_nested_aggregates(a, out, idx)),
            Box::new(extract_nested_aggregates(b, out, idx)),
        ),
        Expr::Or(a, b) => Expr::Or(
            Box::new(extract_nested_aggregates(a, out, idx)),
            Box::new(extract_nested_aggregates(b, out, idx)),
        ),
        Expr::Xor(a, b) => Expr::Xor(
            Box::new(extract_nested_aggregates(a, out, idx)),
            Box::new(extract_nested_aggregates(b, out, idx)),
        ),
        Expr::Compare { op, left, right } => Expr::Compare {
            op: *op,
            left: Box::new(extract_nested_aggregates(left, out, idx)),
            right: Box::new(extract_nested_aggregates(right, out, idx)),
        },
        Expr::IsNull { negated, inner } => Expr::IsNull {
            negated: *negated,
            inner: Box::new(extract_nested_aggregates(inner, out, idx)),
        },
        Expr::PropertyAccess { base, key } => Expr::PropertyAccess {
            base: Box::new(extract_nested_aggregates(base, out, idx)),
            key: key.clone(),
        },
        Expr::IndexAccess { base, index } => Expr::IndexAccess {
            base: Box::new(extract_nested_aggregates(base, out, idx)),
            index: Box::new(extract_nested_aggregates(index, out, idx)),
        },
        Expr::SliceAccess { base, start, end } => Expr::SliceAccess {
            base: Box::new(extract_nested_aggregates(base, out, idx)),
            start: start
                .as_deref()
                .map(|e| Box::new(extract_nested_aggregates(e, out, idx))),
            end: end
                .as_deref()
                .map(|e| Box::new(extract_nested_aggregates(e, out, idx))),
        },
        Expr::List(items) => Expr::List(
            items
                .iter()
                .map(|e| extract_nested_aggregates(e, out, idx))
                .collect(),
        ),
        Expr::Map(entries) => Expr::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), extract_nested_aggregates(v, out, idx)))
                .collect(),
        ),
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => Expr::Case {
            scrutinee: scrutinee
                .as_deref()
                .map(|s| Box::new(extract_nested_aggregates(s, out, idx))),
            branches: branches
                .iter()
                .map(|(c, r)| {
                    (
                        extract_nested_aggregates(c, out, idx),
                        extract_nested_aggregates(r, out, idx),
                    )
                })
                .collect(),
            else_expr: else_expr
                .as_deref()
                .map(|e| Box::new(extract_nested_aggregates(e, out, idx))),
        },
        // List / map / reduce sub-constructs also need to walk
        // their sub-expressions: an aggregate nested inside
        // `[x IN collect(r) WHERE ...]` must be lifted out so
        // the RETURN's Aggregate operator sees it. Leaving these
        // untouched would leave `collect` inside the list
        // comprehension's source, which then errors at eval time
        // as "not a scalar function".
        Expr::ListComprehension {
            var,
            source,
            predicate,
            projection,
        } => Expr::ListComprehension {
            var: var.clone(),
            source: Box::new(extract_nested_aggregates(source, out, idx)),
            predicate: predicate
                .as_deref()
                .map(|p| Box::new(extract_nested_aggregates(p, out, idx))),
            projection: projection
                .as_deref()
                .map(|p| Box::new(extract_nested_aggregates(p, out, idx))),
        },
        Expr::ListPredicate {
            kind,
            var,
            list,
            predicate,
        } => Expr::ListPredicate {
            kind: *kind,
            var: var.clone(),
            list: Box::new(extract_nested_aggregates(list, out, idx)),
            predicate: Box::new(extract_nested_aggregates(predicate, out, idx)),
        },
        Expr::Reduce {
            acc_var,
            acc_init,
            elem_var,
            source,
            body,
        } => Expr::Reduce {
            acc_var: acc_var.clone(),
            acc_init: Box::new(extract_nested_aggregates(acc_init, out, idx)),
            elem_var: elem_var.clone(),
            source: Box::new(extract_nested_aggregates(source, out, idx)),
            body: Box::new(extract_nested_aggregates(body, out, idx)),
        },
        Expr::InList { element, list } => Expr::InList {
            element: Box::new(extract_nested_aggregates(element, out, idx)),
            list: Box::new(extract_nested_aggregates(list, out, idx)),
        },
        Expr::HasLabels { expr, labels } => Expr::HasLabels {
            expr: Box::new(extract_nested_aggregates(expr, out, idx)),
            labels: labels.clone(),
        },
        other => other.clone(),
    }
}

/// Returns true when `name` (case-insensitive) is a built-in
/// scalar function recognised by the executor's `call_scalar`
/// dispatcher — kept in sync with that function. Used by the
/// RETURN scope validator to flag `RETURN foo(a)` as an
/// `UnknownFunction` at compile time when the graph happens to
/// be empty and the pipeline would otherwise finish without
/// ever reaching a runtime error.
/// Scalar functions whose output depends on something other than
/// their arguments — `rand()` returns a fresh value per call and
/// `timestamp()` reads a clock. openCypher forbids them inside
/// aggregate arguments because aggregation assumes a deterministic
/// value to group or combine over.
fn is_non_deterministic_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "rand" | "timestamp" | "randomuuid"
    )
}

fn is_known_scalar_function(name: &str) -> bool {
    // `apoc.*` functions are opt-in Cargo features resolved at
    // runtime by the executor's APOC adapter. The planner doesn't
    // know which sub-features the current build carries, so
    // accept any `apoc.*` name here and let the runtime dispatcher
    // raise a precise `UnknownFunction` if the namespace / name
    // isn't available in this build.
    if name.to_ascii_lowercase().starts_with("apoc.") {
        return true;
    }
    matches!(
        name.to_ascii_lowercase().as_str(),
        "size"
            | "length"
            | "char_length"
            | "character_length"
            | "nodes"
            | "relationships"
            | "labels"
            | "keys"
            | "type"
            | "id"
            | "elementid"
            | "startnode"
            | "endnode"
            | "properties"
            | "exists"
            | "isempty"
            | "isnan"
            | "tolower"
            | "toupper"
            | "tostring"
            | "tostringornull"
            | "tostringlist"
            | "tointeger"
            | "tointegerornull"
            | "tointegerlist"
            | "toboolean"
            | "tobooleanornull"
            | "tobooleanlist"
            | "tofloat"
            | "tofloatornull"
            | "tofloatlist"
            | "valuetype"
            | "coalesce"
            | "substring"
            | "left"
            | "right"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "replace"
            | "split"
            | "range"
            | "head"
            | "last"
            | "tail"
            | "reverse"
            | "abs"
            | "ceil"
            | "floor"
            | "round"
            | "sqrt"
            | "sign"
            | "pi"
            | "e"
            | "exp"
            | "log"
            | "ln"
            | "log10"
            | "sin"
            | "cos"
            | "tan"
            | "cot"
            | "asin"
            | "acos"
            | "atan"
            | "atan2"
            | "degrees"
            | "radians"
            | "haversin"
            | "rand"
            | "randomuuid"
            | "date"
            | "datetime"
            | "localdatetime"
            | "time"
            | "localtime"
            | "timestamp"
            | "duration"
            | "point"
            | "point.distance"
            | "point.withinbbox"
            | "distance"
    )
}

/// Walk each RETURN item's expression and flag any function
/// call whose name isn't an aggregate or a known scalar. Empty
/// graphs would otherwise skip the runtime dispatcher entirely
/// and return no rows, hiding the `UnknownFunction` error.
fn reject_unknown_functions(expr: &Expr) -> Result<()> {
    let mut err: Option<Error> = None;
    walk_expr(expr, &mut |e| {
        if let Expr::Call { name, .. } = e {
            if aggregate_fn_from_name(name).is_none() && !is_known_scalar_function(name) {
                err = Some(Error::Plan(format!("unknown function `{name}`")));
            }
        }
        Ok(())
    })?;
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Walk `expr` and reject any `size(p)` where `p` is a
/// statically-known Path-typed variable — openCypher raises
/// `InvalidArgumentType` because `size()` is defined on
/// strings and lists; path length is spelled `length()`.
fn reject_size_on_path(expr: &Expr, bound: &HashMap<String, VarType>) -> Result<()> {
    let mut err: Option<Error> = None;
    walk_expr(expr, &mut |e| {
        if let Expr::Call { name, args } = e {
            if name.eq_ignore_ascii_case("size") {
                if let CallArgs::Exprs(es) = args {
                    if es.len() == 1 {
                        if let Expr::Identifier(n) = &es[0] {
                            if matches!(bound.get(n), Some(VarType::Path)) {
                                err = Some(Error::Plan(
                                    "size() is not defined for path values; use length() \
                                     instead"
                                        .into(),
                                ));
                            }
                        }
                    }
                }
            }
            // `type()` is defined for relationships only; calling
            // it on a known node variable is a compile-time
            // `InvalidArgumentType` in openCypher.
            if name.eq_ignore_ascii_case("type") {
                if let CallArgs::Exprs(es) = args {
                    if es.len() == 1 {
                        if let Expr::Identifier(n) = &es[0] {
                            if matches!(bound.get(n), Some(VarType::Node)) {
                                err = Some(Error::Plan(
                                    "type() is not defined for node values; it applies \
                                     to relationships"
                                        .into(),
                                ));
                            }
                        }
                    }
                }
            }
            // `length()` is defined for paths only; lists / strings
            // use `size()` and scalars like nodes / edges have no
            // length. openCypher raises `InvalidArgumentType`.
            if name.eq_ignore_ascii_case("length") {
                if let CallArgs::Exprs(es) = args {
                    if es.len() == 1 {
                        if let Expr::Identifier(n) = &es[0] {
                            if matches!(bound.get(n), Some(VarType::Node | VarType::Edge)) {
                                err = Some(Error::Plan(
                                    "length() is not defined for node or edge \
                                     values; it applies to paths"
                                        .into(),
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    })?;
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Walk `expr` and return `true` iff every referenced bare
/// identifier / Property-base is in `cols`. Used by the ORDER
/// BY placement decision: post-project sort is only valid when
/// the sort expression doesn't reach past the projection to a
/// dropped binding. Expressions that don't reference any
/// variable (pure literals, aggregates over constants) return
/// `true` vacuously.
fn all_refs_in(expr: &Expr, cols: &std::collections::HashSet<String>) -> bool {
    let mut all_in = true;
    let _ = walk_expr(expr, &mut |e| {
        match e {
            Expr::Identifier(n) => {
                if !cols.contains(n) {
                    all_in = false;
                }
            }
            Expr::Property { var, .. } => {
                if !cols.contains(var) {
                    all_in = false;
                }
            }
            _ => {}
        }
        Ok(())
    });
    all_in
}

/// Reject `path.key` / `path[idx]` on a known Path-typed
/// variable — properties belong to nodes/edges and openCypher
/// raises `InvalidArgumentType` at compile time.
fn reject_property_access_on_path(expr: &Expr, bound: &HashMap<String, VarType>) -> Result<()> {
    let mut err: Option<Error> = None;
    walk_expr(expr, &mut |e| {
        match e {
            Expr::Property { var, .. } => {
                if matches!(bound.get(var), Some(VarType::Path)) {
                    err = Some(Error::Plan(
                        "property access is not defined on path values".into(),
                    ));
                }
            }
            Expr::PropertyAccess { base, .. } => {
                if let Expr::Identifier(name) = base.as_ref() {
                    if matches!(bound.get(name), Some(VarType::Path)) {
                        err = Some(Error::Plan(
                            "property access is not defined on path values".into(),
                        ));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    })?;
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Reject aggregate calls that appear inside any scoping
/// construct with its own row iteration scope (list
/// comprehension, list predicate, reduce). openCypher flags
/// these as `InvalidAggregation` — `[x IN xs | count(*)]`
/// doesn't make sense because each iteration has only one row
/// in scope.
fn reject_aggregate_inside_nested_scope(expr: &Expr) -> Result<()> {
    match expr {
        Expr::ListComprehension {
            source,
            predicate,
            projection,
            ..
        } => {
            reject_aggregate_inside_nested_scope(source)?;
            if let Some(p) = predicate {
                if contains_aggregate(p) {
                    return Err(Error::Plan(
                        "aggregate functions are not allowed inside list comprehensions".into(),
                    ));
                }
            }
            if let Some(p) = projection {
                if contains_aggregate(p) {
                    return Err(Error::Plan(
                        "aggregate functions are not allowed inside list comprehensions".into(),
                    ));
                }
            }
        }
        Expr::ListPredicate {
            list, predicate, ..
        } => {
            reject_aggregate_inside_nested_scope(list)?;
            if contains_aggregate(predicate) {
                return Err(Error::Plan(
                    "aggregate functions are not allowed inside list predicates".into(),
                ));
            }
        }
        Expr::Reduce {
            acc_init,
            source,
            body,
            ..
        } => {
            reject_aggregate_inside_nested_scope(acc_init)?;
            reject_aggregate_inside_nested_scope(source)?;
            if contains_aggregate(body) {
                return Err(Error::Plan(
                    "aggregate functions are not allowed inside reduce".into(),
                ));
            }
        }
        Expr::Not(inner) | Expr::IsNull { inner, .. } | Expr::UnaryOp { operand: inner, .. } => {
            reject_aggregate_inside_nested_scope(inner)?;
        }
        Expr::And(a, b) | Expr::Or(a, b) | Expr::Xor(a, b) => {
            reject_aggregate_inside_nested_scope(a)?;
            reject_aggregate_inside_nested_scope(b)?;
        }
        Expr::Compare { left, right, .. } | Expr::BinaryOp { left, right, .. } => {
            reject_aggregate_inside_nested_scope(left)?;
            reject_aggregate_inside_nested_scope(right)?;
        }
        Expr::PropertyAccess { base, .. } => {
            reject_aggregate_inside_nested_scope(base)?;
        }
        Expr::IndexAccess { base, index } => {
            reject_aggregate_inside_nested_scope(base)?;
            reject_aggregate_inside_nested_scope(index)?;
        }
        Expr::SliceAccess { base, start, end } => {
            reject_aggregate_inside_nested_scope(base)?;
            if let Some(s) = start {
                reject_aggregate_inside_nested_scope(s)?;
            }
            if let Some(e) = end {
                reject_aggregate_inside_nested_scope(e)?;
            }
        }
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            if let Some(s) = scrutinee {
                reject_aggregate_inside_nested_scope(s)?;
            }
            for (c, r) in branches {
                reject_aggregate_inside_nested_scope(c)?;
                reject_aggregate_inside_nested_scope(r)?;
            }
            if let Some(e) = else_expr {
                reject_aggregate_inside_nested_scope(e)?;
            }
        }
        Expr::List(items) => {
            for it in items {
                reject_aggregate_inside_nested_scope(it)?;
            }
        }
        Expr::Map(entries) => {
            for (_, v) in entries {
                reject_aggregate_inside_nested_scope(v)?;
            }
        }
        Expr::Call { args, .. } => match args {
            CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) => {
                for e in es {
                    reject_aggregate_inside_nested_scope(e)?;
                }
            }
            CallArgs::Star => {}
        },
        Expr::InList { element, list } => {
            reject_aggregate_inside_nested_scope(element)?;
            reject_aggregate_inside_nested_scope(list)?;
        }
        Expr::HasLabels { expr, .. } => {
            reject_aggregate_inside_nested_scope(expr)?;
        }
        _ => {}
    }
    Ok(())
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, .. } if aggregate_fn_from_name(name).is_some() => true,
        Expr::Not(inner) => contains_aggregate(inner),
        Expr::And(a, b) | Expr::Or(a, b) | Expr::Xor(a, b) => {
            contains_aggregate(a) || contains_aggregate(b)
        }
        Expr::Compare { left, right, .. } | Expr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::IsNull { inner, .. } | Expr::UnaryOp { operand: inner, .. } => {
            contains_aggregate(inner)
        }
        Expr::Call {
            args: CallArgs::Exprs(es),
            ..
        }
        | Expr::Call {
            args: CallArgs::DistinctExprs(es),
            ..
        } => es.iter().any(contains_aggregate),
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => {
            scrutinee
                .as_deref()
                .map(contains_aggregate)
                .unwrap_or(false)
                || branches
                    .iter()
                    .any(|(c, r)| contains_aggregate(c) || contains_aggregate(r))
                || else_expr
                    .as_deref()
                    .map(contains_aggregate)
                    .unwrap_or(false)
        }
        Expr::List(items) => items.iter().any(contains_aggregate),
        Expr::Map(entries) => entries.iter().any(|(_, v)| contains_aggregate(v)),
        Expr::ListComprehension {
            source,
            predicate,
            projection,
            ..
        } => {
            contains_aggregate(source)
                || predicate
                    .as_deref()
                    .map(contains_aggregate)
                    .unwrap_or(false)
                || projection
                    .as_deref()
                    .map(contains_aggregate)
                    .unwrap_or(false)
        }
        Expr::ListPredicate {
            list, predicate, ..
        } => contains_aggregate(list) || contains_aggregate(predicate),
        Expr::Reduce {
            acc_init,
            source,
            body,
            ..
        } => contains_aggregate(acc_init) || contains_aggregate(source) || contains_aggregate(body),
        Expr::InList { element, list } => contains_aggregate(element) || contains_aggregate(list),
        Expr::HasLabels { expr, .. } => contains_aggregate(expr),
        _ => false,
    }
}

/// Rewrite expressions that match an already-projected aggregate
/// call so they reference the output column (by identifier) instead
/// of re-evaluating the aggregate function. Used for ORDER BY items
/// that repeat a RETURN aggregate — the post-aggregation pipeline
/// runs in a scalar context where `max(...)` / `count(*)` aren't
/// directly callable.
fn rewrite_aggregate_refs(expr: Expr, map: &[(Expr, String)]) -> Expr {
    for (candidate, alias) in map {
        if &expr == candidate {
            return Expr::Identifier(alias.clone());
        }
    }
    match expr {
        Expr::Not(inner) => Expr::Not(Box::new(rewrite_aggregate_refs(*inner, map))),
        Expr::And(a, b) => Expr::And(
            Box::new(rewrite_aggregate_refs(*a, map)),
            Box::new(rewrite_aggregate_refs(*b, map)),
        ),
        Expr::Or(a, b) => Expr::Or(
            Box::new(rewrite_aggregate_refs(*a, map)),
            Box::new(rewrite_aggregate_refs(*b, map)),
        ),
        Expr::Xor(a, b) => Expr::Xor(
            Box::new(rewrite_aggregate_refs(*a, map)),
            Box::new(rewrite_aggregate_refs(*b, map)),
        ),
        Expr::Compare { op, left, right } => Expr::Compare {
            op,
            left: Box::new(rewrite_aggregate_refs(*left, map)),
            right: Box::new(rewrite_aggregate_refs(*right, map)),
        },
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op,
            left: Box::new(rewrite_aggregate_refs(*left, map)),
            right: Box::new(rewrite_aggregate_refs(*right, map)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op,
            operand: Box::new(rewrite_aggregate_refs(*operand, map)),
        },
        Expr::IsNull { negated, inner } => Expr::IsNull {
            negated,
            inner: Box::new(rewrite_aggregate_refs(*inner, map)),
        },
        Expr::PropertyAccess { base, key } => Expr::PropertyAccess {
            base: Box::new(rewrite_aggregate_refs(*base, map)),
            key,
        },
        Expr::IndexAccess { base, index } => Expr::IndexAccess {
            base: Box::new(rewrite_aggregate_refs(*base, map)),
            index: Box::new(rewrite_aggregate_refs(*index, map)),
        },
        other => other,
    }
}

/// Substitute alias references in `expr` using `map`. Used to push
/// a WHERE filter attached to a WITH clause upstream of the
/// projection — the predicate may reference both pre-WITH variables
/// and WITH aliases, so alias occurrences are rewritten to the
/// expression that defines them (leaving pre-WITH vars untouched).
fn substitute_aliases(expr: Expr, map: &std::collections::HashMap<String, Expr>) -> Expr {
    match expr {
        Expr::Identifier(ref name) => {
            if let Some(repl) = map.get(name) {
                repl.clone()
            } else {
                expr
            }
        }
        Expr::Property { var, key } => {
            if let Some(repl) = map.get(&var) {
                Expr::PropertyAccess {
                    base: Box::new(repl.clone()),
                    key,
                }
            } else {
                Expr::Property { var, key }
            }
        }
        Expr::Not(inner) => Expr::Not(Box::new(substitute_aliases(*inner, map))),
        Expr::And(a, b) => Expr::And(
            Box::new(substitute_aliases(*a, map)),
            Box::new(substitute_aliases(*b, map)),
        ),
        Expr::Or(a, b) => Expr::Or(
            Box::new(substitute_aliases(*a, map)),
            Box::new(substitute_aliases(*b, map)),
        ),
        Expr::Xor(a, b) => Expr::Xor(
            Box::new(substitute_aliases(*a, map)),
            Box::new(substitute_aliases(*b, map)),
        ),
        Expr::Compare { op, left, right } => Expr::Compare {
            op,
            left: Box::new(substitute_aliases(*left, map)),
            right: Box::new(substitute_aliases(*right, map)),
        },
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op,
            left: Box::new(substitute_aliases(*left, map)),
            right: Box::new(substitute_aliases(*right, map)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op,
            operand: Box::new(substitute_aliases(*operand, map)),
        },
        Expr::IsNull { negated, inner } => Expr::IsNull {
            negated,
            inner: Box::new(substitute_aliases(*inner, map)),
        },
        Expr::PropertyAccess { base, key } => Expr::PropertyAccess {
            base: Box::new(substitute_aliases(*base, map)),
            key,
        },
        Expr::IndexAccess { base, index } => Expr::IndexAccess {
            base: Box::new(substitute_aliases(*base, map)),
            index: Box::new(substitute_aliases(*index, map)),
        },
        Expr::SliceAccess { base, start, end } => Expr::SliceAccess {
            base: Box::new(substitute_aliases(*base, map)),
            start: start.map(|e| Box::new(substitute_aliases(*e, map))),
            end: end.map(|e| Box::new(substitute_aliases(*e, map))),
        },
        Expr::HasLabels { expr, labels } => Expr::HasLabels {
            expr: Box::new(substitute_aliases(*expr, map)),
            labels,
        },
        Expr::InList { element, list } => Expr::InList {
            element: Box::new(substitute_aliases(*element, map)),
            list: Box::new(substitute_aliases(*list, map)),
        },
        Expr::Call { name, args } => Expr::Call {
            name,
            args: match args {
                CallArgs::Star => CallArgs::Star,
                CallArgs::Exprs(es) => {
                    CallArgs::Exprs(es.into_iter().map(|e| substitute_aliases(e, map)).collect())
                }
                CallArgs::DistinctExprs(es) => CallArgs::DistinctExprs(
                    es.into_iter().map(|e| substitute_aliases(e, map)).collect(),
                ),
            },
        },
        Expr::Case {
            scrutinee,
            branches,
            else_expr,
        } => Expr::Case {
            scrutinee: scrutinee.map(|s| Box::new(substitute_aliases(*s, map))),
            branches: branches
                .into_iter()
                .map(|(c, r)| (substitute_aliases(c, map), substitute_aliases(r, map)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(substitute_aliases(*e, map))),
        },
        Expr::List(items) => Expr::List(
            items
                .into_iter()
                .map(|e| substitute_aliases(e, map))
                .collect(),
        ),
        Expr::Map(entries) => Expr::Map(
            entries
                .into_iter()
                .map(|(k, v)| (k, substitute_aliases(v, map)))
                .collect(),
        ),
        Expr::ListComprehension {
            var,
            source,
            predicate,
            projection,
        } => Expr::ListComprehension {
            var,
            source: Box::new(substitute_aliases(*source, map)),
            predicate: predicate.map(|p| Box::new(substitute_aliases(*p, map))),
            projection: projection.map(|p| Box::new(substitute_aliases(*p, map))),
        },
        other => other,
    }
}
