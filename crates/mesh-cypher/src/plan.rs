use crate::ast::{
    BinaryOp, CallArgs, CompareOp, CreateStmt, Direction, Expr, IndexDdl, Literal, MatchStmt,
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
    /// `(label, property)` pairs corresponding to active property
    /// indexes in the backing store. Stored as a flat Vec rather than
    /// a HashMap because N is small (single digits in practice) and
    /// iterating a Vec is faster than hashing for that size.
    pub indexes: Vec<(String, String)>,
}

impl PlannerContext {
    pub fn has_index(&self, label: &str, property: &str) -> bool {
        self.indexes
            .iter()
            .any(|(l, p)| l == label && p == property)
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
    /// Equality lookup through a property index. The executor evaluates
    /// `value` (which may be a literal or a parameter) at run time,
    /// converts it to a concrete [`mesh_core::Property`], then calls
    /// `reader.nodes_by_property(label, property, value)`. Emitted
    /// only when the planner context confirms the `(label, property)`
    /// index exists; otherwise the planner falls back to a
    /// `NodeScanByLabels` + `Filter`.
    IndexSeek {
        var: String,
        label: String,
        property: String,
        value: Expr,
    },
    /// Schema DDL — declare a new property index. Has no input and
    /// produces no rows. The executor short-circuits this before
    /// constructing the operator pipeline, dispatching to
    /// `Store::create_property_index`.
    CreatePropertyIndex {
        label: String,
        property: String,
    },
    /// Schema DDL — tear down a property index. Same dispatch
    /// pattern as [`LogicalPlan::CreatePropertyIndex`].
    DropPropertyIndex {
        label: String,
        property: String,
    },
    /// Schema DDL — emit one row per registered property index
    /// describing `(label, property)`. The executor builds the rows
    /// from `Store::list_property_indexes`.
    ShowPropertyIndexes,
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
        Statement::CreateIndex(IndexDdl { label, property }) => LogicalPlan::CreatePropertyIndex {
            label: label.clone(),
            property: property.clone(),
        },
        Statement::DropIndex(IndexDdl { label, property }) => LogicalPlan::DropPropertyIndex {
            label: label.clone(),
            property: property.clone(),
        },
        Statement::ShowIndexes => LogicalPlan::ShowPropertyIndexes,
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
        LogicalPlan::OptionalApply { input, body, null_vars } => {
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
            property,
            ..
        } => {
            buf.push_str(&format!("{indent}IndexSeek({var}:{label}.{property})\n"));
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
        LogicalPlan::CreatePropertyIndex { label, property } => {
            buf.push_str(&format!(
                "{indent}CreatePropertyIndex({label}.{property})\n"
            ));
        }
        LogicalPlan::DropPropertyIndex { label, property } => {
            buf.push_str(&format!("{indent}DropPropertyIndex({label}.{property})\n"));
        }
        LogicalPlan::ShowPropertyIndexes => {
            buf.push_str(&format!("{indent}ShowPropertyIndexes\n"));
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
            // ExistsSubquery/CountSubquery bodies are validated
            // when planned at execution time.
            Ok(())
        })
    })
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

fn check_pattern_vars_bound(
    pattern: &Pattern,
    bound: &HashMap<String, VarType>,
) -> Result<()> {
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
fn reject_non_boolean_where_predicate(
    expr: &Expr,
    bound: &HashMap<String, VarType>,
) -> Result<()> {
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
        LogicalPlan::IndexSeek { value, .. } => visit(value),
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
        | LogicalPlan::ShowPropertyIndexes => Ok(()),
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
        Expr::ExistsSubquery { .. } | Expr::CountSubquery { .. } => Ok(()),
        // Pattern comprehension: the pattern itself carries no
        // sub-expressions the walker cares about (same rule as
        // `PatternExists`), but the WHERE / projection are plain
        // expressions and need to be visited so any pattern
        // predicates, parameter uses, etc. inside them are
        // validated.
        Expr::PatternComprehension { predicate, projection, .. } => {
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
        | Statement::CallProcedure(_) => Err(Error::Plan(
            "UNION branches must be read queries (MATCH / UNWIND / RETURN); \
             DDL and CREATE-only statements are not allowed"
                .into(),
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
                        es.iter().map(render_expr_key).collect::<Vec<_>>().join(", ")
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
            format!("{}{op_str}{}", render_expr_key(left), render_expr_key(right))
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
            format!("{}{op_str}{}", render_expr_key(left), render_expr_key(right))
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
        Expr::PatternExists(_) | Expr::ExistsSubquery { .. } | Expr::CountSubquery { .. } => {
            Ok(())
        }
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
            SetAssignment::Replace { properties, .. }
            | SetAssignment::Merge { properties, .. } => {
                for (_, e) in properties {
                    reject_pattern_predicate_in_projection(e, "SET right-hand side")?;
                    check_set_expr_scope(e, bound)?;
                }
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
            return Err(Error::Plan(
                "RETURN * has no variables in scope".into(),
            ));
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
    plan_pattern_with_bound_edges(
        pattern,
        pattern_idx,
        ctx,
        &HashSet::new(),
        &HashSet::new(),
    )
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
                return Err(Error::Plan(format!(
                    "variable-length path min ({}) > max ({})",
                    vl.min, vl.max
                )));
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
/// exactly one label and at least one pattern property covered by a
/// registered index, emit [`LogicalPlan::IndexSeek`] for that property
/// and return the rest as a residual filter. Otherwise fall back to
/// the existing `NodeScanAll` / `NodeScanByLabels` path with all
/// pattern properties as residuals.
///
/// Only the *first* covered property is picked — a node pattern with
/// two indexed properties uses the first one as the seek key and the
/// second as a residual filter. Richer multi-predicate seek costing
/// can replace this later without changing the IndexSeek variant.
fn plan_start_node(
    var: &str,
    labels: &[String],
    properties: &[(String, Expr)],
    ctx: &PlannerContext,
) -> (LogicalPlan, Vec<(String, Expr)>) {
    if labels.len() == 1 && !properties.is_empty() {
        let label = &labels[0];
        if let Some(seek_idx) = properties.iter().position(|(k, _)| ctx.has_index(label, k)) {
            let (key, value_expr) = &properties[seek_idx];
            let seek = LogicalPlan::IndexSeek {
                var: var.to_string(),
                label: label.clone(),
                property: key.clone(),
                value: value_expr.clone(),
            };
            let residual: Vec<(String, Expr)> = properties
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != seek_idx)
                .map(|(_, kv)| kv.clone())
                .collect();
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

    let seek_idx = conjuncts
        .iter()
        .position(|c| extract_indexed_eq(c, &scan_var, &scan_label, ctx).is_some());
    let Some(seek_idx) = seek_idx else {
        return rebuild_filter_chain(current, conjuncts);
    };
    let (seek_key, seek_value) =
        extract_indexed_eq(&conjuncts[seek_idx], &scan_var, &scan_label, ctx)
            .expect("position above just confirmed match");

    let seek = LogicalPlan::IndexSeek {
        var: scan_var,
        label: scan_label,
        property: seek_key,
        value: seek_value,
    };
    let residual: Vec<Expr> = conjuncts
        .into_iter()
        .enumerate()
        .filter(|(i, _)| *i != seek_idx)
        .map(|(_, c)| c)
        .collect();
    rebuild_filter_chain(seek, residual)
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
fn extract_indexed_eq(
    c: &Expr,
    scan_var: &str,
    scan_label: &str,
    ctx: &PlannerContext,
) -> Option<(String, Expr)> {
    use crate::ast::CompareOp;
    let Expr::Compare { op, left, right } = c else {
        return None;
    };
    if *op != CompareOp::Eq {
        return None;
    }
    if let Some((key, value)) = match_property_eq(left, right, scan_var) {
        if ctx.has_index(scan_label, &key) && expr_is_row_independent(&value) {
            return Some((key, value));
        }
    }
    if let Some((key, value)) = match_property_eq(right, left, scan_var) {
        if ctx.has_index(scan_label, &key) && expr_is_row_independent(&value) {
            return Some((key, value));
        }
    }
    None
}

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
            if hop.rel.var_length.is_some()
                && matches!(out.get(var), Some(VarType::NonNode))
            {
                continue;
            }
            check_var_type_conflict_allow_scalar(var, VarType::Edge, out)?;
            out.insert(var.clone(), VarType::Edge);
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
fn validate_delete_expr(
    expr: &Expr,
    bound_vars: &HashMap<String, VarType>,
) -> Result<()> {
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
                        Some(plan_pattern_from_bound(current, pattern, pattern_offset, &bound_vars)?)
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
                        for (k, v) in &bound_vars {
                            match v {
                                VarType::Edge => {
                                    outer_edges.insert(k.clone());
                                }
                                VarType::NonNode => {
                                    outer_edge_lists.insert(k.clone());
                                }
                                _ => {}
                            }
                        }
                        let rhs = plan_pattern_with_bound_edges(
                            pattern,
                            pattern_offset,
                            ctx,
                            &outer_edges,
                            &outer_edge_lists,
                        )?;
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
                plan = Some(optimize_filter_chain_to_index_seek(current, ctx));
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
                        let (merge_src, merge_dst) = if matches!(hop.rel.direction, Direction::Incoming) {
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
                                "aggregate functions are not allowed in CALL arguments"
                                    .into(),
                            ));
                        }
                    }
                }
                match &pc.yield_spec {
                    Some(crate::ast::YieldSpec::Items(items)) => {
                        for yi in items {
                            let bind_name =
                                yi.alias.as_ref().unwrap_or(&yi.column).clone();
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
                                && (!hop.target.labels.is_empty()
                                    || hop.target.has_property_clause)
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
                    build_create_pattern(pattern, &mut nodes, &mut edges, &mut var_idx, &bound_vars)?;
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
                let assignments: Vec<SetAssignment> = items
                    .iter()
                    .map(set_item_to_assignment)
                    .collect();
                check_set_assignments_scope(&assignments, &bound_vars)?;
                let current = plan.take().ok_or_else(|| {
                    Error::Plan("SET requires a preceding clause".into())
                })?;
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
                let current = plan.take().ok_or_else(|| {
                    Error::Plan("DELETE requires a preceding clause".into())
                })?;
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
                let current = plan.take().ok_or_else(|| {
                    Error::Plan("REMOVE requires a preceding clause".into())
                })?;
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
                let current = plan.take().ok_or_else(|| {
                    Error::Plan("FOREACH requires a preceding clause".into())
                })?;
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
            return Err(Error::Plan(
                "RETURN * has no variables in scope".into(),
            ));
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
            .map(|i| {
                i.alias
                    .clone()
                    .unwrap_or_else(|| render_expr_key(&i.expr))
            })
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
            let mut body = plan_pattern_from_bound(
                body_seed,
                &working,
                0,
                bound_vars,
            )?;
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
                let vl_path_var = if working.path_var.is_some()
                    && working.hops.len() == 1
                {
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

    let has_aggregates = !w.star
        && w.items.iter().any(|it| contains_aggregate(&it.expr));

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
        (Some(pred), false, false) => {
            (Some(substitute_aliases(pred.clone(), &alias_map)), None)
        }
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
    let mut group_keys: Vec<ReturnItem> = Vec::new();
    let mut aggregates: Vec<AggregateSpec> = Vec::new();
    let mut post_items: Vec<ReturnItem> = Vec::new();
    let mut synth_idx = 0usize;
    for (idx, item) in items.iter().enumerate() {
        let is_top_aggregate = matches!(
            &item.expr,
            Expr::Call { name, .. } if aggregate_fn_from_name(name).is_some()
        );
        if is_top_aggregate {
            let Expr::Call { name, args } = &item.expr else {
                unreachable!()
            };
            let func = aggregate_fn_from_name(name).unwrap();
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
                            AggregateFn::PercentileDisc | AggregateFn::PercentileCont
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
            let alias = item
                .alias
                .clone()
                .unwrap_or_else(|| render_expr_key(&item.expr));
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
                            AggregateFn::PercentileDisc | AggregateFn::PercentileCont
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
fn is_known_scalar_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "size"
            | "length"
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
            | "tolower"
            | "toupper"
            | "tostring"
            | "tointeger"
            | "toboolean"
            | "tofloat"
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
            | "rand"
            | "date"
            | "datetime"
            | "localdatetime"
            | "time"
            | "localtime"
            | "timestamp"
            | "duration"
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
                            if matches!(
                                bound.get(n),
                                Some(VarType::Node | VarType::Edge)
                            ) {
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
fn reject_property_access_on_path(
    expr: &Expr,
    bound: &HashMap<String, VarType>,
) -> Result<()> {
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
        Expr::ListPredicate { list, predicate, .. } => {
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
        Expr::ListPredicate { list, predicate, .. } => {
            contains_aggregate(list) || contains_aggregate(predicate)
        }
        Expr::Reduce {
            acc_init,
            source,
            body,
            ..
        } => {
            contains_aggregate(acc_init)
                || contains_aggregate(source)
                || contains_aggregate(body)
        }
        Expr::InList { element, list } => {
            contains_aggregate(element) || contains_aggregate(list)
        }
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
fn substitute_aliases(
    expr: Expr,
    map: &std::collections::HashMap<String, Expr>,
) -> Expr {
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
                CallArgs::Exprs(es) => CallArgs::Exprs(
                    es.into_iter().map(|e| substitute_aliases(e, map)).collect(),
                ),
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
            items.into_iter().map(|e| substitute_aliases(e, map)).collect(),
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
