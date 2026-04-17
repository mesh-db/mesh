use crate::ast::{
    CallArgs, CreateStmt, Direction, Expr, IndexDdl, Literal, MatchStmt, NodePattern, Pattern,
    ReturnItem, ReturnStmt, ShortestKind, SortItem, Statement, UnionStmt, UnwindStmt,
};
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VarType {
    Node,
    Edge,
    Path,
    Scalar,
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
        edge_types: Vec<String>,
        direction: Direction,
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
    },
    VarLengthExpand {
        input: Box<LogicalPlan>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_labels: Vec<String>,
        edge_types: Vec<String>,
        direction: Direction,
        min_hops: u64,
        max_hops: u64,
        path_var: Option<String>,
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
        vars: Vec<String>,
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
        | LogicalPlan::LoadCsv {
            input: Some(input), ..
        }
        | LogicalPlan::BindPath { input, .. }
        | LogicalPlan::ShortestPath { input, .. }
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
        | Statement::ShowIndexes => Err(Error::Plan(
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
        Expr::Call { name, .. } => format!("{name}(...)"),
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
    let start_idx = add_create_node(nodes, var_idx, bound_vars, &pattern.start);
    let mut prev_idx = start_idx;

    for hop in &pattern.hops {
        let target_idx = add_create_node(nodes, var_idx, bound_vars, &hop.target);

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
) -> usize {
    if let Some(name) = &pattern.var {
        if let Some(&idx) = var_idx.get(name) {
            return idx;
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
    idx
}

fn plan_pattern(
    pattern: &Pattern,
    pattern_idx: usize,
    ctx: &PlannerContext,
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

    let plan = chain_hops(plan, &working, &start_var, pattern_idx)?;
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
fn plan_pattern_from_bound(
    input: LogicalPlan,
    pattern: &Pattern,
    pattern_idx: usize,
) -> Result<LogicalPlan> {
    debug_assert!(
        pattern.start.labels.is_empty() && pattern.start.properties.is_empty(),
        "plan_pattern_from_bound requires a pure-reference start; \
         the caller must validate this before dispatching"
    );
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
    let plan = chain_hops(input, &working, &start_var, pattern_idx)?;
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
fn chain_hops(
    mut plan: LogicalPlan,
    pattern: &Pattern,
    start_var: &str,
    pattern_idx: usize,
) -> Result<LogicalPlan> {
    let mut current_var = start_var.to_string();
    for (i, hop) in pattern.hops.iter().enumerate() {
        let dst_var = hop
            .target
            .var
            .clone()
            .unwrap_or_else(|| format!("__p{}_a{}", pattern_idx, i + 1));
        plan = if let Some(vl) = hop.rel.var_length {
            if vl.min > vl.max {
                return Err(Error::Plan(format!(
                    "variable-length path min ({}) > max ({})",
                    vl.min, vl.max
                )));
            }
            LogicalPlan::VarLengthExpand {
                input: Box::new(plan),
                src_var: current_var.clone(),
                edge_var: hop.rel.var.clone(),
                dst_var: dst_var.clone(),
                dst_labels: hop.target.labels.clone(),
                edge_types: hop.rel.edge_types.clone(),
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
            }
        } else {
            LogicalPlan::EdgeExpand {
                input: Box::new(plan),
                src_var: current_var.clone(),
                edge_var: hop.rel.var.clone(),
                dst_var: dst_var.clone(),
                dst_labels: hop.target.labels.clone(),
                edge_types: hop.rel.edge_types.clone(),
                direction: hop.rel.direction,
            }
        };
        // Lower the target node's pattern properties to a Filter
        // wrapping the expand for the same reason as the start node.
        plan = wrap_with_pattern_prop_filter(plan, &dst_var, &hop.target.properties);
        current_var = dst_var;
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
        check_var_type_conflict(var, VarType::Node, out)?;
        out.insert(var.clone(), VarType::Node);
    }
    if let Some(pv) = &pattern.path_var {
        check_var_type_conflict(pv, VarType::Path, out)?;
        out.insert(pv.clone(), VarType::Path);
    }
    for hop in &pattern.hops {
        if let Some(var) = &hop.rel.var {
            check_var_type_conflict(var, VarType::Edge, out)?;
            out.insert(var.clone(), VarType::Edge);
        }
        if let Some(var) = &hop.target.var {
            check_var_type_conflict(var, VarType::Node, out)?;
            out.insert(var.clone(), VarType::Node);
        }
    }
    Ok(())
}

fn infer_expr_type(expr: &Expr, bound_vars: &HashMap<String, VarType>) -> VarType {
    match expr {
        Expr::Identifier(name) => bound_vars.get(name).copied().unwrap_or(VarType::Node),
        Expr::Literal(_) | Expr::Parameter(_) => VarType::Scalar,
        Expr::Call { .. } | Expr::BinaryOp { .. } | Expr::UnaryOp { .. } => VarType::Scalar,
        _ => VarType::Node,
    }
}

fn check_var_type_conflict(
    var: &str,
    new_type: VarType,
    bound_vars: &HashMap<String, VarType>,
) -> Result<()> {
    if let Some(&existing) = bound_vars.get(var) {
        if existing != new_type {
            return Err(Error::Plan(format!(
                "variable '{}' already defined with a different type",
                var
            )));
        }
    }
    Ok(())
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
                    let start_is_pure_reference =
                        pattern.start.labels.is_empty() && pattern.start.properties.is_empty();

                    let mut this_pattern_vars: HashSet<String> = HashSet::new();
                    collect_pattern_vars(pattern, &mut this_pattern_vars);
                    for var in &this_pattern_vars {
                        let is_bound_start =
                            start_var_name == Some(var.as_str()) && bound_vars.contains_key(var);
                        if is_bound_start && !start_is_pure_reference {
                            return Err(Error::Plan(format!(
                                "re-referencing already-bound variable '{}' in a \
                                 chained MATCH requires a pure-reference start node \
                                 (no labels, no properties); \
                                 move the label / property assertion into a WHERE clause",
                                var
                            )));
                        }
                        // Variables from earlier clauses can be
                        // re-referenced if the type matches. Only
                        // reject actual type conflicts.
                        if let Some(&existing_type) = bound_vars.get(var) {
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
                            if existing_type != new_type {
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
                        Some(plan_pattern_from_bound(current, pattern, pattern_offset)?)
                    } else {
                        let rhs = plan_pattern(pattern, pattern_offset, ctx)?;
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
                // Register the WITH's projected aliases so a
                // downstream MATCH can use them as rebind
                // references (e.g. CALL { WITH a MATCH (a)-[...]-> }).
                for item in &w.items {
                    let alias = return_item_column_name(item);
                    let vtype = infer_expr_type(&item.expr, &bound_vars);
                    bound_vars.insert(alias, vtype);
                }
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
                    if bound_vars.contains_key(&var) {
                        return Err(Error::Plan(format!(
                            "variable '{}' is already bound; MERGE cannot rebind \
                             an existing variable in a chained clause",
                            var
                        )));
                    }
                    plan = Some(LogicalPlan::MergeNode {
                        input: plan.take().map(Box::new),
                        var: var.clone(),
                        labels: mc.pattern.start.labels.clone(),
                        properties: mc.pattern.start.properties.clone(),
                        on_create,
                        on_match,
                    });
                    bound_vars.insert(var, VarType::Node);
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

                    // Phase 3: MergeEdge for each hop.
                    let mut current_src = start_var;
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
                            on_create: on_create.clone(),
                            on_match: on_match.clone(),
                        });
                        bound_vars.insert(edge_var, VarType::Node);
                        bound_vars.insert(dst_var.clone(), VarType::Node);
                        current_src = dst_var;
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
                plan = Some(LogicalPlan::CreatePath {
                    input: plan.take().map(Box::new),
                    nodes,
                    edges,
                });
            }
            ReadingClause::Set(items) => {
                let assignments = items
                    .iter()
                    .map(set_item_to_assignment)
                    .collect();
                let current = plan.take().ok_or_else(|| {
                    Error::Plan("SET requires a preceding clause".into())
                })?;
                plan = Some(LogicalPlan::SetProperty {
                    input: Box::new(current),
                    assignments,
                });
            }
            ReadingClause::Delete(dc) => {
                let current = plan.take().ok_or_else(|| {
                    Error::Plan("DELETE requires a preceding clause".into())
                })?;
                plan = Some(LogicalPlan::Delete {
                    input: Box::new(current),
                    detach: dc.detach,
                    vars: dc.vars.clone(),
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
        };
    }
    if !terminal.set_items.is_empty() {
        let assignments = terminal
            .set_items
            .iter()
            .map(set_item_to_assignment)
            .collect();
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

    if terminal.star || !terminal.return_items.is_empty() {
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
    if star {
        plan = LogicalPlan::Identity {
            input: Box::new(plan),
        };
    } else {
        let (group_keys, aggregates, post_items) = classify_return_items(return_items)?;

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
                items: return_items.to_vec(),
            }
        };
    }

    if distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    if !order_by.is_empty() {
        plan = LogicalPlan::OrderBy {
            input: Box::new(plan),
            sort_items: order_by.to_vec(),
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
    for pattern in &clause.patterns {
        if pattern.hops.is_empty() {
            // Bare-node OPTIONAL MATCH: scan for matching nodes.
            // If none match, the caller produces a null-bound row.
            let fresh = plan_pattern(pattern, 0, ctx)?;
            collect_pattern_vars_typed(pattern, bound_vars)?;
            plan = LogicalPlan::CartesianProduct {
                left: Box::new(plan),
                right: Box::new(fresh),
            };
            continue;
        }
        let start_var = pattern
            .start
            .var
            .clone()
            .unwrap_or_else(|| format!("__opt_start_{}", bound_vars.len()));
        if !bound_vars.contains_key(&start_var) {
            // Standalone OPTIONAL MATCH — the start var is not yet
            // bound. Plan it like a regular MATCH: fresh scan + hops.
            // The "optional" semantics are handled by the caller
            // (OPTIONAL MATCH as first clause returns NULL rows if
            // nothing matches, but that's the same as an empty result
            // set which the match already produces).
            let fresh = plan_pattern(pattern, 0, ctx)?;
            collect_pattern_vars_typed(pattern, bound_vars)?;
            plan = fresh;
            continue;
        }

        let mut current_var = start_var;

        for (i, hop) in pattern.hops.iter().enumerate() {
            let dst_var = hop
                .target
                .var
                .clone()
                .unwrap_or_else(|| format!("__opt_dst_{}_{}", bound_vars.len(), i));

            if let Some(vl) = hop.rel.var_length {
                if vl.min > vl.max {
                    return Err(Error::Plan(format!(
                        "variable-length path min ({}) > max ({})",
                        vl.min, vl.max
                    )));
                }
                plan = LogicalPlan::VarLengthExpand {
                    input: Box::new(plan),
                    src_var: current_var.clone(),
                    edge_var: hop.rel.var.clone(),
                    dst_var: dst_var.clone(),
                    dst_labels: hop.target.labels.clone(),
                    edge_types: hop.rel.edge_types.clone(),
                    direction: hop.rel.direction,
                    min_hops: vl.min,
                    max_hops: vl.max,
                    path_var: None,
                };
            } else {
                plan = LogicalPlan::OptionalEdgeExpand {
                    input: Box::new(plan),
                    src_var: current_var.clone(),
                    edge_var: hop.rel.var.clone(),
                    dst_var: dst_var.clone(),
                    dst_labels: hop.target.labels.clone(),
                    dst_properties: hop.target.properties.clone(),
                    edge_types: hop.rel.edge_types.clone(),
                    direction: hop.rel.direction,
                };
            }

            bound_vars.insert(dst_var.clone(), VarType::Node);
            if let Some(ev) = &hop.rel.var {
                bound_vars.insert(ev.clone(), VarType::Edge);
            }
            current_var = dst_var;
        }
    }

    if let Some(predicate) = &clause.where_clause {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
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

    // Post-projection WHERE — references the WITH's aliases,
    // not the pattern's bindings.
    if let Some(predicate) = &w.where_clause {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    if !w.order_by.is_empty() {
        plan = LogicalPlan::OrderBy {
            input: Box::new(plan),
            sort_items: w.order_by.clone(),
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
            let agg_arg = match args {
                CallArgs::Star => {
                    if !matches!(func, AggregateFn::Count) {
                        return Err(Error::Plan("only count(*) accepts a star argument".into()));
                    }
                    AggregateArg::Star
                }
                CallArgs::Exprs(es) if es.len() == 1 => AggregateArg::Expr(es[0].clone()),
                // percentileDisc/percentileCont take 2 args (expr, percentile)
                // We use only the first arg for aggregation; the second is a constant
                CallArgs::Exprs(es)
                    if es.len() == 2
                        && matches!(
                            func,
                            AggregateFn::PercentileDisc | AggregateFn::PercentileCont
                        ) =>
                {
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
                .unwrap_or_else(|| format!("{}_{}", name.to_lowercase(), idx));
            aggregates.push(AggregateSpec {
                alias,
                function: func,
                arg: agg_arg,
            });
        } else {
            if contains_aggregate(&item.expr) {
                let mut rewrites: Vec<(String, AggregateFn, AggregateArg)> = Vec::new();
                let rewritten =
                    extract_nested_aggregates(&item.expr, &mut rewrites, &mut synth_idx);
                for (alias, func, arg) in rewrites {
                    aggregates.push(AggregateSpec {
                        alias,
                        function: func,
                        arg,
                    });
                }
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("__nested_agg_{}", idx));
                post_items.push(ReturnItem {
                    expr: rewritten,
                    alias: Some(alias),
                });
            } else {
                group_keys.push(item.clone());
            }
        }
    }
    Ok((group_keys, aggregates, post_items))
}

fn extract_nested_aggregates(
    expr: &Expr,
    out: &mut Vec<(String, AggregateFn, AggregateArg)>,
    idx: &mut usize,
) -> Expr {
    match expr {
        Expr::Call { name, args } if aggregate_fn_from_name(name).is_some() => {
            let func = aggregate_fn_from_name(name).unwrap();
            let agg_arg = match args {
                CallArgs::Star => AggregateArg::Star,
                CallArgs::Exprs(es) if es.len() == 1 => AggregateArg::Expr(es[0].clone()),
                CallArgs::Exprs(es) if es.is_empty() => AggregateArg::Star,
                CallArgs::DistinctExprs(es) if es.len() == 1 => {
                    AggregateArg::DistinctExpr(es[0].clone())
                }
                _ => AggregateArg::Star,
            };
            let alias = format!("__agg_{}_{}", name.to_lowercase(), *idx);
            *idx += 1;
            out.push((alias.clone(), func, agg_arg));
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
        other => other.clone(),
    }
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
        _ => false,
    }
}
