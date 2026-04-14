use crate::ast::{
    CallArgs, CreateStmt, Direction, Expr, IndexDdl, Literal, MatchStmt, MergeStmt, NodePattern,
    Pattern, ReturnItem, ReturnStmt, SortItem, Statement, UnwindStmt,
};
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};

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
        edge_type: Option<String>,
        direction: Direction,
    },
    VarLengthExpand {
        input: Box<LogicalPlan>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_labels: Vec<String>,
        edge_type: Option<String>,
        direction: Direction,
        min_hops: u64,
        max_hops: u64,
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
        count: i64,
    },
    Limit {
        input: Box<LogicalPlan>,
        count: i64,
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
    /// Match-or-create a single node. If at least one node matches the
    /// `(labels, properties)` pattern, returns one row per match (binding
    /// the existing node to `var`). If none match, creates exactly one
    /// node with the given labels + properties and returns one row.
    /// Property values are `Expr` so they can carry parameters; the
    /// executor evaluates them once at the start of execution.
    MergeNode {
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
    /// Evaluate `expr` once, cast it to a list, and emit one row per element
    /// binding the element to `var`. The expression is evaluated against an
    /// empty row — UNWIND is a top-level producer, not yet chained after MATCH.
    Unwind {
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
    match statement {
        Statement::Create(c) => plan_create(c),
        Statement::Match(m) => plan_match(m, ctx),
        Statement::Merge(m) => plan_merge(m),
        Statement::Unwind(u) => plan_unwind(u),
        Statement::Return(r) => plan_return_only(r),
        Statement::CreateIndex(IndexDdl { label, property }) => {
            Ok(LogicalPlan::CreatePropertyIndex {
                label: label.clone(),
                property: property.clone(),
            })
        }
        Statement::DropIndex(IndexDdl { label, property }) => Ok(LogicalPlan::DropPropertyIndex {
            label: label.clone(),
            property: property.clone(),
        }),
        Statement::ShowIndexes => Ok(LogicalPlan::ShowPropertyIndexes),
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
        stmt.distinct,
        &stmt.order_by,
        stmt.skip,
        stmt.limit,
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
        stmt.distinct,
        &stmt.order_by,
        stmt.skip,
        stmt.limit,
    )
}

fn plan_merge(stmt: &MergeStmt) -> Result<LogicalPlan> {
    // Auto-name the binding when the user wrote `MERGE (:Label {...})` — we
    // still need *some* var so RETURN can refer to it via the auto-name.
    let var = stmt
        .pattern
        .var
        .clone()
        .unwrap_or_else(|| "__merge0".to_string());
    let on_create: Vec<SetAssignment> = stmt.on_create.iter().map(set_item_to_assignment).collect();
    let on_match: Vec<SetAssignment> = stmt.on_match.iter().map(set_item_to_assignment).collect();
    let plan = LogicalPlan::MergeNode {
        var,
        labels: stmt.pattern.labels.clone(),
        properties: stmt.pattern.properties.clone(),
        on_create,
        on_match,
    };

    if stmt.return_items.is_empty() {
        Ok(plan)
    } else {
        apply_return_pipeline(
            plan,
            &stmt.return_items,
            stmt.distinct,
            &stmt.order_by,
            stmt.skip,
            stmt.limit,
        )
    }
}

/// Lower an AST `SetItem` to an executor-side `SetAssignment`.
/// Pulled out of `plan_match` so `plan_merge` can reuse the same
/// mapping for `ON CREATE SET` / `ON MATCH SET` actions.
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
    let no_bindings: HashSet<String> = HashSet::new();

    for pattern in &stmt.patterns {
        build_create_pattern(pattern, &mut nodes, &mut edges, &mut var_idx, &no_bindings)?;
    }

    let plan = LogicalPlan::CreatePath {
        input: None,
        nodes,
        edges,
    };

    if stmt.return_items.is_empty() {
        Ok(plan)
    } else {
        apply_return_pipeline(
            plan,
            &stmt.return_items,
            stmt.distinct,
            &stmt.order_by,
            stmt.skip,
            stmt.limit,
        )
    }
}

fn build_create_pattern(
    pattern: &Pattern,
    nodes: &mut Vec<CreateNodeSpec>,
    edges: &mut Vec<CreateEdgeSpec>,
    var_idx: &mut HashMap<String, usize>,
    bound_vars: &HashSet<String>,
) -> Result<()> {
    let start_idx = add_create_node(nodes, var_idx, bound_vars, &pattern.start);
    let mut prev_idx = start_idx;

    for hop in &pattern.hops {
        let target_idx = add_create_node(nodes, var_idx, bound_vars, &hop.target);

        let edge_type = hop.rel.edge_type.clone().ok_or_else(|| {
            Error::Plan("CREATE relationship must specify a type (e.g. [:KNOWS])".into())
        })?;

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
        });
        prev_idx = target_idx;
    }
    Ok(())
}

fn add_create_node(
    nodes: &mut Vec<CreateNodeSpec>,
    var_idx: &mut HashMap<String, usize>,
    bound_vars: &HashSet<String>,
    pattern: &NodePattern,
) -> usize {
    if let Some(name) = &pattern.var {
        if let Some(&idx) = var_idx.get(name) {
            return idx;
        }
    }
    let idx = nodes.len();
    let spec = match &pattern.var {
        Some(name) if bound_vars.contains(name) => CreateNodeSpec::Reference(name.clone()),
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
    let start_var = pattern
        .start
        .var
        .clone()
        .unwrap_or_else(|| format!("__p{}_a0", pattern_idx));

    let (mut plan, remaining_props) = plan_start_node(
        &start_var,
        &pattern.start.labels,
        &pattern.start.properties,
        ctx,
    );
    // Any pattern properties not consumed by an IndexSeek still need a
    // filter so the executor actually enforces them.
    plan = wrap_with_pattern_prop_filter(plan, &start_var, &remaining_props);

    let mut current_var = start_var;
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
                edge_type: hop.rel.edge_type.clone(),
                direction: hop.rel.direction,
                min_hops: vl.min,
                max_hops: vl.max,
            }
        } else {
            LogicalPlan::EdgeExpand {
                input: Box::new(plan),
                src_var: current_var.clone(),
                edge_var: hop.rel.var.clone(),
                dst_var: dst_var.clone(),
                dst_labels: hop.target.labels.clone(),
                edge_type: hop.rel.edge_type.clone(),
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

fn plan_match(stmt: &MatchStmt, ctx: &PlannerContext) -> Result<LogicalPlan> {
    // Validate no shared variable names across the pattern list (not yet supported).
    let mut all_vars: HashSet<String> = HashSet::new();
    for pattern in &stmt.patterns {
        let mut this_vars: HashSet<String> = HashSet::new();
        collect_pattern_vars(pattern, &mut this_vars);
        for var in this_vars {
            if !all_vars.insert(var.clone()) {
                return Err(Error::Plan(format!(
                    "variable '{}' appears in multiple MATCH patterns; not yet supported",
                    var
                )));
            }
        }
    }

    let mut plans: Vec<LogicalPlan> = Vec::new();
    for (i, pattern) in stmt.patterns.iter().enumerate() {
        plans.push(plan_pattern(pattern, i, ctx)?);
    }
    let mut plan = plans.remove(0);
    for rhs in plans {
        plan = LogicalPlan::CartesianProduct {
            left: Box::new(plan),
            right: Box::new(rhs),
        };
    }

    if let Some(predicate) = &stmt.where_clause {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    // Index-aware WHERE rewrite: scan the Filter chain at the top
    // of the plan and try to extract an indexed equality conjunct
    // into an `IndexSeek` on the underlying `NodeScanByLabels`. The
    // pattern-property rewrite handled the `(n:L {prop: ...})` form
    // earlier; this pass handles the equivalent `WHERE n.prop = ...`
    // shape that drivers actually emit, plus the mixed form
    // `(n:L {nonindexed: ...}) WHERE n.indexed = ...`.
    plan = optimize_filter_chain_to_index_seek(plan, ctx);

    // Apply at most one mutation to the pipeline (they are grammatically exclusive).
    if let Some(delete_clause) = &stmt.delete {
        plan = LogicalPlan::Delete {
            input: Box::new(plan),
            detach: delete_clause.detach,
            vars: delete_clause.vars.clone(),
        };
    } else if !stmt.set_items.is_empty() {
        let assignments = stmt.set_items.iter().map(set_item_to_assignment).collect();
        plan = LogicalPlan::SetProperty {
            input: Box::new(plan),
            assignments,
        };
    } else if !stmt.create_patterns.is_empty() {
        let mut nodes: Vec<CreateNodeSpec> = Vec::new();
        let mut edges: Vec<CreateEdgeSpec> = Vec::new();
        let mut var_idx: HashMap<String, usize> = HashMap::new();
        for pattern in &stmt.create_patterns {
            build_create_pattern(pattern, &mut nodes, &mut edges, &mut var_idx, &all_vars)?;
        }
        plan = LogicalPlan::CreatePath {
            input: Some(Box::new(plan)),
            nodes,
            edges,
        };
    }

    let has_mutation =
        stmt.delete.is_some() || !stmt.set_items.is_empty() || !stmt.create_patterns.is_empty();

    if !stmt.return_items.is_empty() {
        plan = apply_return_pipeline(
            plan,
            &stmt.return_items,
            stmt.distinct,
            &stmt.order_by,
            stmt.skip,
            stmt.limit,
        )?;
    } else if !has_mutation {
        return Err(Error::Plan(
            "MATCH must be followed by RETURN, SET, DELETE, or CREATE".into(),
        ));
    }

    Ok(plan)
}

fn apply_return_pipeline(
    mut plan: LogicalPlan,
    return_items: &[ReturnItem],
    distinct: bool,
    order_by: &[SortItem],
    skip: Option<i64>,
    limit: Option<i64>,
) -> Result<LogicalPlan> {
    let (group_keys, aggregates) = classify_return_items(return_items)?;

    plan = if !aggregates.is_empty() {
        LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys,
            aggregates,
        }
    } else {
        LogicalPlan::Project {
            input: Box::new(plan),
            items: return_items.to_vec(),
        }
    };

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

fn classify_return_items(items: &[ReturnItem]) -> Result<(Vec<ReturnItem>, Vec<AggregateSpec>)> {
    let mut group_keys: Vec<ReturnItem> = Vec::new();
    let mut aggregates: Vec<AggregateSpec> = Vec::new();
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
                return Err(Error::Plan(
                    "aggregates must appear at the top of RETURN items".into(),
                ));
            }
            group_keys.push(item.clone());
        }
    }
    Ok((group_keys, aggregates))
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, .. } if aggregate_fn_from_name(name).is_some() => true,
        Expr::Not(inner) => contains_aggregate(inner),
        Expr::And(a, b) | Expr::Or(a, b) => contains_aggregate(a) || contains_aggregate(b),
        Expr::Compare { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
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
