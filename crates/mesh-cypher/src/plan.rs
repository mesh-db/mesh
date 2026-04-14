use crate::ast::{
    CallArgs, CreateStmt, Direction, Expr, Literal, MatchStmt, MergeStmt, NodePattern, Pattern,
    ReturnItem, SortItem, Statement, UnwindStmt,
};
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};

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
    MergeNode {
        var: String,
        labels: Vec<String>,
        properties: Vec<(String, Literal)>,
    },
    /// Evaluate `expr` once, cast it to a list, and emit one row per element
    /// binding the element to `var`. The expression is evaluated against an
    /// empty row — UNWIND is a top-level producer, not yet chained after MATCH.
    Unwind {
        var: String,
        expr: Expr,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateNodeSpec {
    New {
        var: Option<String>,
        labels: Vec<String>,
        properties: Vec<(String, Literal)>,
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
        properties: Vec<(String, Literal)>,
    },
    Merge {
        var: String,
        properties: Vec<(String, Literal)>,
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

pub fn plan(statement: &Statement) -> Result<LogicalPlan> {
    match statement {
        Statement::Create(c) => plan_create(c),
        Statement::Match(m) => plan_match(m),
        Statement::Merge(m) => plan_merge(m),
        Statement::Unwind(u) => plan_unwind(u),
    }
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
    let plan = LogicalPlan::MergeNode {
        var,
        labels: stmt.pattern.labels.clone(),
        properties: stmt.pattern.properties.clone(),
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

fn plan_create(stmt: &CreateStmt) -> Result<LogicalPlan> {
    let mut nodes: Vec<CreateNodeSpec> = Vec::new();
    let mut edges: Vec<CreateEdgeSpec> = Vec::new();
    let mut var_idx: HashMap<String, usize> = HashMap::new();
    let no_bindings: HashSet<String> = HashSet::new();

    for pattern in &stmt.patterns {
        build_create_pattern(
            pattern,
            &mut nodes,
            &mut edges,
            &mut var_idx,
            &no_bindings,
        )?;
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

fn plan_pattern(pattern: &Pattern, pattern_idx: usize) -> Result<LogicalPlan> {
    let start_var = pattern
        .start
        .var
        .clone()
        .unwrap_or_else(|| format!("__p{}_a0", pattern_idx));

    let mut plan = if pattern.start.labels.is_empty() {
        LogicalPlan::NodeScanAll {
            var: start_var.clone(),
        }
    } else {
        LogicalPlan::NodeScanByLabels {
            var: start_var.clone(),
            labels: pattern.start.labels.clone(),
        }
    };

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
        current_var = dst_var;
    }
    Ok(plan)
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

fn plan_match(stmt: &MatchStmt) -> Result<LogicalPlan> {
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
        plans.push(plan_pattern(pattern, i)?);
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

    // Apply at most one mutation to the pipeline (they are grammatically exclusive).
    if let Some(delete_clause) = &stmt.delete {
        plan = LogicalPlan::Delete {
            input: Box::new(plan),
            detach: delete_clause.detach,
            vars: delete_clause.vars.clone(),
        };
    } else if !stmt.set_items.is_empty() {
        let assignments = stmt
            .set_items
            .iter()
            .map(|s| match s {
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
            })
            .collect();
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

    let has_mutation = stmt.delete.is_some()
        || !stmt.set_items.is_empty()
        || !stmt.create_patterns.is_empty();

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

fn classify_return_items(
    items: &[ReturnItem],
) -> Result<(Vec<ReturnItem>, Vec<AggregateSpec>)> {
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
                        return Err(Error::Plan(
                            "only count(*) accepts a star argument".into(),
                        ));
                    }
                    AggregateArg::Star
                }
                CallArgs::Exprs(es) if es.len() == 1 => AggregateArg::Expr(es[0].clone()),
                CallArgs::Exprs(_) => {
                    return Err(Error::Plan(format!(
                        "{} takes exactly one argument",
                        name
                    )))
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
        Expr::Compare { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::Call { args: CallArgs::Exprs(es), .. }
        | Expr::Call { args: CallArgs::DistinctExprs(es), .. } => {
            es.iter().any(contains_aggregate)
        }
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
