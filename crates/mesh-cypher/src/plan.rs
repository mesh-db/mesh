use crate::ast::{
    CallArgs, CreateStmt, Direction, Expr, Literal, MatchStmt, NodePattern, Pattern, ReturnItem,
    SortItem, Statement,
};
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    NodeScanAll {
        var: String,
    },
    NodeScanByLabel {
        var: String,
        label: String,
    },
    EdgeExpand {
        input: Box<LogicalPlan>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_label: Option<String>,
        edge_type: Option<String>,
        direction: Direction,
    },
    VarLengthExpand {
        input: Box<LogicalPlan>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_label: Option<String>,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateNodeSpec {
    New {
        labels: Vec<String>,
        properties: Vec<(String, Literal)>,
    },
    Reference(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateEdgeSpec {
    pub edge_type: String,
    pub src_idx: usize,
    pub dst_idx: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetAssignment {
    pub var: String,
    pub key: String,
    pub value: Expr,
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

    Ok(LogicalPlan::CreatePath {
        input: None,
        nodes,
        edges,
    })
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
            labels: pattern.label.clone().into_iter().collect(),
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

    let mut plan = match &pattern.start.label {
        Some(label) => LogicalPlan::NodeScanByLabel {
            var: start_var.clone(),
            label: label.clone(),
        },
        None => LogicalPlan::NodeScanAll {
            var: start_var.clone(),
        },
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
                dst_label: hop.target.label.clone(),
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
                dst_label: hop.target.label.clone(),
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

    if !stmt.create_patterns.is_empty() {
        let mut nodes: Vec<CreateNodeSpec> = Vec::new();
        let mut edges: Vec<CreateEdgeSpec> = Vec::new();
        let mut var_idx: HashMap<String, usize> = HashMap::new();
        for pattern in &stmt.create_patterns {
            build_create_pattern(pattern, &mut nodes, &mut edges, &mut var_idx, &all_vars)?;
        }
        return Ok(LogicalPlan::CreatePath {
            input: Some(Box::new(plan)),
            nodes,
            edges,
        });
    }

    if let Some(delete_clause) = &stmt.delete {
        return Ok(LogicalPlan::Delete {
            input: Box::new(plan),
            detach: delete_clause.detach,
            vars: delete_clause.vars.clone(),
        });
    }

    if !stmt.set_items.is_empty() {
        let assignments = stmt
            .set_items
            .iter()
            .map(|s| SetAssignment {
                var: s.var.clone(),
                key: s.key.clone(),
                value: s.value.clone(),
            })
            .collect();
        return Ok(LogicalPlan::SetProperty {
            input: Box::new(plan),
            assignments,
        });
    }

    // Split return items into group keys and aggregate specs.
    let mut group_keys: Vec<ReturnItem> = Vec::new();
    let mut aggregates: Vec<AggregateSpec> = Vec::new();
    for (idx, item) in stmt.return_items.iter().enumerate() {
        if let Expr::Call { name, args } = &item.expr {
            if let Some(func) = aggregate_fn_from_name(name) {
                let agg_arg = match args {
                    CallArgs::Star => {
                        if !matches!(func, AggregateFn::Count) {
                            return Err(Error::Plan(format!(
                                "only count(*) accepts a star argument"
                            )));
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
                continue;
            }
            return Err(Error::Plan(format!("unknown function: {}", name)));
        }
        if contains_aggregate(&item.expr) {
            return Err(Error::Plan(
                "aggregates must appear at the top of RETURN items".into(),
            ));
        }
        group_keys.push(item.clone());
    }

    plan = if !aggregates.is_empty() {
        LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys,
            aggregates,
        }
    } else {
        LogicalPlan::Project {
            input: Box::new(plan),
            items: stmt.return_items.clone(),
        }
    };

    if stmt.distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    if !stmt.order_by.is_empty() {
        plan = LogicalPlan::OrderBy {
            input: Box::new(plan),
            sort_items: stmt.order_by.clone(),
        };
    }

    if let Some(skip) = stmt.skip {
        plan = LogicalPlan::Skip {
            input: Box::new(plan),
            count: skip,
        };
    }

    if let Some(limit) = stmt.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count: limit,
        };
    }

    Ok(plan)
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, .. } if aggregate_fn_from_name(name).is_some() => true,
        Expr::Not(inner) => contains_aggregate(inner),
        Expr::And(a, b) | Expr::Or(a, b) => contains_aggregate(a) || contains_aggregate(b),
        Expr::Compare { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::Call { args: CallArgs::Exprs(es), .. } => es.iter().any(contains_aggregate),
        _ => false,
    }
}
