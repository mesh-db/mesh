use crate::ast::{
    CallArgs, CreateStmt, Direction, Expr, Literal, MatchStmt, NodePattern, ReturnItem, SortItem,
    Statement,
};
use crate::error::{Error, Result};
use std::collections::HashMap;

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
        nodes: Vec<CreateNodeSpec>,
        edges: Vec<CreateEdgeSpec>,
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
pub struct CreateNodeSpec {
    pub labels: Vec<String>,
    pub properties: Vec<(String, Literal)>,
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

    let start_idx = add_create_node(&mut nodes, &mut var_idx, &stmt.pattern.start);
    let mut prev_idx = start_idx;

    for hop in &stmt.pattern.hops {
        let target_idx = add_create_node(&mut nodes, &mut var_idx, &hop.target);

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

    Ok(LogicalPlan::CreatePath { nodes, edges })
}

fn add_create_node(
    nodes: &mut Vec<CreateNodeSpec>,
    var_idx: &mut HashMap<String, usize>,
    pattern: &NodePattern,
) -> usize {
    if let Some(name) = &pattern.var {
        if let Some(&idx) = var_idx.get(name) {
            return idx;
        }
    }
    let idx = nodes.len();
    nodes.push(CreateNodeSpec {
        labels: pattern.label.clone().into_iter().collect(),
        properties: pattern.properties.clone(),
    });
    if let Some(name) = &pattern.var {
        var_idx.insert(name.clone(), idx);
    }
    idx
}

fn plan_match(stmt: &MatchStmt) -> Result<LogicalPlan> {
    let start_var = stmt
        .pattern
        .start
        .var
        .clone()
        .unwrap_or_else(|| "__a0".to_string());

    let mut plan = match &stmt.pattern.start.label {
        Some(label) => LogicalPlan::NodeScanByLabel {
            var: start_var.clone(),
            label: label.clone(),
        },
        None => LogicalPlan::NodeScanAll {
            var: start_var.clone(),
        },
    };

    let mut current_var = start_var;
    for (i, hop) in stmt.pattern.hops.iter().enumerate() {
        let dst_var = hop
            .target
            .var
            .clone()
            .unwrap_or_else(|| format!("__a{}", i + 1));
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

    if let Some(predicate) = &stmt.where_clause {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
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
