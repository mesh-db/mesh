use crate::ast::{
    CreateStmt, Direction, Expr, Literal, MatchStmt, NodePattern, ReturnItem, Statement,
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
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        items: Vec<ReturnItem>,
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
        plan = LogicalPlan::EdgeExpand {
            input: Box::new(plan),
            src_var: current_var.clone(),
            edge_var: hop.rel.var.clone(),
            dst_var: dst_var.clone(),
            dst_label: hop.target.label.clone(),
            edge_type: hop.rel.edge_type.clone(),
            direction: hop.rel.direction,
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

    Ok(LogicalPlan::Project {
        input: Box::new(plan),
        items: stmt.return_items.clone(),
    })
}
