use crate::ast::{CreateStmt, Direction, Expr, Literal, MatchStmt, ReturnItem, Statement};

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
    CreateNode {
        labels: Vec<String>,
        properties: Vec<(String, Literal)>,
    },
}

pub fn plan(statement: &Statement) -> LogicalPlan {
    match statement {
        Statement::Create(c) => plan_create(c),
        Statement::Match(m) => plan_match(m),
    }
}

fn plan_create(stmt: &CreateStmt) -> LogicalPlan {
    let labels = stmt.node.label.clone().into_iter().collect();
    LogicalPlan::CreateNode {
        labels,
        properties: stmt.node.properties.clone(),
    }
}

fn plan_match(stmt: &MatchStmt) -> LogicalPlan {
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

    LogicalPlan::Project {
        input: Box::new(plan),
        items: stmt.return_items.clone(),
    }
}
