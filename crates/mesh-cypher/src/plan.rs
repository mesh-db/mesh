use crate::ast::{CreateStmt, Expr, Literal, MatchStmt, ReturnItem, Statement};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    NodeScanAll {
        var: String,
    },
    NodeScanByLabel {
        var: String,
        label: String,
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
    let var = stmt
        .node
        .var
        .clone()
        .unwrap_or_else(|| "__anon".to_string());

    let mut plan = match &stmt.node.label {
        Some(label) => LogicalPlan::NodeScanByLabel {
            var: var.clone(),
            label: label.clone(),
        },
        None => LogicalPlan::NodeScanAll { var: var.clone() },
    };

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
