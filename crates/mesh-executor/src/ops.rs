use crate::{
    error::{Error, Result},
    eval::{eval_expr, literal_to_property, to_bool},
    value::{Row, Value},
};
use mesh_core::{Edge, EdgeId, Node, NodeId, Property};
use mesh_cypher::{
    CreateEdgeSpec, CreateNodeSpec, Direction, Expr, LogicalPlan, ReturnItem, SetAssignment,
};
use mesh_storage::Store;
use std::collections::HashMap;

pub struct ExecCtx<'a> {
    pub store: &'a Store,
}

pub trait Operator {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>>;
}

pub fn execute(plan: &LogicalPlan, store: &Store) -> Result<Vec<Row>> {
    let mut op = build_op(plan);
    let ctx = ExecCtx { store };
    let mut rows = Vec::new();
    while let Some(row) = op.next(&ctx)? {
        rows.push(row);
    }
    Ok(rows)
}

fn build_op(plan: &LogicalPlan) -> Box<dyn Operator> {
    match plan {
        LogicalPlan::CreatePath { nodes, edges } => {
            Box::new(CreatePathOp::new(nodes.clone(), edges.clone()))
        }
        LogicalPlan::Delete {
            input,
            detach,
            vars,
        } => Box::new(DeleteOp::new(build_op(input), *detach, vars.clone())),
        LogicalPlan::SetProperty { input, assignments } => Box::new(SetPropertyOp::new(
            build_op(input),
            assignments.clone(),
        )),
        LogicalPlan::NodeScanAll { var } => Box::new(NodeScanAllOp::new(var.clone())),
        LogicalPlan::NodeScanByLabel { var, label } => {
            Box::new(NodeScanByLabelOp::new(var.clone(), label.clone()))
        }
        LogicalPlan::EdgeExpand {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_label,
            edge_type,
            direction,
        } => Box::new(EdgeExpandOp::new(
            build_op(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_label.clone(),
            edge_type.clone(),
            *direction,
        )),
        LogicalPlan::Filter { input, predicate } => {
            Box::new(FilterOp::new(build_op(input), predicate.clone()))
        }
        LogicalPlan::Project { input, items } => {
            Box::new(ProjectOp::new(build_op(input), items.clone()))
        }
        LogicalPlan::Skip { input, count } => Box::new(SkipOp::new(build_op(input), *count)),
        LogicalPlan::Limit { input, count } => Box::new(LimitOp::new(build_op(input), *count)),
    }
}

struct CreatePathOp {
    nodes: Vec<CreateNodeSpec>,
    edges: Vec<CreateEdgeSpec>,
    done: bool,
}

impl CreatePathOp {
    fn new(nodes: Vec<CreateNodeSpec>, edges: Vec<CreateEdgeSpec>) -> Self {
        Self {
            nodes,
            edges,
            done: false,
        }
    }
}

impl Operator for CreatePathOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut node_ids: Vec<NodeId> = Vec::with_capacity(self.nodes.len());
        for spec in &self.nodes {
            let mut node = Node::new();
            for label in &spec.labels {
                node.labels.push(label.clone());
            }
            for (k, v) in &spec.properties {
                node.properties.insert(k.clone(), literal_to_property(v));
            }
            ctx.store.put_node(&node)?;
            node_ids.push(node.id);
        }
        for spec in &self.edges {
            let src = node_ids[spec.src_idx];
            let dst = node_ids[spec.dst_idx];
            let edge = Edge::new(spec.edge_type.clone(), src, dst);
            ctx.store.put_edge(&edge)?;
        }
        Ok(None)
    }
}

struct DeleteOp {
    input: Box<dyn Operator>,
    detach: bool,
    vars: Vec<String>,
}

impl DeleteOp {
    fn new(input: Box<dyn Operator>, detach: bool, vars: Vec<String>) -> Self {
        Self {
            input,
            detach,
            vars,
        }
    }
}

impl Operator for DeleteOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(ctx)? {
            for var in &self.vars {
                match row.get(var) {
                    Some(Value::Node(n)) => {
                        if self.detach {
                            ctx.store.detach_delete_node(n.id)?;
                        } else {
                            let out = ctx.store.outgoing(n.id)?;
                            let inc = ctx.store.incoming(n.id)?;
                            if !out.is_empty() || !inc.is_empty() {
                                return Err(Error::CannotDeleteAttachedNode);
                            }
                            ctx.store.detach_delete_node(n.id)?;
                        }
                    }
                    Some(Value::Edge(e)) => {
                        if ctx.store.get_edge(e.id)?.is_some() {
                            ctx.store.delete_edge(e.id)?;
                        }
                    }
                    _ => return Err(Error::UnboundVariable(var.clone())),
                }
            }
        }
        Ok(None)
    }
}

struct SetPropertyOp {
    input: Box<dyn Operator>,
    assignments: Vec<SetAssignment>,
}

impl SetPropertyOp {
    fn new(input: Box<dyn Operator>, assignments: Vec<SetAssignment>) -> Self {
        Self { input, assignments }
    }
}

impl Operator for SetPropertyOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(ctx)? {
            let mut node_updates: HashMap<String, Node> = HashMap::new();
            let mut edge_updates: HashMap<String, Edge> = HashMap::new();

            for SetAssignment { var, key, value } in &self.assignments {
                let evaluated = eval_expr(value, &row)?;
                let prop = match evaluated {
                    Value::Property(p) => p,
                    Value::Null => Property::Null,
                    Value::Node(_) | Value::Edge(_) => return Err(Error::InvalidSetValue),
                };

                match row.get(var) {
                    Some(Value::Node(n)) => {
                        let entry = node_updates
                            .entry(var.clone())
                            .or_insert_with(|| n.clone());
                        entry.properties.insert(key.clone(), prop);
                    }
                    Some(Value::Edge(e)) => {
                        let entry = edge_updates
                            .entry(var.clone())
                            .or_insert_with(|| e.clone());
                        entry.properties.insert(key.clone(), prop);
                    }
                    _ => return Err(Error::UnboundVariable(var.clone())),
                }
            }

            for node in node_updates.into_values() {
                ctx.store.put_node(&node)?;
            }
            for edge in edge_updates.into_values() {
                ctx.store.put_edge(&edge)?;
            }
        }
        Ok(None)
    }
}

struct NodeScanAllOp {
    var: String,
    ids: Option<Vec<NodeId>>,
    cursor: usize,
}

impl NodeScanAllOp {
    fn new(var: String) -> Self {
        Self {
            var,
            ids: None,
            cursor: 0,
        }
    }
}

impl Operator for NodeScanAllOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.ids.is_none() {
            self.ids = Some(ctx.store.all_node_ids()?);
        }
        let ids = self.ids.as_ref().unwrap();
        while self.cursor < ids.len() {
            let id = ids[self.cursor];
            self.cursor += 1;
            if let Some(node) = ctx.store.get_node(id)? {
                let mut row = Row::new();
                row.insert(self.var.clone(), Value::Node(node));
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct NodeScanByLabelOp {
    var: String,
    label: String,
    ids: Option<Vec<NodeId>>,
    cursor: usize,
}

impl NodeScanByLabelOp {
    fn new(var: String, label: String) -> Self {
        Self {
            var,
            label,
            ids: None,
            cursor: 0,
        }
    }
}

impl Operator for NodeScanByLabelOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.ids.is_none() {
            self.ids = Some(ctx.store.nodes_by_label(&self.label)?);
        }
        let ids = self.ids.as_ref().unwrap();
        while self.cursor < ids.len() {
            let id = ids[self.cursor];
            self.cursor += 1;
            if let Some(node) = ctx.store.get_node(id)? {
                let mut row = Row::new();
                row.insert(self.var.clone(), Value::Node(node));
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct EdgeExpandOp {
    input: Box<dyn Operator>,
    src_var: String,
    edge_var: Option<String>,
    dst_var: String,
    dst_label: Option<String>,
    edge_type: Option<String>,
    direction: Direction,
    current_row: Option<Row>,
    pending: Vec<(EdgeId, NodeId)>,
    pending_idx: usize,
}

impl EdgeExpandOp {
    fn new(
        input: Box<dyn Operator>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_label: Option<String>,
        edge_type: Option<String>,
        direction: Direction,
    ) -> Self {
        Self {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_label,
            edge_type,
            direction,
            current_row: None,
            pending: Vec::new(),
            pending_idx: 0,
        }
    }
}

impl Operator for EdgeExpandOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            while self.pending_idx < self.pending.len() {
                let (edge_id, neighbor_id) = self.pending[self.pending_idx];
                self.pending_idx += 1;

                let edge = match ctx.store.get_edge(edge_id)? {
                    Some(e) => e,
                    None => continue,
                };
                if let Some(t) = &self.edge_type {
                    if &edge.edge_type != t {
                        continue;
                    }
                }

                let neighbor = match ctx.store.get_node(neighbor_id)? {
                    Some(n) => n,
                    None => continue,
                };
                if let Some(l) = &self.dst_label {
                    if !neighbor.labels.contains(l) {
                        continue;
                    }
                }

                let base = self
                    .current_row
                    .as_ref()
                    .expect("pending edges without source row");
                let mut out = base.clone();
                if let Some(ev) = &self.edge_var {
                    out.insert(ev.clone(), Value::Edge(edge));
                }
                out.insert(self.dst_var.clone(), Value::Node(neighbor));
                return Ok(Some(out));
            }

            match self.input.next(ctx)? {
                None => return Ok(None),
                Some(row) => {
                    let src_id = match row.get(&self.src_var) {
                        Some(Value::Node(n)) => n.id,
                        _ => return Err(Error::UnboundVariable(self.src_var.clone())),
                    };
                    self.pending = match self.direction {
                        Direction::Outgoing => ctx.store.outgoing(src_id)?,
                        Direction::Incoming => ctx.store.incoming(src_id)?,
                        Direction::Both => {
                            let mut all = ctx.store.outgoing(src_id)?;
                            all.extend(ctx.store.incoming(src_id)?);
                            all
                        }
                    };
                    self.pending_idx = 0;
                    self.current_row = Some(row);
                }
            }
        }
    }
}

struct FilterOp {
    input: Box<dyn Operator>,
    predicate: Expr,
}

impl FilterOp {
    fn new(input: Box<dyn Operator>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Operator for FilterOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(ctx)? {
            let v = eval_expr(&self.predicate, &row)?;
            if to_bool(&v)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct ProjectOp {
    input: Box<dyn Operator>,
    items: Vec<ReturnItem>,
}

impl ProjectOp {
    fn new(input: Box<dyn Operator>, items: Vec<ReturnItem>) -> Self {
        Self { input, items }
    }
}

impl Operator for ProjectOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        match self.input.next(ctx)? {
            Some(row) => {
                let mut out = Row::new();
                for (i, item) in self.items.iter().enumerate() {
                    let name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| default_name(&item.expr, i));
                    let value = eval_expr(&item.expr, &row)?;
                    out.insert(name, value);
                }
                Ok(Some(out))
            }
            None => Ok(None),
        }
    }
}

fn default_name(expr: &Expr, idx: usize) -> String {
    match expr {
        Expr::Identifier(s) => s.clone(),
        Expr::Property { var, key } => format!("{}.{}", var, key),
        _ => format!("col{}", idx),
    }
}

struct SkipOp {
    input: Box<dyn Operator>,
    remaining: i64,
}

impl SkipOp {
    fn new(input: Box<dyn Operator>, count: i64) -> Self {
        Self {
            input,
            remaining: count,
        }
    }
}

impl Operator for SkipOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        while self.remaining > 0 {
            if self.input.next(ctx)?.is_none() {
                return Ok(None);
            }
            self.remaining -= 1;
        }
        self.input.next(ctx)
    }
}

struct LimitOp {
    input: Box<dyn Operator>,
    remaining: i64,
}

impl LimitOp {
    fn new(input: Box<dyn Operator>, count: i64) -> Self {
        Self {
            input,
            remaining: count,
        }
    }
}

impl Operator for LimitOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        match self.input.next(ctx)? {
            Some(row) => {
                self.remaining -= 1;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }
}
