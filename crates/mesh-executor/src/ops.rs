use crate::{
    error::{Error, Result},
    eval::{compare_values, eval_expr, literal_to_property, row_key, to_bool, value_key},
    value::{Row, Value},
};
use mesh_core::{Edge, EdgeId, Node, NodeId, Property};
use mesh_cypher::{
    AggregateArg, AggregateFn, AggregateSpec, CreateEdgeSpec, CreateNodeSpec, Direction, Expr,
    LogicalPlan, ReturnItem, SetAssignment, SortItem,
};
use mesh_storage::Store;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

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
        LogicalPlan::CreatePath {
            input,
            nodes,
            edges,
        } => Box::new(CreatePathOp::new(
            input.as_ref().map(|p| build_op(p)),
            nodes.clone(),
            edges.clone(),
        )),
        LogicalPlan::CartesianProduct { left, right } => Box::new(CartesianProductOp::new(
            build_op(left),
            (**right).clone(),
        )),
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
        LogicalPlan::VarLengthExpand {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_label,
            edge_type,
            direction,
            min_hops,
            max_hops,
        } => Box::new(VarLengthExpandOp::new(
            build_op(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_label.clone(),
            edge_type.clone(),
            *direction,
            *min_hops,
            *max_hops,
        )),
        LogicalPlan::Filter { input, predicate } => {
            Box::new(FilterOp::new(build_op(input), predicate.clone()))
        }
        LogicalPlan::Project { input, items } => {
            Box::new(ProjectOp::new(build_op(input), items.clone()))
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
        } => Box::new(AggregateOp::new(
            build_op(input),
            group_keys.clone(),
            aggregates.clone(),
        )),
        LogicalPlan::Distinct { input } => Box::new(DistinctOp::new(build_op(input))),
        LogicalPlan::OrderBy { input, sort_items } => {
            Box::new(OrderByOp::new(build_op(input), sort_items.clone()))
        }
        LogicalPlan::Skip { input, count } => Box::new(SkipOp::new(build_op(input), *count)),
        LogicalPlan::Limit { input, count } => Box::new(LimitOp::new(build_op(input), *count)),
    }
}

struct CreatePathOp {
    input: Option<Box<dyn Operator>>,
    nodes: Vec<CreateNodeSpec>,
    edges: Vec<CreateEdgeSpec>,
    done: bool,
    buffered: Option<Vec<Row>>,
    cursor: usize,
}

impl CreatePathOp {
    fn new(
        input: Option<Box<dyn Operator>>,
        nodes: Vec<CreateNodeSpec>,
        edges: Vec<CreateEdgeSpec>,
    ) -> Self {
        Self {
            input,
            nodes,
            edges,
            done: false,
            buffered: None,
            cursor: 0,
        }
    }

    fn apply(&self, ctx: &ExecCtx, row: &Row) -> Result<Row> {
        let mut out = row.clone();
        let mut node_ids: Vec<NodeId> = Vec::with_capacity(self.nodes.len());
        for spec in &self.nodes {
            match spec {
                CreateNodeSpec::New {
                    var,
                    labels,
                    properties,
                } => {
                    let mut node = Node::new();
                    for label in labels {
                        node.labels.push(label.clone());
                    }
                    for (k, v) in properties {
                        node.properties.insert(k.clone(), literal_to_property(v));
                    }
                    ctx.store.put_node(&node)?;
                    node_ids.push(node.id);
                    if let Some(v) = var {
                        out.insert(v.clone(), Value::Node(node));
                    }
                }
                CreateNodeSpec::Reference(name) => {
                    let id = match row.get(name) {
                        Some(Value::Node(n)) => n.id,
                        _ => return Err(Error::UnboundVariable(name.clone())),
                    };
                    node_ids.push(id);
                }
            }
        }
        for spec in &self.edges {
            let src = node_ids[spec.src_idx];
            let dst = node_ids[spec.dst_idx];
            let edge = Edge::new(spec.edge_type.clone(), src, dst);
            ctx.store.put_edge(&edge)?;
            if let Some(v) = &spec.var {
                out.insert(v.clone(), Value::Edge(edge));
            }
        }
        Ok(out)
    }
}

impl Operator for CreatePathOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.input.is_some() {
            // Drain the whole input first, then replay sequentially. Draining up
            // front avoids aliasing the source scan's cursor while we're writing
            // to the store (node-label scans cache ids lazily on first call).
            if let Some(buffered) = self.buffered.as_mut() {
                if self.cursor < buffered.len() {
                    let row = buffered[self.cursor].clone();
                    self.cursor += 1;
                    return Ok(Some(self.apply(ctx, &row)?));
                }
                return Ok(None);
            }
            let mut rows: Vec<Row> = Vec::new();
            {
                let input = self.input.as_mut().unwrap();
                while let Some(row) = input.next(ctx)? {
                    rows.push(row);
                }
            }
            self.buffered = Some(rows);
            self.cursor = 0;
            // Fall through to next call via recursion.
            self.next(ctx)
        } else {
            if self.done {
                return Ok(None);
            }
            self.done = true;
            let empty = Row::new();
            Ok(Some(self.apply(ctx, &empty)?))
        }
    }
}

struct CartesianProductOp {
    left: Box<dyn Operator>,
    right_plan: LogicalPlan,
    left_row: Option<Row>,
    right_op: Option<Box<dyn Operator>>,
}

impl CartesianProductOp {
    fn new(left: Box<dyn Operator>, right_plan: LogicalPlan) -> Self {
        Self {
            left,
            right_plan,
            left_row: None,
            right_op: None,
        }
    }
}

impl Operator for CartesianProductOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            if self.left_row.is_none() {
                match self.left.next(ctx)? {
                    None => return Ok(None),
                    Some(row) => {
                        self.left_row = Some(row);
                        self.right_op = Some(build_op(&self.right_plan));
                    }
                }
            }
            let right_op = self.right_op.as_mut().expect("right_op set");
            match right_op.next(ctx)? {
                Some(right_row) => {
                    let mut combined = self.left_row.as_ref().unwrap().clone();
                    for (k, v) in right_row {
                        combined.insert(k, v);
                    }
                    return Ok(Some(combined));
                }
                None => {
                    self.left_row = None;
                    self.right_op = None;
                }
            }
        }
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
        match self.input.next(ctx)? {
            None => Ok(None),
            Some(row) => {
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
                Ok(Some(row))
            }
        }
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
        match self.input.next(ctx)? {
            None => Ok(None),
            Some(mut row) => {
                // Phase 1: evaluate RHS values against the original row bindings.
                let mut computed: Vec<(String, String, Property)> =
                    Vec::with_capacity(self.assignments.len());
                for SetAssignment { var, key, value } in &self.assignments {
                    let evaluated = eval_expr(value, &row)?;
                    let prop = match evaluated {
                        Value::Property(p) => p,
                        Value::Null => Property::Null,
                        Value::Node(_) | Value::Edge(_) | Value::List(_) => {
                            return Err(Error::InvalidSetValue)
                        }
                    };
                    computed.push((var.clone(), key.clone(), prop));
                }

                // Phase 2: apply updates in-place to the row bindings.
                let mut updated_nodes: HashSet<String> = HashSet::new();
                let mut updated_edges: HashSet<String> = HashSet::new();
                for (var, key, prop) in computed {
                    match row.get_mut(&var) {
                        Some(Value::Node(n)) => {
                            n.properties.insert(key, prop);
                            updated_nodes.insert(var);
                        }
                        Some(Value::Edge(e)) => {
                            e.properties.insert(key, prop);
                            updated_edges.insert(var);
                        }
                        _ => return Err(Error::UnboundVariable(var)),
                    }
                }

                // Phase 3: flush each mutated entity once to the store.
                for var in &updated_nodes {
                    if let Some(Value::Node(n)) = row.get(var) {
                        ctx.store.put_node(n)?;
                    }
                }
                for var in &updated_edges {
                    if let Some(Value::Edge(e)) = row.get(var) {
                        ctx.store.put_edge(e)?;
                    }
                }

                Ok(Some(row))
            }
        }
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

struct VarLengthExpandOp {
    input: Box<dyn Operator>,
    src_var: String,
    edge_var: Option<String>,
    dst_var: String,
    dst_label: Option<String>,
    edge_type: Option<String>,
    direction: Direction,
    min_hops: u64,
    max_hops: u64,
    current_row: Option<Row>,
    pending_paths: Vec<Vec<Edge>>,
    pending_targets: Vec<NodeId>,
    pending_idx: usize,
}

impl VarLengthExpandOp {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Box<dyn Operator>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_label: Option<String>,
        edge_type: Option<String>,
        direction: Direction,
        min_hops: u64,
        max_hops: u64,
    ) -> Self {
        Self {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_label,
            edge_type,
            direction,
            min_hops,
            max_hops,
            current_row: None,
            pending_paths: Vec::new(),
            pending_targets: Vec::new(),
            pending_idx: 0,
        }
    }

    fn enumerate(
        &self,
        ctx: &ExecCtx,
        start: NodeId,
    ) -> Result<(Vec<Vec<Edge>>, Vec<NodeId>)> {
        let mut paths: Vec<Vec<Edge>> = Vec::new();
        let mut targets: Vec<NodeId> = Vec::new();
        let mut current: Vec<Edge> = Vec::new();
        let mut used: HashSet<EdgeId> = HashSet::new();
        self.dfs(ctx, start, &mut current, &mut used, &mut paths, &mut targets)?;
        Ok((paths, targets))
    }

    fn dfs(
        &self,
        ctx: &ExecCtx,
        current_node: NodeId,
        path: &mut Vec<Edge>,
        used: &mut HashSet<EdgeId>,
        out_paths: &mut Vec<Vec<Edge>>,
        out_targets: &mut Vec<NodeId>,
    ) -> Result<()> {
        let depth = path.len() as u64;

        if depth >= self.min_hops && depth <= self.max_hops {
            let terminal_ok = match ctx.store.get_node(current_node)? {
                Some(node) => self
                    .dst_label
                    .as_ref()
                    .map_or(true, |l| node.labels.contains(l)),
                None => false,
            };
            if terminal_ok {
                out_paths.push(path.clone());
                out_targets.push(current_node);
            }
        }

        if depth >= self.max_hops {
            return Ok(());
        }

        let neighbors = match self.direction {
            Direction::Outgoing => ctx.store.outgoing(current_node)?,
            Direction::Incoming => ctx.store.incoming(current_node)?,
            Direction::Both => {
                let mut all = ctx.store.outgoing(current_node)?;
                all.extend(ctx.store.incoming(current_node)?);
                all
            }
        };

        for (eid, neighbor_id) in neighbors {
            if used.contains(&eid) {
                continue;
            }
            let edge = match ctx.store.get_edge(eid)? {
                Some(e) => e,
                None => continue,
            };
            if let Some(t) = &self.edge_type {
                if &edge.edge_type != t {
                    continue;
                }
            }
            used.insert(eid);
            path.push(edge);
            self.dfs(ctx, neighbor_id, path, used, out_paths, out_targets)?;
            path.pop();
            used.remove(&eid);
        }

        Ok(())
    }
}

impl Operator for VarLengthExpandOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            while self.pending_idx < self.pending_paths.len() {
                let i = self.pending_idx;
                self.pending_idx += 1;

                let target_id = self.pending_targets[i];
                let target = match ctx.store.get_node(target_id)? {
                    Some(n) => n,
                    None => continue,
                };

                let base = self
                    .current_row
                    .as_ref()
                    .expect("pending without source row");
                let mut out = base.clone();
                out.insert(self.dst_var.clone(), Value::Node(target));
                if let Some(ev) = &self.edge_var {
                    let edges: Vec<Value> = self.pending_paths[i]
                        .iter()
                        .cloned()
                        .map(Value::Edge)
                        .collect();
                    out.insert(ev.clone(), Value::List(edges));
                }
                return Ok(Some(out));
            }

            match self.input.next(ctx)? {
                None => return Ok(None),
                Some(row) => {
                    let src_id = match row.get(&self.src_var) {
                        Some(Value::Node(n)) => n.id,
                        _ => return Err(Error::UnboundVariable(self.src_var.clone())),
                    };
                    let (paths, targets) = self.enumerate(ctx, src_id)?;
                    self.pending_paths = paths;
                    self.pending_targets = targets;
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

struct DistinctOp {
    input: Box<dyn Operator>,
    seen: HashSet<String>,
}

impl DistinctOp {
    fn new(input: Box<dyn Operator>) -> Self {
        Self {
            input,
            seen: HashSet::new(),
        }
    }
}

impl Operator for DistinctOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(ctx)? {
            let key = row_key(&row);
            if self.seen.insert(key) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct OrderByOp {
    input: Box<dyn Operator>,
    sort_items: Vec<SortItem>,
    sorted: Option<Vec<Row>>,
    cursor: usize,
}

impl OrderByOp {
    fn new(input: Box<dyn Operator>, sort_items: Vec<SortItem>) -> Self {
        Self {
            input,
            sort_items,
            sorted: None,
            cursor: 0,
        }
    }
}

impl Operator for OrderByOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.sorted.is_none() {
            let mut rows: Vec<Row> = Vec::new();
            while let Some(row) = self.input.next(ctx)? {
                rows.push(row);
            }
            let mut keyed: Vec<(Vec<Value>, Row)> = Vec::with_capacity(rows.len());
            for row in rows {
                let mut keys = Vec::with_capacity(self.sort_items.len());
                for item in &self.sort_items {
                    keys.push(eval_expr(&item.expr, &row)?);
                }
                keyed.push((keys, row));
            }
            let descs: Vec<bool> = self.sort_items.iter().map(|s| s.descending).collect();
            keyed.sort_by(|a, b| {
                for (i, (va, vb)) in a.0.iter().zip(b.0.iter()).enumerate() {
                    let ord = compare_values(va, vb);
                    let ord = if descs[i] { ord.reverse() } else { ord };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
            self.sorted = Some(keyed.into_iter().map(|(_, r)| r).collect());
        }
        let rows = self.sorted.as_ref().unwrap();
        if self.cursor < rows.len() {
            let row = rows[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

struct AggregateOp {
    input: Box<dyn Operator>,
    group_keys: Vec<ReturnItem>,
    aggregates: Vec<AggregateSpec>,
    results: Option<Vec<Row>>,
    cursor: usize,
}

impl AggregateOp {
    fn new(
        input: Box<dyn Operator>,
        group_keys: Vec<ReturnItem>,
        aggregates: Vec<AggregateSpec>,
    ) -> Self {
        Self {
            input,
            group_keys,
            aggregates,
            results: None,
            cursor: 0,
        }
    }

    fn compute(&mut self, ctx: &ExecCtx) -> Result<()> {
        let mut groups: HashMap<String, GroupState> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        // If there are no input rows AND no group keys, we still emit one row
        // (e.g. `MATCH (n:Missing) RETURN count(*)` must yield one row with 0).
        let mut saw_any = false;

        while let Some(row) = self.input.next(ctx)? {
            saw_any = true;
            let mut key_values = Vec::with_capacity(self.group_keys.len());
            for item in &self.group_keys {
                key_values.push(eval_expr(&item.expr, &row)?);
            }
            let mut hash_key = String::new();
            for v in &key_values {
                hash_key.push_str(&value_key(v));
                hash_key.push('|');
            }
            let entry = groups.entry(hash_key.clone()).or_insert_with(|| {
                order.push(hash_key.clone());
                GroupState {
                    key_values: key_values.clone(),
                    agg_states: self
                        .aggregates
                        .iter()
                        .map(|a| AggState::initial(a.function))
                        .collect(),
                }
            });
            for (i, spec) in self.aggregates.iter().enumerate() {
                entry.agg_states[i].update(&spec.arg, &row)?;
            }
        }

        let mut out = Vec::new();
        if !saw_any && self.group_keys.is_empty() && !self.aggregates.is_empty() {
            // Empty group, single aggregate row
            let mut row = Row::new();
            for spec in &self.aggregates {
                row.insert(spec.alias.clone(), AggState::initial(spec.function).finalize());
            }
            out.push(row);
        } else {
            for key in order {
                let state = groups.remove(&key).unwrap();
                let mut row = Row::new();
                for (i, item) in self.group_keys.iter().enumerate() {
                    let name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| default_name(&item.expr, i));
                    row.insert(name, state.key_values[i].clone());
                }
                for (i, spec) in self.aggregates.iter().enumerate() {
                    row.insert(spec.alias.clone(), state.agg_states[i].finalize());
                }
                out.push(row);
            }
        }
        self.results = Some(out);
        Ok(())
    }
}

impl Operator for AggregateOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.results.is_none() {
            self.compute(ctx)?;
        }
        let rows = self.results.as_ref().unwrap();
        if self.cursor < rows.len() {
            let row = rows[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

struct GroupState {
    key_values: Vec<Value>,
    agg_states: Vec<AggState>,
}

enum AggState {
    Count(i64),
    Sum {
        int_part: i64,
        float_part: f64,
        is_float: bool,
    },
    Avg {
        total: f64,
        count: i64,
    },
    Min(Option<Property>),
    Max(Option<Property>),
    Collect(Vec<Value>),
}

impl AggState {
    fn initial(func: AggregateFn) -> Self {
        match func {
            AggregateFn::Count => AggState::Count(0),
            AggregateFn::Sum => AggState::Sum {
                int_part: 0,
                float_part: 0.0,
                is_float: false,
            },
            AggregateFn::Avg => AggState::Avg {
                total: 0.0,
                count: 0,
            },
            AggregateFn::Min => AggState::Min(None),
            AggregateFn::Max => AggState::Max(None),
            AggregateFn::Collect => AggState::Collect(Vec::new()),
        }
    }

    fn update(&mut self, arg: &AggregateArg, row: &Row) -> Result<()> {
        match self {
            AggState::Count(c) => match arg {
                AggregateArg::Star => *c += 1,
                AggregateArg::Expr(e) => {
                    if !matches!(eval_expr(e, row)?, Value::Null) {
                        *c += 1;
                    }
                }
            },
            AggState::Sum {
                int_part,
                float_part,
                is_float,
            } => {
                let v = expr_arg_value(arg, row)?;
                match v {
                    Value::Null => {}
                    Value::Property(Property::Int64(i)) => *int_part += i,
                    Value::Property(Property::Float64(f)) => {
                        *float_part += f;
                        *is_float = true;
                    }
                    _ => return Err(Error::AggregateTypeError),
                }
            }
            AggState::Avg { total, count } => {
                let v = expr_arg_value(arg, row)?;
                match v {
                    Value::Null => {}
                    Value::Property(Property::Int64(i)) => {
                        *total += i as f64;
                        *count += 1;
                    }
                    Value::Property(Property::Float64(f)) => {
                        *total += f;
                        *count += 1;
                    }
                    _ => return Err(Error::AggregateTypeError),
                }
            }
            AggState::Min(slot) => {
                let v = expr_arg_value(arg, row)?;
                if let Value::Property(p) = v {
                    match slot {
                        None => *slot = Some(p),
                        Some(cur) => {
                            if compare_values(
                                &Value::Property(p.clone()),
                                &Value::Property(cur.clone()),
                            ) == Ordering::Less
                            {
                                *cur = p;
                            }
                        }
                    }
                }
            }
            AggState::Max(slot) => {
                let v = expr_arg_value(arg, row)?;
                if let Value::Property(p) = v {
                    match slot {
                        None => *slot = Some(p),
                        Some(cur) => {
                            if compare_values(
                                &Value::Property(p.clone()),
                                &Value::Property(cur.clone()),
                            ) == Ordering::Greater
                            {
                                *cur = p;
                            }
                        }
                    }
                }
            }
            AggState::Collect(items) => {
                let v = expr_arg_value(arg, row)?;
                if !matches!(v, Value::Null) {
                    items.push(v);
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Value {
        match self {
            AggState::Count(c) => Value::Property(Property::Int64(*c)),
            AggState::Sum {
                int_part,
                float_part,
                is_float,
            } => {
                if *is_float {
                    Value::Property(Property::Float64(*float_part + *int_part as f64))
                } else {
                    Value::Property(Property::Int64(*int_part))
                }
            }
            AggState::Avg { total, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Property(Property::Float64(*total / *count as f64))
                }
            }
            AggState::Min(slot) | AggState::Max(slot) => match slot {
                Some(p) => Value::Property(p.clone()),
                None => Value::Null,
            },
            AggState::Collect(items) => Value::List(items.clone()),
        }
    }
}

fn expr_arg_value(arg: &AggregateArg, row: &Row) -> Result<Value> {
    match arg {
        AggregateArg::Star => Err(Error::AggregateTypeError),
        AggregateArg::Expr(e) => eval_expr(e, row),
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
