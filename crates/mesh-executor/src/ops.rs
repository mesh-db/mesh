use crate::{
    error::{Error, Result},
    eval::{compare_values, eval_expr, row_key, to_bool, value_key},
    reader::GraphReader,
    value::{ParamMap, Row, Value},
    writer::GraphWriter,
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
    pub store: &'a dyn GraphReader,
    pub writer: &'a dyn GraphWriter,
    /// Per-query parameter bindings. Empty for the single-node and Raft
    /// entry points that don't carry a Bolt RUN; populated with the
    /// driver-supplied params on the Bolt path. Threaded into every
    /// `eval_expr` call so `Expr::Parameter("name")` resolves.
    pub params: &'a ParamMap,
}

pub trait Operator {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>>;
}

/// Execute a plan using the given store for both reads and writes.
/// Equivalent to [`execute_with_reader`] with the store acting as both
/// and an empty parameter map.
pub fn execute(plan: &LogicalPlan, store: &Store) -> Result<Vec<Row>> {
    let params = ParamMap::new();
    execute_with_reader(
        plan,
        store as &dyn GraphReader,
        store as &dyn GraphWriter,
        &params,
    )
}

/// Execute a plan against a local [`Store`] for reads, sending mutations to
/// a separate [`GraphWriter`]. Used by in-process setups where reads are
/// always cheap-local (single node or full-replica Raft). No parameters.
pub fn execute_with_writer(
    plan: &LogicalPlan,
    store: &Store,
    writer: &dyn GraphWriter,
) -> Result<Vec<Row>> {
    let params = ParamMap::new();
    execute_with_reader(plan, store as &dyn GraphReader, writer, &params)
}

/// Execute a plan with an arbitrary [`GraphReader`] for reads and a
/// parameter map for `$param` resolution. Routing mode uses a
/// partitioned reader that fans out reads across peers; the Bolt
/// listener supplies the driver-bound param map.
pub fn execute_with_reader(
    plan: &LogicalPlan,
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    params: &ParamMap,
) -> Result<Vec<Row>> {
    // Schema DDL never enters the operator pipeline — it's a
    // side-effect on the backing store, not a row-producing query.
    // Short-circuit here so `build_op` stays a pure plan-to-operator
    // mapping and doesn't need DDL-specific cases.
    if let Some(rows) = try_execute_ddl(plan, writer)? {
        return Ok(rows);
    }
    let mut op = build_op(plan);
    let ctx = ExecCtx {
        store: reader,
        writer,
        params,
    };
    let mut rows = Vec::new();
    while let Some(row) = op.next(&ctx)? {
        rows.push(row);
    }
    Ok(rows)
}

/// DDL dispatch. Returns `Ok(Some(rows))` when `plan` is a schema
/// statement and was handled; `Ok(None)` when it's a regular graph
/// operation that the operator pipeline should execute normally.
///
/// `CREATE INDEX` / `DROP INDEX` return one row carrying an `ok`
/// value so Bolt clients see a non-empty RECORD stream rather than
/// an empty SUCCESS — mirrors how Neo4j reports schema writes.
/// `SHOW INDEXES` returns one row per registered index with
/// `label`, `property`, and `state` columns.
fn try_execute_ddl(plan: &LogicalPlan, writer: &dyn GraphWriter) -> Result<Option<Vec<Row>>> {
    match plan {
        LogicalPlan::CreatePropertyIndex { label, property } => {
            writer.create_property_index(label, property)?;
            Ok(Some(vec![ddl_ack_row("created", label, property)]))
        }
        LogicalPlan::DropPropertyIndex { label, property } => {
            writer.drop_property_index(label, property)?;
            Ok(Some(vec![ddl_ack_row("dropped", label, property)]))
        }
        LogicalPlan::ShowPropertyIndexes => {
            let specs = writer.list_property_indexes()?;
            let rows = specs
                .into_iter()
                .map(|(label, property)| {
                    let mut row = Row::default();
                    row.insert("label".into(), Value::Property(Property::String(label)));
                    row.insert(
                        "property".into(),
                        Value::Property(Property::String(property)),
                    );
                    row.insert(
                        "state".into(),
                        Value::Property(Property::String("online".into())),
                    );
                    row
                })
                .collect();
            Ok(Some(rows))
        }
        _ => Ok(None),
    }
}

fn ddl_ack_row(state: &str, label: &str, property: &str) -> Row {
    let mut row = Row::default();
    row.insert(
        "state".into(),
        Value::Property(Property::String(state.into())),
    );
    row.insert(
        "label".into(),
        Value::Property(Property::String(label.into())),
    );
    row.insert(
        "property".into(),
        Value::Property(Property::String(property.into())),
    );
    row
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
        LogicalPlan::CartesianProduct { left, right } => {
            Box::new(CartesianProductOp::new(build_op(left), (**right).clone()))
        }
        LogicalPlan::Delete {
            input,
            detach,
            vars,
        } => Box::new(DeleteOp::new(build_op(input), *detach, vars.clone())),
        LogicalPlan::SetProperty { input, assignments } => {
            Box::new(SetPropertyOp::new(build_op(input), assignments.clone()))
        }
        LogicalPlan::NodeScanAll { var } => Box::new(NodeScanAllOp::new(var.clone())),
        LogicalPlan::NodeScanByLabels { var, labels } => {
            Box::new(NodeScanByLabelsOp::new(var.clone(), labels.clone()))
        }
        LogicalPlan::EdgeExpand {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            edge_type,
            direction,
        } => Box::new(EdgeExpandOp::new(
            build_op(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
            edge_type.clone(),
            *direction,
        )),
        LogicalPlan::VarLengthExpand {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            edge_type,
            direction,
            min_hops,
            max_hops,
        } => Box::new(VarLengthExpandOp::new(
            build_op(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
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
        LogicalPlan::MergeNode {
            var,
            labels,
            properties,
        } => Box::new(MergeNodeOp::new(
            var.clone(),
            labels.clone(),
            properties.clone(),
        )),
        LogicalPlan::Unwind { var, expr } => Box::new(UnwindOp::new(var.clone(), expr.clone())),
        LogicalPlan::IndexSeek {
            var,
            label,
            property,
            value,
        } => Box::new(IndexSeekOp::new(
            var.clone(),
            label.clone(),
            property.clone(),
            value.clone(),
        )),
        // DDL plans are handled by `try_execute_ddl` before the
        // operator pipeline is built, so they should never reach
        // this point. An assertion here catches any future
        // refactor that forgets to gate them.
        LogicalPlan::CreatePropertyIndex { .. }
        | LogicalPlan::DropPropertyIndex { .. }
        | LogicalPlan::ShowPropertyIndexes => {
            panic!("schema DDL must be dispatched via try_execute_ddl before build_op")
        }
    }
}

struct UnwindOp {
    var: String,
    expr: Expr,
    items: Option<Vec<Value>>,
    cursor: usize,
}

impl UnwindOp {
    fn new(var: String, expr: Expr) -> Self {
        Self {
            var,
            expr,
            items: None,
            cursor: 0,
        }
    }
}

impl Operator for UnwindOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.items.is_none() {
            let empty = Row::new();
            let val = eval_expr(&self.expr, &empty, ctx.params)?;
            let items = match val {
                Value::List(items) => items,
                Value::Property(Property::List(props)) => {
                    props.into_iter().map(Value::Property).collect()
                }
                Value::Null => Vec::new(),
                _ => return Err(Error::TypeMismatch),
            };
            self.items = Some(items);
        }
        let items = self.items.as_ref().unwrap();
        if self.cursor < items.len() {
            let v = items[self.cursor].clone();
            self.cursor += 1;
            let mut row = Row::new();
            row.insert(self.var.clone(), v);
            Ok(Some(row))
        } else {
            Ok(None)
        }
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
                    // Property values are `Expr` (literal | parameter
                    // per the grammar). Evaluate against the current row
                    // + params and convert to a stored Property via the
                    // existing helper, which rejects Node/Edge values.
                    for (k, expr) in properties {
                        let value = eval_expr(expr, row, ctx.params)?;
                        let prop = value_to_property(value)?;
                        node.properties.insert(k.clone(), prop);
                    }
                    ctx.writer.put_node(&node)?;
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
            ctx.writer.put_edge(&edge)?;
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
                                ctx.writer.detach_delete_node(n.id)?;
                            } else {
                                let out = ctx.store.outgoing(n.id)?;
                                let inc = ctx.store.incoming(n.id)?;
                                if !out.is_empty() || !inc.is_empty() {
                                    return Err(Error::CannotDeleteAttachedNode);
                                }
                                ctx.writer.detach_delete_node(n.id)?;
                            }
                        }
                        Some(Value::Edge(e)) => {
                            ctx.writer.delete_edge(e.id)?;
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
                // Phase 1: evaluate any RHSes against the original row bindings.
                enum Action {
                    SetKey {
                        var: String,
                        key: String,
                        prop: Property,
                    },
                    AddLabels {
                        var: String,
                        labels: Vec<String>,
                    },
                    Replace {
                        var: String,
                        props: Vec<(String, Property)>,
                    },
                    Merge {
                        var: String,
                        props: Vec<(String, Property)>,
                    },
                }
                let mut actions: Vec<Action> = Vec::with_capacity(self.assignments.len());
                for a in &self.assignments {
                    match a {
                        SetAssignment::Property { var, key, value } => {
                            let evaluated = eval_expr(value, &row, ctx.params)?;
                            let prop = value_to_property(evaluated)?;
                            actions.push(Action::SetKey {
                                var: var.clone(),
                                key: key.clone(),
                                prop,
                            });
                        }
                        SetAssignment::Labels { var, labels } => {
                            actions.push(Action::AddLabels {
                                var: var.clone(),
                                labels: labels.clone(),
                            });
                        }
                        SetAssignment::Replace { var, properties } => {
                            // Property values are now `Expr`. Evaluate
                            // each against the current row + params and
                            // convert via the shared helper, which
                            // surfaces InvalidSetValue for Node/Edge.
                            let props = properties
                                .iter()
                                .map(|(k, expr)| {
                                    let v = eval_expr(expr, &row, ctx.params)?;
                                    Ok((k.clone(), value_to_property(v)?))
                                })
                                .collect::<Result<Vec<(String, Property)>>>()?;
                            actions.push(Action::Replace {
                                var: var.clone(),
                                props,
                            });
                        }
                        SetAssignment::Merge { var, properties } => {
                            let props = properties
                                .iter()
                                .map(|(k, expr)| {
                                    let v = eval_expr(expr, &row, ctx.params)?;
                                    Ok((k.clone(), value_to_property(v)?))
                                })
                                .collect::<Result<Vec<(String, Property)>>>()?;
                            actions.push(Action::Merge {
                                var: var.clone(),
                                props,
                            });
                        }
                    }
                }

                // Phase 2: apply updates in-place to the row bindings.
                let mut updated_nodes: HashSet<String> = HashSet::new();
                let mut updated_edges: HashSet<String> = HashSet::new();
                for action in actions {
                    match action {
                        Action::SetKey { var, key, prop } => match row.get_mut(&var) {
                            Some(Value::Node(n)) => {
                                n.properties.insert(key, prop);
                                updated_nodes.insert(var);
                            }
                            Some(Value::Edge(e)) => {
                                e.properties.insert(key, prop);
                                updated_edges.insert(var);
                            }
                            _ => return Err(Error::UnboundVariable(var)),
                        },
                        Action::AddLabels { var, labels } => match row.get_mut(&var) {
                            Some(Value::Node(n)) => {
                                for label in labels {
                                    if !n.labels.contains(&label) {
                                        n.labels.push(label);
                                    }
                                }
                                updated_nodes.insert(var);
                            }
                            _ => return Err(Error::UnboundVariable(var)),
                        },
                        Action::Replace { var, props } => match row.get_mut(&var) {
                            Some(Value::Node(n)) => {
                                n.properties.clear();
                                for (k, v) in props {
                                    n.properties.insert(k, v);
                                }
                                updated_nodes.insert(var);
                            }
                            Some(Value::Edge(e)) => {
                                e.properties.clear();
                                for (k, v) in props {
                                    e.properties.insert(k, v);
                                }
                                updated_edges.insert(var);
                            }
                            _ => return Err(Error::UnboundVariable(var)),
                        },
                        Action::Merge { var, props } => match row.get_mut(&var) {
                            Some(Value::Node(n)) => {
                                for (k, v) in props {
                                    n.properties.insert(k, v);
                                }
                                updated_nodes.insert(var);
                            }
                            Some(Value::Edge(e)) => {
                                for (k, v) in props {
                                    e.properties.insert(k, v);
                                }
                                updated_edges.insert(var);
                            }
                            _ => return Err(Error::UnboundVariable(var)),
                        },
                    }
                }

                // Phase 3: flush each mutated entity once to the writer.
                for var in &updated_nodes {
                    if let Some(Value::Node(n)) = row.get(var) {
                        ctx.writer.put_node(n)?;
                    }
                }
                for var in &updated_edges {
                    if let Some(Value::Edge(e)) = row.get(var) {
                        ctx.writer.put_edge(e)?;
                    }
                }

                Ok(Some(row))
            }
        }
    }
}

fn value_to_property(v: Value) -> Result<Property> {
    match v {
        Value::Property(p) => Ok(p),
        Value::Null => Ok(Property::Null),
        Value::Node(_) | Value::Edge(_) | Value::List(_) => Err(Error::InvalidSetValue),
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

struct NodeScanByLabelsOp {
    var: String,
    labels: Vec<String>,
    ids: Option<Vec<NodeId>>,
    cursor: usize,
}

impl NodeScanByLabelsOp {
    fn new(var: String, labels: Vec<String>) -> Self {
        Self {
            var,
            labels,
            ids: None,
            cursor: 0,
        }
    }
}

impl Operator for NodeScanByLabelsOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.ids.is_none() {
            // Use the first label for the index scan, filter the rest per-node.
            let primary = self
                .labels
                .first()
                .expect("NodeScanByLabels must have at least one label");
            self.ids = Some(ctx.store.nodes_by_label(primary)?);
        }
        let ids = self.ids.as_ref().unwrap();
        while self.cursor < ids.len() {
            let id = ids[self.cursor];
            self.cursor += 1;
            if let Some(node) = ctx.store.get_node(id)? {
                if has_all_labels(&node, &self.labels) {
                    let mut row = Row::new();
                    row.insert(self.var.clone(), Value::Node(node));
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }
}

fn has_all_labels(node: &Node, labels: &[String]) -> bool {
    labels.iter().all(|l| node.labels.contains(l))
}

/// Equality-lookup operator backed by a property index. Evaluates
/// the value expression lazily on the first `next()` — crucially
/// so parameters resolve against the per-query `ExecCtx::params`
/// map, not against a literal baked in at plan-construction time.
///
/// Unindexable value types (Float, List, Map, Null, or a Node/Edge
/// that slipped through) surface as `Error::InvalidSetValue`. The
/// planner only emits this op for indexes that do exist, so the
/// reader call should find a populated CF unless a concurrent DROP
/// raced us (in which case the result is just empty, which matches
/// how Neo4j's planner handles the race).
struct IndexSeekOp {
    var: String,
    label: String,
    property: String,
    value_expr: Expr,
    results: Option<Vec<NodeId>>,
    cursor: usize,
}

impl IndexSeekOp {
    fn new(var: String, label: String, property: String, value_expr: Expr) -> Self {
        Self {
            var,
            label,
            property,
            value_expr,
            results: None,
            cursor: 0,
        }
    }
}

impl Operator for IndexSeekOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.results.is_none() {
            let empty = Row::new();
            let value = eval_expr(&self.value_expr, &empty, ctx.params)?;
            let property = match value {
                Value::Property(p) => p,
                Value::Null => Property::Null,
                Value::Node(_) | Value::Edge(_) | Value::List(_) => {
                    return Err(Error::InvalidSetValue);
                }
            };
            let ids = ctx
                .store
                .nodes_by_property(&self.label, &self.property, &property)?;
            self.results = Some(ids);
        }
        let ids = self.results.as_ref().unwrap();
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

fn matches_pattern_props(node: &Node, props: &[(String, Property)]) -> bool {
    props.iter().all(|(k, v)| {
        node.properties
            .get(k)
            .map(|stored| stored == v)
            .unwrap_or(false)
    })
}

struct MergeNodeOp {
    var: String,
    labels: Vec<String>,
    /// Pattern property expressions as they came from the planner. These
    /// stay as `Expr` because evaluation needs `ExecCtx::params`, which
    /// isn't available until `next()` is called.
    properties: Vec<(String, Expr)>,
    initialized: bool,
    matched: Vec<Node>,
    /// Resolved property values stashed by `scan()` so the create
    /// branch in `next()` doesn't have to re-evaluate the expressions.
    resolved_for_create: Option<Vec<(String, Property)>>,
    cursor: usize,
}

impl MergeNodeOp {
    fn new(var: String, labels: Vec<String>, properties: Vec<(String, Expr)>) -> Self {
        Self {
            var,
            labels,
            properties,
            initialized: false,
            matched: Vec::new(),
            resolved_for_create: None,
            cursor: 0,
        }
    }

    fn scan(&mut self, ctx: &ExecCtx) -> Result<()> {
        // Evaluate the pattern property expressions (literals or
        // parameters) once against an empty row + the per-query param
        // map. The grammar guarantees no row references in pattern
        // values, so an empty row is sufficient. `value_to_property`
        // surfaces InvalidSetValue for Node/Edge values — a clear error
        // when a caller tries to MERGE on a graph-typed parameter.
        let empty = Row::new();
        let resolved_props: Vec<(String, Property)> = self
            .properties
            .iter()
            .map(|(k, expr)| {
                let v = eval_expr(expr, &empty, ctx.params)?;
                Ok((k.clone(), value_to_property(v)?))
            })
            .collect::<Result<Vec<_>>>()?;

        // Pick a candidate set: by-label index when one is given, else
        // a full node scan. Any extra labels beyond the first get filtered
        // per-node, mirroring NodeScanByLabelsOp.
        let candidate_ids: Vec<NodeId> = if let Some(primary) = self.labels.first() {
            ctx.store.nodes_by_label(primary)?
        } else {
            ctx.store.all_node_ids()?
        };
        for id in candidate_ids {
            if let Some(node) = ctx.store.get_node(id)? {
                if has_all_labels(&node, &self.labels)
                    && matches_pattern_props(&node, &resolved_props)
                {
                    self.matched.push(node);
                }
            }
        }

        // Stash the resolved properties so the create branch in `next`
        // doesn't have to re-evaluate them.
        self.resolved_for_create = Some(resolved_props);
        Ok(())
    }
}

impl Operator for MergeNodeOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if !self.initialized {
            self.scan(ctx)?;
            if self.matched.is_empty() {
                // No match — create exactly one node carrying the pattern's
                // labels and properties, then fall through to emit it. The
                // write goes through `ctx.writer`, so in cluster mode this
                // becomes part of the BufferingGraphWriter's batch and
                // commits atomically with any sibling mutations.
                let mut node = Node::new();
                for label in &self.labels {
                    node.labels.push(label.clone());
                }
                let resolved = self
                    .resolved_for_create
                    .take()
                    .expect("scan() populates resolved_for_create");
                for (k, prop) in resolved {
                    node.properties.insert(k, prop);
                }
                ctx.writer.put_node(&node)?;
                self.matched.push(node);
            }
            self.initialized = true;
        }
        if self.cursor < self.matched.len() {
            let node = self.matched[self.cursor].clone();
            self.cursor += 1;
            let mut row = Row::new();
            row.insert(self.var.clone(), Value::Node(node));
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

struct EdgeExpandOp {
    input: Box<dyn Operator>,
    src_var: String,
    edge_var: Option<String>,
    dst_var: String,
    dst_labels: Vec<String>,
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
        dst_labels: Vec<String>,
        edge_type: Option<String>,
        direction: Direction,
    ) -> Self {
        Self {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
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
                if !has_all_labels(&neighbor, &self.dst_labels) {
                    continue;
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
    dst_labels: Vec<String>,
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
        dst_labels: Vec<String>,
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
            dst_labels,
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

    fn enumerate(&self, ctx: &ExecCtx, start: NodeId) -> Result<(Vec<Vec<Edge>>, Vec<NodeId>)> {
        let mut paths: Vec<Vec<Edge>> = Vec::new();
        let mut targets: Vec<NodeId> = Vec::new();
        let mut current: Vec<Edge> = Vec::new();
        let mut used: HashSet<EdgeId> = HashSet::new();
        self.dfs(
            ctx,
            start,
            &mut current,
            &mut used,
            &mut paths,
            &mut targets,
        )?;
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
                Some(node) => has_all_labels(&node, &self.dst_labels),
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
            let v = eval_expr(&self.predicate, &row, ctx.params)?;
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
                    let value = eval_expr(&item.expr, &row, ctx.params)?;
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
                    keys.push(eval_expr(&item.expr, &row, ctx.params)?);
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
                key_values.push(eval_expr(&item.expr, &row, ctx.params)?);
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
                    distinct_seen: self.aggregates.iter().map(|_| None).collect(),
                }
            });
            for (i, spec) in self.aggregates.iter().enumerate() {
                if let AggregateArg::DistinctExpr(expr) = &spec.arg {
                    let v = eval_expr(expr, &row, ctx.params)?;
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    let key = value_key(&v);
                    let seen = entry.distinct_seen[i].get_or_insert_with(HashSet::new);
                    if !seen.insert(key) {
                        continue;
                    }
                }
                entry.agg_states[i].update(&spec.arg, &row, ctx.params)?;
            }
        }

        let mut out = Vec::new();
        if !saw_any && self.group_keys.is_empty() && !self.aggregates.is_empty() {
            // Empty group, single aggregate row
            let mut row = Row::new();
            for spec in &self.aggregates {
                row.insert(
                    spec.alias.clone(),
                    AggState::initial(spec.function).finalize(),
                );
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
    distinct_seen: Vec<Option<HashSet<String>>>,
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

    fn update(&mut self, arg: &AggregateArg, row: &Row, params: &ParamMap) -> Result<()> {
        match self {
            AggState::Count(c) => match arg {
                AggregateArg::Star => *c += 1,
                AggregateArg::Expr(e) | AggregateArg::DistinctExpr(e) => {
                    if !matches!(eval_expr(e, row, params)?, Value::Null) {
                        *c += 1;
                    }
                }
            },
            AggState::Sum {
                int_part,
                float_part,
                is_float,
            } => {
                let v = expr_arg_value(arg, row, params)?;
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
                let v = expr_arg_value(arg, row, params)?;
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
                let v = expr_arg_value(arg, row, params)?;
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
                let v = expr_arg_value(arg, row, params)?;
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
                let v = expr_arg_value(arg, row, params)?;
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

fn expr_arg_value(arg: &AggregateArg, row: &Row, params: &ParamMap) -> Result<Value> {
    match arg {
        AggregateArg::Star => Err(Error::AggregateTypeError),
        AggregateArg::Expr(e) | AggregateArg::DistinctExpr(e) => eval_expr(e, row, params),
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
