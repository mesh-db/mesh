use crate::{
    error::{Error, Result},
    eval::{compare_values, eval_expr, row_key, to_bool, value_key, values_equal, EvalCtx},
    reader::GraphReader,
    value::{ParamMap, Row, Value},
    writer::GraphWriter,
};
use mesh_core::{Edge, EdgeId, Node, NodeId, Property};
use mesh_cypher::{
    AggregateArg, AggregateFn, AggregateSpec, CreateEdgeSpec, CreateNodeSpec, Direction, Expr,
    LogicalPlan, RemoveSpec, ReturnItem, SetAssignment, SortItem,
};
use mesh_storage::RocksDbStorageEngine;
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

impl<'a> ExecCtx<'a> {
    /// Build an `EvalCtx` wrapping the given row alongside this
    /// exec context's params and reader. Used by operators to
    /// bridge the per-operator context into the expression
    /// evaluator without repeating the field list at every
    /// `eval_expr` call site.
    pub(crate) fn eval_ctx<'b>(&self, row: &'b Row) -> EvalCtx<'b>
    where
        'a: 'b,
    {
        EvalCtx {
            row,
            params: self.params,
            reader: self.store,
        }
    }
}

pub trait Operator {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>>;
}

/// Execute a plan using the given store for both reads and writes.
/// Equivalent to [`execute_with_reader`] with the store acting as both
/// and an empty parameter map.
pub fn execute(plan: &LogicalPlan, store: &RocksDbStorageEngine) -> Result<Vec<Row>> {
    let params = ParamMap::new();
    execute_with_reader(
        plan,
        store as &dyn GraphReader,
        store as &dyn GraphWriter,
        &params,
    )
}

/// Execute a plan against a local [`RocksDbStorageEngine`] for reads,
/// sending mutations to a separate [`GraphWriter`]. Used by in-process
/// setups where reads are always cheap-local (single node or
/// full-replica Raft). No parameters.
pub fn execute_with_writer(
    plan: &LogicalPlan,
    store: &RocksDbStorageEngine,
    writer: &dyn GraphWriter,
) -> Result<Vec<Row>> {
    let params = ParamMap::new();
    execute_with_reader(plan, store as &dyn GraphReader, writer, &params)
}

/// Execute a plan with an arbitrary [`GraphReader`] for reads and a
/// parameter map for `$param` resolution. Routing mode uses a
/// partitioned reader that fans out reads across peers; the Bolt
/// listener supplies the driver-bound param map.
pub fn explain(plan: &LogicalPlan) -> Vec<Row> {
    let text = mesh_cypher::format_plan(plan);
    let mut row = Row::new();
    row.insert("plan".to_string(), Value::Property(Property::String(text)));
    vec![row]
}

pub fn profile(plan: &LogicalPlan, store: &RocksDbStorageEngine) -> Result<Vec<Row>> {
    let result_rows = execute(plan, store)?;
    let row_count = result_rows.len() as i64;
    let plan_text = mesh_cypher::format_plan(plan);
    let summary = format!("{plan_text}\nRows: {row_count}");
    let mut row = Row::new();
    row.insert(
        "profile".to_string(),
        Value::Property(Property::String(summary)),
    );
    row.insert(
        "rows".to_string(),
        Value::Property(Property::Int64(row_count)),
    );
    Ok(vec![row])
}

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
    if let Some(rows) = try_execute_ddl(plan, reader, writer)? {
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
fn try_execute_ddl(
    plan: &LogicalPlan,
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
) -> Result<Option<Vec<Row>>> {
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
            // SHOW is a pure read — source it from the reader so
            // overlay/partitioned readers deliver the right view
            // without round-tripping through a writer.
            let specs = reader.list_property_indexes()?;
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
    build_op_inner(plan, None)
}

fn build_op_inner(plan: &LogicalPlan, seed: Option<&Row>) -> Box<dyn Operator> {
    macro_rules! child {
        ($p:expr) => {
            build_op_inner($p, seed)
        };
    }
    match plan {
        LogicalPlan::CreatePath {
            input,
            nodes,
            edges,
        } => Box::new(CreatePathOp::new(
            input.as_ref().map(|p| child!(p)),
            nodes.clone(),
            edges.clone(),
        )),
        LogicalPlan::CartesianProduct { left, right } => {
            Box::new(CartesianProductOp::new(child!(left), (**right).clone()))
        }
        LogicalPlan::Delete {
            input,
            detach,
            vars,
        } => Box::new(DeleteOp::new(child!(input), *detach, vars.clone())),
        LogicalPlan::SetProperty { input, assignments } => {
            Box::new(SetPropertyOp::new(child!(input), assignments.clone()))
        }
        LogicalPlan::Remove { input, items } => {
            Box::new(RemoveOp::new(child!(input), items.clone()))
        }
        LogicalPlan::LoadCsv {
            input,
            path_expr,
            var,
            with_headers,
        } => Box::new(LoadCsvOp::new(
            input.as_ref().map(|p| child!(p)),
            path_expr.clone(),
            var.clone(),
            *with_headers,
        )),
        LogicalPlan::Foreach {
            input,
            var,
            list_expr,
            set_assignments,
            remove_items,
        } => Box::new(ForeachOp::new(
            child!(input),
            var.clone(),
            list_expr.clone(),
            set_assignments.clone(),
            remove_items.clone(),
        )),
        LogicalPlan::CallSubquery { input, body } => {
            Box::new(CallSubqueryOp::new(child!(input), (**body).clone()))
        }
        LogicalPlan::SeedRow => match seed {
            Some(r) => Box::new(SeededRowOp {
                row: Some(r.clone()),
            }),
            None => Box::new(SeedRowOp { done: false }),
        },
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
            child!(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
            edge_type.clone(),
            *direction,
        )),
        LogicalPlan::OptionalEdgeExpand {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            dst_properties,
            edge_type,
            direction,
        } => Box::new(OptionalEdgeExpandOp::new(
            child!(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
            dst_properties.clone(),
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
            path_var,
        } => Box::new(VarLengthExpandOp::new(
            child!(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
            edge_type.clone(),
            *direction,
            *min_hops,
            *max_hops,
            path_var.clone(),
        )),
        LogicalPlan::Filter { input, predicate } => {
            Box::new(FilterOp::new(child!(input), predicate.clone()))
        }
        LogicalPlan::Project { input, items } => {
            Box::new(ProjectOp::new(child!(input), items.clone()))
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
        } => Box::new(AggregateOp::new(
            child!(input),
            group_keys.clone(),
            aggregates.clone(),
        )),
        LogicalPlan::Identity { input } => Box::new(IdentityOp::new(child!(input))),
        LogicalPlan::Distinct { input } => Box::new(DistinctOp::new(child!(input))),
        LogicalPlan::OrderBy { input, sort_items } => {
            Box::new(OrderByOp::new(child!(input), sort_items.clone()))
        }
        LogicalPlan::Skip { input, count } => Box::new(SkipOp::new(child!(input), *count)),
        LogicalPlan::Limit { input, count } => Box::new(LimitOp::new(child!(input), *count)),
        LogicalPlan::MergeNode {
            input,
            var,
            labels,
            properties,
            on_create,
            on_match,
        } => Box::new(MergeNodeOp::new(
            input.as_ref().map(|p| child!(p)),
            var.clone(),
            labels.clone(),
            properties.clone(),
            on_create.clone(),
            on_match.clone(),
        )),
        LogicalPlan::MergeEdge {
            input,
            edge_var,
            src_var,
            dst_var,
            edge_type,
            on_create,
            on_match,
        } => Box::new(MergeEdgeOp::new(
            child!(input),
            edge_var.clone(),
            src_var.clone(),
            dst_var.clone(),
            edge_type.clone(),
            on_create.clone(),
            on_match.clone(),
        )),
        LogicalPlan::Unwind { var, expr } => Box::new(UnwindOp::new(var.clone(), expr.clone())),
        LogicalPlan::UnwindChain { input, var, expr } => {
            Box::new(UnwindChainOp::new(child!(input), var.clone(), expr.clone()))
        }
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
        LogicalPlan::Union { branches, all } => {
            let branch_ops: Vec<Box<dyn Operator>> = branches.iter().map(|b| child!(b)).collect();
            Box::new(UnionOp::new(branch_ops, *all))
        }
        LogicalPlan::BindPath {
            input,
            path_var,
            node_vars,
            edge_vars,
        } => Box::new(BindPathOp::new(
            child!(input),
            path_var.clone(),
            node_vars.clone(),
            edge_vars.clone(),
        )),
        LogicalPlan::ShortestPath {
            input,
            src_var,
            dst_var,
            path_var,
            edge_type,
            direction,
            max_hops,
            kind,
        } => Box::new(ShortestPathOp::new(
            child!(input),
            src_var.clone(),
            dst_var.clone(),
            path_var.clone(),
            edge_type.clone(),
            *direction,
            *max_hops,
            *kind,
        )),
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
            let ectx = EvalCtx {
                row: &empty,
                params: ctx.params,
                reader: ctx.store,
            };
            let val = eval_expr(&self.expr, &ectx)?;
            self.items = Some(coerce_unwind_list(val)?);
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

/// Per-row UNWIND: pulls one input row, evaluates `expr` against it
/// to produce a list, and emits one output row per list element.
/// Each output row inherits every binding from the input row plus
/// a new `var` binding. Empty / null lists drop the input row and
/// the operator immediately pulls the next input row.
struct UnwindChainOp {
    input: Box<dyn Operator>,
    var: String,
    expr: Expr,
    current_row: Option<Row>,
    items: Vec<Value>,
    cursor: usize,
}

impl UnwindChainOp {
    fn new(input: Box<dyn Operator>, var: String, expr: Expr) -> Self {
        Self {
            input,
            var,
            expr,
            current_row: None,
            items: Vec::new(),
            cursor: 0,
        }
    }
}

impl Operator for UnwindChainOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            if let Some(base) = &self.current_row {
                if self.cursor < self.items.len() {
                    let v = self.items[self.cursor].clone();
                    self.cursor += 1;
                    let mut row = base.clone();
                    row.insert(self.var.clone(), v);
                    return Ok(Some(row));
                }
                self.current_row = None;
                self.items.clear();
                self.cursor = 0;
            }
            let base = match self.input.next(ctx)? {
                Some(r) => r,
                None => return Ok(None),
            };
            let ectx = EvalCtx {
                row: &base,
                params: ctx.params,
                reader: ctx.store,
            };
            let val = eval_expr(&self.expr, &ectx)?;
            self.items = coerce_unwind_list(val)?;
            self.current_row = Some(base);
        }
    }
}

/// Shared list-coercion used by both UNWIND operators. Accepts a
/// native executor `Value::List`, a property-list
/// `Property::List`, or `Null` (treated as an empty list). Any
/// other shape is a type error — UNWIND is defined over lists.
fn coerce_unwind_list(val: Value) -> Result<Vec<Value>> {
    match val {
        Value::List(items) => Ok(items),
        Value::Property(Property::List(props)) => {
            Ok(props.into_iter().map(Value::Property).collect())
        }
        Value::Null => Ok(Vec::new()),
        _ => Err(Error::TypeMismatch),
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
                        let value = eval_expr(expr, &ctx.eval_ctx(row))?;
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
                            let evaluated = eval_expr(value, &ctx.eval_ctx(&row))?;
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
                                    let v = eval_expr(expr, &ctx.eval_ctx(&row))?;
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
                                    let v = eval_expr(expr, &ctx.eval_ctx(&row))?;
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

struct RemoveOp {
    input: Box<dyn Operator>,
    items: Vec<RemoveSpec>,
}

impl RemoveOp {
    fn new(input: Box<dyn Operator>, items: Vec<RemoveSpec>) -> Self {
        Self { input, items }
    }
}

impl Operator for RemoveOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        match self.input.next(ctx)? {
            None => Ok(None),
            Some(mut row) => {
                let mut updated_nodes: HashSet<String> = HashSet::new();
                let mut updated_edges: HashSet<String> = HashSet::new();
                for item in &self.items {
                    match item {
                        RemoveSpec::Property { var, key } => match row.get_mut(var) {
                            Some(Value::Node(n)) => {
                                n.properties.remove(key);
                                updated_nodes.insert(var.clone());
                            }
                            Some(Value::Edge(e)) => {
                                e.properties.remove(key);
                                updated_edges.insert(var.clone());
                            }
                            _ => return Err(Error::UnboundVariable(var.clone())),
                        },
                        RemoveSpec::Labels { var, labels } => match row.get_mut(var) {
                            Some(Value::Node(n)) => {
                                n.labels.retain(|l| !labels.contains(l));
                                updated_nodes.insert(var.clone());
                            }
                            _ => return Err(Error::UnboundVariable(var.clone())),
                        },
                    }
                }
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

struct LoadCsvOp {
    input: Option<Box<dyn Operator>>,
    path_expr: Expr,
    var: String,
    with_headers: bool,
    rows: Option<Vec<Value>>,
    cursor: usize,
}

impl LoadCsvOp {
    fn new(
        input: Option<Box<dyn Operator>>,
        path_expr: Expr,
        var: String,
        with_headers: bool,
    ) -> Self {
        Self {
            input,
            path_expr,
            var,
            with_headers,
            rows: None,
            cursor: 0,
        }
    }

    fn load(&mut self, ctx: &ExecCtx, base_row: &Row) -> Result<()> {
        let ectx = ctx.eval_ctx(base_row);
        let path_val = eval_expr(&self.path_expr, &ectx)?;
        let path = match path_val {
            Value::Property(Property::String(s)) => s,
            _ => return Err(Error::TypeMismatch),
        };
        let content = std::fs::read_to_string(&path).map_err(|e| {
            Error::Unsupported(format!("LOAD CSV: cannot read file '{}': {}", path, e))
        })?;
        let mut lines = content.lines();
        let headers: Option<Vec<String>> = if self.with_headers {
            lines
                .next()
                .map(|h| h.split(',').map(|s| s.trim().to_string()).collect())
        } else {
            None
        };
        let mut csv_rows = Vec::new();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
            if let Some(hdrs) = &headers {
                let mut map = std::collections::HashMap::new();
                for (i, h) in hdrs.iter().enumerate() {
                    let val = fields.get(i).cloned().unwrap_or_default();
                    map.insert(h.clone(), Property::String(val));
                }
                csv_rows.push(Value::Property(Property::Map(map)));
            } else {
                let list: Vec<Value> = fields
                    .into_iter()
                    .map(|f| Value::Property(Property::String(f)))
                    .collect();
                csv_rows.push(Value::List(list));
            }
        }
        self.rows = Some(csv_rows);
        self.cursor = 0;
        Ok(())
    }
}

impl Operator for LoadCsvOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.rows.is_none() {
            let base = if let Some(input) = &mut self.input {
                match input.next(ctx)? {
                    Some(r) => r,
                    None => return Ok(None),
                }
            } else {
                Row::new()
            };
            self.load(ctx, &base)?;
        }
        let rows = self.rows.as_ref().unwrap();
        if self.cursor < rows.len() {
            let val = rows[self.cursor].clone();
            self.cursor += 1;
            let mut row = Row::new();
            row.insert(self.var.clone(), val);
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

struct ForeachOp {
    input: Box<dyn Operator>,
    var: String,
    list_expr: Expr,
    set_assignments: Vec<SetAssignment>,
    remove_items: Vec<RemoveSpec>,
}

impl ForeachOp {
    fn new(
        input: Box<dyn Operator>,
        var: String,
        list_expr: Expr,
        set_assignments: Vec<SetAssignment>,
        remove_items: Vec<RemoveSpec>,
    ) -> Self {
        Self {
            input,
            var,
            list_expr,
            set_assignments,
            remove_items,
        }
    }
}

impl Operator for ForeachOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        let Some(row) = self.input.next(ctx)? else {
            return Ok(None);
        };
        let ectx = ctx.eval_ctx(&row);
        let list_val = eval_expr(&self.list_expr, &ectx)?;
        let items = match list_val {
            Value::List(items) => items,
            Value::Property(Property::List(props)) => {
                props.into_iter().map(Value::Property).collect()
            }
            Value::Null | Value::Property(Property::Null) => Vec::new(),
            _ => return Err(Error::TypeMismatch),
        };
        for item in items {
            let mut scratch = row.clone();
            scratch.insert(self.var.clone(), item);
            for a in &self.set_assignments {
                match a {
                    SetAssignment::Property { var, key, value } => {
                        let evaluated = eval_expr(value, &ctx.eval_ctx(&scratch))?;
                        let prop = value_to_property(evaluated)?;
                        match scratch.get_mut(var) {
                            Some(Value::Node(n)) => {
                                n.properties.insert(key.clone(), prop);
                            }
                            Some(Value::Edge(e)) => {
                                e.properties.insert(key.clone(), prop);
                            }
                            _ => return Err(Error::UnboundVariable(var.clone())),
                        }
                    }
                    SetAssignment::Labels { var, labels } => {
                        if let Some(Value::Node(n)) = scratch.get_mut(var) {
                            for l in labels {
                                if !n.labels.contains(l) {
                                    n.labels.push(l.clone());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            for ri in &self.remove_items {
                match ri {
                    RemoveSpec::Property { var, key } => {
                        if let Some(Value::Node(n)) = scratch.get_mut(var) {
                            n.properties.remove(key);
                        } else if let Some(Value::Edge(e)) = scratch.get_mut(var) {
                            e.properties.remove(key);
                        }
                    }
                    RemoveSpec::Labels { var, labels } => {
                        if let Some(Value::Node(n)) = scratch.get_mut(var) {
                            n.labels.retain(|l| !labels.contains(l));
                        }
                    }
                }
            }
            // Flush mutated entities for each iteration
            for (_, val) in scratch.iter() {
                match val {
                    Value::Node(n) => ctx.writer.put_node(n)?,
                    Value::Edge(e) => ctx.writer.put_edge(e)?,
                    _ => {}
                }
            }
        }
        Ok(Some(row))
    }
}

struct SeedRowOp {
    done: bool,
}

impl Operator for SeedRowOp {
    fn next(&mut self, _ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        Ok(Some(Row::new()))
    }
}

struct SeededRowOp {
    row: Option<Row>,
}

impl Operator for SeededRowOp {
    fn next(&mut self, _ctx: &ExecCtx) -> Result<Option<Row>> {
        Ok(self.row.take())
    }
}

struct CallSubqueryOp {
    input: Box<dyn Operator>,
    body_plan: LogicalPlan,
    pending: Vec<Row>,
    pending_idx: usize,
}

impl CallSubqueryOp {
    fn new(input: Box<dyn Operator>, body_plan: LogicalPlan) -> Self {
        Self {
            input,
            body_plan,
            pending: Vec::new(),
            pending_idx: 0,
        }
    }
}

impl Operator for CallSubqueryOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            if self.pending_idx < self.pending.len() {
                let row = self.pending[self.pending_idx].clone();
                self.pending_idx += 1;
                return Ok(Some(row));
            }
            let outer_row = match self.input.next(ctx)? {
                Some(r) => r,
                None => return Ok(None),
            };
            let mut body_op = build_op_inner(&self.body_plan, Some(&outer_row));
            let mut results = Vec::new();
            while let Some(body_row) = body_op.next(ctx)? {
                let mut merged = outer_row.clone();
                for (k, v) in body_row {
                    merged.insert(k, v);
                }
                results.push(merged);
            }
            if results.is_empty() {
                continue;
            }
            self.pending = results;
            self.pending_idx = 0;
        }
    }
}

fn value_to_property(v: Value) -> Result<Property> {
    match v {
        Value::Property(p) => Ok(p),
        Value::Null => Ok(Property::Null),
        Value::Node(_) | Value::Edge(_) | Value::List(_) | Value::Path { .. } => {
            Err(Error::InvalidSetValue)
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
            let value = eval_expr(&self.value_expr, &ctx.eval_ctx(&empty))?;
            let property = match value {
                Value::Property(p) => p,
                Value::Null => Property::Null,
                Value::Node(_) | Value::Edge(_) | Value::List(_) | Value::Path { .. } => {
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
    /// `ON CREATE SET ...` assignments — applied only when the
    /// merge took the create branch. Evaluated against a row
    /// `{var → Node}` so the value expressions can reference the
    /// just-created node.
    on_create: Vec<SetAssignment>,
    /// `ON MATCH SET ...` assignments — applied to every matched
    /// node when the merge took the match branch. Same row shape
    /// as `on_create`.
    on_match: Vec<SetAssignment>,
    /// Optional upstream operator. `None` means this is a
    /// top-level producer (`MERGE (n) RETURN n`) and emits
    /// rows with a fresh empty base. `Some` means this is a
    /// mid-chain clause (`MATCH (a) MERGE (b) RETURN a, b`)
    /// and each emitted row is a cross-join between an input
    /// row and a merge-result node.
    input: Option<Box<dyn Operator>>,
    /// Cached merge result. Populated on the first `next()`
    /// call by running the scan + maybe-create logic *once*,
    /// then reused for every input row. Running the merge
    /// exactly once sidesteps the read-after-write issue in
    /// buffered-writer mode (a node created by the first input
    /// row wouldn't be visible to a re-scan on the second).
    merged_nodes: Vec<Node>,
    /// Whether `merged_nodes` has been populated. The merge
    /// logic runs lazily on the first `next()` so
    /// `ExecCtx::params` is available.
    merge_done: bool,
    /// Current upstream row — held between calls while we drain
    /// `merged_nodes` against it. `None` for the top-level
    /// case (no input).
    current_input_row: Option<Row>,
    cursor: usize,
}

impl MergeNodeOp {
    fn new(
        input: Option<Box<dyn Operator>>,
        var: String,
        labels: Vec<String>,
        properties: Vec<(String, Expr)>,
        on_create: Vec<SetAssignment>,
        on_match: Vec<SetAssignment>,
    ) -> Self {
        Self {
            var,
            labels,
            properties,
            on_create,
            on_match,
            input,
            merged_nodes: Vec::new(),
            merge_done: false,
            current_input_row: None,
            cursor: 0,
        }
    }

    /// Run the MERGE logic exactly once: scan the store for
    /// existing matches, apply ON MATCH SET to each; or create
    /// a fresh node and apply ON CREATE SET; persist everything
    /// via `ctx.writer`; stash the resulting nodes in
    /// `self.merged_nodes`. Idempotent — subsequent calls are
    /// no-ops once `self.merge_done` is set.
    fn run_merge_once(&mut self, ctx: &ExecCtx) -> Result<()> {
        if self.merge_done {
            return Ok(());
        }
        // Evaluate the pattern property expressions (literals or
        // parameters) once against an empty row + the per-query param
        // map. The grammar guarantees no row references in pattern
        // values, so an empty row is sufficient.
        let empty = Row::new();
        let resolved_props: Vec<(String, Property)> = self
            .properties
            .iter()
            .map(|(k, expr)| {
                let v = eval_expr(expr, &ctx.eval_ctx(&empty))?;
                Ok((k.clone(), value_to_property(v)?))
            })
            .collect::<Result<Vec<_>>>()?;

        // Scan for existing matches: by-label index when one is
        // given, else a full node scan. Any extra labels beyond
        // the first get filtered per-node.
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
                    self.merged_nodes.push(node);
                }
            }
        }

        if self.merged_nodes.is_empty() {
            // Create path — no match, synthesize one node,
            // apply ON CREATE SET, persist.
            let mut node = Node::new();
            for label in &self.labels {
                node.labels.push(label.clone());
            }
            for (k, prop) in resolved_props {
                node.properties.insert(k, prop);
            }
            apply_merge_actions(&mut node, &self.on_create, &self.var, ctx)?;
            ctx.writer.put_node(&node)?;
            self.merged_nodes.push(node);
        } else if !self.on_match.is_empty() {
            // Match path with ON MATCH SET — apply to every
            // matched node and persist each. The outer `if !is_empty`
            // skips the loop in the common no-ON-MATCH case.
            for node in self.merged_nodes.iter_mut() {
                apply_merge_actions(node, &self.on_match, &self.var, ctx)?;
                ctx.writer.put_node(node)?;
            }
        }
        self.merge_done = true;
        Ok(())
    }
}

impl Operator for MergeNodeOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        self.run_merge_once(ctx)?;

        // Top-level case (no upstream): drain merged_nodes once.
        // The output row starts empty and gets the merged node
        // bound into `var`.
        if self.input.is_none() {
            if self.cursor < self.merged_nodes.len() {
                let node = self.merged_nodes[self.cursor].clone();
                self.cursor += 1;
                let mut row = Row::new();
                row.insert(self.var.clone(), Value::Node(node));
                return Ok(Some(row));
            }
            return Ok(None);
        }

        // Input-driven case: each merged node cross-joins with
        // every upstream row. The loop alternates between
        // "drain merged_nodes against the current upstream row"
        // and "pull the next upstream row". Because merged_nodes
        // is computed once (by run_merge_once), the node set is
        // the same for every input row — the only thing that
        // changes row-to-row is `current_input_row`.
        loop {
            if let Some(base) = self.current_input_row.as_ref() {
                if self.cursor < self.merged_nodes.len() {
                    let node = self.merged_nodes[self.cursor].clone();
                    self.cursor += 1;
                    let mut row = base.clone();
                    row.insert(self.var.clone(), Value::Node(node));
                    return Ok(Some(row));
                }
            }
            // Pull a fresh upstream row and restart the drain.
            match self.input.as_mut().unwrap().next(ctx)? {
                None => return Ok(None),
                Some(row) => {
                    self.current_input_row = Some(row);
                    self.cursor = 0;
                }
            }
        }
    }
}

/// Apply MERGE-conditional SET assignments (`ON CREATE` or
/// Find-or-create executor for edge MERGE
/// (`MERGE (a)-[r:KNOWS]->(b)`).
///
/// For every row pulled from `input`, looks up the `src_var`
/// and `dst_var` bindings (which must be `Value::Node` — the
/// planner enforces that they came from a prior MATCH or
/// MERGE), scans `src`'s outgoing edges, and either:
///
/// - Picks the first edge of type `edge_type` whose target is
///   `dst` and applies `on_match` to it, or
/// - Creates a fresh `Edge::new(edge_type, src, dst)`, applies
///   `on_create`, and persists it via `ctx.writer.put_edge`.
///
/// Either way, the resulting edge is bound into `edge_var` in
/// the output row and the row is emitted. v1 restrictions:
/// single directed hop, both endpoints already bound,
/// explicit relationship type.
struct MergeEdgeOp {
    input: Box<dyn Operator>,
    edge_var: String,
    src_var: String,
    dst_var: String,
    edge_type: String,
    on_create: Vec<SetAssignment>,
    on_match: Vec<SetAssignment>,
}

impl MergeEdgeOp {
    fn new(
        input: Box<dyn Operator>,
        edge_var: String,
        src_var: String,
        dst_var: String,
        edge_type: String,
        on_create: Vec<SetAssignment>,
        on_match: Vec<SetAssignment>,
    ) -> Self {
        Self {
            input,
            edge_var,
            src_var,
            dst_var,
            edge_type,
            on_create,
            on_match,
        }
    }
}

impl Operator for MergeEdgeOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            let Some(mut row) = self.input.next(ctx)? else {
                return Ok(None);
            };
            // Resolve src/dst. Both must be Value::Node — the
            // planner enforces that the variables came from an
            // earlier producer, so anything else is a bug or a
            // later-added feature that didn't update the check.
            let src_node = match row.get(&self.src_var) {
                Some(Value::Node(n)) => n.clone(),
                _ => return Err(Error::UnboundVariable(self.src_var.clone())),
            };
            let dst_node = match row.get(&self.dst_var) {
                Some(Value::Node(n)) => n.clone(),
                _ => return Err(Error::UnboundVariable(self.dst_var.clone())),
            };

            // Scan outgoing edges from src, looking for one
            // that targets dst with the matching type.
            let mut matched_edge: Option<Edge> = None;
            for (edge_id, neighbor_id) in ctx.store.outgoing(src_node.id)? {
                if neighbor_id != dst_node.id {
                    continue;
                }
                if let Some(edge) = ctx.store.get_edge(edge_id)? {
                    if edge.edge_type == self.edge_type {
                        matched_edge = Some(edge);
                        break;
                    }
                }
            }

            let edge = if let Some(mut existing) = matched_edge {
                // Match branch: apply ON MATCH SET (if any)
                // and persist if we touched anything.
                if !self.on_match.is_empty() {
                    apply_merge_edge_actions(&mut existing, &self.on_match, &self.edge_var, ctx)?;
                    ctx.writer.put_edge(&existing)?;
                }
                existing
            } else {
                // Create branch: synthesize a new edge, apply
                // ON CREATE SET, persist.
                let mut new_edge = Edge::new(&self.edge_type, src_node.id, dst_node.id);
                apply_merge_edge_actions(&mut new_edge, &self.on_create, &self.edge_var, ctx)?;
                ctx.writer.put_edge(&new_edge)?;
                new_edge
            };

            row.insert(self.edge_var.clone(), Value::Edge(edge));
            return Ok(Some(row));
        }
    }
}

/// Edge-side counterpart of [`apply_merge_actions`]. Evaluates
/// each `SetAssignment` against a temporary row carrying the
/// current edge, mutates the edge in place, and returns. Only
/// property assignments are meaningful on edges (edges have no
/// labels in Mesh's model); a Labels assignment against an
/// edge-bound variable surfaces as `Error::UnboundVariable`
/// via the mismatched-var check below.
fn apply_merge_edge_actions(
    edge: &mut Edge,
    actions: &[SetAssignment],
    var: &str,
    exec_ctx: &ExecCtx,
) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    let mut row = Row::new();
    row.insert(var.to_string(), Value::Edge(edge.clone()));
    for action in actions {
        let sub_ctx = exec_ctx.eval_ctx(&row);
        match action {
            SetAssignment::Property {
                var: target,
                key,
                value,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                let evaluated = eval_expr(value, &sub_ctx)?;
                let prop = value_to_property(evaluated)?;
                edge.properties.insert(key.clone(), prop);
                row.insert(var.to_string(), Value::Edge(edge.clone()));
            }
            SetAssignment::Merge {
                var: target,
                properties,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                let resolved: Vec<(String, Property)> = properties
                    .iter()
                    .map(|(k, expr)| {
                        let v = eval_expr(expr, &sub_ctx)?;
                        Ok((k.clone(), value_to_property(v)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for (k, p) in resolved {
                    edge.properties.insert(k, p);
                }
                row.insert(var.to_string(), Value::Edge(edge.clone()));
            }
            SetAssignment::Replace {
                var: target,
                properties,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                let resolved: Vec<(String, Property)> = properties
                    .iter()
                    .map(|(k, expr)| {
                        let v = eval_expr(expr, &sub_ctx)?;
                        Ok((k.clone(), value_to_property(v)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                edge.properties.clear();
                for (k, p) in resolved {
                    edge.properties.insert(k, p);
                }
                row.insert(var.to_string(), Value::Edge(edge.clone()));
            }
            // Labels on edges don't exist — reject explicitly.
            SetAssignment::Labels { var: target, .. } => {
                return Err(Error::UnboundVariable(target.clone()));
            }
        }
    }
    Ok(())
}

/// `ON MATCH`) to `node` in place. Mirrors the `SetPropertyOp`
/// dispatch but specialized to a single bound variable so we
/// don't have to materialize a full row dispatcher.
///
/// Value expressions are evaluated against a temporary row
/// `{var → Node(node.clone())}` so the RHS can reference the
/// node's existing properties — `MERGE (n) ON MATCH SET n.hits = n.hits + 1`
/// works the same as if the SET ran in a SetPropertyOp pipeline.
fn apply_merge_actions(
    node: &mut Node,
    actions: &[SetAssignment],
    var: &str,
    exec_ctx: &ExecCtx,
) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    let mut row = Row::new();
    row.insert(var.to_string(), Value::Node(node.clone()));
    for action in actions {
        let sub_ctx = exec_ctx.eval_ctx(&row);
        match action {
            SetAssignment::Property {
                var: target,
                key,
                value,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                let evaluated = eval_expr(value, &sub_ctx)?;
                let prop = value_to_property(evaluated)?;
                node.properties.insert(key.clone(), prop);
                row.insert(var.to_string(), Value::Node(node.clone()));
            }
            SetAssignment::Labels {
                var: target,
                labels,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                for label in labels {
                    if !node.labels.contains(label) {
                        node.labels.push(label.clone());
                    }
                }
                row.insert(var.to_string(), Value::Node(node.clone()));
            }
            SetAssignment::Replace {
                var: target,
                properties,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                let resolved: Vec<(String, Property)> = properties
                    .iter()
                    .map(|(k, expr)| {
                        let v = eval_expr(expr, &sub_ctx)?;
                        Ok((k.clone(), value_to_property(v)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                node.properties.clear();
                for (k, p) in resolved {
                    node.properties.insert(k, p);
                }
                row.insert(var.to_string(), Value::Node(node.clone()));
            }
            SetAssignment::Merge {
                var: target,
                properties,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                let resolved: Vec<(String, Property)> = properties
                    .iter()
                    .map(|(k, expr)| {
                        let v = eval_expr(expr, &sub_ctx)?;
                        Ok((k.clone(), value_to_property(v)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for (k, p) in resolved {
                    node.properties.insert(k, p);
                }
                row.insert(var.to_string(), Value::Node(node.clone()));
            }
        }
    }
    Ok(())
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

/// Left-join variant of [`EdgeExpandOp`]. For each input row,
/// expands the adjacency in the configured direction and
/// filters by `edge_type` / `dst_labels` — if **any** neighbor
/// survives the filters, emits rows exactly like
/// `EdgeExpandOp`. If **zero** neighbors survive, emits one row
/// that carries the input row's bindings plus `edge_var` /
/// `dst_var` set to `Value::Null`, preserving the input row in
/// the output stream. This is the left-outer-join semantics
/// OPTIONAL MATCH needs.
///
/// Tracks per-input-row whether any output was produced so the
/// fallback Null row is only emitted after the pending buffer
/// drains without yielding anything. The `yielded_for_current`
/// flag is reset whenever a new input row is pulled.
struct OptionalEdgeExpandOp {
    input: Box<dyn Operator>,
    src_var: String,
    edge_var: Option<String>,
    dst_var: String,
    dst_labels: Vec<String>,
    dst_properties: Vec<(String, Expr)>,
    edge_type: Option<String>,
    direction: Direction,
    current_row: Option<Row>,
    pending: Vec<(EdgeId, NodeId)>,
    pending_idx: usize,
    yielded_for_current: bool,
}

impl OptionalEdgeExpandOp {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Box<dyn Operator>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_labels: Vec<String>,
        dst_properties: Vec<(String, Expr)>,
        edge_type: Option<String>,
        direction: Direction,
    ) -> Self {
        Self {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            dst_properties,
            edge_type,
            direction,
            current_row: None,
            pending: Vec::new(),
            pending_idx: 0,
            yielded_for_current: false,
        }
    }
}

impl Operator for OptionalEdgeExpandOp {
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
                if !self.dst_properties.is_empty() {
                    let base = self
                        .current_row
                        .as_ref()
                        .expect("pending without source row");
                    let ectx = ctx.eval_ctx(base);
                    let mut props_ok = true;
                    for (key, expr) in &self.dst_properties {
                        let expected = eval_expr(expr, &ectx)?;
                        let actual = neighbor
                            .properties
                            .get(key)
                            .cloned()
                            .map(Value::Property)
                            .unwrap_or(Value::Null);
                        if !values_equal(&expected, &actual) {
                            props_ok = false;
                            break;
                        }
                    }
                    if !props_ok {
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
                self.yielded_for_current = true;
                return Ok(Some(out));
            }

            // Pending drained for the current row. If nothing was
            // yielded, emit the left-join fallback: preserve the
            // input row with the optional variables set to Null.
            if let Some(base) = self.current_row.take() {
                if !self.yielded_for_current {
                    let mut out = base;
                    if let Some(ev) = &self.edge_var {
                        out.insert(ev.clone(), Value::Null);
                    }
                    out.insert(self.dst_var.clone(), Value::Null);
                    self.yielded_for_current = true;
                    return Ok(Some(out));
                }
            }

            match self.input.next(ctx)? {
                None => return Ok(None),
                Some(row) => {
                    let src_id = match row.get(&self.src_var) {
                        Some(Value::Node(n)) => n.id,
                        // src_var is Null (because a prior
                        // OPTIONAL MATCH chained before this one
                        // Null-bound it). Skip adjacency entirely
                        // and fall through to the fallback Null
                        // row so downstream clauses see the
                        // preserved input.
                        Some(Value::Null) => {
                            self.pending = Vec::new();
                            self.pending_idx = 0;
                            self.yielded_for_current = false;
                            self.current_row = Some(row);
                            continue;
                        }
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
                    self.yielded_for_current = false;
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
    path_var: Option<String>,
    current_row: Option<Row>,
    pending_paths: Vec<Vec<Edge>>,
    pending_node_paths: Vec<Vec<NodeId>>,
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
        path_var: Option<String>,
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
            path_var,
            current_row: None,
            pending_paths: Vec::new(),
            pending_node_paths: Vec::new(),
            pending_targets: Vec::new(),
            pending_idx: 0,
        }
    }

    fn enumerate(
        &self,
        ctx: &ExecCtx,
        start: NodeId,
    ) -> Result<(Vec<Vec<Edge>>, Vec<Vec<NodeId>>, Vec<NodeId>)> {
        let mut paths: Vec<Vec<Edge>> = Vec::new();
        let mut node_paths: Vec<Vec<NodeId>> = Vec::new();
        let mut targets: Vec<NodeId> = Vec::new();
        let mut edge_buf: Vec<Edge> = Vec::new();
        let mut node_buf: Vec<NodeId> = vec![start];
        let mut used: HashSet<EdgeId> = HashSet::new();
        self.dfs(
            ctx,
            start,
            &mut edge_buf,
            &mut node_buf,
            &mut used,
            &mut paths,
            &mut node_paths,
            &mut targets,
        )?;
        Ok((paths, node_paths, targets))
    }

    #[allow(clippy::too_many_arguments)]
    fn dfs(
        &self,
        ctx: &ExecCtx,
        current_node: NodeId,
        edge_buf: &mut Vec<Edge>,
        node_buf: &mut Vec<NodeId>,
        used: &mut HashSet<EdgeId>,
        out_paths: &mut Vec<Vec<Edge>>,
        out_node_paths: &mut Vec<Vec<NodeId>>,
        out_targets: &mut Vec<NodeId>,
    ) -> Result<()> {
        let depth = edge_buf.len() as u64;

        if depth >= self.min_hops && depth <= self.max_hops {
            let terminal_ok = match ctx.store.get_node(current_node)? {
                Some(node) => has_all_labels(&node, &self.dst_labels),
                None => false,
            };
            if terminal_ok {
                out_paths.push(edge_buf.clone());
                out_node_paths.push(node_buf.clone());
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
            edge_buf.push(edge);
            node_buf.push(neighbor_id);
            self.dfs(
                ctx,
                neighbor_id,
                edge_buf,
                node_buf,
                used,
                out_paths,
                out_node_paths,
                out_targets,
            )?;
            edge_buf.pop();
            node_buf.pop();
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
                out.insert(self.dst_var.clone(), Value::Node(target.clone()));
                if let Some(ev) = &self.edge_var {
                    let edges: Vec<Value> = self.pending_paths[i]
                        .iter()
                        .cloned()
                        .map(Value::Edge)
                        .collect();
                    out.insert(ev.clone(), Value::List(edges));
                }
                if let Some(pv) = &self.path_var {
                    let mut nodes = Vec::with_capacity(self.pending_node_paths[i].len());
                    for nid in &self.pending_node_paths[i] {
                        match ctx.store.get_node(*nid)? {
                            Some(n) => nodes.push(n),
                            None => continue,
                        }
                    }
                    let edges = self.pending_paths[i].clone();
                    out.insert(pv.clone(), Value::Path { nodes, edges });
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
                    let (paths, node_paths, targets) = self.enumerate(ctx, src_id)?;
                    self.pending_paths = paths;
                    self.pending_node_paths = node_paths;
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
            let v = eval_expr(&self.predicate, &ctx.eval_ctx(&row))?;
            if to_bool(&v)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

/// Pass-through operator for `RETURN *` / `WITH *`. Forwards every
/// row from the input unchanged.
struct IdentityOp {
    input: Box<dyn Operator>,
}

impl IdentityOp {
    fn new(input: Box<dyn Operator>) -> Self {
        Self { input }
    }
}

impl Operator for IdentityOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        self.input.next(ctx)
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
                    let value = eval_expr(&item.expr, &ctx.eval_ctx(&row))?;
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
        Expr::PropertyAccess { base, key } => {
            format!("{}.{}", default_name(base, idx), key)
        }
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

/// Assemble a `Value::Path` from a row's already-bound node and
/// edge variables. Emitted by `plan_pattern` when the source
/// pattern carries `path_var`. The operator pulls `node_vars[i]`
/// and `edge_vars[i]` out of every row, walks them in order, and
/// inserts the result into the row under `path_var` before
/// forwarding downstream.
///
/// Rows where any referenced variable is missing or not the
/// expected Node/Edge shape get a `Value::Null` at `path_var` —
/// matching how `OPTIONAL MATCH` flows null bindings through the
/// downstream projection without hard-erroring. Missing bindings
/// shouldn't normally happen (the planner fills in synthetic
/// names via `ensure_path_bindings`), but the null fallback
/// means an unexpected upstream shape degrades gracefully
/// instead of crashing the whole query.
struct BindPathOp {
    input: Box<dyn Operator>,
    path_var: String,
    node_vars: Vec<String>,
    edge_vars: Vec<String>,
}

impl BindPathOp {
    fn new(
        input: Box<dyn Operator>,
        path_var: String,
        node_vars: Vec<String>,
        edge_vars: Vec<String>,
    ) -> Self {
        Self {
            input,
            path_var,
            node_vars,
            edge_vars,
        }
    }
}

impl Operator for BindPathOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        let Some(mut row) = self.input.next(ctx)? else {
            return Ok(None);
        };
        // Extract the ordered node + edge sequence from the row.
        // Any missing or wrong-shaped entry collapses the whole
        // path binding to null and falls through.
        let mut nodes: Vec<mesh_core::Node> = Vec::new();
        let mut edges: Vec<mesh_core::Edge> = Vec::new();
        let mut abort = false;
        // Interleave node/edge vars. For each hop i:
        //   node_vars[i] = start/intermediate node
        //   edge_vars[i] = edge (or sub-path for var-length)
        //   node_vars[i+1] = target node
        // For var-length hops, edge_vars[i] may contain a
        // Value::Path — splice its interior into the running path.
        if let Some(Value::Node(n)) = row.get(&self.node_vars[0]) {
            nodes.push(n.clone());
        } else {
            abort = true;
        }
        if !abort {
            for (i, ev) in self.edge_vars.iter().enumerate() {
                match row.get(ev) {
                    Some(Value::Edge(e)) => {
                        edges.push(e.clone());
                        match row.get(&self.node_vars[i + 1]) {
                            Some(Value::Node(n)) => nodes.push(n.clone()),
                            _ => {
                                abort = true;
                                break;
                            }
                        }
                    }
                    Some(Value::Path {
                        nodes: sub_nodes,
                        edges: sub_edges,
                    }) => {
                        // Splice the sub-path. The sub-path's first
                        // node is the same as nodes.last() (already
                        // pushed), so skip it. All sub-edges go in.
                        // Sub-path interior nodes go in. The sub-path's
                        // last node is the target for this hop.
                        edges.extend(sub_edges.iter().cloned());
                        if sub_nodes.len() > 1 {
                            nodes.extend(sub_nodes[1..].iter().cloned());
                        }
                    }
                    _ => {
                        abort = true;
                        break;
                    }
                }
            }
        }
        if abort {
            row.insert(self.path_var.clone(), Value::Null);
        } else {
            row.insert(self.path_var.clone(), Value::Path { nodes, edges });
        }
        Ok(Some(row))
    }
}

/// `MATCH p = shortestPath((a)-[:R*..N]->(b))`. For each input
/// row (which must already bind both `src_var` and `dst_var`
/// to `Value::Node`), runs a breadth-first search from the
/// source node toward the target, filtering edges by
/// `edge_type` and walking up to `max_hops` steps. Emits one
/// row per successful search with `path_var` set to a
/// `Value::Path` carrying the traversed node/edge sequence;
/// rows where BFS finds no path are dropped entirely (matching
/// Cypher's `MATCH` semantics for an unsatisfiable pattern).
///
/// The BFS uses classic parent-pointer reconstruction: each
/// visited node's `(parent_node_id, edge_id)` pair is stored
/// in a hashmap, and once the target is reached we walk back
/// to the source, reversing the accumulated edges to produce
/// a forward-order path. Cycle detection is a side-effect of
/// the visited set — the first time BFS sees a node is
/// necessarily via a shortest path, so later visits are
/// ignored.
struct ShortestPathOp {
    input: Box<dyn Operator>,
    src_var: String,
    dst_var: String,
    path_var: String,
    edge_type: Option<String>,
    direction: mesh_cypher::Direction,
    max_hops: u64,
    kind: mesh_cypher::ShortestKind,
    /// Buffered `(base_row, path)` pairs from the current
    /// input row, used only by `AllShortest` mode where a
    /// single input can expand into multiple shortest paths
    /// of the same length. Each call to `next` drains one
    /// entry before pulling a fresh input row; in `Shortest`
    /// mode the buffer is always empty or has one element.
    pending: std::collections::VecDeque<(Row, Value)>,
}

impl ShortestPathOp {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Box<dyn Operator>,
        src_var: String,
        dst_var: String,
        path_var: String,
        edge_type: Option<String>,
        direction: mesh_cypher::Direction,
        max_hops: u64,
        kind: mesh_cypher::ShortestKind,
    ) -> Self {
        Self {
            input,
            src_var,
            dst_var,
            path_var,
            edge_type,
            direction,
            max_hops,
            kind,
            pending: std::collections::VecDeque::new(),
        }
    }
}

impl Operator for ShortestPathOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            // Drain buffered paths from a prior input row
            // before pulling the next one. Only reachable in
            // `AllShortest` mode; `Shortest` mode's buffer
            // never accumulates more than one entry.
            if let Some((mut row, path)) = self.pending.pop_front() {
                row.insert(self.path_var.clone(), path);
                return Ok(Some(row));
            }
            let Some(row) = self.input.next(ctx)? else {
                return Ok(None);
            };
            let src = match row.get(&self.src_var) {
                Some(Value::Node(n)) => n.clone(),
                _ => continue,
            };
            let dst = match row.get(&self.dst_var) {
                Some(Value::Node(n)) => n.clone(),
                _ => continue,
            };
            let paths = bfs_shortest_paths(
                &src,
                &dst,
                self.edge_type.as_deref(),
                self.direction,
                self.max_hops,
                self.kind,
                ctx.store,
            )?;
            if paths.is_empty() {
                // No path of length ≤ max_hops — drop the row.
                continue;
            }
            for path in paths {
                self.pending.push_back((row.clone(), path));
            }
        }
    }
}

/// Layered breadth-first search from `src` toward `dst`,
/// constrained by edge type, direction, and max hop count.
/// Returns every path of minimum length when `kind` is
/// `AllShortest`, or just the first discovered shortest path
/// when `kind` is `Shortest`. An empty vector means no path
/// of length ≤ `max_hops` exists.
///
/// `src == dst` is a zero-hop special case that always
/// returns a singleton path regardless of `max_hops`.
///
/// The algorithm builds a parent DAG as it expands each
/// level: every node that was first reached at depth `d+1`
/// records every `(parent, edge)` pair from frontier[d] that
/// reaches it, not just the first one. This lets the
/// reconstruction walk enumerate all source-to-target paths
/// of the discovered shortest length. For `Shortest` mode
/// the reconstruction short-circuits on the first complete
/// path.
fn bfs_shortest_paths(
    src: &Node,
    dst: &Node,
    edge_type: Option<&str>,
    direction: mesh_cypher::Direction,
    max_hops: u64,
    kind: mesh_cypher::ShortestKind,
    reader: &dyn crate::reader::GraphReader,
) -> Result<Vec<Value>> {
    use mesh_cypher::Direction;

    if src.id == dst.id {
        return Ok(vec![Value::Path {
            nodes: vec![src.clone()],
            edges: vec![],
        }]);
    }

    // `dist` tracks the shortest distance from `src` to each
    // reached node. `parents` maps each reached node id to
    // every `(parent_id, edge_id)` pair that reached it at
    // that shortest distance — a node may have multiple
    // parents in the parent DAG for `AllShortest`.
    let mut dist: HashMap<NodeId, u64> = HashMap::new();
    dist.insert(src.id, 0);
    let mut parents: HashMap<NodeId, Vec<(NodeId, EdgeId)>> = HashMap::new();

    let mut frontier: Vec<NodeId> = vec![src.id];
    let mut depth: u64 = 0;
    let mut found = false;

    while !frontier.is_empty() && depth < max_hops && !found {
        let mut next_frontier: Vec<NodeId> = Vec::new();
        for node_id in &frontier {
            let neighbors = match direction {
                Direction::Outgoing => reader.outgoing(*node_id)?,
                Direction::Incoming => reader.incoming(*node_id)?,
                Direction::Both => {
                    let mut out = reader.outgoing(*node_id)?;
                    out.extend(reader.incoming(*node_id)?);
                    out
                }
            };
            for (edge_id, neighbor_id) in neighbors {
                // Edge-type filter. Only fetch the edge record
                // when a type constraint is present.
                if let Some(t) = edge_type {
                    let edge = match reader.get_edge(edge_id)? {
                        Some(e) => e,
                        None => continue,
                    };
                    if edge.edge_type != t {
                        continue;
                    }
                }
                match dist.get(&neighbor_id) {
                    Some(&d) if d == depth + 1 => {
                        // Alternate parent at the same
                        // shortest-path level — record the
                        // additional edge so the reconstruction
                        // can enumerate it, but don't re-add to
                        // the next frontier.
                        parents
                            .entry(neighbor_id)
                            .or_default()
                            .push((*node_id, edge_id));
                    }
                    Some(_) => {
                        // Already reached at a strictly shorter
                        // depth; this edge isn't on a shortest
                        // path.
                    }
                    None => {
                        dist.insert(neighbor_id, depth + 1);
                        parents
                            .entry(neighbor_id)
                            .or_default()
                            .push((*node_id, edge_id));
                        if neighbor_id == dst.id {
                            found = true;
                        } else {
                            next_frontier.push(neighbor_id);
                        }
                    }
                }
            }
        }
        depth += 1;
        if !found {
            frontier = next_frontier;
        }
    }

    if !found {
        return Ok(Vec::new());
    }

    // Enumerate all shortest paths from the parent DAG.
    // `Shortest` mode short-circuits after the first complete
    // walk; `AllShortest` collects every distinct path.
    let mut out: Vec<Value> = Vec::new();
    let mut nodes_rev: Vec<Node> = Vec::new();
    let mut edges_rev: Vec<Edge> = Vec::new();
    let only_first = matches!(kind, mesh_cypher::ShortestKind::Shortest);
    collect_shortest_paths(
        src,
        dst,
        &parents,
        reader,
        &mut nodes_rev,
        &mut edges_rev,
        &mut out,
        only_first,
    )?;
    Ok(out)
}

/// Depth-first walk through the parent DAG built by
/// `bfs_shortest_paths`, collecting every source-to-target
/// path of the BFS-determined shortest length. The recursion
/// carries two scratch stacks (`nodes_rev`, `edges_rev`)
/// representing the partially-reconstructed path in reverse
/// order; on reaching `src` we copy the reversed accumulators
/// into a forward-order `Value::Path` and push it into `out`.
///
/// `only_first = true` short-circuits after the first
/// complete path, giving `Shortest` mode the optimal-case
/// early exit.
#[allow(clippy::too_many_arguments)]
fn collect_shortest_paths(
    src: &Node,
    current: &Node,
    parents: &HashMap<NodeId, Vec<(NodeId, EdgeId)>>,
    reader: &dyn crate::reader::GraphReader,
    nodes_rev: &mut Vec<Node>,
    edges_rev: &mut Vec<Edge>,
    out: &mut Vec<Value>,
    only_first: bool,
) -> Result<()> {
    if current.id == src.id {
        // Complete walk: `nodes_rev` holds dst, ..., (last
        // node before src) in reverse; prepend src and
        // reverse to produce the forward-order node list.
        // Edges are similarly reversed.
        let mut nodes: Vec<Node> = Vec::with_capacity(nodes_rev.len() + 1);
        nodes.push(src.clone());
        nodes.extend(nodes_rev.iter().rev().cloned());
        let edges: Vec<Edge> = edges_rev.iter().rev().cloned().collect();
        out.push(Value::Path { nodes, edges });
        return Ok(());
    }
    let Some(parent_edges) = parents.get(&current.id) else {
        // Unreachable in normal flow — BFS only inserts dst
        // into `parents` when it found at least one incoming
        // edge — but handled defensively.
        return Ok(());
    };
    for (parent_id, edge_id) in parent_edges {
        if only_first && !out.is_empty() {
            return Ok(());
        }
        let edge = reader
            .get_edge(*edge_id)?
            .expect("BFS inserted this edge id; it must still exist");
        let parent_node = reader
            .get_node(*parent_id)?
            .expect("BFS visited this node id; it must still exist");
        nodes_rev.push(current.clone());
        edges_rev.push(edge);
        collect_shortest_paths(
            src,
            &parent_node,
            parents,
            reader,
            nodes_rev,
            edges_rev,
            out,
            only_first,
        )?;
        nodes_rev.pop();
        edges_rev.pop();
    }
    Ok(())
}

/// `UNION` / `UNION ALL`. Drains each branch in order, streaming
/// its rows through. For plain `UNION` (`all = false`) we
/// deduplicate across the combined stream using the same
/// `row_key` the `DistinctOp` uses, so the semantics match
/// Neo4j's "set union" shape regardless of whether duplicates
/// sit inside a single branch or straddle branches. For
/// `UNION ALL` the `seen` set stays `None` and every produced
/// row is forwarded as-is.
struct UnionOp {
    branches: Vec<Box<dyn Operator>>,
    current: usize,
    seen: Option<HashSet<String>>,
}

impl UnionOp {
    fn new(branches: Vec<Box<dyn Operator>>, all: bool) -> Self {
        Self {
            branches,
            current: 0,
            seen: if all { None } else { Some(HashSet::new()) },
        }
    }
}

impl Operator for UnionOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        while self.current < self.branches.len() {
            match self.branches[self.current].next(ctx)? {
                Some(row) => {
                    if let Some(seen) = self.seen.as_mut() {
                        let key = row_key(&row);
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    return Ok(Some(row));
                }
                None => {
                    self.current += 1;
                }
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
                    keys.push(eval_expr(&item.expr, &ctx.eval_ctx(&row))?);
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
                key_values.push(eval_expr(&item.expr, &ctx.eval_ctx(&row))?);
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
                    let v = eval_expr(expr, &ctx.eval_ctx(&row))?;
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    let key = value_key(&v);
                    let seen = entry.distinct_seen[i].get_or_insert_with(HashSet::new);
                    if !seen.insert(key) {
                        continue;
                    }
                }
                entry.agg_states[i].update(&spec.arg, &ctx.eval_ctx(&row))?;
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

    fn update(&mut self, arg: &AggregateArg, ctx: &EvalCtx) -> Result<()> {
        match self {
            AggState::Count(c) => match arg {
                AggregateArg::Star => *c += 1,
                AggregateArg::Expr(e) | AggregateArg::DistinctExpr(e) => {
                    if !matches!(eval_expr(e, ctx)?, Value::Null) {
                        *c += 1;
                    }
                }
            },
            AggState::Sum {
                int_part,
                float_part,
                is_float,
            } => {
                let v = expr_arg_value(arg, ctx)?;
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
                let v = expr_arg_value(arg, ctx)?;
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
                let v = expr_arg_value(arg, ctx)?;
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
                let v = expr_arg_value(arg, ctx)?;
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
                let v = expr_arg_value(arg, ctx)?;
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

fn expr_arg_value(arg: &AggregateArg, ctx: &EvalCtx) -> Result<Value> {
    match arg {
        AggregateArg::Star => Err(Error::AggregateTypeError),
        AggregateArg::Expr(e) | AggregateArg::DistinctExpr(e) => eval_expr(e, ctx),
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
