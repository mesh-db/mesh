use crate::{
    error::{Error, Result},
    eval::{compare_values, eval_expr, row_key, to_bool, value_key, values_equal, EvalCtx},
    procedures::ProcedureRegistry,
    reader::GraphReader,
    value::{ParamMap, Row, Value},
    writer::GraphWriter,
};
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use meshdb_cypher::{
    AggregateArg, AggregateFn, AggregateSpec, BinaryOp, CallArgs, CompareOp, ConstraintKind,
    ConstraintScope as CypherConstraintScope, CreateEdgeSpec, CreateNodeSpec, Direction, Expr,
    Literal, LogicalPlan, PointSeekBounds, PropertyType as CypherPropertyType, RemoveSpec,
    ReturnItem, SetAssignment, SortItem, UnaryOp, YieldSpec,
};
use meshdb_storage::{
    ConstraintScope as StorageConstraintScope, PropertyConstraintKind,
    PropertyType as StoragePropertyType, RocksDbStorageEngine,
};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// Shared tombstone set for nodes / edges that have been DELETEd
/// earlier in the current query. Property / label / type / keys /
/// id accesses consult this set and raise `DeletedEntityAccess`
/// so `MATCH (n) DELETE n RETURN n.num` surfaces the error at
/// runtime instead of reading off a stale clone.
#[derive(Default)]
pub struct Tombstones {
    pub nodes: RefCell<HashSet<meshdb_core::NodeId>>,
    pub edges: RefCell<HashSet<meshdb_core::EdgeId>>,
}

pub struct ExecCtx<'a> {
    pub store: &'a dyn GraphReader,
    pub writer: &'a dyn GraphWriter,
    /// Per-query parameter bindings. Empty for the single-node and Raft
    /// entry points that don't carry a Bolt RUN; populated with the
    /// driver-supplied params on the Bolt path. Threaded into every
    /// `eval_expr` call so `Expr::Parameter("name")` resolves.
    pub params: &'a ParamMap,
    /// Procedures known to this execution. Defaults to an empty
    /// registry on call sites that don't care; the TCK harness and
    /// any future server startup plug in a populated registry so
    /// `CALL ns.name(...)` resolves.
    pub procedures: &'a ProcedureRegistry,
    /// Outer-scope rows contributed by enclosing operators. The
    /// innermost slot (first entry) is the nearest outer. Set by
    /// [`CartesianProductOp`] when running its right side so that
    /// operators inside — typically `EdgeExpandOp` /
    /// `VarLengthExpandOp` constraint lookups — can resolve
    /// variables the right-side scan didn't bind directly. Empty
    /// at the top level.
    pub outer_rows: &'a [&'a Row],
    /// Tombstones for entities deleted earlier in the same query.
    /// Shared by reference so DeleteOp can insert and downstream
    /// eval can check.
    pub tombstones: &'a Tombstones,
}

pub(crate) struct NoOpWriter;
impl GraphWriter for NoOpWriter {
    fn put_node(&self, _: &Node) -> Result<()> {
        Ok(())
    }
    fn put_edge(&self, _: &Edge) -> Result<()> {
        Ok(())
    }
    fn delete_edge(&self, _: EdgeId) -> Result<()> {
        Ok(())
    }
    fn detach_delete_node(&self, _: NodeId) -> Result<()> {
        Ok(())
    }
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
            procedures: self.procedures,
            outer_rows: self.outer_rows,
            tombstones: self.tombstones,
        }
    }

    /// Look up a variable in `row` first, then in each outer-scope
    /// row in order. Returns the nearest match. Used for constraint
    /// lookups inside right-side operators of a `CartesianProduct`
    /// so a fresh scan that references an outer binding
    /// (`MATCH ()-[r]-() MATCH (n)-[r]-(m)`) can still resolve it.
    pub(crate) fn lookup_binding<'r>(&'r self, row: &'r Row, name: &str) -> Option<&'r Value> {
        if let Some(v) = row.get(name) {
            return Some(v);
        }
        for outer in self.outer_rows {
            if let Some(v) = outer.get(name) {
                return Some(v);
            }
        }
        None
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
    let text = meshdb_cypher::format_plan(plan);
    let mut row = Row::new();
    row.insert("plan".to_string(), Value::Property(Property::String(text)));
    vec![row]
}

pub fn profile(plan: &LogicalPlan, store: &RocksDbStorageEngine) -> Result<Vec<Row>> {
    let result_rows = execute(plan, store)?;
    let row_count = result_rows.len() as i64;
    let plan_text = meshdb_cypher::format_plan(plan);
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
    let empty_procs = ProcedureRegistry::new();
    execute_with_reader_and_procs(plan, reader, writer, params, &empty_procs)
}

/// Like [`execute_with_reader`] but with an explicit procedure
/// registry in scope. Used by the TCK harness to mount mock
/// procedures declared by `there exists a procedure ...` steps,
/// and reserved for the server startup path once built-in
/// procedures land.
pub fn execute_with_reader_and_procs(
    plan: &LogicalPlan,
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    params: &ParamMap,
    procedures: &ProcedureRegistry,
) -> Result<Vec<Row>> {
    // `datetime()`, `localtime()`, and friends cache "now" in a
    // thread-local so multiple calls inside the same statement see
    // the same instant. Clear it at the start of every execution so
    // the next query gets a fresh reading.
    crate::eval::reset_statement_time();
    // Schema DDL never enters the operator pipeline — it's a
    // side-effect on the backing store, not a row-producing query.
    // Short-circuit here so `build_op` stays a pure plan-to-operator
    // mapping and doesn't need DDL-specific cases.
    if let Some(rows) = try_execute_ddl(plan, reader, writer)? {
        return Ok(rows);
    }
    let suppress_output = is_write_only_plan(plan);
    let mut op = build_op(plan);
    let tombstones = Tombstones::default();
    let ctx = ExecCtx {
        store: reader,
        writer,
        params,
        procedures,
        outer_rows: &[],
        tombstones: &tombstones,
    };
    let mut rows = Vec::new();
    while let Some(row) = op.next(&ctx)? {
        rows.push(row);
    }
    if suppress_output {
        Ok(Vec::new())
    } else {
        Ok(rows)
    }
}

fn is_write_only_plan(plan: &LogicalPlan) -> bool {
    // A plan is write-only when its top-level operator is a mutation
    // with no projection (Project/Identity) wrapping it. Mutations
    // with RETURN have a Project on top.
    match plan {
        LogicalPlan::CreatePath { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::SetProperty { .. }
        | LogicalPlan::Remove { .. }
        | LogicalPlan::Foreach { .. }
        | LogicalPlan::MergeNode { .. }
        | LogicalPlan::MergeEdge { .. } => true,
        _ => false,
    }
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
        LogicalPlan::CreatePropertyIndex { label, properties } => {
            writer.create_property_index(label, properties)?;
            Ok(Some(vec![node_index_ddl_ack_row(
                "created", label, properties,
            )]))
        }
        LogicalPlan::DropPropertyIndex { label, properties } => {
            writer.drop_property_index(label, properties)?;
            Ok(Some(vec![node_index_ddl_ack_row(
                "dropped", label, properties,
            )]))
        }
        LogicalPlan::CreateEdgePropertyIndex {
            edge_type,
            properties,
        } => {
            writer.create_edge_property_index(edge_type, properties)?;
            Ok(Some(vec![edge_index_ddl_ack_row(
                "created", edge_type, properties,
            )]))
        }
        LogicalPlan::DropEdgePropertyIndex {
            edge_type,
            properties,
        } => {
            writer.drop_edge_property_index(edge_type, properties)?;
            Ok(Some(vec![edge_index_ddl_ack_row(
                "dropped", edge_type, properties,
            )]))
        }
        LogicalPlan::ShowPropertyIndexes => {
            // SHOW is a pure read — source it from the reader so
            // overlay/partitioned readers deliver the right view
            // without round-tripping through a writer. Both
            // node-scoped and edge-scoped indexes land in the same
            // row stream, distinguished by the `scope` column.
            let mut rows: Vec<Row> = Vec::new();
            for (label, properties) in reader.list_property_indexes()? {
                rows.push(show_index_row("NODE", label, properties));
            }
            for (edge_type, properties) in reader.list_edge_property_indexes()? {
                rows.push(show_index_row("RELATIONSHIP", edge_type, properties));
            }
            Ok(Some(rows))
        }
        LogicalPlan::CreatePointIndex { label, property } => {
            writer.create_point_index(label, property)?;
            Ok(Some(vec![point_index_ddl_ack_row(
                "created", label, property,
            )]))
        }
        LogicalPlan::DropPointIndex { label, property } => {
            writer.drop_point_index(label, property)?;
            Ok(Some(vec![point_index_ddl_ack_row(
                "dropped", label, property,
            )]))
        }
        LogicalPlan::ShowPointIndexes => {
            // Kept on its own row stream rather than folded into
            // `SHOW INDEXES` so the point-specific `type` column
            // stays a stable part of the shape and callers can
            // filter without depending on string-matching `type`
            // across mixed rows.
            let rows: Vec<Row> = reader
                .list_point_indexes()?
                .into_iter()
                .map(|(label, property)| show_point_index_row(label, property))
                .collect();
            Ok(Some(rows))
        }
        LogicalPlan::CreatePropertyConstraint {
            name,
            scope,
            properties,
            kind,
            if_not_exists,
        } => {
            let storage_kind = match kind {
                ConstraintKind::Unique => PropertyConstraintKind::Unique,
                ConstraintKind::NotNull => PropertyConstraintKind::NotNull,
                ConstraintKind::NodeKey => PropertyConstraintKind::NodeKey,
                ConstraintKind::PropertyType(t) => {
                    PropertyConstraintKind::PropertyType(cypher_to_storage_property_type(*t))
                }
            };
            let storage_scope = cypher_to_storage_scope(scope);
            let spec = writer.create_property_constraint(
                name.as_deref(),
                &storage_scope,
                properties,
                storage_kind,
                *if_not_exists,
            )?;
            Ok(Some(vec![constraint_ack_row("created", &spec)]))
        }
        LogicalPlan::DropPropertyConstraint { name, if_exists } => {
            writer.drop_property_constraint(name, *if_exists)?;
            let mut row = Row::default();
            row.insert(
                "state".into(),
                Value::Property(Property::String("dropped".into())),
            );
            row.insert(
                "name".into(),
                Value::Property(Property::String(name.clone())),
            );
            Ok(Some(vec![row]))
        }
        LogicalPlan::ShowPropertyConstraints => {
            let specs = reader.list_property_constraints()?;
            let rows = specs.into_iter().map(constraint_show_row).collect();
            Ok(Some(rows))
        }
        _ => Ok(None),
    }
}

/// One-row acknowledgement emitted after `CREATE CONSTRAINT`. Carries
/// the resolved spec so callers see the final name when they omitted
/// it. Columns match `SHOW CONSTRAINTS` (plus `state`) so Bolt clients
/// can uniformly format DDL responses.
fn constraint_ack_row(state: &str, spec: &meshdb_storage::PropertyConstraintSpec) -> Row {
    let mut row = constraint_show_row(spec.clone());
    row.insert(
        "state".into(),
        Value::Property(Property::String(state.into())),
    );
    row
}

/// Row shape for `SHOW CONSTRAINTS` and `db.constraints()`. Columns:
/// `name`, `scope` ("NODE" / "RELATIONSHIP"), `label` (label or edge
/// type, depending on scope), `properties` (list), and `type`.
/// Single-property constraints still carry a one-element `properties`
/// list; composite kinds (e.g. `NodeKey`) carry the full tuple.
fn constraint_show_row(spec: meshdb_storage::PropertyConstraintSpec) -> Row {
    let mut row = Row::default();
    row.insert("name".into(), Value::Property(Property::String(spec.name)));
    let (scope_tag, target) = match spec.scope {
        meshdb_storage::ConstraintScope::Node(l) => ("NODE", l),
        meshdb_storage::ConstraintScope::Relationship(t) => ("RELATIONSHIP", t),
    };
    row.insert(
        "scope".into(),
        Value::Property(Property::String(scope_tag.into())),
    );
    // `label` stays for backwards compatibility — it carries the
    // scope's target regardless of node vs relationship, matching
    // the index DDL row's column for historical consistency. A
    // separate `scope` column tells you what kind of target it is.
    row.insert("label".into(), Value::Property(Property::String(target)));
    let props: Vec<Property> = spec.properties.into_iter().map(Property::String).collect();
    row.insert("properties".into(), Value::Property(Property::List(props)));
    row.insert(
        "type".into(),
        Value::Property(Property::String(spec.kind.as_string())),
    );
    row
}

/// Convert the Cypher AST's `ConstraintScope` into the storage-layer
/// enum. Narrow bridge — same shape on both sides; kept in
/// different crates to preserve the dependency direction.
fn cypher_to_storage_scope(scope: &CypherConstraintScope) -> StorageConstraintScope {
    match scope {
        CypherConstraintScope::Node(l) => StorageConstraintScope::Node(l.clone()),
        CypherConstraintScope::Relationship(t) => StorageConstraintScope::Relationship(t.clone()),
    }
}

/// Convert the Cypher AST's `PropertyType` into the storage-layer
/// enum. Narrow bridge — the two enums carry the same four variants
/// but live in different crates to keep the dependency direction
/// clean (cypher → executor → storage, never the reverse).
fn cypher_to_storage_property_type(t: CypherPropertyType) -> StoragePropertyType {
    match t {
        CypherPropertyType::String => StoragePropertyType::String,
        CypherPropertyType::Integer => StoragePropertyType::Integer,
        CypherPropertyType::Float => StoragePropertyType::Float,
        CypherPropertyType::Boolean => StoragePropertyType::Boolean,
    }
}

/// One-row acknowledgement emitted after `CREATE INDEX` /
/// `DROP INDEX` on a node scope. Columns mirror the `SHOW INDEXES`
/// shape (`scope`, `label`, `property`) so Bolt clients can format
/// DDL responses uniformly with schema-read responses. `label` is
/// kept as a column name for backwards-compat with pre-edge-index
/// clients; the `scope` column disambiguates node from edge.
fn node_index_ddl_ack_row(state: &str, label: &str, properties: &[String]) -> Row {
    let mut row = Row::default();
    row.insert(
        "state".into(),
        Value::Property(Property::String(state.into())),
    );
    row.insert(
        "scope".into(),
        Value::Property(Property::String("NODE".into())),
    );
    row.insert(
        "label".into(),
        Value::Property(Property::String(label.into())),
    );
    // `property` stays a single string for backward compat with
    // drivers that read it directly. Composite specs render as a
    // comma-joined preview; the `properties` column carries the
    // authoritative list.
    row.insert(
        "property".into(),
        Value::Property(Property::String(properties.join(","))),
    );
    row.insert("properties".into(), properties_list_value(properties));
    row
}

/// One-row acknowledgement emitted after `CREATE INDEX` /
/// `DROP INDEX` on a relationship scope. Mirrors
/// [`node_index_ddl_ack_row`] but the target lives under
/// `edge_type` (and `label` carries the same string so existing
/// driver code that read `label` still sees the target name).
fn edge_index_ddl_ack_row(state: &str, edge_type: &str, properties: &[String]) -> Row {
    let mut row = Row::default();
    row.insert(
        "state".into(),
        Value::Property(Property::String(state.into())),
    );
    row.insert(
        "scope".into(),
        Value::Property(Property::String("RELATIONSHIP".into())),
    );
    row.insert(
        "label".into(),
        Value::Property(Property::String(edge_type.into())),
    );
    row.insert(
        "edge_type".into(),
        Value::Property(Property::String(edge_type.into())),
    );
    row.insert(
        "property".into(),
        Value::Property(Property::String(properties.join(","))),
    );
    row.insert("properties".into(), properties_list_value(properties));
    row
}

fn properties_list_value(properties: &[String]) -> Value {
    Value::List(
        properties
            .iter()
            .map(|p| Value::Property(Property::String(p.clone())))
            .collect(),
    )
}

/// One-row ack emitted after `CREATE POINT INDEX` / `DROP POINT
/// INDEX`. Same column shape as the non-point variant plus a
/// `type: "POINT"` column so clients consuming a unified DDL
/// response stream can distinguish the kind at a glance.
fn point_index_ddl_ack_row(state: &str, label: &str, property: &str) -> Row {
    let mut row = Row::default();
    row.insert(
        "state".into(),
        Value::Property(Property::String(state.into())),
    );
    row.insert(
        "scope".into(),
        Value::Property(Property::String("NODE".into())),
    );
    row.insert(
        "type".into(),
        Value::Property(Property::String("POINT".into())),
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

/// Row shape for `SHOW POINT INDEXES`. Kept minimal — one row per
/// registered point index with its label + property + a stable
/// `type` marker so drivers can filter without inspecting row
/// provenance.
fn show_point_index_row(label: String, property: String) -> Row {
    let mut row = Row::default();
    row.insert(
        "scope".into(),
        Value::Property(Property::String("NODE".into())),
    );
    row.insert(
        "type".into(),
        Value::Property(Property::String("POINT".into())),
    );
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
}

/// Row shape for `SHOW INDEXES` output. Columns: `scope`
/// (`"NODE"` / `"RELATIONSHIP"`), `label` (scope target — label for
/// node, edge type for relationship; kept for backwards-compat),
/// `property` (comma-joined preview of the properties for composite
/// specs), `properties` (full list — authoritative), and `state`.
/// `label` and `edge_type` carry the same string when
/// `scope == "RELATIONSHIP"` so clients can read either column.
fn show_index_row(scope: &str, target: String, properties: Vec<String>) -> Row {
    let mut row = Row::default();
    row.insert(
        "scope".into(),
        Value::Property(Property::String(scope.into())),
    );
    row.insert(
        "label".into(),
        Value::Property(Property::String(target.clone())),
    );
    if scope == "RELATIONSHIP" {
        row.insert(
            "edge_type".into(),
            Value::Property(Property::String(target)),
        );
    }
    row.insert(
        "property".into(),
        Value::Property(Property::String(properties.join(","))),
    );
    row.insert("properties".into(), properties_list_value(&properties));
    row.insert(
        "state".into(),
        Value::Property(Property::String("online".into())),
    );
    row
}

fn build_op(plan: &LogicalPlan) -> Box<dyn Operator> {
    build_op_inner(plan, None)
}

pub(crate) fn build_op_inner(plan: &LogicalPlan, seed: Option<&Row>) -> Box<dyn Operator> {
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
            exprs,
        } => Box::new(DeleteOp::new(
            child!(input),
            *detach,
            vars.clone(),
            exprs.clone(),
        )),
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
        LogicalPlan::OptionalApply {
            input,
            body,
            null_vars,
        } => Box::new(OptionalApplyOp::new(
            child!(input),
            (**body).clone(),
            null_vars.clone(),
        )),
        LogicalPlan::ProcedureCall {
            input,
            qualified_name,
            args,
            yield_spec,
            standalone,
        } => Box::new(ProcedureCallOp::new(
            input.as_ref().map(|p| child!(p)),
            qualified_name.clone(),
            args.clone(),
            yield_spec.clone(),
            *standalone,
        )),
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
            edge_properties,
            edge_types,
            direction,
            edge_constraint_var,
        } => Box::new(EdgeExpandOp::new(
            child!(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
            edge_properties.clone(),
            edge_types.clone(),
            *direction,
            edge_constraint_var.clone(),
        )),
        LogicalPlan::OptionalEdgeExpand {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            dst_properties,
            edge_types,
            direction,
            dst_constraint_var,
            edge_constraint_var,
        } => Box::new(OptionalEdgeExpandOp::new(
            child!(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
            dst_properties.clone(),
            edge_types.clone(),
            *direction,
            dst_constraint_var.clone(),
            edge_constraint_var.clone(),
        )),
        LogicalPlan::VarLengthExpand {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            edge_types,
            edge_properties,
            direction,
            min_hops,
            max_hops,
            path_var,
            optional,
            dst_constraint_var,
            bound_edge_list_var,
            excluded_edge_vars,
        } => Box::new(VarLengthExpandOp::new(
            child!(input),
            src_var.clone(),
            edge_var.clone(),
            dst_var.clone(),
            dst_labels.clone(),
            edge_types.clone(),
            edge_properties.clone(),
            *direction,
            *min_hops,
            *max_hops,
            path_var.clone(),
            *optional,
            dst_constraint_var.clone(),
            bound_edge_list_var.clone(),
            excluded_edge_vars.clone(),
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
        LogicalPlan::CoalesceNullRow { input, null_vars } => {
            Box::new(CoalesceNullRowOp::new(child!(input), null_vars.clone()))
        }
        LogicalPlan::Distinct { input } => Box::new(DistinctOp::new(child!(input))),
        LogicalPlan::OrderBy { input, sort_items } => {
            Box::new(OrderByOp::new(child!(input), sort_items.clone()))
        }
        LogicalPlan::Skip { input, count } => Box::new(SkipOp::new(child!(input), count.clone())),
        LogicalPlan::Limit { input, count } => Box::new(LimitOp::new(child!(input), count.clone())),
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
            undirected,
            properties,
            on_create,
            on_match,
        } => Box::new(MergeEdgeOp::new(
            child!(input),
            edge_var.clone(),
            src_var.clone(),
            dst_var.clone(),
            edge_type.clone(),
            *undirected,
            properties.clone(),
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
            properties,
            values,
        } => Box::new(IndexSeekOp::new(
            var.clone(),
            label.clone(),
            properties.clone(),
            values.clone(),
        )),
        LogicalPlan::PointIndexSeek {
            var,
            label,
            property,
            bounds,
        } => Box::new(PointIndexSeekOp::new(
            var.clone(),
            label.clone(),
            property.clone(),
            bounds.clone(),
        )),
        LogicalPlan::EdgeSeek {
            edge_var,
            src_var,
            dst_var,
            edge_type,
            property,
            value,
            direction,
            residual_properties,
        } => Box::new(EdgeSeekOp::new(
            edge_var.clone(),
            src_var.clone(),
            dst_var.clone(),
            edge_type.clone(),
            property.clone(),
            value.clone(),
            *direction,
            residual_properties.clone(),
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
            edge_types,
            direction,
            max_hops,
            kind,
        } => Box::new(ShortestPathOp::new(
            child!(input),
            src_var.clone(),
            dst_var.clone(),
            path_var.clone(),
            edge_types.clone(),
            *direction,
            *max_hops,
            *kind,
        )),
        LogicalPlan::CreatePropertyIndex { .. }
        | LogicalPlan::DropPropertyIndex { .. }
        | LogicalPlan::CreateEdgePropertyIndex { .. }
        | LogicalPlan::DropEdgePropertyIndex { .. }
        | LogicalPlan::ShowPropertyIndexes
        | LogicalPlan::CreatePointIndex { .. }
        | LogicalPlan::DropPointIndex { .. }
        | LogicalPlan::ShowPointIndexes
        | LogicalPlan::CreatePropertyConstraint { .. }
        | LogicalPlan::DropPropertyConstraint { .. }
        | LogicalPlan::ShowPropertyConstraints => {
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
                procedures: ctx.procedures,
                outer_rows: ctx.outer_rows,
                tombstones: ctx.tombstones,
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
                procedures: ctx.procedures,
                outer_rows: ctx.outer_rows,
                tombstones: ctx.tombstones,
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
                    // Null-valued properties aren't stored — openCypher
                    // treats `{k: null}` as "no such property," so
                    // `keys(n)` and `n.k IS NOT NULL` both skip it.
                    for (k, expr) in properties {
                        let value = eval_expr(expr, &ctx.eval_ctx(&out))?;
                        let prop = value_to_property(value)?;
                        if matches!(prop, Property::Null) {
                            continue;
                        }
                        node.properties.insert(k.clone(), prop);
                    }
                    ctx.writer.put_node(&node)?;
                    node_ids.push(node.id);
                    if let Some(v) = var {
                        out.insert(v.clone(), Value::Node(node));
                    }
                }
                CreateNodeSpec::Reference(name) => {
                    let id = match out.get(name) {
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
            let mut edge = Edge::new(spec.edge_type.clone(), src, dst);
            for (k, expr) in &spec.properties {
                let value = eval_expr(expr, &ctx.eval_ctx(&out))?;
                let prop = value_to_property(value)?;
                if matches!(prop, Property::Null) {
                    continue;
                }
                edge.properties.insert(k.clone(), prop);
            }
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
            // Expose the current left row to right-side operators
            // via a shadow context so constraint lookups (for
            // bound edges / bound edge lists) can find vars that
            // the right-side scan itself doesn't bind.
            let left_ref = self.left_row.as_ref().unwrap();
            let mut stacked: Vec<&Row> = Vec::with_capacity(ctx.outer_rows.len() + 1);
            stacked.push(left_ref);
            stacked.extend_from_slice(ctx.outer_rows);
            let inner_ctx = ExecCtx {
                store: ctx.store,
                writer: ctx.writer,
                params: ctx.params,
                procedures: ctx.procedures,
                outer_rows: &stacked,
                tombstones: ctx.tombstones,
            };
            match right_op.next(&inner_ctx)? {
                Some(right_row) => {
                    let mut combined = left_ref.clone();
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
    #[allow(dead_code)]
    vars: Vec<String>,
    exprs: Vec<Expr>,
    /// Rows drained from `input` before any deletes have run.
    /// Populated on first call to `next`. openCypher treats DELETE
    /// as a batch mutation: the whole preceding row set is
    /// materialised, then the mutations happen, then rows are
    /// forwarded. Without buffering, `MATCH (a)-[r]-(b) DELETE r,
    /// a, b RETURN count(*)` deletes the first row's nodes before
    /// the pipelined scan reaches the second row.
    buffered: Option<Vec<Row>>,
    cursor: usize,
}

impl DeleteOp {
    fn new(input: Box<dyn Operator>, detach: bool, vars: Vec<String>, exprs: Vec<Expr>) -> Self {
        Self {
            input,
            detach,
            vars,
            exprs,
            buffered: None,
            cursor: 0,
        }
    }

    /// Gather every edge and node id referenced by the row's
    /// DELETE expressions and run the delete in two phases —
    /// edges first, then nodes. The two-phase ordering is what
    /// lets `DELETE p1, p2` succeed when two paths share nodes
    /// connected by each other's edges: by the time the node
    /// phase runs, the in-batch edges are already gone. The
    /// non-detach check probes the graph then filters out
    /// any edge we just deleted, so the batch-level detachment
    /// is counted as "no longer attached" rather than failing.
    fn apply_deletes(
        &self,
        ctx: &ExecCtx,
        row: &Row,
        seen_edges: &mut HashSet<meshdb_core::EdgeId>,
        seen_nodes: &mut HashSet<meshdb_core::NodeId>,
    ) -> Result<()> {
        let mut edge_ids: Vec<meshdb_core::EdgeId> = Vec::new();
        let mut node_ids: Vec<meshdb_core::NodeId> = Vec::new();
        for expr in &self.exprs {
            let v = eval_expr(expr, &ctx.eval_ctx(row))?;
            match v {
                Value::Node(n) => node_ids.push(n.id),
                Value::Edge(e) => edge_ids.push(e.id),
                Value::Path { nodes, edges } => {
                    for e in edges {
                        edge_ids.push(e.id);
                    }
                    for n in nodes {
                        node_ids.push(n.id);
                    }
                }
                Value::Null | Value::Property(Property::Null) => continue,
                _ => return Err(Error::TypeMismatch),
            }
        }
        for eid in &edge_ids {
            if seen_edges.insert(*eid) {
                ctx.writer.delete_edge(*eid)?;
                ctx.tombstones.edges.borrow_mut().insert(*eid);
            }
        }
        for nid in &node_ids {
            if !seen_nodes.insert(*nid) {
                continue;
            }
            if self.detach {
                // DETACH DELETE also removes every attached edge;
                // tombstone them too so a later projection over a
                // formerly-attached edge clone also raises rather
                // than reading stale properties.
                for (eid, _) in ctx.store.outgoing(*nid)? {
                    ctx.tombstones.edges.borrow_mut().insert(eid);
                }
                for (eid, _) in ctx.store.incoming(*nid)? {
                    ctx.tombstones.edges.borrow_mut().insert(eid);
                }
                ctx.writer.detach_delete_node(*nid)?;
            } else {
                let out = ctx.store.outgoing(*nid)?;
                let inc = ctx.store.incoming(*nid)?;
                let still_attached = out
                    .iter()
                    .chain(inc.iter())
                    .any(|(eid, _)| !seen_edges.contains(eid));
                if still_attached {
                    return Err(Error::CannotDeleteAttachedNode);
                }
                ctx.writer.detach_delete_node(*nid)?;
            }
            ctx.tombstones.nodes.borrow_mut().insert(*nid);
        }
        Ok(())
    }
}

impl Operator for DeleteOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        // First call: drain the input, run all deletes, then
        // start forwarding rows. Buffering protects against the
        // pipelined-scan-vs-mutation race: `MATCH (a)-[r]-(b)
        // DELETE r, a, b RETURN count(*)` must observe every
        // MATCH row before any node or edge disappears, otherwise
        // the undirected expansion hits an already-deleted source
        // on the second iteration.
        if self.buffered.is_none() {
            let mut rows: Vec<Row> = Vec::new();
            while let Some(row) = self.input.next(ctx)? {
                rows.push(row);
            }
            let mut seen_edges: HashSet<meshdb_core::EdgeId> = HashSet::new();
            let mut seen_nodes: HashSet<meshdb_core::NodeId> = HashSet::new();
            for row in &rows {
                self.apply_deletes(ctx, row, &mut seen_edges, &mut seen_nodes)?;
            }
            self.buffered = Some(rows);
            self.cursor = 0;
        }
        let rows = self.buffered.as_ref().unwrap();
        if self.cursor < rows.len() {
            let row = rows[self.cursor].clone();
            self.cursor += 1;
            return Ok(Some(row));
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
                        SetAssignment::ReplaceFromExpr {
                            var,
                            source,
                            replace,
                        } => {
                            let v = eval_expr(source, &ctx.eval_ctx(&row))?;
                            let props = extract_property_map(&v)?;
                            if *replace {
                                actions.push(Action::Replace {
                                    var: var.clone(),
                                    props,
                                });
                            } else {
                                actions.push(Action::Merge {
                                    var: var.clone(),
                                    props,
                                });
                            }
                        }
                    }
                }

                // Phase 2: apply updates in-place to the row bindings.
                let mut updated_nodes: HashSet<String> = HashSet::new();
                let mut updated_edges: HashSet<String> = HashSet::new();
                for action in actions {
                    match action {
                        Action::SetKey { var, key, prop } => match row.get_mut(&var) {
                            // openCypher: SET on a null target (from
                            // OPTIONAL MATCH that didn't bind) is a
                            // silent no-op rather than an error.
                            Some(Value::Null) | Some(Value::Property(Property::Null)) | None => {
                                continue
                            }
                            // openCypher: SET a.key = null removes the
                            // property rather than storing a null value.
                            Some(Value::Node(n)) => {
                                if matches!(prop, Property::Null) {
                                    n.properties.remove(&key);
                                } else {
                                    n.properties.insert(key, prop);
                                }
                                updated_nodes.insert(var);
                            }
                            Some(Value::Edge(e)) => {
                                if matches!(prop, Property::Null) {
                                    e.properties.remove(&key);
                                } else {
                                    e.properties.insert(key, prop);
                                }
                                updated_edges.insert(var);
                            }
                            _ => return Err(Error::UnboundVariable(var)),
                        },
                        Action::AddLabels { var, labels } => match row.get_mut(&var) {
                            Some(Value::Null) | Some(Value::Property(Property::Null)) | None => {
                                continue
                            }
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
                            Some(Value::Null) | Some(Value::Property(Property::Null)) | None => {
                                continue
                            }
                            Some(Value::Node(n)) => {
                                n.properties.clear();
                                for (k, v) in props {
                                    if !matches!(v, Property::Null) {
                                        n.properties.insert(k, v);
                                    }
                                }
                                updated_nodes.insert(var);
                            }
                            Some(Value::Edge(e)) => {
                                e.properties.clear();
                                for (k, v) in props {
                                    if !matches!(v, Property::Null) {
                                        e.properties.insert(k, v);
                                    }
                                }
                                updated_edges.insert(var);
                            }
                            _ => return Err(Error::UnboundVariable(var)),
                        },
                        Action::Merge { var, props } => match row.get_mut(&var) {
                            Some(Value::Null) | Some(Value::Property(Property::Null)) | None => {
                                continue
                            }
                            Some(Value::Node(n)) => {
                                for (k, v) in props {
                                    if matches!(v, Property::Null) {
                                        n.properties.remove(&k);
                                    } else {
                                        n.properties.insert(k, v);
                                    }
                                }
                                updated_nodes.insert(var);
                            }
                            Some(Value::Edge(e)) => {
                                for (k, v) in props {
                                    if matches!(v, Property::Null) {
                                        e.properties.remove(&k);
                                    } else {
                                        e.properties.insert(k, v);
                                    }
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
                            // Null target (from OPTIONAL MATCH) is a
                            // no-op, matching Neo4j's REMOVE semantics.
                            Some(Value::Null) | Some(Value::Property(Property::Null)) | None => {
                                continue
                            }
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
                            Some(Value::Null) | Some(Value::Property(Property::Null)) | None => {
                                continue
                            }
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

/// Per-input-row left-join driver (see
/// `LogicalPlan::OptionalApply`). Replays `body_plan` once per
/// outer row with the row as its seed; forwards all emitted rows
/// merged with the outer bindings, and emits one null-fallback
/// row (outer bindings plus `null_vars` bound to Null) only when
/// the body produced zero rows for that input.
struct OptionalApplyOp {
    input: Box<dyn Operator>,
    body_plan: LogicalPlan,
    null_vars: Vec<String>,
    pending: Vec<Row>,
    pending_idx: usize,
}

impl OptionalApplyOp {
    fn new(input: Box<dyn Operator>, body_plan: LogicalPlan, null_vars: Vec<String>) -> Self {
        Self {
            input,
            body_plan,
            null_vars,
            pending: Vec::new(),
            pending_idx: 0,
        }
    }
}

impl Operator for OptionalApplyOp {
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
            // Push the outer row onto `outer_rows` so Filter /
            // eval_expr inside the body can resolve outer
            // bindings (`OPTIONAL MATCH ... WHERE outer_var = x`).
            let mut stacked: Vec<&Row> = Vec::with_capacity(ctx.outer_rows.len() + 1);
            stacked.push(&outer_row);
            stacked.extend_from_slice(ctx.outer_rows);
            let inner_ctx = ExecCtx {
                store: ctx.store,
                writer: ctx.writer,
                params: ctx.params,
                procedures: ctx.procedures,
                outer_rows: &stacked,
                tombstones: ctx.tombstones,
            };
            let mut results = Vec::new();
            while let Some(body_row) = body_op.next(&inner_ctx)? {
                let mut merged = outer_row.clone();
                for (k, v) in body_row {
                    merged.insert(k, v);
                }
                results.push(merged);
            }
            if results.is_empty() {
                let mut fallback = outer_row;
                for v in &self.null_vars {
                    fallback.insert(v.clone(), Value::Null);
                }
                return Ok(Some(fallback));
            }
            self.pending = results;
            self.pending_idx = 0;
        }
    }
}

/// Runtime operator for `CALL ns.name[(args)] [YIELD ...]`.
///
/// Resolves the procedure against the [`ProcedureRegistry`] at
/// `next()` time, validates arity / types / form (implicit args,
/// `YIELD *`, in-query YIELD requirement), then scans the registered
/// data-table for rows whose input cells match the call arguments.
/// Each matching row is projected according to the YIELD spec and
/// merged into the upstream row.
///
/// Two shapes are supported at once:
/// * Standalone (`input = None`): the op is the full plan. It
///   runs the procedure exactly once (against a synthetic empty row)
///   and emits the matching rows directly.
/// * In-query (`input = Some(...)`): for each upstream row, runs
///   the procedure with args evaluated against that row and emits
///   one merged row per match. Procedures with zero declared output
///   columns act as a pass-through — the upstream row is forwarded
///   unchanged, matching Neo4j's side-effect-only semantics.
struct ProcedureCallOp {
    input: Option<Box<dyn Operator>>,
    qualified_name: Vec<String>,
    args: Option<Vec<Expr>>,
    yield_spec: Option<YieldSpec>,
    standalone: bool,
    buffered: Vec<Row>,
    buffered_idx: usize,
    // Only set for the standalone form, which drives itself off a
    // synthetic seed row exactly once.
    done: bool,
}

impl ProcedureCallOp {
    fn new(
        input: Option<Box<dyn Operator>>,
        qualified_name: Vec<String>,
        args: Option<Vec<Expr>>,
        yield_spec: Option<YieldSpec>,
        standalone: bool,
    ) -> Self {
        Self {
            input,
            qualified_name,
            args,
            yield_spec,
            standalone,
            buffered: Vec::new(),
            buffered_idx: 0,
            done: false,
        }
    }

    /// Resolve the projection list `(source_column, output_alias)`
    /// from the procedure signature and this op's yield spec. Also
    /// validates the spec — unknown YIELD columns, duplicate
    /// aliases, and `YIELD *` / no-YIELD in disallowed contexts all
    /// surface as `Error::Procedure`.
    fn resolve_projection(
        &self,
        proc: &crate::procedures::Procedure,
    ) -> Result<Vec<(String, String)>> {
        match &self.yield_spec {
            None => {
                if !self.standalone {
                    // In-query CALL with no YIELD: legal only for
                    // side-effect-only procedures (zero declared
                    // outputs). A procedure with outputs but no
                    // YIELD leaves those outputs unbound, so any
                    // downstream RETURN that references them would
                    // see `UndefinedVariable`; reject here so the
                    // error surfaces instead of silently emitting
                    // nulls.
                    if proc.outputs.is_empty() {
                        return Ok(Vec::new());
                    }
                    return Err(Error::Procedure(format!(
                        "procedure '{}' has outputs but no YIELD clause",
                        self.qualified_name.join(".")
                    )));
                }
                Ok(proc
                    .outputs
                    .iter()
                    .map(|o| (o.name.clone(), o.name.clone()))
                    .collect())
            }
            Some(YieldSpec::Star) => {
                if !self.standalone {
                    return Err(Error::Procedure(
                        "YIELD * is only allowed on standalone CALL".into(),
                    ));
                }
                Ok(proc
                    .outputs
                    .iter()
                    .map(|o| (o.name.clone(), o.name.clone()))
                    .collect())
            }
            Some(YieldSpec::Items(items)) => {
                let mut projection = Vec::with_capacity(items.len());
                let mut seen_aliases: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for yi in items {
                    if !proc.outputs.iter().any(|o| o.name == yi.column) {
                        return Err(Error::Procedure(format!(
                            "procedure '{}' has no output column '{}'",
                            self.qualified_name.join("."),
                            yi.column
                        )));
                    }
                    let alias = yi.alias.clone().unwrap_or_else(|| yi.column.clone());
                    if !seen_aliases.insert(alias.clone()) {
                        return Err(Error::Procedure(format!(
                            "variable '{alias}' already bound by YIELD"
                        )));
                    }
                    projection.push((yi.column.clone(), alias));
                }
                Ok(projection)
            }
        }
    }

    /// Evaluate the call's argument list against `row`. For the
    /// implicit-args form (`args = None`), each declared input
    /// column's value comes from the per-query parameter map
    /// (keyed by the input-column name). Returns the argument
    /// values in declaration order. Raises `ProcedureError` on
    /// arity mismatch, type mismatch, or missing parameter.
    fn evaluate_args(
        &self,
        ctx: &ExecCtx,
        row: &Row,
        proc: &crate::procedures::Procedure,
    ) -> Result<Vec<Value>> {
        match &self.args {
            Some(exprs) => {
                if exprs.len() != proc.inputs.len() {
                    return Err(Error::Procedure(format!(
                        "procedure '{}' expects {} argument(s), got {}",
                        self.qualified_name.join("."),
                        proc.inputs.len(),
                        exprs.len()
                    )));
                }
                let eval_ctx = ctx.eval_ctx(row);
                let mut values = Vec::with_capacity(exprs.len());
                for (expr, spec) in exprs.iter().zip(proc.inputs.iter()) {
                    let v = eval_expr(expr, &eval_ctx)?;
                    if !spec.ty.accepts(&v) {
                        return Err(Error::Procedure(format!(
                            "argument '{}' has wrong type for procedure '{}'",
                            spec.name,
                            self.qualified_name.join(".")
                        )));
                    }
                    values.push(coerce_arg(v, spec.ty));
                }
                Ok(values)
            }
            None => {
                // Implicit-arg form only valid standalone.
                if !self.standalone {
                    return Err(Error::Procedure(
                        "in-query CALL requires explicit argument list".into(),
                    ));
                }
                let mut values = Vec::with_capacity(proc.inputs.len());
                for spec in &proc.inputs {
                    let v = ctx.params.get(&spec.name).cloned().ok_or_else(|| {
                        Error::Procedure(format!(
                            "missing parameter ${} for procedure '{}'",
                            spec.name,
                            self.qualified_name.join(".")
                        ))
                    })?;
                    if !spec.ty.accepts(&v) {
                        return Err(Error::Procedure(format!(
                            "parameter '{}' has wrong type",
                            spec.name
                        )));
                    }
                    values.push(coerce_arg(v, spec.ty));
                }
                Ok(values)
            }
        }
    }

    /// Invoke the procedure once for `input_row` and emit zero or
    /// more output rows into `out`. Handles the "zero-output-column
    /// pass-through" case that keeps `MATCH (n) CALL test.doNothing()
    /// RETURN n.name` from filtering the match rows.
    fn invoke_once(
        &self,
        ctx: &ExecCtx,
        input_row: &Row,
        proc: &crate::procedures::Procedure,
        projection: &[(String, String)],
        out: &mut Vec<Row>,
    ) -> Result<()> {
        // Zero-output-column procedures are side-effect-only in
        // the TCK; they either suppress rows entirely (standalone)
        // or pass the input row through unchanged (in-query).
        if proc.outputs.is_empty() {
            if !self.standalone {
                out.push(input_row.clone());
            }
            return Ok(());
        }
        let args = self.evaluate_args(ctx, input_row, proc)?;
        let rows = proc.resolve_rows(ctx.store)?;
        for proc_row in &rows {
            if !proc.row_matches(proc_row, &args) {
                continue;
            }
            let mut merged = if self.standalone {
                Row::new()
            } else {
                input_row.clone()
            };
            for (src, alias) in projection {
                let v = proc_row.get(src).cloned().unwrap_or(Value::Null);
                merged.insert(alias.clone(), v);
            }
            out.push(merged);
        }
        Ok(())
    }
}

/// Cast an int to a float when the declared type is `FLOAT`. Other
/// declared types leave the value as-is (accept() has already
/// gated on kind) so the comparison in `row_matches` sees a
/// consistent shape.
fn coerce_arg(v: Value, ty: crate::procedures::ProcType) -> Value {
    use crate::procedures::ProcType;
    if matches!(ty, ProcType::Float) {
        if let Value::Property(Property::Int64(n)) = v {
            return Value::Property(Property::Float64(n as f64));
        }
    }
    v
}

impl Operator for ProcedureCallOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            if self.buffered_idx < self.buffered.len() {
                let row = self.buffered[self.buffered_idx].clone();
                self.buffered_idx += 1;
                return Ok(Some(row));
            }
            self.buffered.clear();
            self.buffered_idx = 0;

            let proc = match ctx.procedures.get(&self.qualified_name) {
                Some(p) => p,
                None => {
                    return Err(Error::Procedure(format!(
                        "procedure '{}' not found",
                        self.qualified_name.join(".")
                    )));
                }
            };
            let projection = self.resolve_projection(proc)?;

            let input_row = match &mut self.input {
                Some(inp) => match inp.next(ctx)? {
                    Some(r) => r,
                    None => return Ok(None),
                },
                None => {
                    if self.done {
                        return Ok(None);
                    }
                    self.done = true;
                    Row::new()
                }
            };

            let mut produced = Vec::new();
            self.invoke_once(ctx, &input_row, proc, &projection, &mut produced)?;
            if produced.is_empty() {
                if self.input.is_some() {
                    continue;
                }
                return Ok(None);
            }
            self.buffered = produced;
        }
    }
}

/// Pull a property map out of a value. Supports node / edge
/// (uses their live property map) and map values (parameter or
/// map literal). Null propagates as an empty map — `SET x = null`
/// is spec'd as a property clear / no-op and SET = <null-binding>
/// (from an unmatched OPTIONAL MATCH) matches that shape.
fn extract_property_map(v: &Value) -> Result<Vec<(String, Property)>> {
    match v {
        Value::Node(n) => Ok(n.properties.clone().into_iter().collect()),
        Value::Edge(e) => Ok(e.properties.clone().into_iter().collect()),
        Value::Map(pairs) => pairs
            .iter()
            .map(|(k, vv)| Ok((k.clone(), value_to_property(vv.clone())?)))
            .collect(),
        Value::Property(Property::Map(entries)) => Ok(entries
            .iter()
            .map(|(k, p)| (k.clone(), p.clone()))
            .collect()),
        Value::Null | Value::Property(Property::Null) => Ok(Vec::new()),
        _ => Err(Error::InvalidSetValue),
    }
}

fn value_to_property(v: Value) -> Result<Property> {
    match v {
        Value::Property(Property::Map(_)) => Err(Error::InvalidSetValue),
        Value::Property(p) => Ok(p),
        Value::Null => Ok(Property::Null),
        Value::List(items) => {
            let props: Vec<Property> = items
                .into_iter()
                .map(value_to_property)
                .collect::<Result<_>>()?;
            Ok(Property::List(props))
        }
        // Graph-aware `Value::Map` and graph elements can't be
        // stored as node / edge property values; SET will reject
        // them.
        Value::Map(_) | Value::Node(_) | Value::Edge(_) | Value::Path { .. } => {
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
    properties: Vec<String>,
    value_exprs: Vec<Expr>,
    results: Option<Vec<NodeId>>,
    cursor: usize,
}

impl IndexSeekOp {
    fn new(var: String, label: String, properties: Vec<String>, value_exprs: Vec<Expr>) -> Self {
        assert_eq!(
            properties.len(),
            value_exprs.len(),
            "IndexSeekOp: properties and values must have equal length"
        );
        Self {
            var,
            label,
            properties,
            value_exprs,
            results: None,
            cursor: 0,
        }
    }
}

impl Operator for IndexSeekOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.results.is_none() {
            let empty = Row::new();
            let mut values: Vec<Property> = Vec::with_capacity(self.value_exprs.len());
            for expr in &self.value_exprs {
                let value = eval_expr(expr, &ctx.eval_ctx(&empty))?;
                let property = match value {
                    Value::Property(p) => p,
                    Value::Null => Property::Null,
                    Value::Node(_)
                    | Value::Edge(_)
                    | Value::List(_)
                    | Value::Map(_)
                    | Value::Path { .. } => {
                        return Err(Error::InvalidSetValue);
                    }
                };
                values.push(property);
            }
            let ids = ctx
                .store
                .nodes_by_properties(&self.label, &self.properties, &values)?;
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

/// Physical operator for [`LogicalPlan::PointIndexSeek`]. Evaluates
/// its bounds once on first `next()` and drives
/// `reader.nodes_in_bbox(...)`.
///
/// For [`PointSeekBounds::Corners`] the operator passes the corners
/// through as-is and short-circuits on SRID mismatch / null / non-Point
/// corners — `point.withinbbox` returns null in those cases, which
/// excludes the row from a filter, so "no rows" is the same
/// observable outcome.
///
/// For [`PointSeekBounds::Radius`] the operator derives the enclosing
/// bbox from the center's SRID: Cartesian gets a `(cx ± r, cy ± r)`
/// square; WGS-84 converts `r` metres to lat/lon spans using the
/// `cos(lat)` factor for longitude. The enclosing bbox is always a
/// *superset* of the circle, so the planner keeps the original
/// distance predicate as a residual `Filter` above the seek — the
/// corners of the square that fall outside the circle are culled
/// there.
struct PointIndexSeekOp {
    var: String,
    label: String,
    property: String,
    bounds: PointSeekBounds,
    results: Option<Vec<NodeId>>,
    cursor: usize,
}

impl PointIndexSeekOp {
    fn new(var: String, label: String, property: String, bounds: PointSeekBounds) -> Self {
        Self {
            var,
            label,
            property,
            bounds,
            results: None,
            cursor: 0,
        }
    }
}

impl Operator for PointIndexSeekOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.results.is_none() {
            let empty = Row::new();
            let ectx = ctx.eval_ctx(&empty);
            let ids = match &self.bounds {
                PointSeekBounds::Corners { lo, hi } => {
                    let lo_pt = extract_point(&eval_expr(lo, &ectx)?);
                    let hi_pt = extract_point(&eval_expr(hi, &ectx)?);
                    match (lo_pt, hi_pt) {
                        (Some(lo), Some(hi)) if lo.srid == hi.srid => ctx.store.nodes_in_bbox(
                            &self.label,
                            &self.property,
                            lo.srid,
                            lo.x,
                            lo.y,
                            hi.x,
                            hi.y,
                        )?,
                        _ => Vec::new(),
                    }
                }
                PointSeekBounds::Radius { center, radius } => {
                    let center_pt = extract_point(&eval_expr(center, &ectx)?);
                    let radius_val = extract_f64(&eval_expr(radius, &ectx)?);
                    match (center_pt, radius_val) {
                        (Some(c), Some(r)) if r.is_finite() && r >= 0.0 => {
                            let (xlo, ylo, xhi, yhi) = enclosing_bbox(&c, r);
                            ctx.store.nodes_in_bbox(
                                &self.label,
                                &self.property,
                                c.srid,
                                xlo,
                                ylo,
                                xhi,
                                yhi,
                            )?
                        }
                        // Null / non-point center or null / negative / NaN radius → no rows.
                        _ => Vec::new(),
                    }
                }
            };
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

fn extract_point(v: &Value) -> Option<meshdb_core::Point> {
    match v {
        Value::Property(Property::Point(p)) => Some(*p),
        _ => None,
    }
}

fn extract_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Property(Property::Float64(f)) => Some(*f),
        Value::Property(Property::Int64(i)) => Some(*i as f64),
        _ => None,
    }
}

/// Enclosing axis-aligned bbox for a circle of radius `r` around
/// `center`. Always a superset of the circle, so callers must still
/// apply a precision filter on distance. Cartesian SRIDs use a
/// straight `center ± r` square in the CRS's own units. Geographic
/// SRIDs (WGS-84, SRID 4326 / 4979) convert `r` metres to lat/lon
/// spans via the standard flat-earth approximation with a `cos(lat)`
/// longitude-span correction; the factor is clamped away from zero
/// so near-pole circles don't collapse into a zero-width bbox (the
/// pathological case falls back to the full longitude range).
fn enclosing_bbox(center: &meshdb_core::Point, r: f64) -> (f64, f64, f64, f64) {
    if center.is_geographic() {
        // One degree of latitude ≈ 111_320 metres on a spherical
        // Earth. One degree of longitude shrinks by cos(latitude).
        const METRES_PER_DEG: f64 = 111_320.0;
        let dlat = r / METRES_PER_DEG;
        let cos_lat = center.y.to_radians().cos().abs();
        // `cos(lat)` goes to 0 at the poles; clamp so a polar
        // circle maps to a wide-but-finite longitude span rather
        // than a divide-by-zero.
        let cos_lat_floor = cos_lat.max(1.0e-6);
        let dlon = r / (METRES_PER_DEG * cos_lat_floor);
        (
            center.x - dlon,
            center.y - dlat,
            center.x + dlon,
            center.y + dlat,
        )
    } else {
        (center.x - r, center.y - r, center.x + r, center.y + r)
    }
}

/// Relationship-scope analogue of [`IndexSeekOp`]. Seeks edges
/// through the `(edge_type, property)` index, hydrates each one,
/// and emits a row per matching edge binding `edge_var`, `src_var`,
/// `dst_var`. For `Direction::Both` each edge emits two rows — one
/// per orientation — so an undirected pattern surfaces both
/// endpoint assignments. `residual_properties` carries any
/// non-indexed pattern-property equalities that still need
/// per-edge filtering.
struct EdgeSeekOp {
    edge_var: String,
    src_var: String,
    dst_var: String,
    edge_type: String,
    property: String,
    value_expr: Expr,
    direction: Direction,
    residual_properties: Vec<(String, Expr)>,
    /// Pre-materialized output rows. Built lazily on the first
    /// `next()` call so the seek + endpoint fetches run once.
    results: Option<Vec<Row>>,
    cursor: usize,
}

impl EdgeSeekOp {
    #[allow(clippy::too_many_arguments)]
    fn new(
        edge_var: String,
        src_var: String,
        dst_var: String,
        edge_type: String,
        property: String,
        value_expr: Expr,
        direction: Direction,
        residual_properties: Vec<(String, Expr)>,
    ) -> Self {
        Self {
            edge_var,
            src_var,
            dst_var,
            edge_type,
            property,
            value_expr,
            direction,
            residual_properties,
            results: None,
            cursor: 0,
        }
    }
}

impl Operator for EdgeSeekOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.results.is_none() {
            let empty = Row::new();
            let seek_value = eval_expr(&self.value_expr, &ctx.eval_ctx(&empty))?;
            let property = match seek_value {
                Value::Property(p) => p,
                Value::Null => Property::Null,
                Value::Node(_)
                | Value::Edge(_)
                | Value::List(_)
                | Value::Map(_)
                | Value::Path { .. } => {
                    return Err(Error::InvalidSetValue);
                }
            };
            let ids = ctx
                .store
                .edges_by_property(&self.edge_type, &self.property, &property)?;
            let mut rows: Vec<Row> = Vec::with_capacity(ids.len());
            for id in ids {
                let Some(edge) = ctx.store.get_edge(id)? else {
                    continue;
                };
                // Residual pattern-property equality filters — same
                // shape as `EdgeExpandOp`'s inline edge-properties
                // check. Uses an empty row for expr evaluation since
                // an EdgeSeek has no upstream input and the seek's
                // own bindings aren't visible to its own filters.
                let mut residuals_ok = true;
                for (key, expr) in &self.residual_properties {
                    let wanted = eval_expr(expr, &ctx.eval_ctx(&empty))?;
                    let Some(stored) = edge.properties.get(key) else {
                        residuals_ok = false;
                        break;
                    };
                    if !values_equal(&Value::Property(stored.clone()), &wanted) {
                        residuals_ok = false;
                        break;
                    }
                }
                if !residuals_ok {
                    continue;
                }
                // Hydrate both endpoints so downstream operators
                // can read labels / properties off the bound vars.
                // A deleted endpoint drops the whole row (same as
                // `IndexSeekOp` when `get_node` returns None).
                let Some(src_node) = ctx.store.get_node(edge.source)? else {
                    continue;
                };
                let Some(dst_node) = ctx.store.get_node(edge.target)? else {
                    continue;
                };
                // Emit output rows per direction. Outgoing / Incoming
                // bind a single orientation; Both yields two rows —
                // the `-[r]-` pattern in openCypher matches edges in
                // either orientation and binds endpoints accordingly.
                match self.direction {
                    Direction::Outgoing => {
                        rows.push(self.make_row(&edge, &src_node, &dst_node));
                    }
                    Direction::Incoming => {
                        rows.push(self.make_row(&edge, &dst_node, &src_node));
                    }
                    Direction::Both => {
                        rows.push(self.make_row(&edge, &src_node, &dst_node));
                        // Self-loops still only surface once — the
                        // second orientation is the same row.
                        if edge.source != edge.target {
                            rows.push(self.make_row(&edge, &dst_node, &src_node));
                        }
                    }
                }
            }
            self.results = Some(rows);
        }
        let rows = self.results.as_ref().unwrap();
        if self.cursor < rows.len() {
            let row = rows[self.cursor].clone();
            self.cursor += 1;
            return Ok(Some(row));
        }
        Ok(None)
    }
}

impl EdgeSeekOp {
    fn make_row(&self, edge: &Edge, src: &Node, dst: &Node) -> Row {
        let mut row = Row::new();
        row.insert(self.edge_var.clone(), Value::Edge(edge.clone()));
        row.insert(self.src_var.clone(), Value::Node(src.clone()));
        row.insert(self.dst_var.clone(), Value::Node(dst.clone()));
        row
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
    /// Resolve the pattern properties against `base`, scan the
    /// store, and either match existing nodes or create a fresh
    /// one. Returns the resulting node set — can be called
    /// multiple times with different `base` rows for
    /// input-driven merges.
    fn run_merge_for(&mut self, ctx: &ExecCtx, base: &Row) -> Result<Vec<Node>> {
        let resolved_props: Vec<(String, Property)> = self
            .properties
            .iter()
            .map(|(k, expr)| {
                let v = eval_expr(expr, &ctx.eval_ctx(base))?;
                Ok((k.clone(), value_to_property(v)?))
            })
            .collect::<Result<Vec<_>>>()?;

        let candidate_ids: Vec<NodeId> = if let Some(primary) = self.labels.first() {
            ctx.store.nodes_by_label(primary)?
        } else {
            ctx.store.all_node_ids()?
        };
        let mut merged_nodes: Vec<Node> = Vec::new();
        for id in candidate_ids {
            if let Some(node) = ctx.store.get_node(id)? {
                if has_all_labels(&node, &self.labels)
                    && matches_pattern_props(&node, &resolved_props)
                {
                    merged_nodes.push(node);
                }
            }
        }

        if merged_nodes.is_empty() {
            let mut node = Node::new();
            for label in &self.labels {
                node.labels.push(label.clone());
            }
            for (k, prop) in resolved_props {
                node.properties.insert(k, prop);
            }
            apply_merge_actions(&mut node, &self.on_create, &self.var, ctx, base)?;
            ctx.writer.put_node(&node)?;
            merged_nodes.push(node);
        } else if !self.on_match.is_empty() {
            for node in merged_nodes.iter_mut() {
                apply_merge_actions(node, &self.on_match, &self.var, ctx, base)?;
                ctx.writer.put_node(node)?;
            }
        }
        Ok(merged_nodes)
    }
}

impl Operator for MergeNodeOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        // Top-level producer: no upstream context, so the pattern
        // properties can only reference literals / parameters.
        // Run the merge once against an empty row, then emit
        // each result.
        if self.input.is_none() {
            if !self.merge_done {
                let empty = Row::new();
                let nodes = self.run_merge_for(ctx, &empty)?;
                self.merged_nodes = nodes;
                self.merge_done = true;
            }
            if self.cursor < self.merged_nodes.len() {
                let node = self.merged_nodes[self.cursor].clone();
                self.cursor += 1;
                let mut row = Row::new();
                row.insert(self.var.clone(), Value::Node(node));
                return Ok(Some(row));
            }
            return Ok(None);
        }

        // Input-driven case: evaluate pattern properties *per
        // input row* so references like `MERGE (:City {name:
        // person.bornIn})` resolve against the bound `person`.
        // Each incoming row produces its own merged-node set,
        // which is then cross-joined with that row before
        // emission.
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
            match self.input.as_mut().unwrap().next(ctx)? {
                None => return Ok(None),
                Some(row) => {
                    let nodes = self.run_merge_for(ctx, &row)?;
                    self.merged_nodes = nodes;
                    self.cursor = 0;
                    self.current_input_row = Some(row);
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
    undirected: bool,
    /// Inline edge property filter from the MERGE pattern
    /// (`[r:T {k: v}]`). Matched edges must satisfy every entry;
    /// the create branch stamps them onto the new edge.
    properties: Vec<(String, Expr)>,
    on_create: Vec<SetAssignment>,
    on_match: Vec<SetAssignment>,
    /// Rows buffered from the current input row's MERGE result —
    /// one per existing matched edge (or a single synthesized edge
    /// if the create branch fired). Drained before the next
    /// `input.next()` call so multi-match semantics stay correct:
    /// `MATCH (a:A),(b:B) MERGE (a)-[r:T]->(b)` against two pre-
    /// existing edges has to yield two rows, not one.
    pending: std::collections::VecDeque<Row>,
}

impl MergeEdgeOp {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Box<dyn Operator>,
        edge_var: String,
        src_var: String,
        dst_var: String,
        edge_type: String,
        undirected: bool,
        properties: Vec<(String, Expr)>,
        on_create: Vec<SetAssignment>,
        on_match: Vec<SetAssignment>,
    ) -> Self {
        Self {
            input,
            edge_var,
            src_var,
            dst_var,
            edge_type,
            undirected,
            properties,
            on_create,
            on_match,
            pending: std::collections::VecDeque::new(),
        }
    }
}

impl Operator for MergeEdgeOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                return Ok(Some(row));
            }
            let Some(row) = self.input.next(ctx)? else {
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

            // Evaluate the inline edge property filter once per
            // input row. These are AST expressions so they can
            // reference outer bindings (`MERGE (a)-[r:T {k: a.v}]->(b)`).
            let required_props: Vec<(String, Property)> = self
                .properties
                .iter()
                .map(|(k, expr)| {
                    let v = eval_expr(expr, &ctx.eval_ctx(&row))?;
                    Ok((k.clone(), value_to_property(v)?))
                })
                .collect::<Result<Vec<_>>>()?;
            let edge_matches = |edge: &Edge| -> bool {
                required_props.iter().all(|(k, want)| {
                    edge.properties
                        .get(k)
                        .map(|have| have == want)
                        .unwrap_or(false)
                })
            };

            // Collect every edge of type `edge_type` from src to
            // dst (and, for undirected patterns, dst to src) that
            // also satisfies the inline property filter. If any
            // exist we take the match branch and yield one row per
            // match. If none exist we synthesize one and yield a
            // single row from the create branch.
            let mut matched: Vec<Edge> = Vec::new();
            for (edge_id, neighbor_id) in ctx.store.outgoing(src_node.id)? {
                if neighbor_id != dst_node.id {
                    continue;
                }
                if let Some(edge) = ctx.store.get_edge(edge_id)? {
                    if edge.edge_type == self.edge_type && edge_matches(&edge) {
                        matched.push(edge);
                    }
                }
            }
            if self.undirected {
                for (edge_id, neighbor_id) in ctx.store.incoming(src_node.id)? {
                    if neighbor_id != dst_node.id {
                        continue;
                    }
                    if let Some(edge) = ctx.store.get_edge(edge_id)? {
                        if edge.edge_type == self.edge_type && edge_matches(&edge) {
                            matched.push(edge);
                        }
                    }
                }
            }

            if matched.is_empty() {
                let mut new_edge = Edge::new(&self.edge_type, src_node.id, dst_node.id);
                for (k, p) in &required_props {
                    new_edge.properties.insert(k.clone(), p.clone());
                }
                let mut row_out = row.clone();
                apply_merge_edge_actions(
                    &mut new_edge,
                    &self.on_create,
                    &self.edge_var,
                    ctx,
                    &mut row_out,
                )?;
                ctx.writer.put_edge(&new_edge)?;
                row_out.insert(self.edge_var.clone(), Value::Edge(new_edge));
                self.pending.push_back(row_out);
            } else {
                for mut existing in matched {
                    let mut row_out = row.clone();
                    if !self.on_match.is_empty() {
                        apply_merge_edge_actions(
                            &mut existing,
                            &self.on_match,
                            &self.edge_var,
                            ctx,
                            &mut row_out,
                        )?;
                        ctx.writer.put_edge(&existing)?;
                    }
                    row_out.insert(self.edge_var.clone(), Value::Edge(existing));
                    self.pending.push_back(row_out);
                }
            }
        }
    }
}

/// Edge-side counterpart of [`apply_merge_actions`]. Evaluates
/// each `SetAssignment` against the outer `row` (augmented with
/// the current edge binding) and mutates either the edge itself
/// or a non-edge target node from the outer row. MERGE's ON
/// CREATE / ON MATCH clauses are scoped against the whole input
/// row, so `MERGE (a)-[:T]->(b) ON CREATE SET b.k = 1` has to
/// reach outside the edge-local binding. Non-edge mutations
/// are persisted via `ctx.writer.put_node` so the change is
/// visible to later clauses.
fn apply_merge_edge_actions(
    edge: &mut Edge,
    actions: &[SetAssignment],
    var: &str,
    exec_ctx: &ExecCtx,
    outer: &mut Row,
) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    // Edge binding is live in `outer` while we evaluate — RHS can
    // reference both the edge and any sibling node binding.
    outer.insert(var.to_string(), Value::Edge(edge.clone()));
    for action in actions {
        match action {
            SetAssignment::Property {
                var: target,
                key,
                value,
            } => {
                let sub_ctx = exec_ctx.eval_ctx(outer);
                let evaluated = eval_expr(value, &sub_ctx)?;
                let prop = value_to_property(evaluated)?;
                if target == var {
                    if matches!(prop, Property::Null) {
                        edge.properties.remove(key);
                    } else {
                        edge.properties.insert(key.clone(), prop);
                    }
                    outer.insert(var.to_string(), Value::Edge(edge.clone()));
                } else {
                    apply_set_prop_to_outer(outer, exec_ctx, target, key, prop)?;
                }
            }
            SetAssignment::Merge {
                var: target,
                properties,
            } => {
                let sub_ctx = exec_ctx.eval_ctx(outer);
                let resolved: Vec<(String, Property)> = properties
                    .iter()
                    .map(|(k, expr)| {
                        let v = eval_expr(expr, &sub_ctx)?;
                        Ok((k.clone(), value_to_property(v)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if target == var {
                    for (k, p) in resolved {
                        edge.properties.insert(k, p);
                    }
                    outer.insert(var.to_string(), Value::Edge(edge.clone()));
                } else {
                    apply_set_map_to_outer(outer, exec_ctx, target, resolved, false)?;
                }
            }
            SetAssignment::Replace {
                var: target,
                properties,
            } => {
                let sub_ctx = exec_ctx.eval_ctx(outer);
                let resolved: Vec<(String, Property)> = properties
                    .iter()
                    .map(|(k, expr)| {
                        let v = eval_expr(expr, &sub_ctx)?;
                        Ok((k.clone(), value_to_property(v)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if target == var {
                    edge.properties.clear();
                    for (k, p) in resolved {
                        edge.properties.insert(k, p);
                    }
                    outer.insert(var.to_string(), Value::Edge(edge.clone()));
                } else {
                    apply_set_map_to_outer(outer, exec_ctx, target, resolved, true)?;
                }
            }
            SetAssignment::Labels {
                var: target,
                labels,
            } => {
                if target == var {
                    // Edges don't carry labels.
                    return Err(Error::UnboundVariable(target.clone()));
                }
                apply_set_labels_to_outer(outer, exec_ctx, target, labels)?;
            }
            SetAssignment::ReplaceFromExpr {
                var: target,
                source,
                replace,
            } => {
                let sub_ctx = exec_ctx.eval_ctx(outer);
                let v = eval_expr(source, &sub_ctx)?;
                let props = extract_property_map(&v)?;
                if target == var {
                    if *replace {
                        edge.properties.clear();
                    }
                    for (k, p) in props {
                        edge.properties.insert(k, p);
                    }
                    outer.insert(var.to_string(), Value::Edge(edge.clone()));
                } else {
                    apply_set_map_to_outer(outer, exec_ctx, target, props, *replace)?;
                }
            }
        }
    }
    Ok(())
}

/// Apply a single `SET target.key = prop` to a node or edge bound
/// in the outer row. Used by MERGE's ON CREATE / ON MATCH when
/// the target isn't the merge edge itself (the common case being
/// `MERGE (a)-[:R]->(b) ON CREATE SET b.k = v`).
fn apply_set_prop_to_outer(
    outer: &mut Row,
    exec_ctx: &ExecCtx,
    target: &str,
    key: &str,
    prop: Property,
) -> Result<()> {
    match outer.get_mut(target) {
        Some(Value::Null) | Some(Value::Property(Property::Null)) | None => {
            // SET on a null target (typically an unmatched OPTIONAL
            // MATCH binding) is a silent no-op in openCypher.
            return Ok(());
        }
        Some(Value::Node(n)) => {
            if matches!(prop, Property::Null) {
                n.properties.remove(key);
            } else {
                n.properties.insert(key.to_string(), prop);
            }
            exec_ctx.writer.put_node(n)?;
        }
        Some(Value::Edge(e)) => {
            if matches!(prop, Property::Null) {
                e.properties.remove(key);
            } else {
                e.properties.insert(key.to_string(), prop);
            }
            exec_ctx.writer.put_edge(e)?;
        }
        _ => return Err(Error::UnboundVariable(target.to_string())),
    }
    Ok(())
}

/// Apply a property-map assignment (`SET target = {..}` when
/// `replace`, or `SET target += {..}` when not) to a node or
/// edge bound in the outer row.
fn apply_set_map_to_outer(
    outer: &mut Row,
    exec_ctx: &ExecCtx,
    target: &str,
    props: Vec<(String, Property)>,
    replace: bool,
) -> Result<()> {
    match outer.get_mut(target) {
        Some(Value::Null) | Some(Value::Property(Property::Null)) | None => Ok(()),
        Some(Value::Node(n)) => {
            if replace {
                n.properties.clear();
            }
            for (k, p) in props {
                if replace || !matches!(p, Property::Null) {
                    n.properties.insert(k, p);
                } else {
                    n.properties.remove(&k);
                }
            }
            exec_ctx.writer.put_node(n)?;
            Ok(())
        }
        Some(Value::Edge(e)) => {
            if replace {
                e.properties.clear();
            }
            for (k, p) in props {
                if replace || !matches!(p, Property::Null) {
                    e.properties.insert(k, p);
                } else {
                    e.properties.remove(&k);
                }
            }
            exec_ctx.writer.put_edge(e)?;
            Ok(())
        }
        _ => Err(Error::UnboundVariable(target.to_string())),
    }
}

/// Apply a labels assignment to a node bound in the outer row.
fn apply_set_labels_to_outer(
    outer: &mut Row,
    exec_ctx: &ExecCtx,
    target: &str,
    labels: &[String],
) -> Result<()> {
    match outer.get_mut(target) {
        Some(Value::Null) | Some(Value::Property(Property::Null)) | None => Ok(()),
        Some(Value::Node(n)) => {
            for label in labels {
                if !n.labels.contains(label) {
                    n.labels.push(label.clone());
                }
            }
            exec_ctx.writer.put_node(n)?;
            Ok(())
        }
        _ => Err(Error::UnboundVariable(target.to_string())),
    }
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
    base_row: &Row,
) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    // Start from the upstream row so `ON CREATE SET n.prop = other.field`
    // can resolve `other` — then overlay the merged node under `var`.
    let mut row = base_row.clone();
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
            SetAssignment::ReplaceFromExpr {
                var: target,
                source,
                replace,
            } => {
                if target != var {
                    return Err(Error::UnboundVariable(target.clone()));
                }
                let v = eval_expr(source, &sub_ctx)?;
                let props = extract_property_map(&v)?;
                if *replace {
                    node.properties.clear();
                }
                for (k, p) in props {
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
    edge_properties: Vec<(String, Expr)>,
    edge_types: Vec<String>,
    direction: Direction,
    /// When set, only the specific edge whose id matches the
    /// row-bound value counts as a match. Used by fresh-scan
    /// MATCH patterns that reuse an edge variable from a prior
    /// clause so the new hop stays an existence check instead of
    /// rebinding and clobbering the outer edge.
    edge_constraint_var: Option<String>,
    current_row: Option<Row>,
    pending: Vec<(EdgeId, NodeId)>,
    pending_idx: usize,
}

impl EdgeExpandOp {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Box<dyn Operator>,
        src_var: String,
        edge_var: Option<String>,
        dst_var: String,
        dst_labels: Vec<String>,
        edge_properties: Vec<(String, Expr)>,
        edge_types: Vec<String>,
        direction: Direction,
        edge_constraint_var: Option<String>,
    ) -> Self {
        Self {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            edge_properties,
            edge_types,
            direction,
            edge_constraint_var,
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
                if !self.edge_types.is_empty()
                    && !self.edge_types.iter().any(|t| t == &edge.edge_type)
                {
                    continue;
                }
                // Pre-bound edge constraint: only accept the edge
                // whose id matches the row-bound value. Falls
                // through to outer scopes so a fresh right-side
                // scan of a CartesianProduct can still see the
                // bound edge from the left side. Non-edge / null
                // bindings trigger no matches at all — the
                // expansion yields nothing for that input row.
                if let Some(constraint_var) = &self.edge_constraint_var {
                    let base = self
                        .current_row
                        .as_ref()
                        .expect("pending edges without source row");
                    let expected = match ctx.lookup_binding(base, constraint_var) {
                        Some(Value::Edge(e)) => Some(e.id),
                        _ => None,
                    };
                    match expected {
                        Some(id) if id != edge.id => continue,
                        None => continue,
                        _ => {}
                    }
                }
                // Inline edge property filter: every (key, value)
                // must match the traversed edge's property of the
                // same name via `=` equality. Missing keys fail the
                // check (matching Neo4j).
                if !self.edge_properties.is_empty() {
                    let base = self
                        .current_row
                        .as_ref()
                        .expect("pending edges without source row");
                    let ectx = ctx.eval_ctx(base);
                    let mut ok = true;
                    for (key, expr) in &self.edge_properties {
                        let expected = eval_expr(expr, &ectx)?;
                        let actual = match edge.properties.get(key) {
                            Some(v) => Value::Property(v.clone()),
                            None => {
                                ok = false;
                                break;
                            }
                        };
                        if !values_equal(&actual, &expected) {
                            ok = false;
                            break;
                        }
                    }
                    if !ok {
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
                        // A null source (e.g. from OPTIONAL MATCH
                        // that matched nothing) drops the input
                        // row — `MATCH (a)-->(b)` against a null
                        // `a` is just empty, not an error.
                        Some(Value::Null) | Some(Value::Property(meshdb_core::Property::Null)) => {
                            continue
                        }
                        _ => return Err(Error::UnboundVariable(self.src_var.clone())),
                    };
                    self.pending = match self.direction {
                        Direction::Outgoing => ctx.store.outgoing(src_id)?,
                        Direction::Incoming => ctx.store.incoming(src_id)?,
                        Direction::Both => {
                            // For undirected traversal, a self-loop
                            // appears once as outgoing and once as
                            // incoming. The Cypher spec visits each
                            // physical edge once, so dedupe by
                            // edge_id before emitting.
                            let mut all = ctx.store.outgoing(src_id)?;
                            let mut seen: std::collections::HashSet<EdgeId> =
                                all.iter().map(|(e, _)| *e).collect();
                            for (e, n) in ctx.store.incoming(src_id)? {
                                if seen.insert(e) {
                                    all.push((e, n));
                                }
                            }
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
    edge_types: Vec<String>,
    direction: Direction,
    /// When set, edges whose target id differs from the node
    /// bound at this variable in the current row are skipped
    /// inside the expansion loop. A row whose outgoing edges all
    /// fail the constraint then triggers the same null-fallback
    /// as having no edges at all.
    dst_constraint_var: Option<String>,
    /// When set, the expansion only considers the single edge
    /// whose id matches the edge already bound at this row
    /// variable — used when the OPTIONAL MATCH pattern reuses
    /// an edge variable from a prior clause.
    edge_constraint_var: Option<String>,
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
        edge_types: Vec<String>,
        direction: Direction,
        dst_constraint_var: Option<String>,
        edge_constraint_var: Option<String>,
    ) -> Self {
        Self {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            dst_properties,
            edge_types,
            direction,
            dst_constraint_var,
            edge_constraint_var,
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
                if !self.edge_types.is_empty()
                    && !self.edge_types.iter().any(|t| t == &edge.edge_type)
                {
                    continue;
                }
                // Pre-bound edge constraint: only the specific
                // edge whose id matches the row-bound value
                // counts as a match. Falls through to outer
                // scopes so the constraint works for fresh
                // scans on the right side of a CartesianProduct.
                if let Some(constraint_var) = &self.edge_constraint_var {
                    let base = self
                        .current_row
                        .as_ref()
                        .expect("pending without source row");
                    let expected = match ctx.lookup_binding(base, constraint_var) {
                        Some(Value::Edge(e)) => Some(e.id),
                        _ => None,
                    };
                    match expected {
                        Some(id) if id != edge.id => continue,
                        None => continue,
                        _ => {}
                    }
                }

                let neighbor = match ctx.store.get_node(neighbor_id)? {
                    Some(n) => n,
                    None => continue,
                };
                if !has_all_labels(&neighbor, &self.dst_labels) {
                    continue;
                }
                // Bound-endpoint constraint: when the declared
                // target is already bound in the row, only edges
                // that lead to that exact node count as a match.
                // Edges failing the constraint are silently
                // skipped — if every candidate fails, the
                // per-row left-join fallback below still fires.
                if let Some(constraint_var) = &self.dst_constraint_var {
                    let base = self
                        .current_row
                        .as_ref()
                        .expect("pending without source row");
                    let bound_id = match base.get(constraint_var) {
                        Some(Value::Node(n)) => Some(n.id),
                        Some(Value::Null)
                        | Some(Value::Property(meshdb_core::Property::Null))
                        | None => None,
                        _ => None,
                    };
                    match bound_id {
                        Some(id) if id != neighbor.id => continue,
                        None => continue,
                        _ => {}
                    }
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
            //
            // Exception: when an edge / dst constraint names a
            // variable that was pre-bound in the input row, the
            // fallback must NOT clobber that variable — the
            // constraint turns the expansion into an existence
            // check and the outer value should survive through.
            if let Some(base) = self.current_row.take() {
                if !self.yielded_for_current {
                    let mut out = base;
                    if let Some(ev) = &self.edge_var {
                        let preserve = self
                            .edge_constraint_var
                            .as_ref()
                            .map(|c| c == ev)
                            .unwrap_or(false);
                        if !preserve {
                            out.insert(ev.clone(), Value::Null);
                        }
                    }
                    let preserve_dst = self
                        .dst_constraint_var
                        .as_ref()
                        .map(|c| c == &self.dst_var)
                        .unwrap_or(false);
                    if !preserve_dst {
                        out.insert(self.dst_var.clone(), Value::Null);
                    }
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
                            // For undirected traversal, a self-loop
                            // appears once as outgoing and once as
                            // incoming. The Cypher spec visits each
                            // physical edge once, so dedupe by
                            // edge_id before emitting.
                            let mut all = ctx.store.outgoing(src_id)?;
                            let mut seen: std::collections::HashSet<EdgeId> =
                                all.iter().map(|(e, _)| *e).collect();
                            for (e, n) in ctx.store.incoming(src_id)? {
                                if seen.insert(e) {
                                    all.push((e, n));
                                }
                            }
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
    edge_types: Vec<String>,
    /// Per-edge property filter — every edge along the walked
    /// path must have these `(key, value)` pairs. Mirrors the
    /// inline filter on `EdgeExpandOp`; applied during DFS so
    /// failing edges prune the branch instead of generating
    /// wrong-length results.
    edge_properties: Vec<(String, Expr)>,
    direction: Direction,
    min_hops: u64,
    max_hops: u64,
    path_var: Option<String>,
    /// Per-row left-join mode: when `true`, an input row that
    /// produces no matching paths still emits one row with the
    /// expansion's output vars bound to Null. Set for
    /// `OPTIONAL MATCH (a)-[*]->(b)` so the outer row survives
    /// even when the path search is empty.
    optional: bool,
    /// When set, paths whose terminal node id differs from the
    /// node bound at this variable in the current row are
    /// filtered out before counting as a match. Combined with
    /// `optional`, an input row whose candidate paths all miss
    /// the bound target triggers the null-fallback instead of
    /// silently dropping.
    dst_constraint_var: Option<String>,
    /// Replay mode: read the walked edge sequence from the list
    /// already bound at this row variable instead of doing DFS.
    /// Used by openCypher's "walk a pre-bound edge list" form.
    bound_edge_list_var: Option<String>,
    /// Row variables whose bound edges (or edge lists) must not
    /// appear in the walked path — enforces openCypher's
    /// relationship-uniqueness rule across hops within the same
    /// MATCH pattern. Each entry resolves against the current row
    /// (falling through to outer-scope rows) at run time; the
    /// union of every referenced edge id becomes the DFS
    /// exclusion set.
    excluded_edge_vars: Vec<String>,
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
        edge_types: Vec<String>,
        edge_properties: Vec<(String, Expr)>,
        direction: Direction,
        min_hops: u64,
        max_hops: u64,
        path_var: Option<String>,
        optional: bool,
        dst_constraint_var: Option<String>,
        bound_edge_list_var: Option<String>,
        excluded_edge_vars: Vec<String>,
    ) -> Self {
        Self {
            input,
            src_var,
            edge_var,
            dst_var,
            dst_labels,
            edge_types,
            edge_properties,
            direction,
            min_hops,
            max_hops,
            path_var,
            optional,
            dst_constraint_var,
            bound_edge_list_var,
            excluded_edge_vars,
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
        input_row: &Row,
    ) -> Result<(Vec<Vec<Edge>>, Vec<Vec<NodeId>>, Vec<NodeId>)> {
        let mut paths: Vec<Vec<Edge>> = Vec::new();
        let mut node_paths: Vec<Vec<NodeId>> = Vec::new();
        let mut targets: Vec<NodeId> = Vec::new();
        let mut edge_buf: Vec<Edge> = Vec::new();
        let mut node_buf: Vec<NodeId> = vec![start];
        // Seed `used` with edges from outer-scope bindings the
        // pattern flags as exclusions (e.g. the other hops'
        // edge / edge-list vars). A fresh walk can't revisit an
        // edge that's already bound as a relationship variable
        // elsewhere in the MATCH — that's openCypher's
        // relationship-uniqueness rule.
        let mut used: HashSet<EdgeId> = HashSet::new();
        for var in &self.excluded_edge_vars {
            match ctx.lookup_binding(input_row, var) {
                Some(Value::Edge(e)) => {
                    used.insert(e.id);
                }
                Some(Value::List(items)) => {
                    for item in items {
                        if let Value::Edge(e) = item {
                            used.insert(e.id);
                        }
                    }
                }
                _ => {}
            }
        }
        // Evaluate edge-property expected values once per input row
        // — they may reference row bindings or `$`-parameters, but
        // don't vary per walked edge.
        let expected_edge_props: Vec<(String, Value)> = if self.edge_properties.is_empty() {
            Vec::new()
        } else {
            let ectx = ctx.eval_ctx(input_row);
            self.edge_properties
                .iter()
                .map(|(k, expr)| eval_expr(expr, &ectx).map(|v| (k.clone(), v)))
                .collect::<Result<Vec<_>>>()?
        };
        self.dfs(
            ctx,
            start,
            &expected_edge_props,
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
        expected_edge_props: &[(String, Value)],
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
            if !self.edge_types.is_empty() && !self.edge_types.iter().any(|t| t == &edge.edge_type)
            {
                continue;
            }
            // Inline edge property filter: skip edges whose
            // properties don't equal the expected values computed
            // per input row. A missing property fails the check,
            // matching `EdgeExpandOp`.
            if !expected_edge_props.is_empty() {
                let mut ok = true;
                for (key, expected) in expected_edge_props {
                    let actual = match edge.properties.get(key) {
                        Some(v) => Value::Property(v.clone()),
                        None => {
                            ok = false;
                            break;
                        }
                    };
                    if !values_equal(&actual, expected) {
                        ok = false;
                        break;
                    }
                }
                if !ok {
                    continue;
                }
            }
            used.insert(eid);
            edge_buf.push(edge);
            node_buf.push(neighbor_id);
            self.dfs(
                ctx,
                neighbor_id,
                expected_edge_props,
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

/// Replay the edge sequence stored at `list_var` in `row` as a
/// var-length walk starting from `src_id`. Returns `(paths,
/// node_paths, targets)` in the same shape `enumerate` produces
/// — either one entry when the list reads as a valid connected
/// walk in `direction`, or empty when it doesn't.
///
/// Validation per step:
/// * the list element is a `Value::Edge`,
/// * the edge's optional type filter matches `edge_types`,
/// * the edge actually touches the current node and proceeds to
///   the other endpoint in the requested direction (undirected
///   accepts either).
///
/// A null / missing / non-list value, a null source, or any
/// failed step all produce an empty result — which, when the
/// caller is in optional mode, triggers the left-join null
/// fallback.
fn replay_edge_list(
    ctx: &ExecCtx,
    row: &Row,
    list_var: &str,
    src_id: Option<NodeId>,
    direction: Direction,
    edge_types: &[String],
) -> Result<(Vec<Vec<Edge>>, Vec<Vec<NodeId>>, Vec<NodeId>)> {
    let start = match src_id {
        Some(id) => id,
        None => return Ok((Vec::new(), Vec::new(), Vec::new())),
    };
    let list = match ctx.lookup_binding(row, list_var) {
        Some(Value::List(items)) => items.clone(),
        Some(Value::Property(meshdb_core::Property::List(items))) => items
            .iter()
            .cloned()
            .map(Value::Property)
            .collect::<Vec<_>>(),
        _ => return Ok((Vec::new(), Vec::new(), Vec::new())),
    };
    let mut edge_buf: Vec<Edge> = Vec::with_capacity(list.len());
    let mut node_buf: Vec<NodeId> = Vec::with_capacity(list.len() + 1);
    node_buf.push(start);
    let mut current = start;
    for item in list {
        let edge = match item {
            Value::Edge(e) => e,
            _ => return Ok((Vec::new(), Vec::new(), Vec::new())),
        };
        if !edge_types.is_empty() && !edge_types.iter().any(|t| t == &edge.edge_type) {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let next_node = match direction {
            Direction::Outgoing => {
                if edge.source != current {
                    return Ok((Vec::new(), Vec::new(), Vec::new()));
                }
                edge.target
            }
            Direction::Incoming => {
                if edge.target != current {
                    return Ok((Vec::new(), Vec::new(), Vec::new()));
                }
                edge.source
            }
            Direction::Both => {
                if edge.source == current {
                    edge.target
                } else if edge.target == current {
                    edge.source
                } else {
                    return Ok((Vec::new(), Vec::new(), Vec::new()));
                }
            }
        };
        // The stored edge shape should still exist in the graph;
        // a missing node mid-walk means the snapshot shifted and
        // the list is no longer valid.
        if ctx.store.get_node(next_node)?.is_none() {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        edge_buf.push(edge);
        node_buf.push(next_node);
        current = next_node;
    }
    Ok((vec![edge_buf], vec![node_buf], vec![current]))
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
                        Some(Value::Node(n)) => Some(n.id),
                        // Null source → no paths. Same
                        // null-propagating semantics as
                        // `EdgeExpandOp`. In optional mode we
                        // still emit the left-join fallback;
                        // otherwise we just skip.
                        Some(Value::Null) | Some(Value::Property(meshdb_core::Property::Null)) => {
                            None
                        }
                        _ => return Err(Error::UnboundVariable(self.src_var.clone())),
                    };
                    // Replay path: when the hop names an already-
                    // bound edge list (`MATCH (a)-[rs*]->(b)` with
                    // `rs = [r1, r2]` from an earlier WITH), skip
                    // DFS entirely and verify the listed edges
                    // form a connected walk from `src_id` in the
                    // required direction. Produces at most one
                    // path — the exact list that was supplied.
                    let (mut paths, mut node_paths, mut targets) =
                        if let Some(list_var) = &self.bound_edge_list_var {
                            replay_edge_list(
                                ctx,
                                &row,
                                list_var,
                                src_id,
                                self.direction,
                                &self.edge_types,
                            )?
                        } else {
                            match src_id {
                                Some(id) => self.enumerate(ctx, id, &row)?,
                                None => (Vec::new(), Vec::new(), Vec::new()),
                            }
                        };
                    // Bound-endpoint constraint: drop paths whose
                    // terminal node doesn't match the node already
                    // bound at the constraint var. When combined
                    // with `optional`, an input whose paths are
                    // all filtered out still triggers the
                    // null-fallback below.
                    if let Some(constraint_var) = &self.dst_constraint_var {
                        let target_id = match row.get(constraint_var) {
                            Some(Value::Node(n)) => Some(n.id),
                            _ => None,
                        };
                        match target_id {
                            Some(id) => {
                                let mut kept_paths = Vec::new();
                                let mut kept_node_paths = Vec::new();
                                let mut kept_targets = Vec::new();
                                for ((p, np), t) in paths
                                    .drain(..)
                                    .zip(node_paths.drain(..))
                                    .zip(targets.drain(..))
                                {
                                    if t == id {
                                        kept_paths.push(p);
                                        kept_node_paths.push(np);
                                        kept_targets.push(t);
                                    }
                                }
                                paths = kept_paths;
                                node_paths = kept_node_paths;
                                targets = kept_targets;
                            }
                            None => {
                                paths.clear();
                                node_paths.clear();
                                targets.clear();
                            }
                        }
                    }
                    if paths.is_empty() && self.optional {
                        // Left-join fallback: emit one row with
                        // the expansion's output vars set to Null
                        // so the outer OPTIONAL MATCH preserves
                        // this input row.
                        let mut out = row;
                        if let Some(ev) = &self.edge_var {
                            out.insert(ev.clone(), Value::Null);
                        }
                        out.insert(self.dst_var.clone(), Value::Null);
                        if let Some(pv) = &self.path_var {
                            out.insert(pv.clone(), Value::Null);
                        }
                        return Ok(Some(out));
                    }
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
            let v = match eval_expr(&self.predicate, &ctx.eval_ctx(&row)) {
                Ok(v) => v,
                // Type mismatches and non-boolean errors in filter predicates
                // are treated as false (row filtered out), not hard errors
                Err(Error::TypeMismatch) | Err(Error::NotBoolean) => Value::Null,
                Err(e) => return Err(e),
            };
            if to_bool(&v).unwrap_or(false) {
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

/// Passes input rows through; if the input produces zero rows,
/// emits exactly one row with `null_vars` bound to Value::Null.
/// Implements standalone OPTIONAL MATCH semantics (e.g.
/// `OPTIONAL MATCH (n) RETURN n` on an empty graph yields one
/// row with n=null rather than the empty result set).
struct CoalesceNullRowOp {
    input: Box<dyn Operator>,
    null_vars: Vec<String>,
    produced_any: bool,
    done: bool,
}

impl CoalesceNullRowOp {
    fn new(input: Box<dyn Operator>, null_vars: Vec<String>) -> Self {
        Self {
            input,
            null_vars,
            produced_any: false,
            done: false,
        }
    }
}

impl Operator for CoalesceNullRowOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.done {
            return Ok(None);
        }
        match self.input.next(ctx)? {
            Some(row) => {
                self.produced_any = true;
                Ok(Some(row))
            }
            None => {
                self.done = true;
                if self.produced_any {
                    Ok(None)
                } else {
                    let mut row = Row::new();
                    for v in &self.null_vars {
                        row.insert(v.clone(), Value::Null);
                    }
                    Ok(Some(row))
                }
            }
        }
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
                    let name = item.alias.clone().unwrap_or_else(|| {
                        item.raw_text
                            .clone()
                            .unwrap_or_else(|| default_name(&item.expr, i))
                    });
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
    render_expr_name(expr).unwrap_or_else(|| format!("col{}", idx))
}

fn render_expr_name(expr: &Expr) -> Option<String> {
    Some(match expr {
        Expr::Identifier(s) => s.clone(),
        Expr::Property { var, key } => format!("{var}.{key}"),
        Expr::PropertyAccess { base, key } => {
            // Match the source syntax's parenthesisation of a
            // bracketed base: `(list[1]).k` round-trips, while a
            // plain identifier base stays bare (`a.b`).
            if matches!(
                base.as_ref(),
                Expr::IndexAccess { .. } | Expr::SliceAccess { .. }
            ) {
                format!("({}).{key}", render_expr_name(base)?)
            } else {
                format!("{}.{key}", render_expr_name(base)?)
            }
        }
        Expr::Parameter(name) => format!("${name}"),
        Expr::Literal(Literal::String(s)) => format!("'{s}'"),
        Expr::Literal(Literal::Integer(i)) => i.to_string(),
        Expr::Literal(Literal::Float(f)) => f.to_string(),
        Expr::Literal(Literal::Boolean(b)) => b.to_string(),
        Expr::Literal(Literal::Null) => "NULL".into(),
        Expr::Call { name, args } => {
            let arg_str = match args {
                CallArgs::Star => "*".into(),
                CallArgs::Exprs(es) | CallArgs::DistinctExprs(es) => {
                    let prefix = if matches!(args, CallArgs::DistinctExprs(_)) {
                        "DISTINCT "
                    } else {
                        ""
                    };
                    let inner: Vec<String> = es.iter().filter_map(render_expr_name).collect();
                    if inner.len() != es.len() {
                        return None;
                    }
                    format!("{prefix}{}", inner.join(", "))
                }
            };
            format!("{name}({arg_str})")
        }
        Expr::BinaryOp { op, left, right } => {
            let op_str = match op {
                BinaryOp::Add => " + ",
                BinaryOp::Sub => " - ",
                BinaryOp::Mul => " * ",
                BinaryOp::Div => " / ",
                BinaryOp::Mod => " % ",
                BinaryOp::Pow => " ^ ",
            };
            format!(
                "{}{op_str}{}",
                render_expr_name(left)?,
                render_expr_name(right)?
            )
        }
        Expr::UnaryOp { op, operand } => {
            let op_str = match op {
                UnaryOp::Neg => "-",
            };
            format!("{op_str}{}", render_expr_name(operand)?)
        }
        Expr::Not(inner) => format!("NOT {}", render_expr_name(inner)?),
        Expr::IsNull { negated, inner } => {
            if *negated {
                format!("{} IS NOT NULL", render_expr_name(inner)?)
            } else {
                format!("{} IS NULL", render_expr_name(inner)?)
            }
        }
        Expr::Compare { op, left, right } => {
            let op_str = match op {
                CompareOp::Eq => " = ",
                CompareOp::Ne => " <> ",
                CompareOp::Lt => " < ",
                CompareOp::Le => " <= ",
                CompareOp::Gt => " > ",
                CompareOp::Ge => " >= ",
                CompareOp::StartsWith => " STARTS WITH ",
                CompareOp::EndsWith => " ENDS WITH ",
                CompareOp::Contains => " CONTAINS ",
                CompareOp::RegexMatch => " =~ ",
            };
            format!(
                "{}{op_str}{}",
                render_expr_name(left)?,
                render_expr_name(right)?
            )
        }
        Expr::List(items) => {
            let inner: Vec<String> = items.iter().filter_map(render_expr_name).collect();
            if inner.len() != items.len() {
                return None;
            }
            format!("[{}]", inner.join(", "))
        }
        Expr::Map(entries) => {
            let inner: Vec<String> = entries
                .iter()
                .map(|(k, v)| render_expr_name(v).map(|vn| format!("{k}: {vn}")))
                .collect::<Option<Vec<_>>>()?;
            format!("{{{}}}", inner.join(", "))
        }
        Expr::IndexAccess { base, index } => {
            format!("{}[{}]", render_expr_name(base)?, render_expr_name(index)?)
        }
        Expr::InList { element, list } => {
            format!(
                "{} IN {}",
                render_expr_name(element)?,
                render_expr_name(list)?
            )
        }
        Expr::HasLabels { expr, labels } => {
            let mut s = format!("({}", render_expr_name(expr)?);
            for l in labels {
                s.push(':');
                s.push_str(l);
            }
            s.push(')');
            s
        }
        _ => return None,
    })
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
        let mut nodes: Vec<meshdb_core::Node> = Vec::new();
        let mut edges: Vec<meshdb_core::Edge> = Vec::new();
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
    edge_types: Vec<String>,
    direction: meshdb_cypher::Direction,
    max_hops: u64,
    kind: meshdb_cypher::ShortestKind,
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
        edge_types: Vec<String>,
        direction: meshdb_cypher::Direction,
        max_hops: u64,
        kind: meshdb_cypher::ShortestKind,
    ) -> Self {
        Self {
            input,
            src_var,
            dst_var,
            path_var,
            edge_types,
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
                &self.edge_types,
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
    edge_types: &[String],
    direction: meshdb_cypher::Direction,
    max_hops: u64,
    kind: meshdb_cypher::ShortestKind,
    reader: &dyn crate::reader::GraphReader,
) -> Result<Vec<Value>> {
    use meshdb_cypher::Direction;

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
                if !edge_types.is_empty() {
                    let edge = match reader.get_edge(edge_id)? {
                        Some(e) => e,
                        None => continue,
                    };
                    if !edge_types.iter().any(|t| t == &edge.edge_type) {
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
    let only_first = matches!(kind, meshdb_cypher::ShortestKind::Shortest);
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
                // The percentile is a constant expression — evaluate
                // it once against the first row we see and stash it
                // in the state so finalize() has a number to use.
                if let Some(extra_expr) = &spec.extra_arg {
                    let need_resolve = matches!(
                        &entry.agg_states[i],
                        AggState::PercentileDisc {
                            percentile: None,
                            ..
                        } | AggState::PercentileCont {
                            percentile: None,
                            ..
                        }
                    );
                    if need_resolve {
                        let pv = eval_expr(extra_expr, &ctx.eval_ctx(&row))?;
                        let p = match pv {
                            Value::Property(Property::Float64(f)) => f,
                            Value::Property(Property::Int64(i)) => i as f64,
                            _ => 0.0,
                        };
                        // openCypher requires the percentile to
                        // lie in [0.0, 1.0]; anything else is an
                        // `ArgumentError: NumberOutOfRange`.
                        if !(0.0..=1.0).contains(&p) || p.is_nan() {
                            return Err(Error::Procedure(format!("percentile out of range: {p}")));
                        }
                        match &mut entry.agg_states[i] {
                            AggState::PercentileDisc { percentile, .. }
                            | AggState::PercentileCont { percentile, .. } => {
                                *percentile = Some(p);
                            }
                            _ => {}
                        }
                    }
                }
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
    Min(Option<Value>),
    Max(Option<Value>),
    Collect(Vec<Value>),
    StDev {
        sum: f64,
        sum_sq: f64,
        count: i64,
    },
    StDevP {
        sum: f64,
        sum_sq: f64,
        count: i64,
    },
    PercentileDisc {
        items: Vec<Value>,
        percentile: Option<f64>,
    },
    PercentileCont {
        items: Vec<Value>,
        percentile: Option<f64>,
    },
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
            AggregateFn::StDev => AggState::StDev {
                sum: 0.0,
                sum_sq: 0.0,
                count: 0,
            },
            AggregateFn::StDevP => AggState::StDevP {
                sum: 0.0,
                sum_sq: 0.0,
                count: 0,
            },
            AggregateFn::PercentileDisc => AggState::PercentileDisc {
                items: Vec::new(),
                percentile: None,
            },
            AggregateFn::PercentileCont => AggState::PercentileCont {
                items: Vec::new(),
                percentile: None,
            },
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
                // `min` / `max` ignore null inputs and operate
                // over any `Value` (including lists, nodes etc.),
                // not just scalar Properties. The ordering is
                // `compare_values`: within a single type the
                // natural order, across types the
                // `type_order_value` rank.
                let v = expr_arg_value(arg, ctx)?;
                if matches!(v, Value::Null | Value::Property(Property::Null)) {
                    // skip
                } else {
                    match slot {
                        None => *slot = Some(v),
                        Some(cur) => {
                            if compare_values(&v, cur) == Ordering::Less {
                                *cur = v;
                            }
                        }
                    }
                }
            }
            AggState::Max(slot) => {
                let v = expr_arg_value(arg, ctx)?;
                if matches!(v, Value::Null | Value::Property(Property::Null)) {
                    // skip
                } else {
                    match slot {
                        None => *slot = Some(v),
                        Some(cur) => {
                            if compare_values(&v, cur) == Ordering::Greater {
                                *cur = v;
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
            AggState::PercentileDisc { items, .. } | AggState::PercentileCont { items, .. } => {
                let v = expr_arg_value(arg, ctx)?;
                if !matches!(v, Value::Null) {
                    items.push(v);
                }
            }
            AggState::StDev { sum, sum_sq, count } | AggState::StDevP { sum, sum_sq, count } => {
                let v = expr_arg_value(arg, ctx)?;
                match v {
                    Value::Null => {}
                    Value::Property(Property::Int64(i)) => {
                        let f = i as f64;
                        *sum += f;
                        *sum_sq += f * f;
                        *count += 1;
                    }
                    Value::Property(Property::Float64(f)) => {
                        *sum += f;
                        *sum_sq += f * f;
                        *count += 1;
                    }
                    _ => return Err(Error::AggregateTypeError),
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
                Some(v) => v.clone(),
                None => Value::Null,
            },
            AggState::Collect(items) => Value::List(items.clone()),
            AggState::StDevP { sum, sum_sq, count } => {
                if *count == 0 {
                    Value::Property(Property::Float64(0.0))
                } else {
                    let n = *count as f64;
                    let variance = *sum_sq / n - (*sum / n).powi(2);
                    Value::Property(Property::Float64(variance.max(0.0).sqrt()))
                }
            }
            AggState::StDev { sum, sum_sq, count } => {
                if *count < 2 {
                    Value::Property(Property::Float64(0.0))
                } else {
                    let n = *count as f64;
                    let variance = (*sum_sq - *sum * *sum / n) / (n - 1.0);
                    Value::Property(Property::Float64(variance.max(0.0).sqrt()))
                }
            }
            AggState::PercentileDisc { items, percentile } => {
                percentile_disc(items, percentile.unwrap_or(0.0))
            }
            AggState::PercentileCont { items, percentile } => {
                percentile_cont(items, percentile.unwrap_or(0.0))
            }
        }
    }
}

fn expr_arg_value(arg: &AggregateArg, ctx: &EvalCtx) -> Result<Value> {
    match arg {
        AggregateArg::Star => Err(Error::AggregateTypeError),
        AggregateArg::Expr(e) | AggregateArg::DistinctExpr(e) => eval_expr(e, ctx),
    }
}

/// Coerce a collected aggregate value into an f64 for percentile
/// math. Unhandled types fall back to NaN; the caller sorts NaN
/// out of the stream before computing the percentile.
fn value_to_f64(v: &Value) -> f64 {
    match v {
        Value::Property(Property::Int64(i)) => *i as f64,
        Value::Property(Property::Float64(f)) => *f,
        _ => f64::NAN,
    }
}

/// `percentileDisc(expr, p)` — discrete percentile. Returns the
/// smallest value at or above the `p`-ranked position. Numbers only;
/// non-numeric values get sorted to the end and are effectively
/// ignored unless the percentile lands on one.
fn percentile_disc(items: &[Value], p: f64) -> Value {
    let mut nums: Vec<(f64, Value)> = items
        .iter()
        .map(|v| (value_to_f64(v), v.clone()))
        .filter(|(f, _)| !f.is_nan())
        .collect();
    if nums.is_empty() {
        return Value::Null;
    }
    nums.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let p = p.clamp(0.0, 1.0);
    let n = nums.len();
    // Neo4j spec: ceil(p * n) - 1, clamped at 0.
    let idx = ((p * n as f64).ceil() as isize - 1).max(0) as usize;
    nums[idx.min(n - 1)].1.clone()
}

/// `percentileCont(expr, p)` — continuous percentile. Linearly
/// interpolates between the two ranks that bracket the fractional
/// position `p * (n - 1)`. Returns a Float64.
fn percentile_cont(items: &[Value], p: f64) -> Value {
    let mut nums: Vec<f64> = items
        .iter()
        .map(value_to_f64)
        .filter(|f| !f.is_nan())
        .collect();
    if nums.is_empty() {
        return Value::Null;
    }
    nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p = p.clamp(0.0, 1.0);
    let n = nums.len();
    if n == 1 {
        return Value::Property(Property::Float64(nums[0]));
    }
    let pos = p * (n as f64 - 1.0);
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    let frac = pos - lo as f64;
    let v = nums[lo] + (nums[hi] - nums[lo]) * frac;
    Value::Property(Property::Float64(v))
}

struct SkipOp {
    input: Box<dyn Operator>,
    count_expr: Expr,
    remaining: Option<i64>,
}

impl SkipOp {
    fn new(input: Box<dyn Operator>, count_expr: Expr) -> Self {
        Self {
            input,
            count_expr,
            remaining: None,
        }
    }
}

impl Operator for SkipOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.remaining.is_none() {
            let empty = Row::new();
            let ectx = ctx.eval_ctx(&empty);
            let val = eval_expr(&self.count_expr, &ectx)?;
            self.remaining = Some(expr_to_count(val)?);
        }
        let rem = self.remaining.as_mut().unwrap();
        while *rem > 0 {
            if self.input.next(ctx)?.is_none() {
                return Ok(None);
            }
            *rem -= 1;
        }
        self.input.next(ctx)
    }
}

struct LimitOp {
    input: Box<dyn Operator>,
    count_expr: Expr,
    remaining: Option<i64>,
}

impl LimitOp {
    fn new(input: Box<dyn Operator>, count_expr: Expr) -> Self {
        Self {
            input,
            count_expr,
            remaining: None,
        }
    }
}

impl Operator for LimitOp {
    fn next(&mut self, ctx: &ExecCtx) -> Result<Option<Row>> {
        if self.remaining.is_none() {
            let empty = Row::new();
            let ectx = ctx.eval_ctx(&empty);
            let val = eval_expr(&self.count_expr, &ectx)?;
            self.remaining = Some(expr_to_count(val)?);
        }
        let rem = self.remaining.as_mut().unwrap();
        if *rem <= 0 {
            return Ok(None);
        }
        match self.input.next(ctx)? {
            Some(row) => {
                *rem -= 1;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }
}

fn expr_to_count(val: Value) -> Result<i64> {
    match val {
        Value::Null | Value::Property(Property::Null) => Ok(0),
        Value::Property(Property::Int64(n)) if n >= 0 => Ok(n),
        // openCypher: SKIP/LIMIT require an integer. Float values
        // (including integer-valued floats) are rejected as
        // InvalidArgumentType — they'd only be valid after an
        // explicit cast, which the user must write themselves.
        _ => Err(Error::TypeMismatch),
    }
}
