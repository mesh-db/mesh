#[cfg(feature = "apoc-create")]
use crate::error::Error;
use crate::error::Result;
use crate::reader::GraphReader;
use crate::value::Value;
#[cfg(feature = "apoc-create")]
use crate::writer::GraphWriter;
#[cfg(feature = "apoc-create")]
use meshdb_core::Node;
use meshdb_core::Property;
use std::collections::{BTreeSet, HashMap};

/// Declared argument / output type for a procedure signature.
/// Mirrors the openCypher type names the TCK uses (`STRING?`,
/// `INTEGER?`, `FLOAT?`, `NUMBER?`, `BOOLEAN?`, `ANY?`). Nullability
/// is not tracked separately — every TCK type in practice is nullable
/// (`?`) and the match logic treats nulls uniformly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcType {
    String,
    Integer,
    Float,
    Number,
    Boolean,
    Any,
}

impl ProcType {
    pub fn parse(s: &str) -> Self {
        let trimmed = s.trim().trim_end_matches('?').trim();
        match trimmed.to_ascii_uppercase().as_str() {
            "STRING" => ProcType::String,
            "INTEGER" | "INT" => ProcType::Integer,
            "FLOAT" => ProcType::Float,
            "NUMBER" | "NUMERIC" => ProcType::Number,
            "BOOLEAN" | "BOOL" => ProcType::Boolean,
            _ => ProcType::Any,
        }
    }

    /// True when `value` is acceptable as a procedure argument of
    /// this declared type. Follows Neo4j's assignable-type rules:
    /// `FLOAT` accepts integers (coerced), `NUMBER` accepts both
    /// numeric kinds, `ANY` accepts everything.
    pub fn accepts(&self, value: &Value) -> bool {
        if matches!(value, Value::Null) {
            return true;
        }
        match (self, value) {
            (ProcType::Any, _) => true,
            (ProcType::String, Value::Property(Property::String(_))) => true,
            (ProcType::Integer, Value::Property(Property::Int64(_))) => true,
            (ProcType::Float, Value::Property(Property::Float64(_))) => true,
            (ProcType::Float, Value::Property(Property::Int64(_))) => true,
            (ProcType::Number, Value::Property(Property::Int64(_))) => true,
            (ProcType::Number, Value::Property(Property::Float64(_))) => true,
            (ProcType::Boolean, Value::Property(Property::Bool(_))) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcArgSpec {
    pub name: String,
    pub ty: ProcType,
}

#[derive(Debug, Clone)]
pub struct ProcOutSpec {
    pub name: String,
    pub ty: ProcType,
}

/// A procedure registered with a [`ProcedureRegistry`]. The TCK
/// harness builds one per `And there exists a procedure ...` step by
/// collating the signature and the gherkin data table: each data row
/// contributes one entry to `rows` where the leading cells are the
/// input-column values (matched against call arguments) and the
/// trailing cells are the output-column values (projected by
/// YIELD).
///
/// Built-in procedures — `db.labels()` and friends — leave `rows`
/// empty and set `builtin` so the executor materialises the row set
/// live from the current graph via [`Procedure::resolve_rows`].
#[derive(Debug, Clone)]
pub struct Procedure {
    pub qualified_name: Vec<String>,
    pub inputs: Vec<ProcArgSpec>,
    pub outputs: Vec<ProcOutSpec>,
    pub rows: Vec<ProcRow>,
    pub builtin: Option<BuiltinProc>,
}

/// Identifies a procedure whose rows are derived from the live graph
/// at call time rather than pre-populated in [`Procedure::rows`].
/// Keeps the procedure surface uniform — the executor still iterates
/// `ProcRow`s; the only difference is who produced them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinProc {
    /// `db.labels()` — yields one row per distinct node label.
    DbLabels,
    /// `db.relationshipTypes()` — yields one row per distinct edge type.
    DbRelationshipTypes,
    /// `db.propertyKeys()` — yields one row per distinct property key
    /// observed on any node or edge.
    DbPropertyKeys,
    /// `db.constraints()` — yields one row per registered constraint,
    /// carrying `name`, `label`, `property`, and `type` columns.
    /// Mirrors the `SHOW CONSTRAINTS` surface.
    DbConstraints,
    /// `apoc.create.node(labels, props)` — write builtin that
    /// creates a node with the given labels and properties and
    /// yields it as `node`. The first builtin that mutates the
    /// store, so it goes through [`Procedure::resolve_write_rows`]
    /// rather than [`Procedure::resolve_rows`] — the args drive
    /// the row directly, no candidate-row filtering needed.
    #[cfg(feature = "apoc-create")]
    ApocCreateNode,
}

/// One data-table row. Columns are keyed by declared column name
/// so the registry can look up either the input side (for arg
/// matching) or the output side (for YIELD projection) without
/// recomputing offsets.
pub type ProcRow = HashMap<String, Value>;

impl Procedure {
    /// True when the call arguments match this row's input columns.
    /// Applied per row during execution — rows whose input cells
    /// differ from the supplied arg values are filtered out.
    /// Argument-type coercion (`FLOAT` accepts an integer, etc.) is
    /// handled by the caller converting the call arg to the declared
    /// type before comparing here.
    pub fn row_matches(&self, row: &ProcRow, args: &[Value]) -> bool {
        for (spec, arg) in self.inputs.iter().zip(args.iter()) {
            let cell = row.get(&spec.name).unwrap_or(&Value::Null);
            if !values_equal_for_procedure(cell, arg) {
                return false;
            }
        }
        true
    }

    /// True when this procedure mutates the store and therefore
    /// needs to be dispatched through [`Self::resolve_write_rows`]
    /// (which receives the writer and the already-evaluated args)
    /// rather than [`Self::resolve_rows`]. Read-only built-ins and
    /// pre-populated rows return `false`.
    pub fn is_write_builtin(&self) -> bool {
        match self.builtin {
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateNode) => true,
            _ => false,
        }
    }

    /// Produce the row set the executor should iterate. Static
    /// procedures simply hand back their pre-populated `rows`;
    /// built-ins derive their rows from the live graph via `reader`.
    pub fn resolve_rows(&self, reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
        match self.builtin {
            None => Ok(self.rows.clone()),
            Some(BuiltinProc::DbLabels) => builtin_db_labels(reader),
            Some(BuiltinProc::DbRelationshipTypes) => builtin_db_relationship_types(reader),
            Some(BuiltinProc::DbPropertyKeys) => builtin_db_property_keys(reader),
            Some(BuiltinProc::DbConstraints) => builtin_db_constraints(reader),
            #[cfg(feature = "apoc-create")]
            Some(BuiltinProc::ApocCreateNode) => Err(Error::Procedure(
                "apoc.create.node is a write procedure — call via resolve_write_rows".into(),
            )),
        }
    }

    /// Write-procedure dispatch path. The args are already
    /// evaluated and type-checked; the row is produced as a side
    /// effect of the mutation (e.g. the newly-created node) so
    /// `row_matches` is skipped for these procedures.
    #[cfg(feature = "apoc-create")]
    pub fn resolve_write_rows(
        &self,
        _reader: &dyn GraphReader,
        writer: &dyn GraphWriter,
        args: &[Value],
    ) -> Result<Vec<ProcRow>> {
        match self.builtin {
            Some(BuiltinProc::ApocCreateNode) => apoc_create_node(writer, args),
            _ => Err(Error::Procedure("procedure is not a write builtin".into())),
        }
    }
}

fn str_row(column: &str, value: String) -> ProcRow {
    let mut row = HashMap::new();
    row.insert(column.to_string(), Value::Property(Property::String(value)));
    row
}

fn builtin_db_labels(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let mut labels: BTreeSet<String> = BTreeSet::new();
    for id in reader.all_node_ids()? {
        if let Some(n) = reader.get_node(id)? {
            for l in n.labels {
                labels.insert(l);
            }
        }
    }
    Ok(labels.into_iter().map(|l| str_row("label", l)).collect())
}

fn builtin_db_relationship_types(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let mut types: BTreeSet<String> = BTreeSet::new();
    for id in reader.all_node_ids()? {
        for (edge_id, _) in reader.outgoing(id)? {
            if let Some(e) = reader.get_edge(edge_id)? {
                types.insert(e.edge_type);
            }
        }
    }
    Ok(types
        .into_iter()
        .map(|t| str_row("relationshipType", t))
        .collect())
}

fn builtin_db_constraints(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let specs = reader.list_property_constraints()?;
    Ok(specs
        .into_iter()
        .map(|spec| {
            let mut row: ProcRow = HashMap::new();
            row.insert("name".into(), Value::Property(Property::String(spec.name)));
            let (scope_tag, target) = match spec.scope {
                meshdb_storage::ConstraintScope::Node(l) => ("NODE", l),
                meshdb_storage::ConstraintScope::Relationship(t) => ("RELATIONSHIP", t),
            };
            row.insert(
                "scope".into(),
                Value::Property(Property::String(scope_tag.into())),
            );
            row.insert("label".into(), Value::Property(Property::String(target)));
            let props: Vec<Property> = spec.properties.into_iter().map(Property::String).collect();
            row.insert("properties".into(), Value::Property(Property::List(props)));
            row.insert(
                "type".into(),
                Value::Property(Property::String(spec.kind.as_string())),
            );
            row
        })
        .collect())
}

fn builtin_db_property_keys(reader: &dyn GraphReader) -> Result<Vec<ProcRow>> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for id in reader.all_node_ids()? {
        if let Some(n) = reader.get_node(id)? {
            for k in n.properties.keys() {
                keys.insert(k.clone());
            }
            for (edge_id, _) in reader.outgoing(id)? {
                if let Some(e) = reader.get_edge(edge_id)? {
                    for k in e.properties.keys() {
                        keys.insert(k.clone());
                    }
                }
            }
        }
    }
    Ok(keys
        .into_iter()
        .map(|k| str_row("propertyKey", k))
        .collect())
}

/// Implementation of the `apoc.create.node(labels, props)` write
/// builtin. Constructs a [`Node`] from the args, persists it via
/// `writer.put_node`, and yields a single row with the new node
/// under the `node` column. Both args may be Null (Neo4j allows
/// `apoc.create.node(null, null)` — yields an empty unlabeled
/// node), but a non-null first arg must be a list of strings and
/// a non-null second arg must be a map.
#[cfg(feature = "apoc-create")]
fn apoc_create_node(writer: &dyn GraphWriter, args: &[Value]) -> Result<Vec<ProcRow>> {
    let labels = match &args[0] {
        Value::Null | Value::Property(Property::Null) => Vec::new(),
        Value::List(items) => items
            .iter()
            .map(|v| match v {
                Value::Property(Property::String(s)) => Ok(s.clone()),
                other => Err(Error::Procedure(format!(
                    "apoc.create.node: labels must be strings, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        Value::Property(Property::List(items)) => items
            .iter()
            .map(|p| match p {
                Property::String(s) => Ok(s.clone()),
                other => Err(Error::Procedure(format!(
                    "apoc.create.node: labels must be strings, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        other => {
            return Err(Error::Procedure(format!(
                "apoc.create.node: first argument must be a list of strings, got {other:?}"
            )));
        }
    };
    let props: HashMap<String, Property> = match &args[1] {
        Value::Null | Value::Property(Property::Null) => HashMap::new(),
        Value::Map(pairs) => pairs
            .iter()
            .map(|(k, v)| Ok((k.clone(), value_to_storable_property(v)?)))
            .collect::<Result<HashMap<_, _>>>()?,
        Value::Property(Property::Map(entries)) => entries
            .iter()
            .map(|(k, p)| (k.clone(), p.clone()))
            .collect(),
        other => {
            return Err(Error::Procedure(format!(
                "apoc.create.node: second argument must be a map, got {other:?}"
            )));
        }
    };
    let mut node = Node::new();
    node.labels = labels;
    // Skip null property values — matches the openCypher rule used
    // by `CREATE (n {k: null})`: the key is treated as absent.
    for (k, p) in props {
        if !matches!(p, Property::Null) {
            node.properties.insert(k, p);
        }
    }
    writer.put_node(&node)?;
    let mut row = HashMap::new();
    row.insert("node".to_string(), Value::Node(node));
    Ok(vec![row])
}

/// Convert a [`Value`] supplied as a procedure arg into a
/// storable [`Property`]. Mirrors the `value_to_property` helper
/// in `ops.rs` but lives here so procedures.rs can stay
/// self-contained — graph elements (Node/Edge/Path) and
/// graph-aware Maps aren't valid as stored properties.
#[cfg(feature = "apoc-create")]
fn value_to_storable_property(v: &Value) -> Result<Property> {
    match v {
        Value::Property(p) => Ok(p.clone()),
        Value::Null => Ok(Property::Null),
        Value::List(items) => {
            let props = items
                .iter()
                .map(value_to_storable_property)
                .collect::<Result<Vec<_>>>()?;
            Ok(Property::List(props))
        }
        Value::Map(_) | Value::Node(_) | Value::Edge(_) | Value::Path { .. } => {
            Err(Error::Procedure(
                "apoc.create.node: property values can't be graph elements or graph-aware maps"
                    .into(),
            ))
        }
    }
}

fn values_equal_for_procedure(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Property(Property::Int64(x)), Value::Property(Property::Int64(y))) => x == y,
        (Value::Property(Property::Float64(x)), Value::Property(Property::Float64(y))) => x == y,
        (Value::Property(Property::Int64(i)), Value::Property(Property::Float64(f)))
        | (Value::Property(Property::Float64(f)), Value::Property(Property::Int64(i))) => {
            *f == (*i as f64)
        }
        (Value::Property(Property::String(x)), Value::Property(Property::String(y))) => x == y,
        (Value::Property(Property::Bool(x)), Value::Property(Property::Bool(y))) => x == y,
        _ => a == b,
    }
}

/// Lookup table for registered procedures, keyed by fully qualified
/// name (`test.my.proc`). The executor consults this at run time;
/// callers (TCK harness, server startup) build an instance and pass
/// it to [`crate::execute_with_reader_and_procs`]. An empty registry
/// is the default, meaning no procedures are known and any CALL
/// raises `ProcedureNotFound`.
#[derive(Debug, Clone, Default)]
pub struct ProcedureRegistry {
    procs: HashMap<String, Procedure>,
}

impl ProcedureRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, proc: Procedure) {
        let key = proc.qualified_name.join(".");
        self.procs.insert(key, proc);
    }

    pub fn get(&self, qualified_name: &[String]) -> Option<&Procedure> {
        self.procs.get(&qualified_name.join("."))
    }

    /// Register the built-in `db.labels`, `db.relationshipTypes`, and
    /// `db.propertyKeys` procedures. Call once at server startup —
    /// the executor materialises each call's row set from the live
    /// graph, so no data needs to be recomputed here when new
    /// labels / types / keys appear.
    pub fn register_defaults(&mut self) {
        self.register(Procedure {
            qualified_name: vec!["db".into(), "labels".into()],
            inputs: Vec::new(),
            outputs: vec![ProcOutSpec {
                name: "label".into(),
                ty: ProcType::String,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbLabels),
        });
        self.register(Procedure {
            qualified_name: vec!["db".into(), "relationshipTypes".into()],
            inputs: Vec::new(),
            outputs: vec![ProcOutSpec {
                name: "relationshipType".into(),
                ty: ProcType::String,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbRelationshipTypes),
        });
        self.register(Procedure {
            qualified_name: vec!["db".into(), "propertyKeys".into()],
            inputs: Vec::new(),
            outputs: vec![ProcOutSpec {
                name: "propertyKey".into(),
                ty: ProcType::String,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbPropertyKeys),
        });
        self.register(Procedure {
            qualified_name: vec!["db".into(), "constraints".into()],
            inputs: Vec::new(),
            outputs: vec![
                ProcOutSpec {
                    name: "name".into(),
                    ty: ProcType::String,
                },
                ProcOutSpec {
                    name: "scope".into(),
                    ty: ProcType::String,
                },
                ProcOutSpec {
                    name: "label".into(),
                    ty: ProcType::String,
                },
                ProcOutSpec {
                    name: "properties".into(),
                    // Returned as a list of strings; the type lattice
                    // doesn't have a dedicated `List<String>` variant
                    // so `ANY` is the closest fit — the executor
                    // preserves the actual List value at call time.
                    ty: ProcType::Any,
                },
                ProcOutSpec {
                    name: "type".into(),
                    ty: ProcType::String,
                },
            ],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::DbConstraints),
        });
        #[cfg(feature = "apoc-create")]
        self.register(Procedure {
            qualified_name: vec!["apoc".into(), "create".into(), "node".into()],
            inputs: vec![
                ProcArgSpec {
                    name: "labels".into(),
                    // List<String> declared as ANY because the type
                    // lattice doesn't carry container parameters; the
                    // builtin validates structure at call time.
                    ty: ProcType::Any,
                },
                ProcArgSpec {
                    name: "props".into(),
                    ty: ProcType::Any,
                },
            ],
            outputs: vec![ProcOutSpec {
                name: "node".into(),
                ty: ProcType::Any,
            }],
            rows: Vec::new(),
            builtin: Some(BuiltinProc::ApocCreateNode),
        });
    }
}
