//! Implementation of the `apoc.export.*` procedure family.
//!
//! Mirror image of [`crate::apoc_load`]: three formats (CSV,
//! JSON, re-runnable Cypher), two entry points each (`*.all` to
//! serialise the whole graph, `*.query` to serialise a Cypher
//! result set). Reuses [`crate::apoc_load::ImportConfig`]'s
//! `allow_file` / `file_root` gates — one operator permission
//! governs both directions — plus the new
//! [`crate::apoc_load::resolve_export_path`] helper that
//! allows writing to a file which may not yet exist.
//!
//! Each procedure yields a single stats row (`file` / `source` /
//! `format` / `nodes` / `relationships` / `properties` /
//! `rows` / `time`) so dashboards can diff before / after.
//! The write itself is eager — full graph walk for `.all`, full
//! query exec for `.query` — because the yield shape is a
//! single aggregate; streaming buys nothing here.
//!
//! Skipped for this cut (deferred until a use case demands
//! them): `.data` / `.graph` variants (specific subsets),
//! `.schema` (DDL-only exports), HTTP destinations, progress /
//! batch reporting.

use crate::apoc_load::{resolve_export_path, ImportConfig};
use crate::error::{Error, Result};
use crate::procedures::{ProcRow, ProcedureRegistry};
use crate::reader::GraphReader;
use crate::value::Value;
use meshdb_core::{Edge, Node, NodeId, Property};
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

/// Rolled-up stats returned to the caller. Column names mirror
/// Neo4j APOC so ported dashboards keep working. `time` is
/// milliseconds since call entry. `rows` is populated for the
/// `.query` variants; for `.all` it equals `nodes + relationships`
/// (what Neo4j reports, since the "database source" emits one row
/// per element).
#[derive(Debug, Default)]
pub struct ExportStats {
    pub file: PathBuf,
    pub source: String,
    pub format: &'static str,
    pub nodes: i64,
    pub relationships: i64,
    pub properties: i64,
    pub rows: i64,
    pub time_ms: i64,
}

impl ExportStats {
    fn into_proc_row(self) -> ProcRow {
        let mut row: ProcRow = HashMap::new();
        row.insert(
            "file".to_string(),
            Value::Property(Property::String(self.file.display().to_string())),
        );
        row.insert(
            "source".to_string(),
            Value::Property(Property::String(self.source)),
        );
        row.insert(
            "format".to_string(),
            Value::Property(Property::String(self.format.to_string())),
        );
        row.insert(
            "nodes".to_string(),
            Value::Property(Property::Int64(self.nodes)),
        );
        row.insert(
            "relationships".to_string(),
            Value::Property(Property::Int64(self.relationships)),
        );
        row.insert(
            "properties".to_string(),
            Value::Property(Property::Int64(self.properties)),
        );
        row.insert(
            "rows".to_string(),
            Value::Property(Property::Int64(self.rows)),
        );
        row.insert(
            "time".to_string(),
            Value::Property(Property::Int64(self.time_ms)),
        );
        row
    }
}

/// Shared entry point for the three `.all` variants. Walks every
/// node and relationship via the reader, hands them to the
/// format-specific writer, and returns a single stats row.
fn export_all<W: GraphElementWriter>(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    file_arg: &str,
    mut writer: W,
) -> Result<Vec<ProcRow>> {
    let path = resolve_export_path(cfg, file_arg)?;
    let start = Instant::now();
    let mut sink = File::create(&path).map_err(|e| {
        Error::Procedure(format!(
            "apoc.export.{0}: cannot create file {path:?}: {e}",
            W::FORMAT
        ))
    })?;
    writer.begin(&mut sink)?;
    let mut stats = ExportStats {
        file: path.clone(),
        source: "database".to_string(),
        format: W::FORMAT,
        ..Default::default()
    };
    // Walk nodes first, then relationships, so the downstream
    // CSV header + Cypher re-import order matches what Neo4j
    // APOC writes.
    for nid in reader.all_node_ids()? {
        if let Some(node) = reader.get_node(nid)? {
            stats.properties += node.properties.len() as i64;
            writer.node(&mut sink, &node)?;
            stats.nodes += 1;
        }
    }
    for nid in reader.all_node_ids()? {
        for (edge_id, _) in reader.outgoing(nid)? {
            if let Some(edge) = reader.get_edge(edge_id)? {
                stats.properties += edge.properties.len() as i64;
                writer.relationship(&mut sink, &edge)?;
                stats.relationships += 1;
            }
        }
    }
    writer.end(&mut sink)?;
    sink.flush().map_err(|e| {
        Error::Procedure(format!(
            "apoc.export.{0}: flushing {path:?}: {e}",
            W::FORMAT
        ))
    })?;
    // `.all` reports `rows` as the total element count — matches
    // Neo4j APOC's "one row per node / rel emitted" counter.
    stats.rows = stats.nodes + stats.relationships;
    stats.time_ms = start.elapsed().as_millis() as i64;
    Ok(vec![stats.into_proc_row()])
}

/// Shared entry point for the three `.query` variants. Parses /
/// plans / executes the supplied Cypher, rejects plans containing
/// writes, and hands each result row to the format-specific
/// writer.
fn export_query<W: QueryRowWriter>(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    procedures: &ProcedureRegistry,
    query_arg: &str,
    file_arg: &str,
    mut writer: W,
) -> Result<Vec<ProcRow>> {
    let path = resolve_export_path(cfg, file_arg)?;
    let start = Instant::now();
    // Parse + plan first so we fail before touching the
    // filesystem if the query is ill-formed.
    let stmt = meshdb_cypher::parse(query_arg).map_err(|e| {
        Error::Procedure(format!(
            "apoc.export.{0}.query: parse error in inner query: {e}",
            W::FORMAT
        ))
    })?;
    let plan = meshdb_cypher::plan(&stmt).map_err(|e| {
        Error::Procedure(format!("apoc.export.{0}.query: plan error: {e}", W::FORMAT))
    })?;
    if crate::ops::plan_contains_writes(&plan) {
        return Err(Error::Procedure(format!(
            "apoc.export.{0}.query: inner query contains writes — export expects a read-only query",
            W::FORMAT
        )));
    }
    // A no-op writer shim — `apoc.export.*` is read-only from
    // the graph's perspective, but `execute_with_reader_and_procs`
    // requires a writer for type reasons. The read-only check
    // above ensures no write ever actually flows through this.
    let write_stub = NoOpWriter;
    let inner_params = meshdb_executor_params_empty();
    let rows = crate::ops::execute_with_reader_and_procs(
        &plan,
        reader,
        &write_stub as &dyn crate::writer::GraphWriter,
        &inner_params,
        procedures,
    )?;
    let mut sink = File::create(&path).map_err(|e| {
        Error::Procedure(format!(
            "apoc.export.{0}.query: cannot create file {path:?}: {e}",
            W::FORMAT
        ))
    })?;
    writer.begin(&mut sink, &rows)?;
    let mut stats = ExportStats {
        file: path.clone(),
        source: query_arg.to_string(),
        format: W::FORMAT,
        ..Default::default()
    };
    for row in &rows {
        let (n, r, p) = writer.row(&mut sink, row)?;
        stats.nodes += n;
        stats.relationships += r;
        stats.properties += p;
        stats.rows += 1;
    }
    writer.end(&mut sink)?;
    sink.flush().map_err(|e| {
        Error::Procedure(format!(
            "apoc.export.{0}.query: flushing {path:?}: {e}",
            W::FORMAT
        ))
    })?;
    stats.time_ms = start.elapsed().as_millis() as i64;
    Ok(vec![stats.into_proc_row()])
}

/// Wrap a call-site empty [`ParamMap`] in something the
/// executor accepts. Kept as a helper so the rest of the file
/// stays free of explicit param construction.
fn meshdb_executor_params_empty() -> crate::value::ParamMap {
    crate::value::ParamMap::new()
}

/// No-op `GraphWriter` used by `apoc.export.*.query` to satisfy
/// the [`crate::ops::execute_with_reader_and_procs`] signature.
/// Every mutation method returns `Error::Unsupported`; the
/// `plan_contains_writes` check ahead of execution ensures no
/// write ever actually flows through, so these paths are
/// defensive only — triggering one indicates the write check
/// was bypassed.
struct NoOpWriter;

impl crate::writer::GraphWriter for NoOpWriter {
    fn put_node(&self, _node: &meshdb_core::Node) -> Result<()> {
        Err(Error::Unsupported(
            "apoc.export.*.query: inner query attempted a write — the read-only \
             check should have rejected this before execution"
                .into(),
        ))
    }
    fn put_edge(&self, _edge: &meshdb_core::Edge) -> Result<()> {
        Err(Error::Unsupported(
            "apoc.export.*.query: inner query attempted a write".into(),
        ))
    }
    fn delete_edge(&self, _id: meshdb_core::EdgeId) -> Result<()> {
        Err(Error::Unsupported(
            "apoc.export.*.query: inner query attempted a write".into(),
        ))
    }
    fn detach_delete_node(&self, _id: meshdb_core::NodeId) -> Result<()> {
        Err(Error::Unsupported(
            "apoc.export.*.query: inner query attempted a write".into(),
        ))
    }
}

/// Trait implemented by the three `.all` format writers. The
/// walker drives via `begin` / `node` / `relationship` / `end`.
/// Each method writes to the passed-in file handle and bumps
/// whatever internal counters the writer needs — ExportStats
/// lives on the caller, so writers don't carry their own.
trait GraphElementWriter {
    const FORMAT: &'static str;
    fn begin(&mut self, out: &mut File) -> Result<()>;
    fn node(&mut self, out: &mut File, node: &Node) -> Result<()>;
    fn relationship(&mut self, out: &mut File, edge: &Edge) -> Result<()>;
    fn end(&mut self, out: &mut File) -> Result<()>;
}

/// Trait for `.query` writers. Given a list of pre-executed
/// result rows (so headers can be computed up front when
/// needed), emit each row in the format's shape. `row` returns
/// `(nodes, rels, props)` that the row contributed — used by
/// the shared driver to accumulate stats that match Neo4j
/// APOC's counters.
trait QueryRowWriter {
    const FORMAT: &'static str;
    fn begin(&mut self, out: &mut File, rows: &[crate::value::Row]) -> Result<()>;
    fn row(&mut self, out: &mut File, row: &crate::value::Row) -> Result<(i64, i64, i64)>;
    fn end(&mut self, out: &mut File) -> Result<()>;
}

// ---------------- CSV ----------------

/// CSV writer for `apoc.export.csv.all`. Emits a single table
/// with union-of-columns header: `_id,_labels,_start,_end,_type,<every
/// observed property name>`. Matches Neo4j APOC's default CSV
/// shape (one CSV, mixed nodes + relationships, `_*` metadata
/// columns identify the row type).
struct CsvAllWriter {
    /// Columns in emission order. Stored on the writer so `begin`
    /// can scan all nodes / rels to collect the union before
    /// writing the header — the caller's walker does the actual
    /// emission.
    property_columns: Vec<String>,
    /// Finalised header written during `begin` (empty before).
    header: Vec<String>,
    inner: Option<csv::Writer<File>>,
    collected_nodes: Vec<Node>,
    collected_edges: Vec<Edge>,
}

impl CsvAllWriter {
    fn new() -> Self {
        Self {
            property_columns: Vec::new(),
            header: Vec::new(),
            inner: None,
            collected_nodes: Vec::new(),
            collected_edges: Vec::new(),
        }
    }

    fn flush_header(&mut self, out: &mut File) -> Result<()> {
        // Build column union across every collected node + edge.
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for n in &self.collected_nodes {
            for k in n.properties.keys() {
                seen.insert(k.clone());
            }
        }
        for e in &self.collected_edges {
            for k in e.properties.keys() {
                seen.insert(k.clone());
            }
        }
        self.property_columns = seen.into_iter().collect();
        // Fixed metadata columns ahead of the property columns.
        self.header = vec![
            "_id".to_string(),
            "_labels".into(),
            "_start".into(),
            "_end".into(),
            "_type".into(),
        ];
        self.header.extend(self.property_columns.iter().cloned());
        let mut writer =
            csv::Writer::from_writer(out.try_clone().map_err(|e| {
                Error::Procedure(format!("apoc.export.csv: cloning file handle: {e}"))
            })?);
        writer
            .write_record(&self.header)
            .map_err(|e| Error::Procedure(format!("apoc.export.csv: writing header: {e}")))?;
        self.inner = Some(writer);
        Ok(())
    }
}

impl GraphElementWriter for CsvAllWriter {
    const FORMAT: &'static str = "csv";

    fn begin(&mut self, _out: &mut File) -> Result<()> {
        // No-op — we can't write the header until the walker has
        // handed us every node and relationship (for the column
        // union). `node` / `relationship` buffer; `end` flushes.
        Ok(())
    }

    fn node(&mut self, _out: &mut File, node: &Node) -> Result<()> {
        self.collected_nodes.push(node.clone());
        Ok(())
    }

    fn relationship(&mut self, _out: &mut File, edge: &Edge) -> Result<()> {
        self.collected_edges.push(edge.clone());
        Ok(())
    }

    fn end(&mut self, out: &mut File) -> Result<()> {
        self.flush_header(out)?;
        let writer = self
            .inner
            .as_mut()
            .expect("flush_header initialised the writer");
        for node in &self.collected_nodes {
            let mut record: Vec<String> = Vec::with_capacity(self.header.len());
            record.push(format!("{:?}", node.id));
            record.push(node.labels.join(":"));
            record.push(String::new());
            record.push(String::new());
            record.push(String::new());
            for col in &self.property_columns {
                record.push(
                    node.properties
                        .get(col)
                        .map(property_to_csv_cell)
                        .unwrap_or_default(),
                );
            }
            writer
                .write_record(&record)
                .map_err(|e| Error::Procedure(format!("apoc.export.csv: writing node: {e}")))?;
        }
        for edge in &self.collected_edges {
            let mut record: Vec<String> = Vec::with_capacity(self.header.len());
            record.push(format!("{:?}", edge.id));
            record.push(String::new());
            record.push(format!("{:?}", edge.source));
            record.push(format!("{:?}", edge.target));
            record.push(edge.edge_type.clone());
            for col in &self.property_columns {
                record.push(
                    edge.properties
                        .get(col)
                        .map(property_to_csv_cell)
                        .unwrap_or_default(),
                );
            }
            writer
                .write_record(&record)
                .map_err(|e| Error::Procedure(format!("apoc.export.csv: writing edge: {e}")))?;
        }
        writer
            .flush()
            .map_err(|e| Error::Procedure(format!("apoc.export.csv: flushing csv writer: {e}")))?;
        Ok(())
    }
}

/// CSV writer for `apoc.export.csv.query`. Emits one CSV row
/// per input query result, with columns from the first row's
/// keys (sorted) used as the header.
struct CsvQueryWriter {
    header: Vec<String>,
    inner: Option<csv::Writer<File>>,
}

impl CsvQueryWriter {
    fn new() -> Self {
        Self {
            header: Vec::new(),
            inner: None,
        }
    }
}

impl QueryRowWriter for CsvQueryWriter {
    const FORMAT: &'static str = "csv";

    fn begin(&mut self, out: &mut File, rows: &[crate::value::Row]) -> Result<()> {
        // Header = union of keys across all rows. Sorted for
        // determinism.
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for row in rows {
            for k in row.keys() {
                seen.insert(k.clone());
            }
        }
        self.header = seen.into_iter().collect();
        let mut writer = csv::Writer::from_writer(out.try_clone().map_err(|e| {
            Error::Procedure(format!("apoc.export.csv.query: cloning file handle: {e}"))
        })?);
        writer
            .write_record(&self.header)
            .map_err(|e| Error::Procedure(format!("apoc.export.csv.query: writing header: {e}")))?;
        self.inner = Some(writer);
        Ok(())
    }

    fn row(&mut self, _out: &mut File, row: &crate::value::Row) -> Result<(i64, i64, i64)> {
        let writer = self.inner.as_mut().expect("begin initialised the writer");
        let mut record: Vec<String> = Vec::with_capacity(self.header.len());
        let mut props = 0i64;
        for col in &self.header {
            let cell = match row.get(col) {
                Some(v) => {
                    props += 1;
                    value_to_csv_cell(v)
                }
                None => String::new(),
            };
            record.push(cell);
        }
        writer
            .write_record(&record)
            .map_err(|e| Error::Procedure(format!("apoc.export.csv.query: writing row: {e}")))?;
        Ok((0, 0, props))
    }

    fn end(&mut self, _out: &mut File) -> Result<()> {
        if let Some(writer) = self.inner.as_mut() {
            writer.flush().map_err(|e| {
                Error::Procedure(format!("apoc.export.csv.query: flushing writer: {e}"))
            })?;
        }
        Ok(())
    }
}

/// Lower a `Property` to a string suitable for one CSV cell.
/// Nulls are the empty string; lists / maps round-trip through
/// `serde_json` for a compact one-line form — keeps the CSV
/// flat while still capturing structure.
fn property_to_csv_cell(p: &Property) -> String {
    match p {
        Property::Null => String::new(),
        Property::String(s) => s.clone(),
        Property::Int64(n) => n.to_string(),
        Property::Float64(f) => f.to_string(),
        Property::Bool(b) => b.to_string(),
        other => {
            // Lists, maps, temporals, points — use the JSON
            // representation so the cell stays single-line.
            property_to_json(other).to_string()
        }
    }
}

/// CSV-cell variant for a `Value` (only encountered via
/// `.query`, where rows can carry Node / Edge / Path).
fn value_to_csv_cell(v: &Value) -> String {
    match v {
        Value::Property(p) => property_to_csv_cell(p),
        Value::Null => String::new(),
        Value::Node(n) => node_to_json(n).to_string(),
        Value::Edge(e) => edge_to_json(e).to_string(),
        Value::Path { nodes, edges } => path_to_json(nodes, edges).to_string(),
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect()).to_string()
        }
        Value::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj).to_string()
        }
    }
}

// ---------------- JSON ----------------

/// JSON writer for `apoc.export.json.all`. Writes one JSON
/// document per line (JSONL), matching Neo4j APOC's default.
struct JsonAllWriter;

impl GraphElementWriter for JsonAllWriter {
    const FORMAT: &'static str = "json";

    fn begin(&mut self, _out: &mut File) -> Result<()> {
        Ok(())
    }

    fn node(&mut self, out: &mut File, node: &Node) -> Result<()> {
        let obj = node_to_json(node);
        writeln!(out, "{}", obj)
            .map_err(|e| Error::Procedure(format!("apoc.export.json: writing node: {e}")))
    }

    fn relationship(&mut self, out: &mut File, edge: &Edge) -> Result<()> {
        let obj = edge_to_json(edge);
        writeln!(out, "{}", obj)
            .map_err(|e| Error::Procedure(format!("apoc.export.json: writing edge: {e}")))
    }

    fn end(&mut self, _out: &mut File) -> Result<()> {
        Ok(())
    }
}

/// JSON writer for `apoc.export.json.query`. Same JSONL format,
/// but each row becomes a JSON object keyed by the result-set
/// column names.
struct JsonQueryWriter;

impl QueryRowWriter for JsonQueryWriter {
    const FORMAT: &'static str = "json";

    fn begin(&mut self, _out: &mut File, _rows: &[crate::value::Row]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, out: &mut File, row: &crate::value::Row) -> Result<(i64, i64, i64)> {
        let mut obj = serde_json::Map::new();
        let mut nodes = 0i64;
        let mut rels = 0i64;
        let mut props = 0i64;
        for (k, v) in row {
            match v {
                Value::Node(_) => nodes += 1,
                Value::Edge(_) => rels += 1,
                _ => {}
            }
            // Property counting is approximate — one per column.
            props += 1;
            obj.insert(k.clone(), value_to_json(v));
        }
        writeln!(out, "{}", serde_json::Value::Object(obj))
            .map_err(|e| Error::Procedure(format!("apoc.export.json.query: writing row: {e}")))?;
        Ok((nodes, rels, props))
    }

    fn end(&mut self, _out: &mut File) -> Result<()> {
        Ok(())
    }
}

fn node_to_json(node: &Node) -> serde_json::Value {
    let props: serde_json::Map<String, serde_json::Value> = node
        .properties
        .iter()
        .map(|(k, p)| (k.clone(), property_to_json(p)))
        .collect();
    let labels = serde_json::Value::Array(
        node.labels
            .iter()
            .map(|l| serde_json::Value::String(l.clone()))
            .collect(),
    );
    serde_json::json!({
        "type": "node",
        "id": format!("{:?}", node.id),
        "labels": labels,
        "properties": serde_json::Value::Object(props),
    })
}

fn edge_to_json(edge: &Edge) -> serde_json::Value {
    let props: serde_json::Map<String, serde_json::Value> = edge
        .properties
        .iter()
        .map(|(k, p)| (k.clone(), property_to_json(p)))
        .collect();
    serde_json::json!({
        "type": "relationship",
        "id": format!("{:?}", edge.id),
        "label": edge.edge_type,
        "start": { "id": format!("{:?}", edge.source) },
        "end": { "id": format!("{:?}", edge.target) },
        "properties": serde_json::Value::Object(props),
    })
}

fn path_to_json(nodes: &[Node], edges: &[Edge]) -> serde_json::Value {
    let mut path: Vec<serde_json::Value> = Vec::with_capacity(nodes.len() + edges.len());
    for (i, n) in nodes.iter().enumerate() {
        path.push(node_to_json(n));
        if i < edges.len() {
            path.push(edge_to_json(&edges[i]));
        }
    }
    serde_json::Value::Array(path)
}

fn property_to_json(p: &Property) -> serde_json::Value {
    match p {
        Property::Null => serde_json::Value::Null,
        Property::String(s) => serde_json::Value::String(s.clone()),
        Property::Int64(n) => serde_json::Value::Number((*n).into()),
        Property::Float64(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Property::Bool(b) => serde_json::Value::Bool(*b),
        Property::List(items) => {
            serde_json::Value::Array(items.iter().map(property_to_json).collect())
        }
        Property::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, p)| (k.clone(), property_to_json(p)))
                .collect();
            serde_json::Value::Object(obj)
        }
        // Temporal / spatial types round-trip as strings for
        // now — matches Neo4j APOC's default JSON mapping.
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Property(p) => property_to_json(p),
        Value::Null => serde_json::Value::Null,
        Value::Node(n) => node_to_json(n),
        Value::Edge(e) => edge_to_json(e),
        Value::Path { nodes, edges } => path_to_json(nodes, edges),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

// ---------------- Cypher ----------------

/// Writer for `apoc.export.cypher.all`. Emits CREATE statements
/// for each node followed by MATCH+CREATE for each relationship.
/// Node identity is preserved via a `_mesh_id` property equal to
/// the node's internal id string — the generated script
/// re-materialises the same graph if run on an empty database.
struct CypherAllWriter {
    /// Map of original node id → the `_mesh_id` literal used by
    /// the MATCH clauses. Same representation, just renamed to
    /// signal the export-provenance.
    id_labels: HashMap<NodeId, String>,
}

impl CypherAllWriter {
    fn new() -> Self {
        Self {
            id_labels: HashMap::new(),
        }
    }
}

impl GraphElementWriter for CypherAllWriter {
    const FORMAT: &'static str = "cypher";

    fn begin(&mut self, out: &mut File) -> Result<()> {
        // Preamble comment so someone reading the generated
        // script can tell where it came from.
        writeln!(out, "// exported by apoc.export.cypher.all")
            .map_err(|e| Error::Procedure(format!("apoc.export.cypher: writing preamble: {e}")))
    }

    fn node(&mut self, out: &mut File, node: &Node) -> Result<()> {
        let id_literal = format!("{:?}", node.id);
        self.id_labels.insert(node.id, id_literal.clone());
        let labels = if node.labels.is_empty() {
            String::new()
        } else {
            format!(":{}", node.labels.join(":"))
        };
        // Emit a map literal carrying every property plus the
        // synthetic `_mesh_id` that MATCH clauses key on.
        let mut props = node.properties.clone();
        props.insert("_mesh_id".into(), Property::String(id_literal));
        writeln!(
            out,
            "CREATE (n{labels} {}) RETURN n;",
            cypher_map_literal(&props)
        )
        .map_err(|e| Error::Procedure(format!("apoc.export.cypher: writing node: {e}")))
    }

    fn relationship(&mut self, out: &mut File, edge: &Edge) -> Result<()> {
        let src = self
            .id_labels
            .get(&edge.source)
            .cloned()
            .unwrap_or_else(|| format!("{:?}", edge.source));
        let dst = self
            .id_labels
            .get(&edge.target)
            .cloned()
            .unwrap_or_else(|| format!("{:?}", edge.target));
        let props = if edge.properties.is_empty() {
            String::new()
        } else {
            format!(" {}", cypher_map_literal(&edge.properties))
        };
        writeln!(
            out,
            "MATCH (a {{_mesh_id: {}}}), (b {{_mesh_id: {}}}) \
             CREATE (a)-[:{}{}]->(b);",
            cypher_string_literal(&src),
            cypher_string_literal(&dst),
            edge.edge_type,
            props
        )
        .map_err(|e| Error::Procedure(format!("apoc.export.cypher: writing edge: {e}")))
    }

    fn end(&mut self, _out: &mut File) -> Result<()> {
        Ok(())
    }
}

/// Writer for `apoc.export.cypher.query` — produces a sequence
/// of RETURN-style statements reconstructing the query's row
/// shape. Much less opinionated than `.all` since there's no
/// graph to reconstruct; each row becomes a `WITH` that binds
/// the columns to their values. Rare in practice (most folks
/// use JSON for tabular re-import); included for parity.
struct CypherQueryWriter;

impl QueryRowWriter for CypherQueryWriter {
    const FORMAT: &'static str = "cypher";

    fn begin(&mut self, out: &mut File, _rows: &[crate::value::Row]) -> Result<()> {
        writeln!(out, "// exported by apoc.export.cypher.query").map_err(|e| {
            Error::Procedure(format!("apoc.export.cypher.query: writing preamble: {e}"))
        })
    }

    fn row(&mut self, out: &mut File, row: &crate::value::Row) -> Result<(i64, i64, i64)> {
        // Build a map literal and return it. A consumer
        // running `UNWIND $rows AS row RETURN row` could ingest
        // the emitted script by wrapping in a list — for now
        // each row is an independent statement, matching Neo4j
        // APOC's format.
        let mut entries: Vec<(String, Property)> = row
            .iter()
            .filter_map(|(k, v)| match value_to_property(v) {
                Some(p) => Some((k.clone(), p)),
                None => None,
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let props_map: HashMap<String, Property> = entries.into_iter().collect();
        writeln!(out, "RETURN {} AS row;", cypher_map_literal(&props_map))
            .map_err(|e| Error::Procedure(format!("apoc.export.cypher.query: writing row: {e}")))?;
        let props = row.len() as i64;
        Ok((0, 0, props))
    }

    fn end(&mut self, _out: &mut File) -> Result<()> {
        Ok(())
    }
}

/// Convert a `Value` to a `Property` for the Cypher export
/// writer — since the generated script can only bind scalar /
/// list / map literals. Graph elements (Node/Edge/Path) drop
/// their identity and round-trip as their property maps so the
/// emitted script stays valid Cypher.
fn value_to_property(v: &Value) -> Option<Property> {
    match v {
        Value::Property(p) => Some(p.clone()),
        Value::Null => Some(Property::Null),
        Value::Node(n) => Some(Property::Map(n.properties.clone())),
        Value::Edge(e) => Some(Property::Map(e.properties.clone())),
        Value::Path { .. } => None,
        Value::List(items) => Some(Property::List(
            items.iter().filter_map(value_to_property).collect(),
        )),
        Value::Map(entries) => Some(Property::Map(
            entries
                .iter()
                .filter_map(|(k, v)| value_to_property(v).map(|p| (k.clone(), p)))
                .collect(),
        )),
    }
}

fn cypher_map_literal(map: &HashMap<String, Property>) -> String {
    let mut entries: Vec<(&String, &Property)> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let body = entries
        .iter()
        .map(|(k, p)| format!("{}: {}", k, cypher_property_literal(p)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{body}}}")
}

fn cypher_property_literal(p: &Property) -> String {
    match p {
        Property::Null => "null".to_string(),
        Property::String(s) => cypher_string_literal(s),
        Property::Int64(n) => n.to_string(),
        Property::Float64(f) => {
            if f.is_finite() {
                format!("{f}")
            } else {
                "null".to_string()
            }
        }
        Property::Bool(b) => b.to_string(),
        Property::List(items) => {
            let body = items
                .iter()
                .map(cypher_property_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{body}]")
        }
        Property::Map(entries) => cypher_map_literal(entries),
        // Temporal / spatial literals in Cypher have constructor
        // call syntax (`datetime('...')`); for v1 we serialise
        // the debug form as a string literal and trust the
        // caller to post-process.
        other => cypher_string_literal(&format!("{other:?}")),
    }
}

fn cypher_string_literal(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n");
    format!("'{escaped}'")
}

// ---------------- Public entry points ----------------

/// `apoc.export.csv.all(file, config)`
pub fn export_csv_all(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    file_arg: &str,
) -> Result<Vec<ProcRow>> {
    export_all(reader, cfg, file_arg, CsvAllWriter::new())
}

/// `apoc.export.csv.query(query, file, config)`
pub fn export_csv_query(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    procedures: &ProcedureRegistry,
    query: &str,
    file_arg: &str,
) -> Result<Vec<ProcRow>> {
    export_query(
        reader,
        cfg,
        procedures,
        query,
        file_arg,
        CsvQueryWriter::new(),
    )
}

/// `apoc.export.json.all(file, config)`
pub fn export_json_all(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    file_arg: &str,
) -> Result<Vec<ProcRow>> {
    export_all(reader, cfg, file_arg, JsonAllWriter)
}

/// `apoc.export.json.query(query, file, config)`
pub fn export_json_query(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    procedures: &ProcedureRegistry,
    query: &str,
    file_arg: &str,
) -> Result<Vec<ProcRow>> {
    export_query(reader, cfg, procedures, query, file_arg, JsonQueryWriter)
}

/// `apoc.export.cypher.all(file, config)`
pub fn export_cypher_all(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    file_arg: &str,
) -> Result<Vec<ProcRow>> {
    export_all(reader, cfg, file_arg, CypherAllWriter::new())
}

/// `apoc.export.cypher.query(query, file, config)`
pub fn export_cypher_query(
    reader: &dyn GraphReader,
    cfg: &ImportConfig,
    procedures: &ProcedureRegistry,
    query: &str,
    file_arg: &str,
) -> Result<Vec<ProcRow>> {
    export_query(reader, cfg, procedures, query, file_arg, CypherQueryWriter)
}

/// Convert the procedure-call argument slice to `(file_arg)`
/// for the `.all` procedures. Rejects missing / wrong-type /
/// null inputs with a clear message naming the affected slot.
pub fn expect_all_args(args: &[Value]) -> Result<String> {
    if args.is_empty() {
        return Err(Error::Procedure(
            "apoc.export.*.all: expects at least 1 argument (file)".into(),
        ));
    }
    expect_string(&args[0], "first argument (file)")
}

/// Convert the procedure-call argument slice to `(query, file)`
/// for the `.query` procedures.
pub fn expect_query_args(args: &[Value]) -> Result<(String, String)> {
    if args.len() < 2 {
        return Err(Error::Procedure(
            "apoc.export.*.query: expects 2 arguments (query, file)".into(),
        ));
    }
    Ok((
        expect_string(&args[0], "first argument (query)")?,
        expect_string(&args[1], "second argument (file)")?,
    ))
}

fn expect_string(v: &Value, position: &str) -> Result<String> {
    match v {
        Value::Property(Property::String(s)) => Ok(s.clone()),
        Value::Null | Value::Property(Property::Null) => Err(Error::Procedure(format!(
            "apoc.export.*: {position} must be a string, got null"
        ))),
        other => Err(Error::Procedure(format!(
            "apoc.export.*: {position} must be a string, got {other:?}"
        ))),
    }
}
