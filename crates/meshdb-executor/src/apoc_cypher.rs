//! Implementation of the `apoc.cypher.*` procedure family.
//!
//! These procedures let a Cypher query execute another Cypher
//! query at run time. The string is parsed, planned, and
//! executed against the same reader / writer / procedure
//! registry the outer query is using — so inner writes land in
//! the same buffered commit, and inner `CALL` sites resolve
//! against the same set of registered procedures (including
//! other `apoc.cypher.*` invocations, though nothing prevents
//! callers from recursing unboundedly).
//!
//! Two entry points are exposed:
//!
//! * `apoc.cypher.run(cypher, params)` — rejects plans
//!   containing native write operators. Nested procedure calls
//!   *are* allowed, so the read-only guarantee is structural,
//!   not transitive.
//! * `apoc.cypher.doIt(cypher, params)` — allows everything the
//!   outer query could do; the writer is threaded through.
//!
//! Both are registered as write built-ins so `resolve_write_rows`
//! gets the procedure registry (needed to recurse into the
//! executor). The read-only check in `run` runs after planning
//! and before execution.

use crate::error::{Error, Result};
use crate::procedures::{ProcRow, ProcedureRegistry};
use crate::reader::GraphReader;
use crate::value::{ParamMap, Value};
use crate::writer::GraphWriter;
use meshdb_core::Property;
use std::collections::HashMap;

/// Shared entry point for `apoc.cypher.run` (`allow_writes =
/// false`) and `apoc.cypher.doIt` (`allow_writes = true`).
/// Materialises the full result set eagerly — a downstream
/// `LIMIT` on the outer CALL still truncates the output rows
/// but doesn't stop the inner execution early. Good enough for
/// the typical use case (moderate result sets); streaming would
/// require exposing the operator pipeline as a lazy iterator.
pub fn run_cypher(
    reader: &dyn GraphReader,
    writer: &dyn GraphWriter,
    args: &[Value],
    procedures: &ProcedureRegistry,
    allow_writes: bool,
) -> Result<Vec<ProcRow>> {
    if args.len() != 2 {
        return Err(Error::Procedure(format!(
            "apoc.cypher.* expects 2 arguments (cypher, params), got {}",
            args.len()
        )));
    }
    let cypher = match &args[0] {
        Value::Property(Property::String(s)) => s.clone(),
        Value::Null | Value::Property(Property::Null) => {
            return Err(Error::Procedure(
                "apoc.cypher.*: first argument (cypher) must not be null".into(),
            ));
        }
        other => {
            return Err(Error::Procedure(format!(
                "apoc.cypher.*: first argument (cypher) must be a string, got {other:?}"
            )));
        }
    };
    let inner_params = params_from_arg(&args[1])?;
    let stmt = meshdb_cypher::parse(&cypher)
        .map_err(|e| Error::Procedure(format!("apoc.cypher.*: parse error in inner query: {e}")))?;
    let plan = meshdb_cypher::plan(&stmt)
        .map_err(|e| Error::Procedure(format!("apoc.cypher.*: plan error: {e}")))?;
    if !allow_writes && crate::ops::plan_contains_writes(&plan) {
        return Err(Error::Procedure(
            "apoc.cypher.run cannot execute a query that contains writes — use apoc.cypher.doIt"
                .into(),
        ));
    }
    let rows = crate::ops::execute_with_reader_and_procs(
        &plan,
        reader,
        writer,
        &inner_params,
        procedures,
    )?;
    // Each inner result row becomes one procedure output row
    // under `value`. Row is already a `HashMap<String, Value>`,
    // which is exactly what `Value::Map` carries — no lowering
    // needed. Nodes / edges / paths flow through unchanged so
    // the caller can project `value.node` or `value.path` and
    // still see full graph-element shapes.
    Ok(rows
        .into_iter()
        .map(|row| {
            let mut out: ProcRow = HashMap::new();
            out.insert("value".to_string(), Value::Map(row));
            out
        })
        .collect())
}

/// Convert the `params` argument into a `ParamMap`. Accepts a
/// `Value::Map` (preserves graph-element entries), a
/// `Value::Property(Property::Map)` (scalar-only), or null
/// (empty). Anything else is a type error — callers must pass a
/// map literal or `$params` placeholder.
fn params_from_arg(v: &Value) -> Result<ParamMap> {
    match v {
        Value::Null | Value::Property(Property::Null) => Ok(HashMap::new()),
        Value::Map(entries) => Ok(entries.clone()),
        Value::Property(Property::Map(entries)) => Ok(entries
            .iter()
            .map(|(k, p)| (k.clone(), Value::Property(p.clone())))
            .collect()),
        other => Err(Error::Procedure(format!(
            "apoc.cypher.*: second argument (params) must be a map, got {other:?}"
        ))),
    }
}
