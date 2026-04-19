use crate::value::Value;
use mesh_core::Property;
use std::collections::HashMap;

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
#[derive(Debug, Clone)]
pub struct Procedure {
    pub qualified_name: Vec<String>,
    pub inputs: Vec<ProcArgSpec>,
    pub outputs: Vec<ProcOutSpec>,
    pub rows: Vec<ProcRow>,
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
}

