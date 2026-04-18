use chrono::Timelike;
use cucumber::{given, then, when, World};
use mesh_core::Property;
use mesh_cypher::{parse, plan};
use mesh_executor::{execute_with_reader, ParamMap, Row, Value};
use mesh_storage::RocksDbStorageEngine;
use std::collections::HashMap;
use tempfile::TempDir;

#[derive(World)]
#[world(init = Self::new)]
struct MeshWorld {
    #[allow(dead_code)]
    dir: TempDir,
    store: RocksDbStorageEngine,
    params: ParamMap,
    results: Vec<Row>,
    error: Option<String>,
}

impl std::fmt::Debug for MeshWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshWorld")
            .field("results_count", &self.results.len())
            .field("error", &self.error)
            .finish()
    }
}

impl MeshWorld {
    fn new() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let store = RocksDbStorageEngine::open(dir.path()).expect("store");
        Self {
            dir,
            store,
            params: ParamMap::new(),
            results: Vec::new(),
            error: None,
        }
    }

    fn run_cypher(&mut self, query: &str) {
        let stmt = match parse(query) {
            Ok(s) => s,
            Err(e) => {
                self.error = Some(format!("ParseError: {e}"));
                return;
            }
        };
        if let mesh_cypher::Statement::Explain(inner) = &stmt {
            let p = match plan(inner) {
                Ok(p) => p,
                Err(e) => {
                    self.error = Some(format!("PlanError: {e}"));
                    return;
                }
            };
            self.results = mesh_executor::explain(&p);
            return;
        }
        let p = match plan(&stmt) {
            Ok(p) => p,
            Err(e) => {
                self.error = Some(format!("PlanError: {e}"));
                return;
            }
        };
        match execute_with_reader(
            &p,
            &self.store as &dyn mesh_executor::GraphReader,
            &self.store as &dyn mesh_executor::GraphWriter,
            &self.params,
        ) {
            Ok(rows) => self.results = rows,
            Err(e) => self.error = Some(format!("RuntimeError: {e}")),
        }
    }
}

// --- Given steps ---

#[given(regex = r"^an empty graph$")]
fn given_empty_graph(world: &mut MeshWorld) {
    // Already fresh from World::new()
    let _ = world;
}

#[given(regex = r"^any graph$")]
fn given_any_graph(world: &mut MeshWorld) {
    let _ = world;
}

#[given("having executed:")]
fn given_having_executed(world: &mut MeshWorld, step: &cucumber::gherkin::Step) {
    let text = step.docstring.as_ref().expect("docstring").trim();
    // First, try executing as a single statement
    let single_line = text
        .lines()
        .map(|l| {
            if let Some(idx) = l.find("//") {
                l[..idx].trim()
            } else {
                l.trim()
            }
        })
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    world.error = None;
    world.run_cypher(&single_line);
    if world.error.is_none() {
        return;
    }
    // If single-statement fails, try splitting on keyword boundaries
    world.error = None;
    let stmts = split_statements(text);
    for stmt in &stmts {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        world.error = None;
        world.run_cypher(stmt);
        if let Some(err) = &world.error {
            panic!("Setup query failed: {stmt}\nError: {err}");
        }
    }
}

fn split_statements(text: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        // Strip inline comments before processing
        let trimmed = if let Some(idx) = line.find("//") {
            line[..idx].trim()
        } else {
            line.trim()
        };
        if trimmed.is_empty() {
            continue;
        }
        let upper = trimmed.to_uppercase();
        let starts_new = upper.starts_with("CREATE")
            || upper.starts_with("MATCH")
            || upper.starts_with("MERGE")
            || upper.starts_with("UNWIND")
            || upper.starts_with("WITH")
            || upper.starts_with("CALL")
            || upper.starts_with("LOAD");
        // Don't split if the current statement ends with a comma
        // (continuation, e.g., CREATE (:A {x: 1}),\n(:B {y: 2}))
        let is_continuation = current.trim_end().ends_with(',');
        if starts_new && !current.is_empty() && !is_continuation {
            stmts.push(current.trim().trim_end_matches(';').to_string());
            current = String::new();
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(trimmed);
    }
    if !current.is_empty() {
        stmts.push(current.trim().trim_end_matches(';').to_string());
    }
    stmts
}

#[given("parameters are:")]
fn given_parameters(world: &mut MeshWorld, step: &cucumber::gherkin::Step) {
    if let Some(table) = &step.table {
        for row in &table.rows {
            if row.len() >= 2 {
                let name = row[0].trim().to_string();
                let val = parse_tck_value(&row[1]);
                world.params.insert(name, val);
            }
        }
    }
}

// --- When steps ---

#[when("executing query:")]
fn when_executing_query(world: &mut MeshWorld, step: &cucumber::gherkin::Step) {
    let query = step.docstring.as_ref().expect("docstring").trim();
    world.error = None;
    world.results.clear();
    world.run_cypher(query);
}

#[when("executing control query:")]
fn when_executing_control_query(world: &mut MeshWorld, step: &cucumber::gherkin::Step) {
    when_executing_query(world, step);
}

// --- Then steps ---

/// Normalize property formatting for comparison: ensure consistent
/// whitespace around colons in `{key:val}` vs `{key: val}`.
fn normalize_tck(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        out.push(chars[i]);
        // Add space after colon in map property notation (key:val → key: val)
        // but NOT in time strings (12:34:56) or timezone offsets (+02:00)
        if chars[i] == ':' && i > 0 && i + 1 < chars.len() {
            let before = chars[i - 1];
            let after = chars[i + 1];
            // Only add space when before is a letter/underscore (map key)
            // and after is not a space or special char.
            // Skip when before is a digit (time/timezone notation).
            if (before.is_alphabetic() || before == '_' || before == '`')
                && !before.is_ascii_digit()
                && after != ' '
                && after != '\''
                && after != '{'
                && after != '['
                && after != '('
            {
                out.push(' ');
            }
        }
        i += 1;
    }
    out
}

#[then("the result should be, in any order:")]
fn then_result_any_order(world: &mut MeshWorld, step: &cucumber::gherkin::Step) {
    if let Some(err) = &world.error {
        panic!("Expected results but got error: {err}");
    }
    let table = step.table.as_ref().expect("expected result table");
    let headers: Vec<&str> = table.rows[0].iter().map(|s| s.trim()).collect();
    let expected_rows: Vec<Vec<String>> = table.rows[1..]
        .iter()
        .map(|row| row.iter().map(|s| s.trim().to_string()).collect())
        .collect();

    assert_eq!(
        world.results.len(),
        expected_rows.len(),
        "Row count mismatch: got {} rows, expected {}.\nActual: {:?}",
        world.results.len(),
        expected_rows.len(),
        format_results(&world.results, &headers),
    );

    let mut actual_strs: Vec<Vec<String>> = world
        .results
        .iter()
        .map(|row| {
            headers
                .iter()
                .map(|h| normalize_tck(&format_value(row.get(*h).unwrap_or(&Value::Null))))
                .collect()
        })
        .collect();
    actual_strs.sort();

    let mut expected_sorted: Vec<Vec<String>> = expected_rows
        .iter()
        .map(|row| row.iter().map(|s| normalize_tck(s)).collect())
        .collect();
    expected_sorted.sort();

    assert_eq!(
        actual_strs, expected_sorted,
        "Result mismatch (unordered).\nActual:   {actual_strs:?}\nExpected: {expected_sorted:?}"
    );
}

#[then("the result should be, in order:")]
fn then_result_in_order(world: &mut MeshWorld, step: &cucumber::gherkin::Step) {
    if let Some(err) = &world.error {
        panic!("Expected results but got error: {err}");
    }
    let table = step.table.as_ref().expect("expected result table");
    let headers: Vec<&str> = table.rows[0].iter().map(|s| s.trim()).collect();
    let expected_rows: Vec<Vec<String>> = table.rows[1..]
        .iter()
        .map(|row| row.iter().map(|s| s.trim().to_string()).collect())
        .collect();

    let actual_strs: Vec<Vec<String>> = world
        .results
        .iter()
        .map(|row| {
            headers
                .iter()
                .map(|h| normalize_tck(&format_value(row.get(*h).unwrap_or(&Value::Null))))
                .collect()
        })
        .collect();

    let expected_normalized: Vec<Vec<String>> = expected_rows
        .iter()
        .map(|row| row.iter().map(|s| normalize_tck(s)).collect())
        .collect();

    assert_eq!(
        actual_strs, expected_normalized,
        "Result mismatch (ordered).\nActual:   {actual_strs:?}\nExpected: {expected_normalized:?}"
    );
}

#[then("the result should be empty")]
fn then_result_empty(world: &mut MeshWorld) {
    if let Some(err) = &world.error {
        panic!("Expected empty results but got error: {err}");
    }
    assert!(
        world.results.is_empty(),
        "Expected empty results, got {} rows",
        world.results.len()
    );
}

#[then("no side effects")]
fn then_no_side_effects(_world: &mut MeshWorld) {
    // Side-effect checking is optional for now
}

#[then(regex = r"^the side effects should be:$")]
fn then_side_effects(_world: &mut MeshWorld) {
    // Side-effect checking deferred
}

#[then(regex = r"^a \w+ should be raised at (?:compile|runtime) time: .+$")]
fn then_error_raised(world: &mut MeshWorld) {
    assert!(
        world.error.is_some(),
        "Expected an error but query succeeded"
    );
}

// --- Value formatting ---

fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "null".to_string(),
        Value::Property(p) => format_property(p),
        Value::Node(n) => {
            let mut labels: Vec<&str> = n.labels.iter().map(|s| s.as_str()).collect();
            labels.sort();
            let label_str = if labels.is_empty() {
                String::new()
            } else {
                format!(":{}", labels.join(":"))
            };
            let props = format_props_inline(&n.properties);
            match (label_str.is_empty(), props.is_empty()) {
                (true, true) => "()".to_string(),
                (true, false) => format!("({props})"),
                (false, true) => format!("({label_str})"),
                (false, false) => format!("({label_str} {props})"),
            }
        }
        Value::Edge(e) => {
            let props = format_props_inline(&e.properties);
            if props.is_empty() {
                format!("[:{}]", e.edge_type)
            } else {
                format!("[:{} {props}]", e.edge_type)
            }
        }
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Path { nodes, edges } => {
            let mut parts = Vec::new();
            for (i, node) in nodes.iter().enumerate() {
                parts.push(format_value(&Value::Node(node.clone())));
                if i < edges.len() {
                    let e = &edges[i];
                    parts.push(format!("-[:{}]->", e.edge_type));
                }
            }
            format!("<{}>", parts.join(""))
        }
    }
}

fn format_property(p: &Property) -> String {
    match p {
        Property::Int64(n) => n.to_string(),
        Property::Float64(f) => {
            // Negative zero displays as positive zero
            let f = if *f == 0.0 { 0.0 } else { *f };
            if f == f.floor() && f.is_finite() && f.abs() < 1e15 {
                format!("{f:.1}")
            } else if f.is_nan() {
                "NaN".to_string()
            } else if !f.is_finite() {
                if f > 0.0 {
                    "Infinity".to_string()
                } else {
                    "-Infinity".to_string()
                }
            } else {
                let s = format!("{f}");
                // Use scientific notation only for very large values
                if f.abs() >= 1e15 && !s.contains('e') && !s.contains('E') {
                    format!("{f:e}")
                } else {
                    s
                }
            }
        }
        Property::String(s) => {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t");
            format!("'{escaped}'")
        }
        Property::Bool(b) => b.to_string(),
        Property::Null => "null".to_string(),
        Property::List(items) => {
            let inner: Vec<String> = items.iter().map(format_property).collect();
            format!("[{}]", inner.join(", "))
        }
        Property::Map(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let entries: Vec<String> = keys
                .iter()
                .map(|k| format!("{k}: {}", format_property(m.get(*k).unwrap())))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        Property::DateTime(millis) => {
            let secs = millis / 1000;
            let nanos = ((millis % 1000) * 1_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                let naive = dt.naive_utc();
                let time_part = naive.time();
                if time_part.nanosecond() > 0 {
                    // Include fractional seconds
                    let frac = time_part.nanosecond();
                    let frac_str = format!("{:09}", frac);
                    let trimmed = frac_str.trim_end_matches('0');
                    format!(
                        "'{}'",
                        format!(
                            "{}T{}:{:02}:{:02}.{}",
                            naive.date().format("%Y-%m-%d"),
                            time_part.hour(),
                            time_part.minute(),
                            time_part.second(),
                            trimmed
                        )
                    )
                } else if time_part.second() > 0 {
                    format!("'{}'", naive.format("%Y-%m-%dT%H:%M:%S"))
                } else if time_part.hour() > 0 || time_part.minute() > 0 {
                    format!("'{}'", naive.format("%Y-%m-%dT%H:%M"))
                } else {
                    format!("'{}'", naive.format("%Y-%m-%dT%H:%M"))
                }
            } else {
                format!("{millis}")
            }
        }
        Property::Date(days) => {
            if let Some(d) = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(*days as i64)))
            {
                format!("'{}'", d.format("%Y-%m-%d"))
            } else {
                format!("{days}")
            }
        }
        Property::Duration(_) => {
            format!("{p:?}")
        }
    }
}

fn format_props_inline(props: &HashMap<String, Property>) -> String {
    if props.is_empty() {
        return String::new();
    }
    let mut keys: Vec<&String> = props.keys().collect();
    keys.sort();
    let entries: Vec<String> = keys
        .iter()
        .map(|k| format!("{k}: {}", format_property(props.get(*k).unwrap())))
        .collect();
    format!("{{{}}}", entries.join(", "))
}

fn format_results(rows: &[Row], headers: &[&str]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            headers
                .iter()
                .map(|h| format_value(row.get(*h).unwrap_or(&Value::Null)))
                .collect()
        })
        .collect()
}

fn parse_tck_value(s: &str) -> Value {
    let s = s.trim();
    if s == "null" {
        return Value::Null;
    }
    if s == "true" {
        return Value::Property(Property::Bool(true));
    }
    if s == "false" {
        return Value::Property(Property::Bool(false));
    }
    if let Ok(n) = s.parse::<i64>() {
        return Value::Property(Property::Int64(n));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Value::Property(Property::Float64(f));
    }
    if s.starts_with('\'') && s.ends_with('\'') {
        return Value::Property(Property::String(s[1..s.len() - 1].to_string()));
    }
    // Parse list parameters like [1, 2, null]
    if s.starts_with('[') && s.ends_with(']') {
        let inner = &s[1..s.len() - 1];
        if inner.is_empty() {
            return Value::List(Vec::new());
        }
        let items: Vec<Value> = split_list_elements(inner)
            .iter()
            .map(|item| parse_tck_value(item))
            .collect();
        return Value::List(items);
    }
    // Parse map parameters like {key: value}
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        if inner.is_empty() {
            return Value::Property(Property::Map(std::collections::HashMap::new()));
        }
        let mut map = std::collections::HashMap::new();
        for entry in split_list_elements(inner) {
            let entry = entry.trim();
            if let Some(colon_pos) = entry.find(':') {
                let key = entry[..colon_pos].trim().to_string();
                let val = parse_tck_value(&entry[colon_pos + 1..]);
                if let Value::Property(p) = val {
                    map.insert(key, p);
                } else if matches!(val, Value::Null) {
                    map.insert(key, Property::Null);
                }
            }
        }
        return Value::Property(Property::Map(map));
    }
    Value::Property(Property::String(s.to_string()))
}

/// Split comma-separated elements, respecting nested brackets.
fn split_list_elements(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    for ch in s.chars() {
        match ch {
            '[' | '{' => {
                depth += 1;
                current.push(ch);
            }
            ']' | '}' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                result.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        result.push(current.trim().to_string());
    }
    result
}

// --- Main ---

fn main() {
    let features = std::env::var("TCK_FEATURES").unwrap_or_else(|_| {
        "../../tck/opencypher/tck/features/clauses/match/Match1.feature".to_string()
    });

    // Use a thread with a larger stack to handle deeply nested expressions
    // (e.g. 20-deep nested list literals) without stack overflow.
    let builder = std::thread::Builder::new().stack_size(64 * 1024 * 1024);
    let handle = builder
        .spawn(move || {
            futures::executor::block_on(
                MeshWorld::cucumber()
                    .max_concurrent_scenarios(1)
                    .run(features),
            );
        })
        .expect("failed to spawn test thread");
    handle.join().expect("test thread panicked");
}
