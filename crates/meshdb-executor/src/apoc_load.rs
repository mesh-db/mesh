//! Implementation of the `apoc.load.*` procedure family.
//!
//! The shipped surface is `apoc.load.json` and `apoc.load.csv`
//! — enough to cover the common "pull data into the graph
//! from somewhere" pattern without adding the XML parser. Both
//! stream: CSV streams row-by-row natively via the `csv`
//! crate; JSON fetches the full body first (unavoidable
//! without a streaming JSON parser — Neo4j APOC does the same)
//! and then iterates over the top-level array.
//!
//! # Security model
//!
//! `apoc.load.*` opens a door — once enabled, a Cypher query
//! can read any file the server process can access and make
//! arbitrary HTTP requests. The default [`ImportConfig`] is
//! strict-disabled; operators opt in explicitly via the
//! `[apoc.import]` section of the server config. Gates:
//!
//! * **Master switch** (`enabled`): off by default. When off,
//!   every `apoc.load.*` call fails with a message explaining
//!   which config key to flip.
//! * **File vs HTTP**: `allow_file` / `allow_http` are
//!   independent — an operator who wants read-only-from-disk
//!   sets `allow_file` but leaves `allow_http` off, closing
//!   the SSRF vector.
//! * **Scoping**: `file_root` restricts file reads to a
//!   directory tree (no traversal past symlinks). `url_allowlist`
//!   restricts HTTP fetches to prefix-matched URLs.
//! * **Dev escape hatch**: `allow_unrestricted` disables all
//!   checks, for local dev only. Never flip on a prod server.
//!
//! [`resolve_source`] is the single choke point — every cursor
//! calls it before opening any I/O handle. Adding a new load
//! surface means consuming the resolved [`Source`], never
//! re-parsing the input string.

use crate::error::{Error, Result};
use crate::procedures::{ProcCursor, ProcRow, ProcedureRegistry};
use crate::reader::GraphReader;
use crate::value::Value;
use meshdb_core::Property;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

/// Runtime security configuration for `apoc.load.*` (and,
/// ultimately, `apoc.export.*`). The top-level `enabled` flag
/// is the master switch; a still-enabled config with both
/// `allow_file` and `allow_http` off is legal but makes every
/// call fail — useful for wiring the feature in on a staging
/// host while the allowlists are still being decided.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ImportConfig {
    /// Master gate. When `false`, every `apoc.load.*` call
    /// refuses with a message naming the config key.
    pub enabled: bool,
    /// When `true`, `file://` and plain-path inputs are
    /// accepted. Subject to [`Self::file_root`].
    pub allow_file: bool,
    /// When `true`, `http://` and `https://` URLs are accepted.
    /// Subject to [`Self::url_allowlist`].
    pub allow_http: bool,
    /// When set, file reads are pinned to this directory tree.
    /// An input path must be inside it after canonicalisation;
    /// symlinks pointing outside are rejected. `None` means no
    /// restriction — `allow_file = true` with no root is
    /// equivalent to "any file the process can read", so prefer
    /// setting it even in trusted environments.
    pub file_root: Option<PathBuf>,
    /// When non-empty, an HTTP URL must start with one of these
    /// prefixes. Simple prefix match — no regex — to keep the
    /// audit story straightforward. An empty list plus
    /// `allow_http = true` permits any URL.
    #[serde(default)]
    pub url_allowlist: Vec<String>,
    /// Dev-only escape hatch. When `true`, all other checks are
    /// bypassed — useful for local testing but absolutely never
    /// for production. Keeps the separate gates visible in
    /// config review.
    pub allow_unrestricted: bool,
}

/// A resolved, vetted import source. Constructed only by
/// [`resolve_source`], which enforces every ImportConfig gate
/// before handing out a variant. Downstream cursors consume
/// this — they never see the raw user string.
#[derive(Debug)]
pub enum Source {
    File(PathBuf),
    Url(String),
}

/// Apply every security gate and resolve `input` to a
/// [`Source`] or a clear error naming which gate refused.
///
/// Rejection precedence (highest first):
///
/// 1. `enabled` off → refuse everything.
/// 2. Scheme detection (`http://` / `https://` / `file://` /
///    bare path).
/// 3. `allow_http` / `allow_file` off for the detected scheme.
/// 4. `file_root` canonicalisation mismatch / `url_allowlist`
///    prefix miss.
///
/// `allow_unrestricted` short-circuits 2–4 but never 1 —
/// disabling the feature always wins.
pub fn resolve_source(cfg: &ImportConfig, input: &str) -> Result<Source> {
    if !cfg.enabled {
        return Err(Error::Procedure(
            "apoc.load.* is disabled — set apoc.import.enabled = true in the server config".into(),
        ));
    }
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(Error::Procedure(
            "apoc.load.*: source path/URL must not be empty".into(),
        ));
    }

    // Scheme detection. `http://` / `https://` → URL; `file://`
    // strips the scheme and falls through to the path branch;
    // anything else is treated as a local path.
    let (is_http, raw_path) = if let Some(rest) = trimmed.strip_prefix("http://") {
        (true, Some(format!("http://{rest}")))
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        (true, Some(format!("https://{rest}")))
    } else if let Some(rest) = trimmed.strip_prefix("file://") {
        (false, Some(rest.to_string()))
    } else {
        (false, Some(trimmed.to_string()))
    };

    if cfg.allow_unrestricted {
        return if is_http {
            Ok(Source::Url(raw_path.unwrap()))
        } else {
            Ok(Source::File(PathBuf::from(raw_path.unwrap())))
        };
    }

    if is_http {
        if !cfg.allow_http {
            return Err(Error::Procedure(
                "apoc.load.*: HTTP access is disabled — set apoc.import.allow_http = true".into(),
            ));
        }
        let url = raw_path.unwrap();
        if !cfg.url_allowlist.is_empty()
            && !cfg
                .url_allowlist
                .iter()
                .any(|prefix| url.starts_with(prefix))
        {
            return Err(Error::Procedure(format!(
                "apoc.load.*: URL '{url}' does not match any entry in apoc.import.url_allowlist"
            )));
        }
        Ok(Source::Url(url))
    } else {
        if !cfg.allow_file {
            return Err(Error::Procedure(
                "apoc.load.*: file access is disabled — set apoc.import.allow_file = true".into(),
            ));
        }
        let path = PathBuf::from(raw_path.unwrap());
        if let Some(root) = &cfg.file_root {
            let canonical_root = root
                .canonicalize()
                .map_err(|e| Error::Procedure(format!("apoc.load.*: file_root {root:?}: {e}")))?;
            // Resolve relative paths against the root, absolute
            // paths stay absolute; either way we canonicalise
            // and confirm the result is inside the root.
            let target = if path.is_absolute() {
                path.clone()
            } else {
                canonical_root.join(&path)
            };
            let canonical_target = target.canonicalize().map_err(|e| {
                Error::Procedure(format!(
                    "apoc.load.*: failed to resolve path '{}': {e}",
                    path.display()
                ))
            })?;
            if !canonical_target.starts_with(&canonical_root) {
                return Err(Error::Procedure(format!(
                    "apoc.load.*: path '{}' is outside the configured import root '{}'",
                    canonical_target.display(),
                    canonical_root.display()
                )));
            }
            Ok(Source::File(canonical_target))
        } else {
            Ok(Source::File(path))
        }
    }
}

/// Resolve a file-destination string for `apoc.export.*` against
/// the same ImportConfig gates `resolve_source` uses. Differences:
///
/// * HTTP URLs are rejected — exports are local-only in this
///   release.
/// * The target file may not yet exist; we canonicalise the
///   parent directory and join the filename onto it, confirming
///   the parent sits inside `file_root` when configured.
/// * Existing files are silently truncated on write (the
///   individual export functions are responsible for opening
///   with `File::create` — this helper only vets the path).
#[cfg(feature = "apoc-export")]
pub fn resolve_export_path(cfg: &ImportConfig, input: &str) -> Result<PathBuf> {
    if !cfg.enabled {
        return Err(Error::Procedure(
            "apoc.export.* is disabled — set apoc.import.enabled = true in the server config"
                .into(),
        ));
    }
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(Error::Procedure(
            "apoc.export.*: destination path must not be empty".into(),
        ));
    }
    // Reject HTTP explicitly — the user is probably expecting
    // something we don't support, so a clear error beats
    // accepting the path as a filename.
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Err(Error::Procedure(
            "apoc.export.*: HTTP destinations are not supported — write to a local file".into(),
        ));
    }
    let raw_path = if let Some(rest) = trimmed.strip_prefix("file://") {
        rest.to_string()
    } else {
        trimmed.to_string()
    };
    if cfg.allow_unrestricted {
        return Ok(PathBuf::from(raw_path));
    }
    if !cfg.allow_file {
        return Err(Error::Procedure(
            "apoc.export.*: file writes disabled — set apoc.import.allow_file = true".into(),
        ));
    }
    let path = PathBuf::from(raw_path);
    if let Some(root) = &cfg.file_root {
        let canonical_root = root
            .canonicalize()
            .map_err(|e| Error::Procedure(format!("apoc.export.*: file_root {root:?}: {e}")))?;
        // Split into parent + file_name so we can canonicalise
        // just the parent (which must exist) while leaving the
        // target filename unresolved.
        let target = if path.is_absolute() {
            path.clone()
        } else {
            canonical_root.join(&path)
        };
        let parent = target.parent().ok_or_else(|| {
            Error::Procedure(format!(
                "apoc.export.*: destination '{}' has no parent directory",
                target.display()
            ))
        })?;
        let file_name = target.file_name().ok_or_else(|| {
            Error::Procedure(format!(
                "apoc.export.*: destination '{}' has no file name component",
                target.display()
            ))
        })?;
        let canonical_parent = parent.canonicalize().map_err(|e| {
            Error::Procedure(format!(
                "apoc.export.*: parent directory '{}' does not exist: {e}",
                parent.display()
            ))
        })?;
        if !canonical_parent.starts_with(&canonical_root) {
            return Err(Error::Procedure(format!(
                "apoc.export.*: path '{}' is outside the configured import root '{}'",
                canonical_parent.display(),
                canonical_root.display()
            )));
        }
        Ok(canonical_parent.join(file_name))
    } else {
        Ok(path)
    }
}

/// Fetch a resolved [`Source`] as a byte buffer. Files go
/// through `std::fs`; URLs go through `reqwest::blocking`.
/// Both errors surface with a clear prefix so the call site
/// doesn't need to re-decorate them.
fn fetch_bytes(source: &Source) -> Result<Vec<u8>> {
    match source {
        Source::File(path) => std::fs::read(path)
            .map_err(|e| Error::Procedure(format!("apoc.load.*: cannot read file {path:?}: {e}"))),
        Source::Url(url) => {
            let resp = reqwest::blocking::get(url).map_err(|e| {
                Error::Procedure(format!("apoc.load.*: HTTP request to {url} failed: {e}"))
            })?;
            let status = resp.status();
            if !status.is_success() {
                return Err(Error::Procedure(format!(
                    "apoc.load.*: HTTP {status} from {url}"
                )));
            }
            resp.bytes()
                .map(|b| b.to_vec())
                .map_err(|e| Error::Procedure(format!("apoc.load.*: reading body from {url}: {e}")))
        }
    }
}

/// Convert a `serde_json::Value` to a Mesh `Property`. Numbers
/// that fit in `i64` stay integer; bigger / fractional numbers
/// become `Float64`. JSON null maps to `Property::Null`.
/// Containers recurse.
fn json_to_property(value: &serde_json::Value) -> Property {
    match value {
        serde_json::Value::Null => Property::Null,
        serde_json::Value::Bool(b) => Property::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Property::Int64(i)
            } else {
                Property::Float64(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Property::String(s.clone()),
        serde_json::Value::Array(items) => {
            Property::List(items.iter().map(json_to_property).collect())
        }
        serde_json::Value::Object(entries) => Property::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), json_to_property(v)))
                .collect(),
        ),
    }
}

/// Cursor for `apoc.load.json(urlOrPath [, path])`. Fetches
/// the full body on first advance (there's no streaming JSON
/// parser in the workspace and Neo4j APOC uses the same
/// strategy), then walks the top-level container one element
/// at a time. `path` (a JSON Pointer like `/items`) descends
/// into the document before iteration starts; a missing
/// pointer target errors loudly.
pub struct LoadJsonCursor {
    config: ImportConfig,
    input: String,
    json_pointer: Option<String>,
    /// Parsed body after the first advance. `None` until
    /// iteration starts, then `Some` with the array / single-
    /// element vector.
    items: Option<Vec<serde_json::Value>>,
    idx: usize,
}

impl LoadJsonCursor {
    pub fn new(config: ImportConfig, input: String, json_pointer: Option<String>) -> Self {
        Self {
            config,
            input,
            json_pointer,
            items: None,
            idx: 0,
        }
    }

    /// Lazily fetch + parse the source on first advance. Caches
    /// the resulting Vec in `self.items` so subsequent calls
    /// just walk the index.
    fn ensure_loaded(&mut self) -> Result<()> {
        if self.items.is_some() {
            return Ok(());
        }
        let source = resolve_source(&self.config, &self.input)?;
        let bytes = fetch_bytes(&source)?;
        let root: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Procedure(format!("apoc.load.json: parse error: {e}")))?;
        let descended = if let Some(ptr) = &self.json_pointer {
            root.pointer(ptr).cloned().ok_or_else(|| {
                Error::Procedure(format!(
                    "apoc.load.json: JSON pointer '{ptr}' did not resolve in the document"
                ))
            })?
        } else {
            root
        };
        // Arrays iterate; anything else yields a single row.
        // An empty object still yields one row ({}) per APOC
        // convention; an empty array yields zero rows.
        let items = match descended {
            serde_json::Value::Array(arr) => arr,
            other => vec![other],
        };
        self.items = Some(items);
        Ok(())
    }
}

impl ProcCursor for LoadJsonCursor {
    fn advance(&mut self, _reader: &dyn GraphReader) -> Result<Option<ProcRow>> {
        self.ensure_loaded()?;
        let items = self.items.as_ref().expect("ensure_loaded set self.items");
        if self.idx >= items.len() {
            return Ok(None);
        }
        let item = &items[self.idx];
        self.idx += 1;
        let mut row: ProcRow = HashMap::new();
        row.insert("value".to_string(), Value::Property(json_to_property(item)));
        Ok(Some(row))
    }
}

/// Per-call configuration for `apoc.load.csv`. Parsed from the
/// procedure's third argument (a map) at call time.
#[derive(Debug, Clone)]
struct LoadCsvConfig {
    /// When `true` (default), the first row is treated as the
    /// header — the `map` output column is keyed by these
    /// names. When `false`, only `lineNo` and `list` are
    /// populated; `map` is empty.
    headers: bool,
    /// Column separator. Defaults to `,`. Single-byte only —
    /// multi-byte separators aren't in Neo4j APOC either.
    delimiter: u8,
}

impl Default for LoadCsvConfig {
    fn default() -> Self {
        Self {
            headers: true,
            delimiter: b',',
        }
    }
}

/// Cursor for `apoc.load.csv(urlOrPath [, config])`. Uses
/// `csv::Reader` so records stream rather than materialising
/// the full file. The header row (when enabled) is read
/// eagerly on first advance so every subsequent row can zip
/// against it.
pub struct LoadCsvCursor {
    config: ImportConfig,
    input: String,
    csv_config: LoadCsvConfig,
    /// Lazy reader — initialised on first advance since
    /// construction is fallible (network / file open).
    state: Option<CsvState>,
    line_no: i64,
}

/// Initialised CSV reader state. Separated out so the `Option`
/// wrapper on the cursor is clean.
struct CsvState {
    reader: csv::Reader<Box<dyn std::io::Read>>,
    /// Header names captured off the first record; empty when
    /// `headers = false`.
    headers: Vec<String>,
}

impl LoadCsvCursor {
    pub fn new(config: ImportConfig, input: String, csv_config_map: Option<&Property>) -> Self {
        let csv_config = match csv_config_map {
            Some(Property::Map(entries)) => parse_csv_config(entries),
            _ => LoadCsvConfig::default(),
        };
        Self {
            config,
            input,
            csv_config,
            state: None,
            line_no: 0,
        }
    }

    fn ensure_opened(&mut self) -> Result<()> {
        if self.state.is_some() {
            return Ok(());
        }
        let source = resolve_source(&self.config, &self.input)?;
        // For file sources stream from disk; for URLs we have
        // to fetch the full body first (reqwest::blocking hands
        // back `bytes()` not a `Read`), then wrap in a Cursor.
        // True HTTP streaming would want `reqwest::blocking::Response::copy_to`
        // but the csv::Reader API takes owned Read, so we'd
        // need an adapter — skip for now; large CSVs from HTTP
        // pay the buffer cost.
        let reader_box: Box<dyn std::io::Read> = match source {
            Source::File(path) => {
                let f = std::fs::File::open(&path).map_err(|e| {
                    Error::Procedure(format!("apoc.load.csv: cannot open file {path:?}: {e}"))
                })?;
                Box::new(f)
            }
            Source::Url(url) => {
                let resp = reqwest::blocking::get(&url).map_err(|e| {
                    Error::Procedure(format!("apoc.load.csv: HTTP request to {url} failed: {e}"))
                })?;
                let status = resp.status();
                if !status.is_success() {
                    return Err(Error::Procedure(format!(
                        "apoc.load.csv: HTTP {status} from {url}"
                    )));
                }
                let bytes = resp.bytes().map_err(|e| {
                    Error::Procedure(format!("apoc.load.csv: reading body from {url}: {e}"))
                })?;
                Box::new(std::io::Cursor::new(bytes.to_vec()))
            }
        };
        let mut builder = csv::ReaderBuilder::new();
        builder
            .has_headers(self.csv_config.headers)
            .delimiter(self.csv_config.delimiter);
        let mut reader = builder.from_reader(reader_box);
        let headers = if self.csv_config.headers {
            reader
                .headers()
                .map_err(|e| Error::Procedure(format!("apoc.load.csv: reading headers: {e}")))?
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            Vec::new()
        };
        self.state = Some(CsvState { reader, headers });
        Ok(())
    }
}

/// Parse the per-call config map passed as the third arg. Only
/// `header` (singular, per Neo4j APOC) and `sep` (alias for
/// delimiter) are honoured today; unknown keys are ignored so
/// future additions don't break existing queries.
fn parse_csv_config(entries: &HashMap<String, Property>) -> LoadCsvConfig {
    let mut cfg = LoadCsvConfig::default();
    if let Some(Property::Bool(b)) = entries.get("header") {
        cfg.headers = *b;
    }
    if let Some(Property::String(s)) = entries.get("sep") {
        if let Some(first) = s.bytes().next() {
            cfg.delimiter = first;
        }
    }
    cfg
}

impl ProcCursor for LoadCsvCursor {
    fn advance(&mut self, _reader: &dyn GraphReader) -> Result<Option<ProcRow>> {
        self.ensure_opened()?;
        let state = self.state.as_mut().expect("ensure_opened set state");
        let mut record = csv::StringRecord::new();
        let has_record = state
            .reader
            .read_record(&mut record)
            .map_err(|e| Error::Procedure(format!("apoc.load.csv: reading record: {e}")))?;
        if !has_record {
            return Ok(None);
        }
        self.line_no += 1;
        let list: Vec<Property> = record
            .iter()
            .map(|s| Property::String(s.to_string()))
            .collect();
        let map: HashMap<String, Property> = if state.headers.is_empty() {
            HashMap::new()
        } else {
            state
                .headers
                .iter()
                .zip(record.iter())
                .map(|(h, v)| (h.clone(), Property::String(v.to_string())))
                .collect()
        };
        let mut row: ProcRow = HashMap::new();
        row.insert(
            "lineNo".to_string(),
            Value::Property(Property::Int64(self.line_no)),
        );
        row.insert("list".to_string(), Value::Property(Property::List(list)));
        row.insert("map".to_string(), Value::Property(Property::Map(map)));
        Ok(Some(row))
    }
}

/// Extract the [`ImportConfig`] from the [`ProcedureRegistry`].
/// A missing config is treated as "all-disabled" so
/// mis-configured servers fail closed rather than surprising
/// the operator.
pub fn import_config_from_registry(registry: &ProcedureRegistry) -> ImportConfig {
    registry.import_config().cloned().unwrap_or_default()
}

/// Helper for constructing cursors: extract a String arg with
/// null-propagation semantics replaced by an explicit error
/// (null-as-source doesn't make sense for load procedures).
pub fn expect_source_arg(v: &Value, position: &str) -> Result<String> {
    match v {
        Value::Property(Property::String(s)) => Ok(s.clone()),
        Value::Null | Value::Property(Property::Null) => Err(Error::Procedure(format!(
            "apoc.load.*: {position} must be a string, got null"
        ))),
        other => Err(Error::Procedure(format!(
            "apoc.load.*: {position} must be a string, got {other:?}"
        ))),
    }
}

/// Helper for extracting an optional string arg (used by
/// `apoc.load.json`'s second `path` / JSON Pointer argument).
pub fn expect_optional_string(v: &Value, position: &str) -> Result<Option<String>> {
    match v {
        Value::Property(Property::String(s)) if s.is_empty() => Ok(None),
        Value::Property(Property::String(s)) => Ok(Some(s.clone())),
        Value::Null | Value::Property(Property::Null) => Ok(None),
        other => Err(Error::Procedure(format!(
            "apoc.load.*: {position} must be a string or null, got {other:?}"
        ))),
    }
}

/// Hint that the argument is also allowed to be a `Property::Map`,
/// which we pass straight through to [`LoadCsvCursor::new`].
pub fn expect_optional_config_map(v: &Value) -> Result<Option<Property>> {
    match v {
        Value::Null | Value::Property(Property::Null) => Ok(None),
        Value::Property(Property::Map(_)) => Ok(Some(match v {
            Value::Property(p) => p.clone(),
            _ => unreachable!(),
        })),
        Value::Map(entries) => {
            // Lower Value::Map to Property::Map, dropping any
            // graph-element entries (a config map has no reason
            // to carry them).
            let mut out: HashMap<String, Property> = HashMap::new();
            for (k, v) in entries {
                if let Value::Property(p) = v {
                    out.insert(k.clone(), p.clone());
                }
            }
            Ok(Some(Property::Map(out)))
        }
        other => Err(Error::Procedure(format!(
            "apoc.load.*: config argument must be a map or null, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn strict_disabled() -> ImportConfig {
        ImportConfig::default()
    }

    fn allow_files_only(root: Option<&Path>) -> ImportConfig {
        ImportConfig {
            enabled: true,
            allow_file: true,
            allow_http: false,
            file_root: root.map(PathBuf::from),
            url_allowlist: Vec::new(),
            allow_unrestricted: false,
        }
    }

    #[test]
    fn resolve_source_strict_disabled_refuses_everything() {
        let cfg = strict_disabled();
        let err = resolve_source(&cfg, "/tmp/whatever.json").unwrap_err();
        assert!(err.to_string().contains("apoc.import.enabled"));
    }

    #[test]
    fn resolve_source_http_refused_when_allow_http_false() {
        let cfg = allow_files_only(None);
        let err = resolve_source(&cfg, "https://example.com/data.json").unwrap_err();
        assert!(err.to_string().contains("allow_http"));
    }

    #[test]
    fn resolve_source_file_refused_when_allow_file_false() {
        let cfg = ImportConfig {
            enabled: true,
            allow_file: false,
            allow_http: true,
            ..ImportConfig::default()
        };
        let err = resolve_source(&cfg, "/tmp/data.csv").unwrap_err();
        assert!(err.to_string().contains("allow_file"));
    }

    #[test]
    fn resolve_source_file_root_rejects_traversal_outside() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let cfg = allow_files_only(Some(dir.path()));
        let err = resolve_source(&cfg, outside.path().to_str().unwrap()).unwrap_err();
        assert!(err
            .to_string()
            .contains("outside the configured import root"));
    }

    #[test]
    fn resolve_source_file_root_accepts_files_inside() {
        let dir = tempfile::tempdir().unwrap();
        let inside = dir.path().join("data.json");
        std::fs::write(&inside, b"{}").unwrap();
        let cfg = allow_files_only(Some(dir.path()));
        let resolved = resolve_source(&cfg, inside.to_str().unwrap()).unwrap();
        matches!(resolved, Source::File(_));
    }

    #[test]
    fn resolve_source_url_allowlist_gates_prefix() {
        let cfg = ImportConfig {
            enabled: true,
            allow_file: false,
            allow_http: true,
            url_allowlist: vec!["https://data.example.com/".into()],
            ..ImportConfig::default()
        };
        let ok = resolve_source(&cfg, "https://data.example.com/foo.json").unwrap();
        matches!(ok, Source::Url(_));
        let err = resolve_source(&cfg, "https://other.example.com/foo.json").unwrap_err();
        assert!(err.to_string().contains("url_allowlist"));
    }

    #[test]
    fn resolve_source_unrestricted_bypasses_allowlists_but_not_enabled() {
        let cfg = ImportConfig {
            enabled: true,
            allow_unrestricted: true,
            ..ImportConfig::default()
        };
        // File and URL both work.
        assert!(resolve_source(&cfg, "/tmp/data.json").is_ok());
        assert!(resolve_source(&cfg, "https://anything.example/").is_ok());
        // Master switch still wins.
        let disabled = ImportConfig {
            enabled: false,
            allow_unrestricted: true,
            ..ImportConfig::default()
        };
        assert!(resolve_source(&disabled, "/tmp/data.json").is_err());
    }

    #[test]
    fn json_number_conversion_prefers_int_when_possible() {
        let int_val: serde_json::Value = serde_json::from_str("42").unwrap();
        assert!(matches!(json_to_property(&int_val), Property::Int64(42)));
        let float_val: serde_json::Value = serde_json::from_str("3.14").unwrap();
        assert!(matches!(json_to_property(&float_val), Property::Float64(_)));
    }

    #[test]
    fn json_nested_structure_rounds_through_property() {
        let doc: serde_json::Value =
            serde_json::from_str(r#"{"items": [1, 2, {"x": "y"}]}"#).unwrap();
        let p = json_to_property(&doc);
        if let Property::Map(m) = &p {
            assert!(m.contains_key("items"));
            if let Some(Property::List(items)) = m.get("items") {
                assert_eq!(items.len(), 3);
            } else {
                panic!("expected list under items");
            }
        } else {
            panic!("expected map, got {p:?}");
        }
    }
}
