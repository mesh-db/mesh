//! APOC-compatible scalar functions for Mesh Cypher.
//!
//! APOC (Awesome Procedures on Cypher) is a Neo4j community library
//! that ships hundreds of helper procedures and scalar functions
//! under the `apoc.*` namespace. Mesh's implementation lives in a
//! standalone crate so the APOC surface can be compiled in or out
//! at build time.
//!
//! ## Build-time features
//!
//! Each APOC namespace is an opt-in Cargo feature. Users pick the
//! subset they want (or turn them all on with `full`) and the
//! executor-side adapter gates the dispatch on the same features.
//! A build with no APOC features is bitwise identical to a build
//! without this crate depended on at all.
//!
//! | Feature | Surface |
//! |---------|---------|
//! | `coll`  | `apoc.coll.*` collection helpers |
//! | `text`  | `apoc.text.*` string manipulation |
//! | `map`     | `apoc.map.*` map transforms |
//! | `util`    | `apoc.util.*` cryptographic digests |
//! | `convert` | `apoc.convert.*` JSON serialization |
//! | `full`    | Every shipped namespace (convenience umbrella) |
//!
//! More namespaces will add their own feature flags when they
//! land; the umbrella `full` auto-picks them up.
//!
//! The crate deliberately depends only on `meshdb-core` (for
//! [`meshdb_core::Property`]) — the executor-side adapter is what
//! bridges `Value` to the [`Property`]-based entry points here.
//! This setup avoids a cycle between the executor and apoc crates
//! and keeps the apoc surface pure: every function is a
//! side-effect-free transform over primitive types.

use meshdb_core::Property;
use thiserror::Error;

#[cfg(feature = "coll")]
pub mod coll;

#[cfg(feature = "text")]
pub mod text;

#[cfg(feature = "map")]
pub mod map;

#[cfg(feature = "util")]
pub mod util;

#[cfg(feature = "convert")]
pub mod convert;

/// Errors the APOC dispatcher surfaces back to the caller. All
/// variants are recoverable in the sense that the executor can
/// translate them into a user-facing `Error::Unsupported` /
/// `Error::TypeMismatch` without further context.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ApocError {
    /// The caller asked for a function this build of `meshdb-apoc`
    /// doesn't export. Either the name is mistyped or it's part of
    /// a namespace not yet implemented.
    #[error("unknown APOC function: {0}")]
    UnknownFunction(String),
    /// The argument count doesn't match the function's signature.
    /// `expected` carries the documented arity; `got` is the actual
    /// argument count. Separate variant from `TypeMismatch` so
    /// callers can render a more targeted message.
    #[error("{name} expects {expected} argument(s), got {got}")]
    Arity {
        name: String,
        expected: usize,
        got: usize,
    },
    /// An argument had the wrong `Property` kind for the operation.
    /// `details` is free-form but typically names the offending
    /// argument position and the unexpected kind.
    #[error("{name}: type error — {details}")]
    TypeMismatch { name: String, details: String },
}

/// Top-level dispatcher. `name` is the fully-qualified APOC
/// function name (e.g. `apoc.coll.sum`); matching is
/// case-insensitive to follow the Cypher-scalar convention. `args`
/// are already-evaluated [`Property`] values in positional order.
///
/// Each namespace arm is gated on its matching Cargo feature, so a
/// build with `--features coll` routes `apoc.coll.*` but reports
/// `UnknownFunction` for, say, `apoc.text.join`. That means the
/// per-namespace cost is opt-in at both the source-size and
/// binary-size levels.
pub fn call_scalar(name: &str, args: &[Property]) -> Result<Property, ApocError> {
    // `_lc` stays unused when every namespace feature is off, so
    // the leading underscore silences the warning without needing
    // a cfg_attr on the binding.
    let _lc = name.to_ascii_lowercase();

    #[cfg(feature = "coll")]
    if let Some(suffix) = _lc.strip_prefix("apoc.coll.") {
        return coll::call(suffix, args);
    }

    #[cfg(feature = "text")]
    if let Some(suffix) = _lc.strip_prefix("apoc.text.") {
        return text::call(suffix, args);
    }

    #[cfg(feature = "map")]
    if let Some(suffix) = _lc.strip_prefix("apoc.map.") {
        return map::call(suffix, args);
    }

    #[cfg(feature = "util")]
    if let Some(suffix) = _lc.strip_prefix("apoc.util.") {
        return util::call(suffix, args);
    }

    #[cfg(feature = "convert")]
    if let Some(suffix) = _lc.strip_prefix("apoc.convert.") {
        return convert::call(suffix, args);
    }

    let _ = args;
    Err(ApocError::UnknownFunction(name.to_string()))
}
