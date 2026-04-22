//! APOC-compatible scalar functions for Mesh Cypher.
//!
//! APOC (Awesome Procedures on Cypher) is a Neo4j community library
//! that ships hundreds of helper procedures and scalar functions
//! under the `apoc.*` namespace. Mesh's implementation lives in a
//! standalone crate so the APOC surface can be compiled in or out
//! at build time — `cargo build --features apoc` turns it on,
//! default builds leave it out. That keeps the minimal-binary story
//! intact for users who don't want the extra code on their disk.
//!
//! The crate deliberately depends only on `meshdb-core` (for
//! [`meshdb_core::Property`]) — the executor-side adapter is what
//! bridges `Value` to the [`Property`]-based entry points here.
//! This setup avoids a cycle between the executor and apoc crates
//! and keeps the apoc surface pure: every function is a side-effect-
//! free transform over primitive types.
//!
//! ## Coverage
//!
//! - [`coll`] — `apoc.coll.*` collection operations (sum, avg, max,
//!   min, toSet, sort, sortDesc, reverse, contains, union,
//!   intersection, subtract, flatten).
//!
//! More namespaces (`apoc.text`, `apoc.map`, `apoc.util`) will land
//! as follow-up slices.

use meshdb_core::Property;
use thiserror::Error;

pub mod coll;

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
/// Returns [`ApocError::UnknownFunction`] for any name outside the
/// currently-implemented set so the executor's fall-through can
/// surface it as the usual "unknown scalar function" error.
pub fn call_scalar(name: &str, args: &[Property]) -> Result<Property, ApocError> {
    let lc = name.to_ascii_lowercase();
    if let Some(suffix) = lc.strip_prefix("apoc.coll.") {
        return coll::call(suffix, args);
    }
    Err(ApocError::UnknownFunction(name.to_string()))
}
