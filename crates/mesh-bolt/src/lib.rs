//! Bolt wire protocol for Mesh.
//!
//! This crate is the pure protocol layer — it knows how to encode and
//! decode PackStream values, chunk Bolt messages, negotiate a version
//! in the handshake, and turn a [`BoltMessage`] into bytes and back.
//! It does NOT know anything about the graph executor, storage, or
//! networking beyond the `AsyncRead` / `AsyncWrite` traits it uses to
//! read and write messages.
//!
//! Server integration lives in `mesh-server::bolt`, which composes
//! these primitives into a per-connection state machine that reads
//! messages off a TCP socket, dispatches them to `MeshService`, and
//! writes `RECORD` / `SUCCESS` / `FAILURE` replies back.

mod error;
mod framing;
mod handshake;
mod message;
mod packstream;
mod value;

pub use error::{BoltError, Result};
pub use framing::{read_message, write_message, MAX_CHUNK_SIZE};
pub use handshake::{
    perform_client_handshake, perform_server_handshake, version_bytes, BOLT_4_4, PREAMBLE,
    SUPPORTED,
};
pub use message::{
    BoltMessage, TAG_BEGIN, TAG_COMMIT, TAG_DATE, TAG_DISCARD, TAG_DURATION, TAG_FAILURE,
    TAG_GOODBYE, TAG_HELLO, TAG_IGNORED, TAG_LOCAL_DATE_TIME, TAG_NODE, TAG_PATH, TAG_PULL,
    TAG_RECORD, TAG_RELATIONSHIP, TAG_RESET, TAG_ROLLBACK, TAG_RUN, TAG_SUCCESS,
    TAG_UNBOUND_RELATIONSHIP,
};
pub use packstream::{decode, encode, encode_into};
pub use value::BoltValue;
