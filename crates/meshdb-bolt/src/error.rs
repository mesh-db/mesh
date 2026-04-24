use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BoltError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("unexpected end of PackStream input at offset {0}")]
    Truncated(usize),

    #[error("invalid PackStream marker 0x{0:02X}")]
    InvalidMarker(u8),

    #[error("invalid UTF-8 in PackStream String")]
    InvalidUtf8,

    #[error("chunk size {0} exceeds 65535")]
    ChunkTooLarge(usize),

    #[error("handshake magic bytes did not match the Bolt preamble")]
    BadPreamble,

    #[error("no compatible Bolt version offered by client: {0:?}")]
    NoCompatibleVersion([u32; 4]),

    #[error("unknown Bolt message tag 0x{0:02X}")]
    UnknownMessageTag(u8),

    #[error("malformed Bolt message: {0}")]
    Malformed(String),
}

pub type Result<T> = std::result::Result<T, BoltError>;
