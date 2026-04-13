use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("missing required id field")]
    MissingId,

    #[error("expected 16-byte UUID, got {0} bytes")]
    InvalidUuidLength(usize),

    #[error("unsupported property value (list/map conversions not yet implemented)")]
    UnsupportedProperty,
}
