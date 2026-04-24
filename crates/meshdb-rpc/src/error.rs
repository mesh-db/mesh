use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConvertError {
    #[error("missing required id field")]
    MissingId,

    #[error("expected 16-byte UUID, got {0} bytes")]
    InvalidUuidLength(usize),
}
