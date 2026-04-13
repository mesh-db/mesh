use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("invalid number: {0}")]
    InvalidNumber(String),
}

pub type Result<T> = std::result::Result<T, Error>;
