use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("invalid number: {0}")]
    InvalidNumber(String),

    #[error("plan error: {0}")]
    Plan(String),
}

pub type Result<T> = std::result::Result<T, Error>;
