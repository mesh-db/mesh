mod error;
mod eval;
mod ops;
mod value;

pub use error::{Error, Result};
pub use ops::execute;
pub use value::{Row, Value};
