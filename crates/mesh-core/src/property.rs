use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Property {
    Null,
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    List(Vec<Property>),
    Map(HashMap<String, Property>),
}

impl Property {
    pub fn type_name(&self) -> &'static str {
        match self {
            Property::Null => "Null",
            Property::String(_) => "String",
            Property::Int64(_) => "Int64",
            Property::Float64(_) => "Float64",
            Property::Bool(_) => "Bool",
            Property::List(_) => "List",
            Property::Map(_) => "Map",
        }
    }
}

impl From<String> for Property {
    fn from(v: String) -> Self {
        Property::String(v)
    }
}

impl From<&str> for Property {
    fn from(v: &str) -> Self {
        Property::String(v.to_string())
    }
}

impl From<i64> for Property {
    fn from(v: i64) -> Self {
        Property::Int64(v)
    }
}

impl From<i32> for Property {
    fn from(v: i32) -> Self {
        Property::Int64(v as i64)
    }
}

impl From<f64> for Property {
    fn from(v: f64) -> Self {
        Property::Float64(v)
    }
}

impl From<bool> for Property {
    fn from(v: bool) -> Self {
        Property::Bool(v)
    }
}

impl<T: Into<Property>> From<Vec<T>> for Property {
    fn from(v: Vec<T>) -> Self {
        Property::List(v.into_iter().map(Into::into).collect())
    }
}
