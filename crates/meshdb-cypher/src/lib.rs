mod ast;
mod error;
mod parser;
mod plan;

pub use ast::*;
// `PropertyType` lives in `ast` but needs an explicit re-export
// alongside the glob so downstream `use meshdb_cypher::PropertyType`
// resolves; the glob takes care of the common types and this line
// makes the intent explicit for any future star-less consumer.
pub use ast::PropertyType;
pub use error::{Error, Result};
pub use parser::parse;
pub use plan::{
    format_plan, output_columns, plan, plan_with_context, AggregateArg, AggregateFn, AggregateSpec,
    CreateEdgeSpec, CreateNodeSpec, LogicalPlan, OuterBindingKind, PlannerContext, PointSeekBounds,
    RemoveSpec, SetAssignment,
};
