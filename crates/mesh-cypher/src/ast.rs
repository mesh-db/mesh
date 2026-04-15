#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Create(CreateStmt),
    Match(MatchStmt),
    Merge(MergeStmt),
    Unwind(UnwindStmt),
    /// Bare `RETURN <items>` with no producer clause. Behaves like
    /// `UNWIND [0] AS _ RETURN <items>` — produces exactly one row,
    /// and the projection only references row-independent
    /// expressions (literals, parameters, calls on those). Used by
    /// drivers as a health-probe form (`RETURN 1`) and for
    /// expression-only queries.
    Return(ReturnStmt),
    CreateIndex(IndexDdl),
    DropIndex(IndexDdl),
    ShowIndexes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnStmt {
    pub return_items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

/// Declarative description of a property index for DDL statements.
/// `CreateIndex` and `DropIndex` both use this shape because Mesh
/// identifies indexes by their `(label, property)` pair — users don't
/// assign names — and the two commands carry identical payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDdl {
    pub label: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnwindStmt {
    pub expr: Expr,
    pub alias: String,
    pub where_clause: Option<Expr>,
    pub return_items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeStmt {
    pub pattern: NodePattern,
    /// `ON CREATE SET ...` items — applied only when the MERGE
    /// took the create branch (no existing node matched the
    /// pattern). May be empty.
    pub on_create: Vec<SetItem>,
    /// `ON MATCH SET ...` items — applied to every row when the
    /// MERGE took the match branch (one or more existing nodes
    /// matched). May be empty.
    pub on_match: Vec<SetItem>,
    pub return_items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStmt {
    pub patterns: Vec<Pattern>,
    pub return_items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchStmt {
    pub patterns: Vec<Pattern>,
    pub where_clause: Option<Expr>,
    /// Optional intermediate `WITH` projection between the MATCH
    /// pattern and the final `RETURN`. When present, the plan
    /// tree is `MATCH → [pre-WHERE] → WITH(items, WHERE, ORDER,
    /// SKIP, LIMIT) → RETURN`, and the bindings downstream of
    /// `with_clause` are *only* the names introduced by the WITH
    /// items — pattern variables that aren't re-projected go out
    /// of scope.
    pub with_clause: Option<WithClause>,
    pub return_items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub set_items: Vec<SetItem>,
    pub delete: Option<DeleteClause>,
    pub create_patterns: Vec<Pattern>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,
    pub distinct: bool,
    /// Post-projection filter. Applies to the row stream *after*
    /// the WITH projection and before its ORDER BY / SKIP / LIMIT,
    /// so predicates can reference the newly-introduced aliases.
    pub where_clause: Option<Expr>,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortItem {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetItem {
    Property {
        var: String,
        key: String,
        value: Expr,
    },
    Labels {
        var: String,
        labels: Vec<String>,
    },
    Replace {
        var: String,
        properties: Vec<(String, Expr)>,
    },
    Merge {
        var: String,
        properties: Vec<(String, Expr)>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteClause {
    pub detach: bool,
    pub vars: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub start: NodePattern,
    pub hops: Vec<Hop>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Hop {
    pub rel: RelPattern,
    pub target: NodePattern,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelPattern {
    pub var: Option<String>,
    pub edge_type: Option<String>,
    pub direction: Direction,
    pub var_length: Option<VarLength>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarLength {
    pub min: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub var: Option<String>,
    pub labels: Vec<String>,
    /// Property entries from the pattern's `{key: value}` map. The
    /// grammar restricts `value` to literals or parameters (see
    /// `property_value` in cypher.pest), so every Expr here is either
    /// `Expr::Literal` or `Expr::Parameter`.
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Identifier(String),
    /// `$name` or `$0` — value is bound at execute time from the
    /// per-query parameter map.
    Parameter(String),
    Property {
        var: String,
        key: String,
    },
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Compare {
        op: CompareOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Call {
        name: String,
        args: CallArgs,
    },
    /// `CASE [scrutinee] WHEN v1 THEN r1 WHEN v2 THEN r2 ELSE r END`.
    /// When `scrutinee` is `Some`, each branch's condition is compared for
    /// equality against it (simple form). When `None`, each branch's
    /// condition is treated as a boolean predicate (generic form).
    Case {
        scrutinee: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    /// `[e1, e2, e3]` — produces a List value of eagerly evaluated elements.
    List(Vec<Expr>),
    /// `[var IN source WHERE pred | projection]`. Either `predicate` or
    /// `projection` may be absent; when both are absent the comprehension
    /// just reproduces the source list.
    ListComprehension {
        var: String,
        source: Box<Expr>,
        predicate: Option<Box<Expr>>,
        projection: Option<Box<Expr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CallArgs {
    Star,
    Exprs(Vec<Expr>),
    DistinctExprs(Vec<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}
