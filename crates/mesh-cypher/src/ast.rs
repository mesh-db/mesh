#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Create(CreateStmt),
    Match(MatchStmt),
    Merge(MergeStmt),
    Unwind(UnwindStmt),
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
        properties: Vec<(String, Literal)>,
    },
    Merge {
        var: String,
        properties: Vec<(String, Literal)>,
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
    pub properties: Vec<(String, Literal)>,
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
    Property { var: String, key: String },
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
