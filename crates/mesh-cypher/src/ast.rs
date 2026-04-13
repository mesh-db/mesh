#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Create(CreateStmt),
    Match(MatchStmt),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStmt {
    pub patterns: Vec<Pattern>,
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
pub struct SetItem {
    pub var: String,
    pub key: String,
    pub value: Expr,
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
    pub label: Option<String>,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum CallArgs {
    Star,
    Exprs(Vec<Expr>),
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
