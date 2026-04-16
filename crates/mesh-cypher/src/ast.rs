#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Create(CreateStmt),
    /// A reading-clause-initiated query. Includes both
    /// `MATCH`- and `MERGE`-initiated statements — MERGE-only
    /// queries (like `MERGE (n) RETURN n` or the idempotent
    /// upsert-and-link pattern from real driver code) are
    /// modelled as `MatchStmt::clauses` starting with a
    /// `ReadingClause::Merge`, with an optional terminal.
    Match(MatchStmt),
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
    /// Multiple read statements joined by `UNION` / `UNION ALL`.
    /// Flat: a chain `a UNION b UNION c` produces a single
    /// `Union` with three branches. Mixing `UNION` and
    /// `UNION ALL` in the same chain is rejected at parse time.
    Union(UnionStmt),
    /// `EXPLAIN <query>` — return the logical plan as text instead
    /// of executing the query.
    Explain(Box<Statement>),
    /// `PROFILE <query>` — execute the query and return the plan
    /// annotated with row counts per operator.
    Profile(Box<Statement>),
}

/// `query1 UNION [ALL] query2 [UNION [ALL] query3 ...]`. Each
/// branch must be a read statement — `MATCH`/`MERGE`-initiated,
/// `UNWIND`-initiated, or bare `RETURN`. The planner additionally
/// requires every branch to project the same column list (name +
/// order) so the combined row stream has a single well-defined
/// shape.
#[derive(Debug, Clone, PartialEq)]
pub struct UnionStmt {
    /// Ordered list of branches. Always length ≥ 2 after parsing;
    /// a single branch is flattened back into its inner statement.
    pub branches: Vec<Statement>,
    /// `true` for `UNION ALL` (preserve duplicates), `false` for
    /// plain `UNION` (deduplicate across all branches).
    pub all: bool,
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
pub struct CreateStmt {
    pub patterns: Vec<Pattern>,
    pub return_items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

/// A `MATCH`-initiated query: an ordered list of reading clauses
/// (`MATCH` / `OPTIONAL MATCH` / `WITH`) followed by a single
/// writing/projecting terminal tail. The clause list is always
/// non-empty and the parser guarantees the first entry is a
/// `Match`. The planner walks the list with one uniform loop
/// per clause kind, so chained stages (`MATCH ... WITH ... MATCH
/// ... RETURN` etc.) lower through exactly the same code path
/// as a single-stage query.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchStmt {
    pub clauses: Vec<ReadingClause>,
    pub terminal: TerminalTail,
}

/// One element of a `MatchStmt`'s reading-clause list. Mirrors
/// the openCypher clause taxonomy for reading clauses —
/// producers (`MATCH`), left-joins (`OPTIONAL MATCH`),
/// re-projection stages (`WITH`), and mid-query upserts
/// (`MERGE`, which reads like a clause even though it also
/// writes). Pure-mutation clauses (`SET`, `DELETE`, `CREATE`)
/// live in [`TerminalTail`] because grammatically they only
/// appear at the end.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadingClause {
    Match(MatchClause),
    OptionalMatch(OptionalMatchClause),
    With(WithClause),
    Merge(MergeClause),
    Unwind(UnwindClause),
    /// `CALL { read_stmt }` — runs the body as a subquery per
    /// input row. Correlated subqueries import outer bindings via
    /// `WITH var1, var2` as the first clause inside the body.
    Call(Box<Statement>),
}

/// A mid-query `UNWIND expression AS alias`. Evaluates the
/// expression against each input row, expects the result to be a
/// list, and emits one output row per element with `alias` bound
/// to that element plus every existing binding from the input
/// row. Empty lists drop the input row; null / missing values
/// behave the same as an empty list. Distinct from [`UnwindStmt`],
/// which is the top-level `UNWIND ... RETURN` producer form.
#[derive(Debug, Clone, PartialEq)]
pub struct UnwindClause {
    pub expr: Expr,
    pub alias: String,
}

/// A single `MERGE (pattern) [ON CREATE SET ...] [ON MATCH SET
/// ...]` clause. `pattern` can describe either a lone node
/// (`MERGE (n:Label {k: v})`) or an edge between two nodes
/// (`MERGE (a)-[:REL]->(b)`). The planner dispatches on
/// whether `pattern.hops` is empty:
///
/// * Empty hops → node merge, lowered as `LogicalPlan::MergeNode`.
/// * Non-empty hops → edge merge, lowered as
///   `LogicalPlan::MergeEdge`. v1 restricts edge merges to
///   single-hop, directed, with *both* endpoints already
///   bound by an earlier reading clause.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeClause {
    pub pattern: Pattern,
    /// `ON CREATE SET ...` items — applied only when the MERGE
    /// created the node or edge.
    pub on_create: Vec<SetItem>,
    /// `ON MATCH SET ...` items — applied when the MERGE
    /// found an existing match.
    pub on_match: Vec<SetItem>,
}

/// A single `MATCH pattern_list [WHERE expr]` clause. This is
/// the producer form — it introduces new pattern variables by
/// scanning the store. Multiple `MATCH` clauses in the same
/// query cartesian-join their row streams unless a `WITH`
/// projects between them.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
    pub where_clause: Option<Expr>,
}

/// The writing / projecting tail that terminates a
/// `MatchStmt`. Exactly one of `return_items` /
/// `set_items` / `delete` / `create_patterns` is the "primary"
/// action the query performs; the others sit at their
/// defaults. `RETURN` may appear after a mutating clause as a
/// trailing projection.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TerminalTail {
    pub return_items: Vec<ReturnItem>,
    pub distinct: bool,
    pub order_by: Vec<SortItem>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub set_items: Vec<SetItem>,
    pub delete: Option<DeleteClause>,
    pub create_patterns: Vec<Pattern>,
    pub remove_items: Vec<RemoveItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptionalMatchClause {
    /// Patterns to attempt. v1 restricts this to a single
    /// pattern with exactly one hop whose start node is
    /// already bound from a prior MATCH — violations surface
    /// at plan time with a clear error.
    pub patterns: Vec<Pattern>,
    /// Optional WHERE filter applied to the rows after the
    /// optional expansion. Rows that fail the predicate are
    /// dropped (not Null-bound); matching Neo4j's behavior.
    pub where_clause: Option<Expr>,
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
pub enum RemoveItem {
    Property { var: String, key: String },
    Labels { var: String, labels: Vec<String> },
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
    /// `Some(p)` when the pattern was written as `p = (...)-[...]->(...)`.
    /// Plans with a path variable wrap the expand chain in a
    /// `BindPath` operator that assembles the traversed
    /// node/edge sequence into a `Value::Path` bound to `p`.
    pub path_var: Option<String>,
    /// `Some(kind)` when the pattern body was wrapped in
    /// `shortestPath(...)` or `allShortestPaths(...)`. The
    /// planner dispatches wrapped patterns to a BFS operator
    /// instead of the usual expand chain, and rejects the
    /// wrapping in contexts where it doesn't make sense
    /// (CREATE, zero-hop). `None` for plain patterns.
    pub shortest: Option<ShortestKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortestKind {
    /// `shortestPath(...)` — returns one shortest path per
    /// candidate `(src, dst)` pair from the upstream row
    /// stream. If multiple paths of minimum length exist, one
    /// is returned arbitrarily (whichever the BFS finds
    /// first). The BFS-visited ordering is deterministic on a
    /// given RocksDB snapshot.
    Shortest,
    /// `allShortestPaths(...)` — returns every path of the
    /// minimum length. v1 rejects this at plan time; listed
    /// here so the grammar and AST can still represent it.
    AllShortest,
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
    /// Postfix null check: `IS NULL` when `negated = false`,
    /// `IS NOT NULL` when `negated = true`. Always produces a
    /// Bool; evaluates `inner` and returns whether the result
    /// is `Value::Null` / `Property::Null`.
    IsNull {
        negated: bool,
        inner: Box<Expr>,
    },
    /// Chained property access on an arbitrary expression:
    /// `startNode(r).name`, `(expr).prop`, `x.a.b` (second hop).
    /// The simple `identifier.identifier` case is still handled by
    /// [`Expr::Property`]; this variant covers everything else.
    PropertyAccess {
        base: Box<Expr>,
        key: String,
    },
    /// `expr[index]` — list index access. Evaluates `base` to a
    /// list and `index` to an integer, returns the element at that
    /// position. Zero-based; negative indices count from the end.
    /// Out-of-bounds returns Null. Null base or null index → Null.
    IndexAccess {
        base: Box<Expr>,
        index: Box<Expr>,
    },
    /// `expr[start..end]` — list slice. Both bounds are optional:
    /// `[1..3]`, `[..2]`, `[2..]`, `[..]`. Zero-based; negative
    /// indices count from the end. Out-of-range bounds are clamped.
    SliceAccess {
        base: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    /// `element IN list` — list membership test. Evaluates
    /// `element` and `list`, then checks whether `element` is
    /// equal to any item in `list`. Null-propagating: null
    /// element or null list → false in filter context.
    InList {
        element: Box<Expr>,
        list: Box<Expr>,
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
    /// `{k1: e1, k2: e2, ...}` — map literal in expression context.
    /// The entries vector preserves source order so nested use in
    /// aggregations / comparisons stays deterministic, even though
    /// the backing `Property::Map` is a `HashMap`. Each value expr
    /// is evaluated against the current row; v1 requires values to
    /// evaluate to plain `Property` values (no nested Nodes / Edges).
    Map(Vec<(String, Expr)>),
    /// `[var IN source WHERE pred | projection]`. Either `predicate` or
    /// `projection` may be absent; when both are absent the comprehension
    /// just reproduces the source list.
    ListComprehension {
        var: String,
        source: Box<Expr>,
        predicate: Option<Box<Expr>>,
        projection: Option<Box<Expr>>,
    },
    /// `reduce(acc = init, elem IN source | body)` — a left
    /// fold over a list. `acc_init` is evaluated once against
    /// the outer row; then for each element the evaluator binds
    /// both `acc_var` (carrying the running accumulator) and
    /// `elem_var` (the current element) into a scratch row and
    /// evaluates `body` to produce the next accumulator. The
    /// final accumulator is the expression's value.
    Reduce {
        acc_var: String,
        acc_init: Box<Expr>,
        elem_var: String,
        source: Box<Expr>,
        body: Box<Expr>,
    },
    /// Pattern used as a boolean predicate — `(a)-[:KNOWS]->(b)`
    /// in a WHERE clause. Evaluates to `true` iff the pattern
    /// has at least one match in the graph given the current
    /// row's bindings. The start node's variable must be bound
    /// in the outer row; unbound intermediate/target variables
    /// are existentially quantified (we return true on first
    /// match and never leak new bindings out of the predicate).
    ///
    /// v1 restrictions enforced by the parser/planner: no
    /// variable-length hops, no path variables, and the pattern
    /// must carry at least one hop.
    PatternExists(Pattern),
    /// `EXISTS { MATCH pattern [WHERE expr] }` — Cypher 5
    /// subquery existence form. A strict superset of
    /// [`Expr::PatternExists`]: the WHERE clause can reference
    /// any variable bound along the walked pattern (not just
    /// the pattern shape), and the outer row's bindings are
    /// visible throughout the subquery.
    ///
    /// Evaluates to `true` iff **some** combination of
    /// intermediate bindings satisfies both the pattern and
    /// (if present) the WHERE expression. Inner bindings are
    /// ephemeral — they don't leak out into the outer row.
    ///
    /// v1 restrictions: single pattern (no comma lists), same
    /// pattern-shape rules as [`Expr::PatternExists`] (bound
    /// start, no var-length, no path var, at least one hop).
    ExistsSubquery {
        pattern: Pattern,
        where_clause: Option<Box<Expr>>,
    },
    /// `count { MATCH pattern [WHERE expr] }` — returns the number
    /// of matches as an Int64. Same semantics as ExistsSubquery but
    /// counts all matches instead of short-circuiting on the first.
    CountSubquery {
        pattern: Pattern,
        where_clause: Option<Box<Expr>>,
    },
    /// Binary arithmetic — `+`, `-`, `*`, `/`, `%`. Evaluated
    /// with numeric coercion (Int + Int = Int; any Float operand
    /// widens to Float) and null propagation. `+` additionally
    /// handles string concatenation and list concatenation
    /// when both operands are of the matching type.
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary negation — `-x`. Int and Float only; null
    /// propagates. The grammar leaves negative numeric literals
    /// as `Literal(Integer(-n))` via the `"-"?` prefix inside
    /// `integer` / `float`, so this variant only fires for
    /// negation applied to a non-literal expression (e.g.
    /// `-a.age`, `-(x + y)`).
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
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
    /// `left STARTS WITH right` — both sides must be strings,
    /// returns Bool. Null-propagating (either side Null → Null).
    StartsWith,
    /// `left ENDS WITH right` — same semantics as StartsWith.
    EndsWith,
    /// `left CONTAINS right` — same semantics as StartsWith.
    Contains,
    /// `left =~ right` — regex match. `right` must be a string
    /// containing a valid regular expression; returns Bool. Both
    /// sides Null → Null (null-propagating).
    RegexMatch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}
