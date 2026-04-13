use crate::{
    ast::*,
    error::{Error, Result},
};
use pest::iterators::Pair;
use pest::Parser as _;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "cypher.pest"]
struct CypherParser;

pub fn parse(input: &str) -> Result<Statement> {
    let mut pairs =
        CypherParser::parse(Rule::query, input).map_err(|e| Error::Parse(e.to_string()))?;
    let query = pairs
        .next()
        .ok_or_else(|| Error::Parse("empty input".into()))?;
    let statement = query
        .into_inner()
        .find(|p| p.as_rule() == Rule::statement)
        .ok_or_else(|| Error::Parse("missing statement".into()))?;
    build_statement(statement)
}

fn build_statement(pair: Pair<Rule>) -> Result<Statement> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty statement".into()))?;
    match inner.as_rule() {
        Rule::create_stmt => Ok(Statement::Create(build_create(inner)?)),
        Rule::match_stmt => Ok(Statement::Match(build_match(inner)?)),
        r => Err(Error::Parse(format!("unexpected rule: {:?}", r))),
    }
}

fn build_create(pair: Pair<Rule>) -> Result<CreateStmt> {
    let pattern_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("missing pattern".into()))?;
    Ok(CreateStmt {
        pattern: build_pattern(pattern_pair)?,
    })
}

fn build_match(pair: Pair<Rule>) -> Result<MatchStmt> {
    let mut pattern = None;
    let mut where_clause = None;
    let mut return_items = Vec::new();
    let mut distinct = false;
    let mut order_by: Vec<SortItem> = Vec::new();
    let mut skip = None;
    let mut limit = None;
    let mut set_items = Vec::new();
    let mut delete = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::pattern => pattern = Some(build_pattern(p)?),
            Rule::where_clause => {
                let expr_pair = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty where".into()))?;
                where_clause = Some(build_expression(expr_pair)?);
            }
            Rule::return_tail => {
                for inner in p.into_inner() {
                    match inner.as_rule() {
                        Rule::kw_distinct => distinct = true,
                        Rule::return_items => return_items = build_return_items(inner)?,
                        Rule::order_by_clause => order_by = build_order_by(inner)?,
                        Rule::skip_clause => {
                            let int_pair = inner
                                .into_inner()
                                .next()
                                .ok_or_else(|| Error::Parse("empty skip".into()))?;
                            skip = Some(parse_integer(int_pair.as_str())?);
                        }
                        Rule::limit_clause => {
                            let int_pair = inner
                                .into_inner()
                                .next()
                                .ok_or_else(|| Error::Parse("empty limit".into()))?;
                            limit = Some(parse_integer(int_pair.as_str())?);
                        }
                        r => {
                            return Err(Error::Parse(format!(
                                "unexpected rule in return tail: {:?}",
                                r
                            )))
                        }
                    }
                }
            }
            Rule::set_tail => {
                let set_items_pair = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty set".into()))?;
                for set_item_pair in set_items_pair.into_inner() {
                    let mut ii = set_item_pair.into_inner();
                    let prop_access = ii
                        .next()
                        .ok_or_else(|| Error::Parse("set item missing target".into()))?;
                    let mut pa = prop_access.into_inner();
                    let var = pa
                        .next()
                        .ok_or_else(|| Error::Parse("set target missing var".into()))?
                        .as_str()
                        .to_string();
                    let key = pa
                        .next()
                        .ok_or_else(|| Error::Parse("set target missing key".into()))?
                        .as_str()
                        .to_string();
                    let expr_pair = ii
                        .next()
                        .ok_or_else(|| Error::Parse("set item missing value".into()))?;
                    let value = build_expression(expr_pair)?;
                    set_items.push(SetItem { var, key, value });
                }
            }
            Rule::delete_tail => {
                let mut detach = false;
                let mut vars = Vec::new();
                for inner in p.into_inner() {
                    match inner.as_rule() {
                        Rule::kw_detach => detach = true,
                        Rule::delete_items => {
                            for id in inner.into_inner() {
                                vars.push(id.as_str().to_string());
                            }
                        }
                        r => {
                            return Err(Error::Parse(format!(
                                "unexpected rule in delete tail: {:?}",
                                r
                            )))
                        }
                    }
                }
                delete = Some(DeleteClause { detach, vars });
            }
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in match: {:?}",
                    r
                )))
            }
        }
    }

    Ok(MatchStmt {
        pattern: pattern.ok_or_else(|| Error::Parse("missing pattern".into()))?,
        where_clause,
        return_items,
        distinct,
        order_by,
        skip,
        limit,
        set_items,
        delete,
    })
}

fn build_order_by(pair: Pair<Rule>) -> Result<Vec<SortItem>> {
    let mut sort_items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() != Rule::sort_items {
            continue;
        }
        for sort_item_pair in inner.into_inner() {
            debug_assert_eq!(sort_item_pair.as_rule(), Rule::sort_item);
            let mut ii = sort_item_pair.into_inner();
            let expr_pair = ii
                .next()
                .ok_or_else(|| Error::Parse("empty sort item".into()))?;
            let expr = build_expression(expr_pair)?;
            let descending = match ii.next() {
                Some(dir_pair) if dir_pair.as_rule() == Rule::sort_dir => {
                    let inside = dir_pair
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty sort_dir".into()))?;
                    matches!(inside.as_rule(), Rule::kw_desc)
                }
                _ => false,
            };
            sort_items.push(SortItem { expr, descending });
        }
    }
    Ok(sort_items)
}

fn build_pattern(pair: Pair<Rule>) -> Result<Pattern> {
    let mut inner = pair.into_inner();
    let start_pair = inner
        .next()
        .ok_or_else(|| Error::Parse("empty pattern".into()))?;
    let start = build_node_pattern(start_pair)?;
    let mut hops = Vec::new();
    for hop_pair in inner {
        debug_assert_eq!(hop_pair.as_rule(), Rule::hop);
        hops.push(build_hop(hop_pair)?);
    }
    Ok(Pattern { start, hops })
}

fn build_hop(pair: Pair<Rule>) -> Result<Hop> {
    let mut inner = pair.into_inner();
    let rel_pair = inner
        .next()
        .ok_or_else(|| Error::Parse("missing rel in hop".into()))?;
    let target_pair = inner
        .next()
        .ok_or_else(|| Error::Parse("missing target in hop".into()))?;
    Ok(Hop {
        rel: build_rel_pattern(rel_pair)?,
        target: build_node_pattern(target_pair)?,
    })
}

fn build_rel_pattern(pair: Pair<Rule>) -> Result<RelPattern> {
    debug_assert_eq!(pair.as_rule(), Rule::rel_pattern);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty rel pattern".into()))?;
    let direction = match inner.as_rule() {
        Rule::rel_right => Direction::Outgoing,
        Rule::rel_left => Direction::Incoming,
        Rule::rel_both => Direction::Both,
        r => return Err(Error::Parse(format!("unexpected rel rule: {:?}", r))),
    };

    let mut var = None;
    let mut edge_type = None;
    let mut var_length = None;
    for p in inner.into_inner() {
        if p.as_rule() == Rule::rel_detail {
            for d in p.into_inner() {
                match d.as_rule() {
                    Rule::identifier => var = Some(d.as_str().to_string()),
                    Rule::rel_type_spec => {
                        let id = d
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty rel type".into()))?;
                        edge_type = Some(id.as_str().to_string());
                    }
                    Rule::var_length => {
                        var_length = Some(build_var_length(d)?);
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rel detail rule: {:?}",
                            r
                        )))
                    }
                }
            }
        }
    }

    Ok(RelPattern {
        var,
        edge_type,
        direction,
        var_length,
    })
}

fn build_var_length(pair: Pair<Rule>) -> Result<VarLength> {
    debug_assert_eq!(pair.as_rule(), Rule::var_length);
    let mut range_pair_opt = None;
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::var_length_range {
            range_pair_opt = Some(inner);
        }
    }
    let Some(range_pair) = range_pair_opt else {
        return Ok(VarLength {
            min: 1,
            max: u64::MAX,
        });
    };
    let inner = range_pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty var length range".into()))?;
    match inner.as_rule() {
        Rule::exact_range => {
            let n_pair = inner
                .into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty exact range".into()))?;
            let n = parse_u64(n_pair.as_str())?;
            Ok(VarLength { min: n, max: n })
        }
        Rule::bounded_range => {
            let mut min = 1u64;
            let mut max = u64::MAX;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::range_min => {
                        let n = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty range min".into()))?;
                        min = parse_u64(n.as_str())?;
                    }
                    Rule::range_max => {
                        let n = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| Error::Parse("empty range max".into()))?;
                        max = parse_u64(n.as_str())?;
                    }
                    r => {
                        return Err(Error::Parse(format!(
                            "unexpected rule in bounded range: {:?}",
                            r
                        )))
                    }
                }
            }
            Ok(VarLength { min, max })
        }
        r => Err(Error::Parse(format!(
            "unexpected var length range rule: {:?}",
            r
        ))),
    }
}

fn parse_u64(s: &str) -> Result<u64> {
    s.parse().map_err(|_| Error::InvalidNumber(s.to_string()))
}

fn build_node_pattern(pair: Pair<Rule>) -> Result<NodePattern> {
    let mut var = None;
    let mut label = None;
    let mut properties = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => var = Some(p.as_str().to_string()),
            Rule::label_spec => {
                let label_id = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| Error::Parse("empty label".into()))?;
                label = Some(label_id.as_str().to_string());
            }
            Rule::properties => properties = build_properties(p)?,
            r => {
                return Err(Error::Parse(format!(
                    "unexpected rule in node pattern: {:?}",
                    r
                )))
            }
        }
    }

    Ok(NodePattern {
        var,
        label,
        properties,
    })
}

fn build_properties(pair: Pair<Rule>) -> Result<Vec<(String, Literal)>> {
    let mut entries = Vec::new();
    for entry_pair in pair.into_inner() {
        debug_assert_eq!(entry_pair.as_rule(), Rule::property_entry);
        let mut inner = entry_pair.into_inner();
        let key = inner
            .next()
            .ok_or_else(|| Error::Parse("property key".into()))?
            .as_str()
            .to_string();
        let lit_pair = inner
            .next()
            .ok_or_else(|| Error::Parse("property value".into()))?;
        entries.push((key, build_literal(lit_pair)?));
    }
    Ok(entries)
}

fn build_return_items(pair: Pair<Rule>) -> Result<Vec<ReturnItem>> {
    let mut items = Vec::new();
    for item_pair in pair.into_inner() {
        debug_assert_eq!(item_pair.as_rule(), Rule::return_item);
        let mut inner = item_pair
            .into_inner()
            .filter(|p| p.as_rule() != Rule::kw_as);
        let expr = build_expression(
            inner
                .next()
                .ok_or_else(|| Error::Parse("return expr".into()))?,
        )?;
        let alias = inner.next().map(|p| p.as_str().to_string());
        items.push(ReturnItem { expr, alias });
    }
    Ok(items)
}

fn build_expression(pair: Pair<Rule>) -> Result<Expr> {
    match pair.as_rule() {
        Rule::expression => build_expression(
            pair.into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty expression".into()))?,
        ),
        Rule::or_expr => {
            let mut inner = pair
                .into_inner()
                .filter(|p| p.as_rule() != Rule::kw_or);
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty or_expr".into()))?,
            )?;
            for right in inner {
                let right_expr = build_expression(right)?;
                left = Expr::Or(Box::new(left), Box::new(right_expr));
            }
            Ok(left)
        }
        Rule::and_expr => {
            let mut inner = pair
                .into_inner()
                .filter(|p| p.as_rule() != Rule::kw_and);
            let mut left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty and_expr".into()))?,
            )?;
            for right in inner {
                let right_expr = build_expression(right)?;
                left = Expr::And(Box::new(left), Box::new(right_expr));
            }
            Ok(left)
        }
        Rule::not_expr => {
            let mut inner = pair.into_inner();
            let first = inner
                .next()
                .ok_or_else(|| Error::Parse("empty not_expr".into()))?;
            if first.as_rule() == Rule::kw_not {
                let inner_not = inner
                    .next()
                    .ok_or_else(|| Error::Parse("NOT without operand".into()))?;
                Ok(Expr::Not(Box::new(build_expression(inner_not)?)))
            } else {
                build_expression(first)
            }
        }
        Rule::comparison => {
            let mut inner = pair.into_inner();
            let left = build_expression(
                inner
                    .next()
                    .ok_or_else(|| Error::Parse("empty comparison".into()))?,
            )?;
            if let Some(op_pair) = inner.next() {
                let op = build_compare_op(op_pair)?;
                let right = build_expression(
                    inner
                        .next()
                        .ok_or_else(|| Error::Parse("missing comparison rhs".into()))?,
                )?;
                Ok(Expr::Compare {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                })
            } else {
                Ok(left)
            }
        }
        Rule::primary => build_expression(
            pair.into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty primary".into()))?,
        ),
        Rule::literal => Ok(Expr::Literal(build_literal(pair)?)),
        Rule::function_call => {
            let mut inner = pair.into_inner();
            let name = inner
                .next()
                .ok_or_else(|| Error::Parse("function call missing name".into()))?
                .as_str()
                .to_string();
            let args = match inner.next() {
                None => CallArgs::Exprs(Vec::new()),
                Some(fn_args_pair) => {
                    let inside = fn_args_pair
                        .into_inner()
                        .next()
                        .ok_or_else(|| Error::Parse("empty fn args".into()))?;
                    match inside.as_rule() {
                        Rule::count_star => CallArgs::Star,
                        Rule::fn_arg_list => {
                            let exprs: Result<Vec<Expr>> =
                                inside.into_inner().map(build_expression).collect();
                            CallArgs::Exprs(exprs?)
                        }
                        r => {
                            return Err(Error::Parse(format!(
                                "unexpected fn args inner: {:?}",
                                r
                            )))
                        }
                    }
                }
            };
            Ok(Expr::Call { name, args })
        }
        Rule::property_access => {
            let mut inner = pair.into_inner();
            let var = inner
                .next()
                .ok_or_else(|| Error::Parse("property var".into()))?
                .as_str()
                .to_string();
            let key = inner
                .next()
                .ok_or_else(|| Error::Parse("property key".into()))?
                .as_str()
                .to_string();
            Ok(Expr::Property { var, key })
        }
        Rule::identifier => Ok(Expr::Identifier(pair.as_str().to_string())),
        r => Err(Error::Parse(format!("unexpected rule in expression: {:?}", r))),
    }
}

fn build_literal(pair: Pair<Rule>) -> Result<Literal> {
    let lit_pair = if pair.as_rule() == Rule::literal {
        pair.into_inner()
            .next()
            .ok_or_else(|| Error::Parse("empty literal".into()))?
    } else {
        pair
    };
    match lit_pair.as_rule() {
        Rule::integer => Ok(Literal::Integer(parse_integer(lit_pair.as_str())?)),
        Rule::float => Ok(Literal::Float(
            lit_pair
                .as_str()
                .parse()
                .map_err(|_| Error::InvalidNumber(lit_pair.as_str().into()))?,
        )),
        Rule::string => {
            let inner = lit_pair
                .into_inner()
                .next()
                .ok_or_else(|| Error::Parse("empty string literal".into()))?;
            Ok(Literal::String(inner.as_str().to_string()))
        }
        Rule::boolean_lit => Ok(Literal::Boolean(lit_pair.as_str().eq_ignore_ascii_case("true"))),
        Rule::null_lit => Ok(Literal::Null),
        r => Err(Error::Parse(format!("unexpected literal rule: {:?}", r))),
    }
}

fn parse_integer(s: &str) -> Result<i64> {
    s.parse().map_err(|_| Error::InvalidNumber(s.to_string()))
}

fn build_compare_op(pair: Pair<Rule>) -> Result<CompareOp> {
    debug_assert_eq!(pair.as_rule(), Rule::comparison_op);
    let op_pair = pair
        .into_inner()
        .next()
        .ok_or_else(|| Error::Parse("empty comparison op".into()))?;
    Ok(match op_pair.as_rule() {
        Rule::op_eq => CompareOp::Eq,
        Rule::op_neq => CompareOp::Ne,
        Rule::op_lt => CompareOp::Lt,
        Rule::op_leq => CompareOp::Le,
        Rule::op_gt => CompareOp::Gt,
        Rule::op_geq => CompareOp::Ge,
        r => return Err(Error::Parse(format!("unexpected op: {:?}", r))),
    })
}
