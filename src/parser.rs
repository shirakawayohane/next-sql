use pest::{error::Error, Parser};

use crate::ast::{BuiltInType, Module, Type};

#[derive(Parser)]
#[grammar = "nextsql.pest"]
pub struct NextSqlParser;

pub fn parse_module(input: &str) -> Result<Module, Error<Rule>> {
    let pairs: pest::iterators::Pairs<Rule> =
        NextSqlParser::parse(Rule::module, &input).unwrap_or_else(|e| panic!("{}", e));

    let mut queries = Vec::new();

    let pairs = pairs.peekable().into_iter();
    for pair in pairs {
        match pair.as_rule() {
            Rule::module => {
                let inner_pairs = pair.into_inner();
                for inner_pair in inner_pairs {
                    match inner_pair.as_rule() {
                        Rule::query => {
                            queries.push(parse_query(inner_pair.into_inner()));
                        }
                        Rule::EOI => break,
                        _ => unreachable!(),
                    }
                }
            }
            _ => {
                dbg!(pair);
                unreachable!()
            }
        }
    }

    Ok(crate::ast::Module { queries })
}

fn parse_argument_list(pairs: pest::iterators::Pairs<Rule>) -> Vec<crate::ast::Argument> {
    fn parse_argument(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Argument {
        let mut pairs = pairs.peekable();
        let name = pairs.next().unwrap().as_str().to_string();
        let typ_str = pairs.next().unwrap().as_str().to_string();
        let typ = match typ_str.as_str() {
            "i16" => Type::BuiltIn(BuiltInType::I16),
            "i32" => Type::BuiltIn(BuiltInType::I32),
            "i64" => Type::BuiltIn(BuiltInType::I64),
            "f32" => Type::BuiltIn(BuiltInType::F32),
            "f64" => Type::BuiltIn(BuiltInType::F64),
            "timestamp" => Type::BuiltIn(BuiltInType::Timestamp),
            "timestamptz" => Type::BuiltIn(BuiltInType::Timestamptz),
            "date" => Type::BuiltIn(BuiltInType::Date),
            "uuid" => Type::BuiltIn(BuiltInType::Uuid),
            "string" => Type::BuiltIn(BuiltInType::String),
            "bool" => Type::BuiltIn(BuiltInType::Bool),
            _ => unreachable!(),
        };
        crate::ast::Argument { name, typ }
    }
    let mut arguments = Vec::new();
    for pair in pairs {
        match pair.as_rule() {
            Rule::argument => {
                arguments.push(parse_argument(pair.into_inner()));
            }
            _ => unreachable!(),
        }
    }
    arguments
}

#[test]
fn test_parse_argument_list() {
    let input = "($a: i32, $b: i32)";
    let pairs = match NextSqlParser::parse(Rule::argument_list, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let arguments = parse_argument_list(pairs.peekable().next().unwrap().into_inner());
    dbg!(&arguments);
}

fn parse_query_decl(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::QueryDecl {
    let mut pairs = pairs.peekable();
    let name = pairs.next().unwrap().as_str().to_string();
    let arguments = match pairs.peek().unwrap().as_rule() {
        Rule::argument_list => parse_argument_list(pairs.next().unwrap().into_inner()),
        _ => Vec::new(),
    };
    crate::ast::QueryDecl { name, arguments }
}

#[test]
fn test_parse_query_decl() {
    let input = "query foo ($a: i32, $b: i32)";
    let pairs = match NextSqlParser::parse(Rule::query_decl, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    dbg!(&pairs);
    let decl = parse_query_decl(pairs.peekable().next().unwrap().into_inner());
    dbg!(&decl);
}

fn parse_query(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Query {
    let mut pairs = pairs.peekable();
    let decl = match pairs.peek().unwrap().as_rule() {
        Rule::query_decl => parse_query_decl(pairs.next().unwrap().into_inner()),
        _ => unreachable!(),
    };
    let body = match pairs.peek().unwrap().as_rule() {
        Rule::query_body => parse_query_body(pairs.next().unwrap().into_inner()),
        _ => unreachable!(),
    };
    crate::ast::Query { decl, body }
}

#[test]
fn test_parse_query() {
    let input = include_str!("../examples/simple-select.nsql");
    let pairs = match NextSqlParser::parse(Rule::query, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    dbg!(&pairs);
    let query = parse_query(pairs.peekable().next().unwrap().into_inner());
    dbg!(&query);
}

fn parse_target(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Target {
    let mut pairs = pairs.peekable();
    let name = pairs.next().unwrap().as_str().to_string();
    let alias = pairs.next().map(|p| p.as_str().to_string());
    crate::ast::Target { name, alias }
}

fn parse_join_expr(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::JoinExpr {
    let mut pairs = pairs.peekable();
    let join_type = match pairs.next().unwrap().as_rule() {
        Rule::inner_join => crate::ast::JoinType::Inner,
        Rule::left_join => crate::ast::JoinType::Left,
        Rule::right_join => crate::ast::JoinType::Right,
        Rule::full_outer_join => crate::ast::JoinType::FullOuter,
        Rule::cross_join => crate::ast::JoinType::Cross,
        _ => unreachable!(),
    };

    let target = parse_target(pairs.next().unwrap().into_inner());
    let condition = parse_expression(pairs.next().unwrap().into_inner());
    crate::ast::JoinExpr {
        join_type,
        target,
        condition,
    }
}

fn parse_from_expr(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::FromExpr {
    let mut pairs = pairs.peekable();
    let target = parse_target(pairs.next().unwrap().into_inner());
    let joins = pairs.map(|p| parse_join_expr(p.into_inner())).collect();
    crate::ast::FromExpr { target, joins }
}

fn parse_query_body(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::QueryBody {
    let mut from = None;
    let mut where_clause = None;
    let mut select = Vec::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::from_clause => from = Some(parse_from_expr(pair.into_inner())),
            Rule::where_clause => {
                where_clause = Some(parse_expression(
                    pair.into_inner().next().unwrap().into_inner(),
                ));
            }
            Rule::select_clause => {
                select = pair
                    .into_inner()
                    .map(|p| parse_expression(p.into_inner()))
                    .collect();
            }
            _ => {
                unreachable!()
            }
        }
    }

    crate::ast::QueryBody {
        from: from.unwrap(),
        where_clause,
        select,
    }
}

fn parse_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let pair = pairs.peek().unwrap();
    match pair.as_rule() {
        Rule::logical_expression => parse_logical_expression(pair.into_inner()),
        Rule::equality_expression => parse_equality_expression(pair.into_inner()),
        Rule::relational_expression => parse_relational_expression(pair.into_inner()),
        Rule::additive_expression => parse_additive_expression(pair.into_inner()),
        Rule::multiplicative_expression => parse_multiplicative_expression(pair.into_inner()),
        Rule::unary_expression => parse_unary_expression(pair.into_inner()),
        Rule::atomic_expression => parse_atomic_expression(pair.into_inner()),
        _ => {
            dbg!(&pair);
            unreachable!()
        }
    }
}

#[test]
fn test_parse_expression() {
    let input = "1 + 1";
    let pairs = match NextSqlParser::parse(Rule::expression, &input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {}", e);
            return;
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    dbg!(&expr);
}

fn parse_logical_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let mut pairs = pairs.peekable();
    let left = parse_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::and => crate::ast::LogicalOp::And,
        Rule::or => crate::ast::LogicalOp::Or,
        _ => unreachable!(),
    };
    let right = parse_expression(pairs.next().unwrap().into_inner());
    crate::ast::Expression::Logical {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_equality_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let mut pairs = pairs.peekable();
    let left = parse_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::equal => crate::ast::EqualityOp::Equal,
        Rule::unequal => crate::ast::EqualityOp::Unequal,
        _ => unreachable!(),
    };
    let right = parse_expression(pairs.next().unwrap().into_inner());
    crate::ast::Expression::Equality {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_relational_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let mut pairs = pairs.peekable();
    let left = parse_additive_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::lt => crate::ast::RelationalOp::LessThan,
        Rule::le => crate::ast::RelationalOp::LessThanOrEqual,
        Rule::gt => crate::ast::RelationalOp::GreaterThan,
        Rule::ge => crate::ast::RelationalOp::GreaterThanOrEqual,
        _ => unreachable!(),
    };
    let right = parse_additive_expression(pairs.next().unwrap().into_inner());
    crate::ast::Expression::Relational {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_additive_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let mut pairs = pairs.peekable();
    let left = parse_multiplicative_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::add => crate::ast::AdditiveOp::Add,
        Rule::subtract => crate::ast::AdditiveOp::Subtract,
        _ => unreachable!(),
    };
    let right = parse_multiplicative_expression(pairs.next().unwrap().into_inner());
    crate::ast::Expression::Additive {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_multiplicative_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let mut pairs = pairs.peekable();
    let left = parse_unary_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::multiply => crate::ast::MultiplicativeOp::Multiply,
        Rule::divide => crate::ast::MultiplicativeOp::Divide,
        Rule::rem => crate::ast::MultiplicativeOp::Remainder,
        _ => unreachable!(),
    };
    let right = parse_unary_expression(pairs.next().unwrap().into_inner());
    crate::ast::Expression::Multiplicative {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_unary_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let mut pairs = pairs.peekable();
    match pairs.peek().unwrap().as_rule() {
        Rule::not => {
            pairs.next();
            let expr = parse_atomic_expression(pairs.next().unwrap().into_inner());
            crate::ast::Expression::Unary {
                op: crate::ast::UnaryOp::Not,
                expr: Box::new(expr),
            }
        }
        _ => parse_atomic_expression(pairs.next().unwrap().into_inner()),
    }
}

fn parse_column(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Column {
    let mut pairs = pairs.peekable();
    match pairs.peek().unwrap().as_rule() {
        Rule::wildcard => crate::ast::Column::Wildcard,
        Rule::ident => {
            let first = pairs.next().unwrap().as_str().to_string();
            if pairs.peek().is_none() {
                crate::ast::Column::WildcardOf(first)
            } else {
                let second = pairs.next().unwrap().as_str().to_string();
                crate::ast::Column::Single(first, second)
            }
        }
        _ => unreachable!(),
    }
}

fn parse_atomic_expression(pairs: pest::iterators::Pairs<Rule>) -> crate::ast::Expression {
    let pair = pairs.peek().unwrap();
    match pair.as_rule() {
        Rule::column => crate::ast::Expression::Primary(crate::ast::PrimaryExpression::Column(
            parse_column(pair.into_inner()),
        )),
        Rule::literal => {
            let literal = pair.as_str();
            if let Ok(num) = literal.parse::<f64>() {
                crate::ast::Expression::Primary(crate::ast::PrimaryExpression::Literal(
                    crate::ast::Literal::Numeric(num),
                ))
            } else if literal.starts_with("\"") && literal.ends_with("\"") {
                crate::ast::Expression::Primary(crate::ast::PrimaryExpression::Literal(
                    crate::ast::Literal::String(literal[1..literal.len() - 1].to_string()),
                ))
            } else if literal == "true" || literal == "false" {
                crate::ast::Expression::Primary(crate::ast::PrimaryExpression::Literal(
                    crate::ast::Literal::Boolean(literal == "true"),
                ))
            } else {
                unreachable!()
            }
        }
        Rule::variable => {
            let name = pair.as_str().to_string();
            crate::ast::Expression::Primary(crate::ast::PrimaryExpression::Variable(
                crate::ast::Variable { name },
            ))
        }
        Rule::expression => parse_expression(pair.into_inner()),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_select() {
        let input = include_str!("../examples/simple-select.nsql");
        parse_module(&input).unwrap();
    }

    #[test]
    fn simple_select_and_join() {
        let input = include_str!("../examples/simple-select-and-join.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }
}
