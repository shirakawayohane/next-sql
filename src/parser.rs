use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;

use pest::{error::Error, Parser};

use crate::ast::*;

#[derive(Parser)]
#[grammar = "nextsql.pest"]
pub struct NextSqlParser;

pub fn parse_module(input: &str) -> Result<Module, Error<Rule>> {
    let pairs: pest::iterators::Pairs<Rule> =
        NextSqlParser::parse(Rule::module, &input).unwrap_or_else(|e| panic!("{}", e));

    let mut toplevels = Vec::new();

    let pairs = pairs.peekable().into_iter();
    for pair in pairs {
        match pair.as_rule() {
            Rule::module => {
                let inner_pairs = pair.into_inner();
                for inner_pair in inner_pairs {
                    match inner_pair.as_rule() {
                        Rule::query => {
                            toplevels.push(TopLevel::Query(parse_query(inner_pair.into_inner())));
                        }
                        Rule::mutation => {
                            toplevels
                                .push(TopLevel::Mutation(parse_mutation(inner_pair.into_inner())));
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

    Ok(Module { toplevels })
}

#[test]
fn test_parse_type() {
    let builtin_types = vec![
        "i16",
        "i32",
        "i64",
        "f32",
        "f64",
        "timestamp",
        "timestamptz",
        "date",
        "uuid",
        "string",
        "bool",
    ];

    for input in builtin_types {
        let pairs = match NextSqlParser::parse(Rule::r#type, input) {
            Ok(p) => p,
            Err(e) => panic!("{}のパースでエラーが発生しました: {}", input, e),
        };
        let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
        assert_eq!(input, typ.to_string());
    }

    let input = "Insertable<User>";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{}のパースでエラーが発生しました: {}", input, e),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    let input = "[i32]";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{}のパースでエラーが発生しました: {}", input, e),
    };
    matches!(
        parse_type(pairs.peekable().next().unwrap().into_inner()),
        Type::Array(_)
    );
}
fn parse_type(pairs: pest::iterators::Pairs<Rule>) -> Type {
    fn parse_utility_type(pairs: pest::iterators::Pairs<Rule>) -> Type {
        let mut pairs = pairs.peekable();
        let inner = parse_type(pairs.next().unwrap().into_inner());
        Type::Utility(UtilityType::Insertable(Insertable(Box::new(inner))))
    }
    let mut pairs = pairs.peekable();
    let typ = match pairs.peek().unwrap().as_rule() {
        Rule::builtin_type => match pairs.next().unwrap().into_inner().next().unwrap().as_rule() {
            Rule::i16_type => Type::BuiltIn(BuiltInType::I16),
            Rule::i32_type => Type::BuiltIn(BuiltInType::I32),
            Rule::i64_type => Type::BuiltIn(BuiltInType::I64),
            Rule::f32_type => Type::BuiltIn(BuiltInType::F32),
            Rule::f64_type => Type::BuiltIn(BuiltInType::F64),
            Rule::timestamp_type => Type::BuiltIn(BuiltInType::Timestamp),
            Rule::timestamptz_type => Type::BuiltIn(BuiltInType::Timestamptz),
            Rule::date_type => Type::BuiltIn(BuiltInType::Date),
            Rule::uuid_type => Type::BuiltIn(BuiltInType::Uuid),
            Rule::string_type => Type::BuiltIn(BuiltInType::String),
            Rule::bool_type => Type::BuiltIn(BuiltInType::Bool),
            _ => {
                unreachable!()
            }
        },
        Rule::utility_type => {
            let mut inner_pairs = pairs.next().unwrap().into_inner();
            parse_utility_type(inner_pairs.next().unwrap().into_inner())
        }
        Rule::array_type => {
            let mut inner_pairs = pairs.next().unwrap().into_inner();
            let inner = parse_type(inner_pairs.next().unwrap().into_inner());
            Type::Array(Box::new(inner))
        }
        Rule::user_defined_type => {
            let name = pairs.next().unwrap().as_str().to_string();
            Type::UserDefined(name)
        }
        _ => unreachable!(),
    };
    let is_optional = pairs
        .next()
        .map(|p| p.as_rule() == Rule::question_mark)
        .unwrap_or(false);
    if is_optional {
        Type::Optional(Box::new(typ))
    } else {
        typ
    }
}

fn parse_argument_list(pairs: pest::iterators::Pairs<Rule>) -> Vec<Argument> {
    fn parse_argument(pairs: pest::iterators::Pairs<Rule>) -> Argument {
        let mut pairs = pairs.peekable();
        let name = pairs.next().unwrap().as_str().to_string();
        let typ = parse_type(pairs.next().unwrap().into_inner());
        Argument { name, typ }
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

fn parse_query_decl(pairs: pest::iterators::Pairs<Rule>) -> QueryDecl {
    let mut pairs = pairs.peekable();
    let name = pairs.next().unwrap().as_str().to_string();
    let arguments = match pairs.peek().unwrap().as_rule() {
        Rule::argument_list => parse_argument_list(pairs.next().unwrap().into_inner()),
        _ => Vec::new(),
    };
    QueryDecl { name, arguments }
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

fn parse_query(pairs: pest::iterators::Pairs<Rule>) -> Query {
    let mut pairs = pairs.peekable();
    let decl = match pairs.peek().unwrap().as_rule() {
        Rule::query_decl => parse_query_decl(pairs.next().unwrap().into_inner()),
        _ => unreachable!(),
    };
    let body = match pairs.peek().unwrap().as_rule() {
        Rule::query_body => parse_query_body(pairs.next().unwrap().into_inner()),
        _ => unreachable!(),
    };
    Query { decl, body }
}

fn parse_mutation_decl(pairs: pest::iterators::Pairs<Rule>) -> MutationDecl {
    let mut pairs = pairs.peekable();
    let name = pairs.next().unwrap().as_str().to_string();
    let arguments = if pairs.peek().is_some() {
        match pairs.peek().unwrap().as_rule() {
            Rule::argument_list => parse_argument_list(pairs.next().unwrap().into_inner()),
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };
    MutationDecl { name, arguments }
}

#[test]
fn test_parse_mutation_decl() {
    let input = "mutation foo ($a: i32, $b: i32)";
    let pairs = match NextSqlParser::parse(Rule::mutation_decl, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let decl = parse_mutation_decl(pairs.peekable().next().unwrap().into_inner());
    dbg!(&decl);
}

fn parse_insert_clause(pairs: pest::iterators::Pairs<Rule>) -> String {
    let mut pairs = pairs.peekable();
    pairs.next().unwrap().as_str().to_string()
}

#[test]
fn test_parse_insert_clause() {
    let input = "insert(foo) values(...)";
    let pairs = match NextSqlParser::parse(Rule::insert_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let clause = parse_insert_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(clause, "foo");
}

#[test]
fn test_parse_value_clause() {
    let input = ".value({ a: 1, b: \"abc\" })";
    let pairs = match NextSqlParser::parse(Rule::value_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let clause = parse_value_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        HashSet::from(["a".to_string(), "b".to_string()]),
        match clause {
            Expression::Atomic(e) => {
                if let AtomicExpression::Literal(Literal::Object(obj)) = e {
                    obj.0.keys().cloned().collect()
                } else {
                    unreachable!()
                }
            }
            _ => unreachable!(),
        }
    );
}

fn parse_value_clause(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    parse_expression(pairs.next().unwrap().into_inner())
}

#[test]
fn test_parse_values_clause() {
    let input = ".values({ a: 1, b: \"abc\" }, { a: 2, b: \"def\" })";
    let pairs = match NextSqlParser::parse(Rule::values_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let clause = parse_values_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(2, clause.len());
}

fn parse_values_clause(pairs: pest::iterators::Pairs<Rule>) -> Vec<Expression> {
    let pairs = pairs.peekable();
    pairs.map(|p| parse_expression(p.into_inner())).collect()
}

#[test]
fn test_parse_returning_clause() {
    let input = ".returning(a, b)";
    let pairs = match NextSqlParser::parse(Rule::returning_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let clause = parse_returning_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(2, clause.len());

    let input = ".returning(a.*, b.c)";
    let pairs = match NextSqlParser::parse(Rule::returning_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let clause = parse_returning_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(2, clause.len());

    let input = ".returning(*)";
    let pairs = match NextSqlParser::parse(Rule::returning_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let clause = parse_returning_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(1, clause.len());
}
fn parse_returning_clause(pairs: pest::iterators::Pairs<Rule>) -> Vec<Column> {
    let mut pairs = pairs.peekable();
    pairs.map(|p| parse_column(p.into_inner())).collect()
}

#[test]
fn test_parse_mutation_body() {
    let input = "{ insert(foo).values({ a: 1, b: \"abc\" }).returning(a, b) }";
    let pairs = match NextSqlParser::parse(Rule::mutation_body, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let body = parse_mutation_body(pairs.peekable().next().unwrap().into_inner());
    dbg!(&body);
}
fn parse_mutation_body(pairs: pest::iterators::Pairs<Rule>) -> MutationBody {
    let mut pairs = pairs.peekable();
    let insert = match pairs.peek().unwrap().as_rule() {
        Rule::insert_clause => parse_insert_clause(pairs.next().unwrap().into_inner()),
        _ => unreachable!(),
    };
    let next = pairs.next().unwrap();
    let values = match next.as_rule() {
        Rule::value_clause => vec![parse_value_clause(next.into_inner())],
        Rule::values_clause => parse_values_clause(next.into_inner()),
        _ => Vec::new(),
    };
    let returning = match pairs.peek().map(|p| p.as_rule()) {
        Some(Rule::returning_clause) => {
            Some(parse_returning_clause(pairs.next().unwrap().into_inner()))
        }
        _ => None,
    };
    MutationBody {
        insert,
        values,
        returning,
    }
}

#[test]
fn test_parse_mutation() {
    let input = "mutation foo { insert(foo).values({ a: 1, b: \"abc\" }).returning(a, b) }";
    let pairs = match NextSqlParser::parse(Rule::mutation, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let mutation = parse_mutation(pairs.peekable().next().unwrap().into_inner());
    dbg!(&mutation);
}
fn parse_mutation(pairs: pest::iterators::Pairs<Rule>) -> Mutation {
    let mut pairs = pairs.peekable();
    let decl = parse_mutation_decl(pairs.next().unwrap().into_inner());
    let body = parse_mutation_body(pairs.next().unwrap().into_inner());
    Mutation { body, decl }
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

fn parse_target(pairs: pest::iterators::Pairs<Rule>) -> Target {
    let mut pairs = pairs.peekable();
    let name = pairs.next().unwrap().as_str().to_string();
    let alias = pairs.next().map(|p| p.as_str().to_string());
    Target { name, alias }
}

fn parse_join_expr(pairs: pest::iterators::Pairs<Rule>) -> JoinExpr {
    let mut pairs = pairs.peekable();
    let join_type = match pairs.next().unwrap().as_rule() {
        Rule::inner_join => JoinType::Inner,
        Rule::left_join => JoinType::Left,
        Rule::right_join => JoinType::Right,
        Rule::full_outer_join => JoinType::FullOuter,
        Rule::cross_join => JoinType::Cross,
        _ => unreachable!(),
    };

    let target = parse_target(pairs.next().unwrap().into_inner());
    let condition = parse_expression(pairs.next().unwrap().into_inner());
    JoinExpr {
        join_type,
        target,
        condition,
    }
}

fn parse_from_expr(pairs: pest::iterators::Pairs<Rule>) -> FromExpr {
    let mut pairs = pairs.peekable();
    let target = parse_target(pairs.next().unwrap().into_inner());
    let joins = pairs.map(|p| parse_join_expr(p.into_inner())).collect();
    FromExpr { target, joins }
}

fn parse_query_body(pairs: pest::iterators::Pairs<Rule>) -> QueryBody {
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

    QueryBody {
        from: from.unwrap(),
        where_clause,
        select,
    }
}

fn parse_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
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
    let input = { "{ name: \"alice\" }" };
    let pairs = match NextSqlParser::parse(Rule::expression, &input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {}", e);
            return;
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::Literal(Literal::Object(
            ObjectLiteralExpression(
                vec![(
                    "name".to_string(),
                    Expression::Atomic(AtomicExpression::Literal(Literal::String(
                        "alice".to_string()
                    )))
                )]
                .into_iter()
                .collect()
            )
        ))),
        expr
    );
    let input = "1 + 1";
    let pairs = match NextSqlParser::parse(Rule::expression, &input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {}", e);
            return;
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Binary {
            left: Box::new(Expression::Atomic(AtomicExpression::Literal(
                Literal::Numeric(1.0)
            ))),
            op: BinaryOp::Add,
            right: Box::new(Expression::Atomic(AtomicExpression::Literal(
                Literal::Numeric(1.0)
            )))
        },
        expr
    );

    let input = "[1, 2, 3]";
    let pairs = match NextSqlParser::parse(Rule::expression, &input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {}", e);
            return;
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::Array(ArrayExpression(vec![
            Expression::Atomic(AtomicExpression::Literal(Literal::Numeric(1.0))),
            Expression::Atomic(AtomicExpression::Literal(Literal::Numeric(2.0))),
            Expression::Atomic(AtomicExpression::Literal(Literal::Numeric(3.0))),
        ]))),
        expr
    );
}

fn parse_logical_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let left = parse_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::and => BinaryOp::And,
        Rule::or => BinaryOp::Or,
        _ => unreachable!(),
    };
    let right = parse_expression(pairs.next().unwrap().into_inner());
    Expression::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_equality_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let left = parse_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::equal => BinaryOp::Equal,
        Rule::unequal => BinaryOp::Unequal,
        _ => unreachable!(),
    };
    let right = parse_expression(pairs.next().unwrap().into_inner());
    Expression::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_relational_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let left = parse_additive_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::lt => BinaryOp::LessThan,
        Rule::le => BinaryOp::LessThanOrEqual,
        Rule::gt => BinaryOp::GreaterThan,
        Rule::ge => BinaryOp::GreaterThanOrEqual,
        _ => unreachable!(),
    };
    let right = parse_additive_expression(pairs.next().unwrap().into_inner());
    Expression::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_additive_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let left = parse_multiplicative_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::add => BinaryOp::Add,
        Rule::subtract => BinaryOp::Subtract,
        _ => unreachable!(),
    };
    let right = parse_multiplicative_expression(pairs.next().unwrap().into_inner());
    Expression::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_multiplicative_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let left = parse_unary_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::multiply => BinaryOp::Multiply,
        Rule::divide => BinaryOp::Divide,
        Rule::rem => BinaryOp::Remainder,
        _ => unreachable!(),
    };
    let right = parse_unary_expression(pairs.next().unwrap().into_inner());
    Expression::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn parse_unary_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    match pairs.peek().unwrap().as_rule() {
        Rule::not => {
            pairs.next();
            let expr = parse_atomic_expression(pairs.next().unwrap().into_inner());
            Expression::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            }
        }
        _ => parse_atomic_expression(pairs.next().unwrap().into_inner()),
    }
}

fn parse_column(pairs: pest::iterators::Pairs<Rule>) -> Column {
    let mut pairs = pairs.peekable();
    match pairs.peek().unwrap().as_rule() {
        Rule::wildcard => Column::Wildcard,
        Rule::ident => {
            let first = pairs.next().unwrap().as_str().to_string();
            if let Some(second) = pairs.next() {
                if second.as_str() == "*" {
                    Column::WildcardOf(first)
                } else {
                    Column::ExplicitTarget(first, second.as_str().to_string())
                }
            } else {
                Column::ImplicitTarget(first)
            }
        }
        _ => unreachable!(),
    }
}

fn parse_atomic_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    fn parse_literal_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
        let pair = pairs.peek().unwrap();
        Expression::Atomic(match pair.as_rule() {
            Rule::numeric_literal => {
                dbg!(pair.as_str());
                let num = pair.as_str().trim().parse::<f64>().unwrap();
                AtomicExpression::Literal(Literal::Numeric(num))
            }
            Rule::string_literal => {
                let s = pair.as_str();
                AtomicExpression::Literal(Literal::String(s[1..s.len() - 1].to_string()))
            }
            Rule::boolean_literal => {
                let b = pair.as_str() == "true";
                AtomicExpression::Literal(Literal::Boolean(b))
            }
            Rule::object_literal => {
                let mut pairs = pair.into_inner();
                let mut map: HashMap<String, Expression> = HashMap::new();
                while pairs.peek().is_some() {
                    let key = pairs.next().unwrap().as_str().to_string();
                    let value = parse_expression(pairs.next().unwrap().into_inner());
                    map.insert(key, value);
                }
                AtomicExpression::Literal(Literal::Object(ObjectLiteralExpression(map)))
            }
            _ => unreachable!(),
        })
    }
    let pair = pairs.peek().unwrap();
    match pair.as_rule() {
        Rule::column => {
            Expression::Atomic(AtomicExpression::Column(parse_column(pair.into_inner())))
        }
        Rule::literal => parse_literal_expression(pair.into_inner()),
        Rule::variable => {
            let name = pair.as_str().to_string();
            Expression::Atomic(AtomicExpression::Variable(Variable { name }))
        }
        Rule::array_expression => {
            let pairs = pair.into_inner().peekable();
            let elements = pairs
                .flat_map(|p| match p.as_rule() {
                    Rule::expression => Some(parse_expression(p.into_inner())),
                    _ => None,
                })
                .collect();
            Expression::Atomic(AtomicExpression::Array(ArrayExpression(elements)))
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

    #[test]
    fn insert() {
        let input = include_str!("../examples/insert.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn insert_optional() {
        let input = include_str!("../examples/insert-optional.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn insert_many() {
        let input = include_str!("../examples/insert-many.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn insert_many_with_variable() {
        let input = include_str!("../examples/insert-many-with-variable.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }
}
