use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;

use pest::{error::Error, Parser};
use pest_derive::Parser;

use crate::ast::*;

#[derive(Parser)]
#[grammar = "nextsql.pest"]
pub struct NextSqlParser;

pub fn parse_module(input: &str) -> Result<Module, Error<Rule>> {
    let pairs: pest::iterators::Pairs<Rule> =
        NextSqlParser::parse(Rule::module, &input)?;

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
                        Rule::with_statement => {
                            toplevels.push(TopLevel::With(parse_with_statement(
                                inner_pair.into_inner(),
                            )));
                        }
                        Rule::EOI => break,
                        _ => {
                            // Skip unexpected rules instead of panicking
                            eprintln!("Warning: Unexpected rule in module: {:?}", inner_pair.as_rule());
                        }
                    }
                }
            }
            _ => {
                // Skip unexpected top-level rules instead of panicking
                eprintln!("Warning: Unexpected top-level rule: {:?}", pair.as_rule());
            }
        }
    }

    Ok(Module { toplevels })
}

fn parse_variable(pairs: pest::iterators::Pairs<Rule>) -> Variable {
    let mut pairs = pairs.peekable();
    let name = pairs.next().unwrap().as_str()[1..].to_string();
    Variable { name }
}

#[test]
fn test_simple_query_parse() {
    let input = r#"query test() {
  from(users)
  .select(users.*)
}"#;
    let result = parse_module(input);
    assert!(result.is_ok(), "Failed to parse simple query: {:?}", result.err());
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
        let name = pairs.next().unwrap().as_str()[1..].to_string(); // Strip the $ prefix
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
    let arguments = if let Some(pair) = pairs.peek() {
        match pair.as_rule() {
            Rule::argument_list => parse_argument_list(pairs.next().unwrap().into_inner()),
            _ => Vec::new(),
        }
    } else {
        Vec::new()
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
    // dbg!(&pairs);
    let decl = parse_query_decl(pairs.peekable().next().unwrap().into_inner());
    dbg!(&decl);
}

fn parse_query(pairs: pest::iterators::Pairs<Rule>) -> Query {
    let mut pairs = pairs.peekable();
    let decl = match pairs.peek().unwrap().as_rule() {
        Rule::query_decl => parse_query_decl(pairs.next().unwrap().into_inner()),
        _ => unreachable!(),
    };

    let next = pairs.next().unwrap();
    let body = match next.as_rule() {
        Rule::query_body => {
            let mut statements = Vec::new();
            for pair in next.into_inner() {
                match pair.as_rule() {
                    Rule::query_statement => {
                        let inner = pair.into_inner().next().unwrap();
                        match inner.as_rule() {
                            Rule::alias_statement => {
                                statements.push(QueryStatement::Alias(
                                    parse_alias_statement(inner.into_inner())
                                ));
                            }
                            Rule::select_statement => {
                                statements.push(QueryStatement::Select(
                                    parse_select_statement(inner.into_inner())
                                ));
                            }
                            _ => unreachable!("Unexpected query statement: {:?}", inner.as_rule()),
                        }
                    }
                    _ => unreachable!("Unexpected rule in query body: {:?}", pair.as_rule()),
                }
            }
            QueryBody { statements }
        }
        _ => unreachable!(),
    };
    Query { decl, body }
}

fn parse_with_statement(pairs: pest::iterators::Pairs<Rule>) -> WithStatement {
    let mut pairs = pairs.peekable();
    let name = pairs.next().unwrap().as_str().to_string();
    
    let mut body = Vec::new();
    for pair in pairs {
        match pair.as_rule() {
            Rule::query_statement => {
                let inner = pair.into_inner().next().unwrap();
                match inner.as_rule() {
                    Rule::alias_statement => {
                        body.push(QueryStatement::Alias(
                            parse_alias_statement(inner.into_inner())
                        ));
                    }
                    Rule::select_statement => {
                        body.push(QueryStatement::Select(
                            parse_select_statement(inner.into_inner())
                        ));
                    }
                    _ => unreachable!("Unexpected query statement: {:?}", inner.as_rule()),
                }
            }
            _ => unreachable!("Unexpected rule in with statement: {:?}", pair.as_rule()),
        }
    }
    
    WithStatement { name, body }
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
    let pairs = pairs.peekable();
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
fn parse_insert(pairs: pest::iterators::Pairs<Rule>) -> Insert {
    let mut pairs = pairs.peekable();
    // dbg!(&pairs);
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
    Insert {
        into: insert,
        values,
        returning,
    }
}

#[test]
fn test_parse_update_clause() {
    let input = "update(foo).set({ a: 1, b: \"abc\" }).where(a > 1).returning(a, b)";
    let pairs = match NextSqlParser::parse(Rule::update_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let mut pairs = pairs.peekable().next().unwrap().into_inner();
    let clause = pairs.next().unwrap().as_str().to_string();
    assert_eq!(clause, "foo");
}

#[test]
fn test_parse_where_clause() {
    let input = ".where(u.id == $id)";
    let pairs = match NextSqlParser::parse(Rule::where_clause, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    // dbg!(&pairs);
    let clause = parse_where_clause(pairs.peekable().next().unwrap().into_inner());
    dbg!(&clause);
    assert_eq!(
        Expression::Binary {
            left: Box::new(Expression::Atomic(AtomicExpression::Column(
                Column::ExplicitTarget("u".to_string(), "id".to_string())
            ))),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Atomic(AtomicExpression::Variable(Variable {
                name: "id".to_string()
            })))
        },
        clause
    );
}
fn parse_where_clause(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    match pairs.peek().unwrap().as_rule() {
        Rule::expression => {
            // println!("414: {}", pairs.peek().unwrap());
            parse_expression(pairs.next().unwrap().into_inner())
        }
        _ => unreachable!(),
    }
}

fn parse_update(pairs: pest::iterators::Pairs<Rule>) -> Update {
    // dbg!(&pairs);
    let mut pairs = pairs.peekable();
    let target = match pairs.peek().unwrap().as_rule() {
        Rule::update_clause => {
            let mut pairs = pairs.next().unwrap().into_inner().peekable();
            parse_target(pairs.next().unwrap())
        }
        _ => unreachable!(),
    };
    // where_clause comes before set_clause according to the grammar
    let where_clause = match pairs.peek().map(|p| p.as_rule()) {
        Some(Rule::where_clause) => Some(parse_where_clause(pairs.next().unwrap().into_inner())),
        _ => None,
    };
    let set = match pairs.peek().unwrap().as_rule() {
        Rule::set_clause => {
            let mut pairs = pairs.next().unwrap().into_inner().peekable();
            let mut set = Vec::new();
            while pairs.peek().is_some() {
                let column = pairs.next().unwrap().as_str().to_string();
                let value = parse_expression(pairs.next().unwrap().into_inner());
                set.push((column, value));
            }
            set
        }
        _ => unreachable!(),
    };
    let returning = match pairs.peek().map(|p| p.as_rule()) {
        Some(Rule::returning_clause) => {
            Some(parse_returning_clause(pairs.next().unwrap().into_inner()))
        }
        _ => None,
    };
    Update {
        target,
        set,
        where_clause,
        returning,
    }
}

fn parse_mutation_body(pairs: pest::iterators::Pairs<Rule>) -> MutationBody {
    let mut items = Vec::new();
    
    for pair in pairs {
        match pair.as_rule() {
            Rule::mutation_item => {
                let inner = pair.into_inner().next().unwrap();
                match inner.as_rule() {
                    Rule::alias_statement => {
                        items.push(MutationBodyItem::Alias(
                            parse_alias_statement(inner.into_inner())
                        ));
                    }
                    Rule::mutation_statement => {
                        let statement = parse_mutation_statement(inner.into_inner());
                        items.push(MutationBodyItem::Mutation(statement));
                    }
                    _ => unreachable!("Unexpected mutation item: {:?}", inner.as_rule()),
                }
            }
            _ => unreachable!("Unexpected rule in mutation body: {:?}", pair.as_rule()),
        }
    }
    
    MutationBody { items }
}

fn parse_mutation_statement(pairs: pest::iterators::Pairs<Rule>) -> MutationStatement {
    let rule = pairs.peek().unwrap().as_rule();
    match rule {
        Rule::insert_clause => MutationStatement::Insert(parse_insert(pairs)),
        Rule::update_clause => MutationStatement::Update(parse_update(pairs)),
        Rule::delete_clause => MutationStatement::Delete(parse_delete(pairs)),
        _ => unreachable!("Unexpected mutation statement: {:?}", rule),
    }
}

fn parse_delete(pairs: pest::iterators::Pairs<Rule>) -> Delete {
    let mut pairs = pairs.peekable();
    let target = match pairs.peek().unwrap().as_rule() {
        Rule::delete_clause => {
            let mut pairs = pairs.next().unwrap().into_inner().peekable();
            parse_target(pairs.next().unwrap())
        }
        _ => unreachable!(),
    };
    let where_clause = match pairs.peek().map(|p| p.as_rule()) {
        Some(Rule::where_clause) => Some(parse_where_clause(pairs.next().unwrap().into_inner())),
        _ => None,
    };
    let returning = match pairs.peek().map(|p| p.as_rule()) {
        Some(Rule::returning_clause) => {
            Some(parse_returning_clause(pairs.next().unwrap().into_inner()))
        }
        _ => None,
    };
    Delete {
        target,
        where_clause,
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
    // dbg!(&pairs);
    let decl = parse_mutation_decl(pairs.next().unwrap().into_inner());
    let body = parse_mutation_body(pairs.next().unwrap().into_inner());
    Mutation { body, decl }
}

#[test]
fn test_parse_target() {
    let input = "foo";
    let pairs = match NextSqlParser::parse(Rule::target, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let target = parse_target(pairs.peekable().next().unwrap());
    assert_eq!(
        target,
        Target {
            name: "foo".to_string(),
        }
    );
}
fn parse_target(pair: pest::iterators::Pair<Rule>) -> Target {
    let name = pair.as_str().to_string();
    Target { name }
}

fn parse_join_expr(pairs: pest::iterators::Pairs<Rule>) -> JoinExpr {
    let mut pairs = pairs.peekable();
    
    // Skip dot
    pairs.next();
    
    // Get join method
    let join_method = pairs.next().unwrap();
    let join_type = match join_method.as_str() {
        "innerJoin" => JoinType::Inner,
        "leftJoin" => JoinType::Left,
        "rightJoin" => JoinType::Right,
        "fullOuterJoin" => JoinType::FullOuter,
        "crossJoin" => JoinType::Cross,
        _ => unreachable!(),
    };

    // Parse table name and condition
    let table = pairs.next().unwrap().as_str().to_string();
    let condition = parse_expression(pairs.next().unwrap().into_inner());
    
    JoinExpr {
        join_type,
        table,
        condition,
    }
}

fn parse_from_expr(pairs: pest::iterators::Pairs<Rule>) -> FromExpr {
    let mut pairs = pairs.peekable();
    let table = pairs.next().unwrap().as_str().to_string();
    let joins = pairs.map(|p| parse_join_expr(p.into_inner())).collect();
    FromExpr { table, joins }
}

fn parse_alias_statement(pairs: pest::iterators::Pairs<Rule>) -> AliasStatement {
    let mut pairs = pairs.peekable();
    
    // Skip "alias" keyword
    pairs.next();
    
    // Get alias name
    let alias = pairs.next().unwrap().as_str().to_string();
    
    // Get target name (no need to skip "=" as it's not captured separately)
    let target = pairs.next().unwrap().as_str().to_string();
    
    AliasStatement { alias, target }
}

fn parse_select_expression(pairs: pest::iterators::Pairs<Rule>) -> SelectExpression {
    let mut pairs = pairs.peekable();
    let first_pair = pairs.next().unwrap();

    match first_pair.as_rule() {
        Rule::ident => {
            // This is an aliased expression: alias : expression
            let alias = Some(first_pair.as_str().to_string());
            let expr = parse_expression(pairs.next().unwrap().into_inner());
            SelectExpression { expr, alias }
        }
        Rule::expression => {
            // This is a regular expression without alias
            let expr = parse_expression(first_pair.into_inner());
            SelectExpression { expr, alias: None }
        }
        _ => unreachable!(
            "Unexpected rule in select_expression: {:?}",
            first_pair.as_rule()
        ),
    }
}

fn parse_select_statement(pairs: pest::iterators::Pairs<Rule>) -> SelectStatement {
    let mut from = None;
    let mut clauses = Vec::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::from_clause => from = Some(parse_from_expr(pair.into_inner())),
            Rule::where_clause => {
                let expr = parse_expression(pair.into_inner().next().unwrap().into_inner());
                clauses.push(QueryClause::Where(expr));
            }
            Rule::select_clause => {
                let select_exprs: Vec<SelectExpression> = pair
                    .into_inner()
                    .map(|p| parse_select_expression(p.into_inner()))
                    .collect();
                clauses.push(QueryClause::Select(select_exprs));
            }
            Rule::when_clause => {
                let mut clause_pairs = pair.into_inner();
                let condition = parse_expression(clause_pairs.next().unwrap().into_inner());
                let call_expr = parse_call_expression(clause_pairs.next().unwrap().into_inner());

                // Convert call expression to appropriate clause type
                let clause_body = match call_expr {
                    Expression::Atomic(AtomicExpression::Call(call)) => {
                        match call.callee.as_str() {
                            "where" => QueryClause::Where(call.args[0].clone()),
                            "select" => QueryClause::Select(
                                call.args
                                    .iter()
                                    .map(|arg| SelectExpression {
                                        expr: arg.clone(),
                                        alias: None,
                                    })
                                    .collect(),
                            ),
                            "orderBy" => QueryClause::OrderBy(call.args[0].clone()),
                            "limit" => QueryClause::Limit(call.args[0].clone()),
                            "join" => {
                                // For join, parse the first argument as target and second as condition
                                if call.args.len() != 2 {
                                    unreachable!("Join requires exactly 2 arguments");
                                }

                                // Extract table name from the first argument
                                let table = match &call.args[0] {
                                    Expression::Atomic(AtomicExpression::Column(
                                        Column::ImplicitTarget(name),
                                    )) => name.clone(),
                                    _ => unreachable!(
                                        "Join first argument should be a table reference"
                                    ),
                                };

                                QueryClause::Join(JoinExpr {
                                    join_type: JoinType::Inner,
                                    table,
                                    condition: call.args[1].clone(),
                                })
                            }
                            _ => unreachable!("Unsupported clause type: {}", call.callee),
                        }
                    }
                    _ => unreachable!("Expected call expression"),
                };

                clauses.push(QueryClause::When(WhenClause {
                    condition: Box::new(condition),
                    clause: Box::new(clause_body),
                }));
            }
            Rule::limit_clause => {
                let expr = parse_expression(pair.into_inner().next().unwrap().into_inner());
                clauses.push(QueryClause::Limit(expr));
            }
            Rule::order_by_clause => {
                let expr = parse_expression(pair.into_inner().next().unwrap().into_inner());
                clauses.push(QueryClause::OrderBy(expr));
            }
            Rule::group_by_clause => {
                let group_exprs: Vec<Expression> = pair
                    .into_inner()
                    .map(|p| parse_expression(p.into_inner()))
                    .collect();
                clauses.push(QueryClause::GroupBy(group_exprs));
            }
            Rule::aggregate_clause => {
                let aggregate_exprs: Vec<AggregateExpression> = pair
                    .into_inner()
                    .map(|p| {
                        let mut pairs = p.into_inner();
                        let alias = pairs.next().unwrap().as_str().to_string();
                        let expr = parse_expression(pairs.next().unwrap().into_inner());
                        AggregateExpression { alias, expr }
                    })
                    .collect();
                clauses.push(QueryClause::Aggregate(aggregate_exprs));
            }
            _ => {
                unreachable!()
            }
        }
    }

    SelectStatement {
        from: from.unwrap(),
        clauses,
    }
}

#[test]
fn test_when_expression() {
    let input = "when($includeEmail, users.email)";
    let pairs = match NextSqlParser::parse(Rule::expression, &input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {}", e);
            return;
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    match expr {
        Expression::Atomic(AtomicExpression::Call(call)) => {
            assert_eq!(call.callee, "when");
            assert_eq!(call.args.len(), 2);
        }
        _ => panic!("Expected when call expression, got: {:?}", expr),
    }
}

#[test]
fn test_parse_expression() {
    let input = "a == b";
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
            left: Box::new(Expression::Atomic(AtomicExpression::Column(
                Column::ImplicitTarget("a".to_string())
            ))),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Atomic(AtomicExpression::Column(
                Column::ImplicitTarget("b".to_string())
            )))
        },
        expr
    );

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
        Expression::Atomic(AtomicExpression::Literal(Literal::Array(ArrayExpression(
            vec![
                Expression::Atomic(AtomicExpression::Literal(Literal::Numeric(1.0))),
                Expression::Atomic(AtomicExpression::Literal(Literal::Numeric(2.0))),
                Expression::Atomic(AtomicExpression::Literal(Literal::Numeric(3.0)))
            ]
        )))),
        expr
    );
    let input = "\",\"";
    let pairs = NextSqlParser::parse(Rule::expression, &input).expect("パースエラー");
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::Literal(Literal::String(",".to_string()))),
        expr
    );

    let input = "split(\"a,b,c\", \",\")";
    let pairs = match NextSqlParser::parse(Rule::expression, &input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {}", e);
            panic!();
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::Call(CallExpression {
            callee: "split".to_string(),
            args: vec![
                Expression::Atomic(AtomicExpression::Literal(Literal::String(
                    "a,b,c".to_string()
                ))),
                Expression::Atomic(AtomicExpression::Literal(Literal::String(",".to_string())))
            ]
        })),
        expr
    );
    let input = "call()[0]";
    let pairs = match NextSqlParser::parse(Rule::expression, &input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {}", e);
            panic!();
        }
    };
    println!("{}", &pairs);
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::IndexAccess(IndexAccess {
            target: Box::new(Expression::Atomic(AtomicExpression::Call(CallExpression {
                callee: "call".to_string(),
                args: vec![]
            }))),
            index: Box::new(Expression::Atomic(AtomicExpression::Literal(
                Literal::Numeric(0.0)
            )))
        })),
        expr
    );
}
fn parse_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let pair = pairs.next().unwrap();
    match pair.as_rule() {
        Rule::logical_expression => parse_logical_expression(pair.into_inner()),
        Rule::equality_expression => parse_equality_expression(pair.into_inner()),
        Rule::relational_expression => parse_relational_expression(pair.into_inner()),
        Rule::additive_expression => parse_additive_expression(pair.into_inner()),
        Rule::multiplicative_expression => parse_multiplicative_expression(pair.into_inner()),
        Rule::unary_expression => parse_unary_expression(pair.into_inner()),
        Rule::atomic_expression => parse_atomic_expression(pair.into_inner()),
        Rule::when_expression => {
            let mut pairs = pair.into_inner().peekable();
            let condition = parse_expression(pairs.next().unwrap().into_inner());
            let then_expr = parse_expression(pairs.next().unwrap().into_inner());
            Expression::Atomic(AtomicExpression::When(WhenExpression {
                condition: Box::new(condition),
                then_expr: Box::new(then_expr),
            }))
        }
        Rule::switch_expression => {
            let mut pairs = pair.into_inner().peekable();
            let expr = parse_expression(pairs.next().unwrap().into_inner());
            let mut cases = Vec::new();
            let mut default = None;

            for case_pair in pairs {
                match case_pair.as_rule() {
                    Rule::switch_case => {
                        let mut case_pairs = case_pair.into_inner();
                        let condition = parse_expression(case_pairs.next().unwrap().into_inner());
                        let result = parse_expression(case_pairs.next().unwrap().into_inner());
                        cases.push(SwitchCase {
                            condition: Box::new(condition),
                            result: Box::new(result),
                        });
                    }
                    Rule::switch_default => {
                        let mut default_pairs = case_pair.into_inner();
                        let result = parse_expression(default_pairs.next().unwrap().into_inner());
                        default = Some(Box::new(result));
                    }
                    _ => unreachable!(),
                }
            }

            Expression::Atomic(AtomicExpression::Switch(SwitchExpression {
                expr: Box::new(expr),
                cases,
                default,
            }))
        }
        Rule::expression => {
            let inner_pairs = pair.into_inner();
            parse_expression(inner_pairs)
        }
        _ => {
            // dbg!(&pair);
            unreachable!()
        }
    }
}

fn parse_logical_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let left = parse_equality_expression(pairs.next().unwrap().into_inner());
    if pairs.peek().is_none() {
        return left;
    }
    let op = match pairs.next().unwrap().as_rule() {
        Rule::and => BinaryOp::And,
        Rule::or => BinaryOp::Or,
        _ => unreachable!(),
    };
    let right = parse_equality_expression(pairs.next().unwrap().into_inner());
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

#[test]
fn test_parse_column() {
    let input = "*";
    let pairs = match NextSqlParser::parse(Rule::column, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(Column::Wildcard, column);

    let input = "foo_bar";
    let pairs = match NextSqlParser::parse(Rule::column, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(Column::ImplicitTarget("foo_bar".to_string()), column);

    let input = "foo.bar";
    let pairs = match NextSqlParser::parse(Rule::column, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Column::ExplicitTarget("foo".to_string(), "bar".to_string()),
        column
    );

    let input = "foo.*";
    let pairs = match NextSqlParser::parse(Rule::column, &input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {}", e),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(Column::WildcardOf("foo".to_string()), column);
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

fn parse_literal_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let pair = pairs.peek().unwrap();
    Expression::Atomic(match pair.as_rule() {
        Rule::numeric_literal => {
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
        Rule::null_literal => AtomicExpression::Literal(Literal::Null),
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
        Rule::array_literal => {
            let pairs = pair.into_inner().peekable();
            let elements = pairs
                .flat_map(|p| match p.as_rule() {
                    Rule::expression => Some(parse_expression(p.into_inner())),
                    _ => None,
                })
                .collect();
            AtomicExpression::Literal(Literal::Array(ArrayExpression(elements)))
        }
        _ => unreachable!(),
    })
}

fn parse_call_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let callee = pairs.next().unwrap().as_str().to_string();
    let mut args = Vec::new();
    while pairs.peek().is_some() {
        args.push(parse_expression(pairs.next().unwrap().into_inner()));
    }
    Expression::Atomic(AtomicExpression::Call(CallExpression { callee, args }))
}

fn parse_atomic_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let pair = pairs.peek().unwrap();
    match pair.as_rule() {
        Rule::call_expression => parse_call_expression(pair.into_inner()),
        Rule::column => {
            Expression::Atomic(AtomicExpression::Column(parse_column(pair.into_inner())))
        }
        Rule::literal => parse_literal_expression(pair.into_inner()),
        Rule::variable => Expression::Atomic(AtomicExpression::Variable(parse_variable(pairs))),
        Rule::index_access => {
            let mut pairs = pair.into_inner().peekable();
            // 左再帰を避けるため、index_accessだけ違う構造になっている
            let target = match pairs.peek().unwrap().as_rule() {
                Rule::call_expression => parse_call_expression(pairs.next().unwrap().into_inner()),
                Rule::literal => parse_literal_expression(pairs.next().unwrap().into_inner()),
                Rule::variable => Expression::Atomic(AtomicExpression::Variable(parse_variable(
                    pairs.next().unwrap().into_inner(),
                ))),
                _ => parse_expression(pairs.next().unwrap().into_inner()),
            };
            let index = parse_expression(pairs.next().unwrap().into_inner());
            Expression::Atomic(AtomicExpression::IndexAccess(IndexAccess {
                target: Box::new(target),
                index: Box::new(index),
            }))
        }
        Rule::expression => parse_expression(pair.into_inner()),
        Rule::subquery => {
            let mut pairs = pair.into_inner().peekable();
            let statement = parse_select_statement(pairs.next().unwrap().into_inner());
            Expression::Atomic(AtomicExpression::SubQuery(Box::new(statement)))
        }
        Rule::when_expression => {
            let mut pairs = pair.into_inner().peekable();
            let condition = parse_expression(pairs.next().unwrap().into_inner());
            let then_expr = parse_expression(pairs.next().unwrap().into_inner());
            Expression::Atomic(AtomicExpression::When(WhenExpression {
                condition: Box::new(condition),
                then_expr: Box::new(then_expr),
            }))
        }
        Rule::switch_expression => {
            let mut pairs = pair.into_inner().peekable();
            let expr = parse_expression(pairs.next().unwrap().into_inner());
            let mut cases = Vec::new();
            let mut default = None;

            for case_pair in pairs {
                match case_pair.as_rule() {
                    Rule::switch_case => {
                        let mut case_pairs = case_pair.into_inner();
                        let condition = parse_expression(case_pairs.next().unwrap().into_inner());
                        let result = parse_expression(case_pairs.next().unwrap().into_inner());
                        cases.push(SwitchCase {
                            condition: Box::new(condition),
                            result: Box::new(result),
                        });
                    }
                    Rule::switch_default => {
                        let mut default_pairs = case_pair.into_inner();
                        let result = parse_expression(default_pairs.next().unwrap().into_inner());
                        default = Some(Box::new(result));
                    }
                    _ => unreachable!(),
                }
            }

            Expression::Atomic(AtomicExpression::Switch(SwitchExpression {
                expr: Box::new(expr),
                cases,
                default,
            }))
        }
        Rule::member_expression => parse_member_expression(pair.into_inner()),
        Rule::property_access => parse_property_access(pair.into_inner()),
        Rule::method_call => parse_method_call(pair.into_inner()),
        Rule::basic_expression => parse_basic_expression(pair.into_inner()),
        Rule::exists_expression => {
            let mut pairs = pair.into_inner().peekable();
            let subquery = parse_select_statement(pairs.next().unwrap().into_inner());
            Expression::Atomic(AtomicExpression::Exists(Box::new(subquery)))
        }
        Rule::aggregate_function => {
            let mut pairs = pair.into_inner().peekable();
            let function_type_str = pairs.next().unwrap().as_str();
            let function_type = match function_type_str {
                "SUM" => AggregateFunctionType::Sum,
                "COUNT" => AggregateFunctionType::Count,
                "AVG" => AggregateFunctionType::Avg,
                "MIN" => AggregateFunctionType::Min,
                "MAX" => AggregateFunctionType::Max,
                _ => unreachable!("Unknown aggregate function: {}", function_type_str),
            };
            let expr = parse_expression(pairs.next().unwrap().into_inner());
            Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                function_type,
                expr: Box::new(expr),
            }))
        }
        _ => {
            // dbg!(&pair);
            unreachable!()
        }
    }
}

fn parse_member_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let pair = pairs.peek().unwrap();
    match pair.as_rule() {
        Rule::index_access => parse_index_access(pair.into_inner()),
        Rule::method_call => parse_method_call(pair.into_inner()),
        Rule::property_access => parse_property_access(pair.into_inner()),
        Rule::basic_expression => parse_basic_expression(pair.into_inner()),
        _ => unreachable!(),
    }
}

fn parse_basic_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let pair = pairs.peek().unwrap();
    match pair.as_rule() {
        Rule::call_expression => parse_call_expression(pair.into_inner()),
        Rule::literal => parse_literal_expression(pair.into_inner()),
        Rule::variable => Expression::Atomic(AtomicExpression::Variable(parse_variable(pairs))),
        Rule::column => {
            Expression::Atomic(AtomicExpression::Column(parse_column(pair.into_inner())))
        }
        Rule::expression => parse_expression(pair.into_inner()),
        _ => unreachable!(),
    }
}

fn parse_property_access(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let target_pair = pairs.next().unwrap();
    let target = parse_basic_expression(target_pair.into_inner());
    let property = pairs.next().unwrap().as_str().to_string();
    Expression::Atomic(AtomicExpression::PropertyAccess(PropertyAccess {
        target: Box::new(target),
        property,
    }))
}

fn parse_method_call(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let target_pair = pairs.next().unwrap();
    let mut result = match target_pair.as_rule() {
        Rule::basic_expression => parse_basic_expression(target_pair.into_inner()),
        Rule::property_access => parse_property_access(target_pair.into_inner()),
        _ => unreachable!(),
    };

    // Handle potentially multiple chained method calls
    while pairs.peek().is_some() {
        let method = pairs.next().unwrap().as_str().to_string();
        let mut args = Vec::new();

        // Collect arguments for this method call
        while pairs.peek().is_some() {
            let next_pair = pairs.peek().unwrap();
            match next_pair.as_rule() {
                Rule::expression => {
                    args.push(parse_expression(pairs.next().unwrap().into_inner()));
                }
                Rule::ident => {
                    // This is the next method name, stop collecting args
                    break;
                }
                _ => {
                    panic!("Unexpected rule in method call: {:?}", next_pair.as_rule());
                }
            }
        }

        // Build the method call expression
        result = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(result),
            method,
            args,
        }));
    }

    result
}

fn parse_index_access(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let target_pair = pairs.next().unwrap();
    let target = match target_pair.as_rule() {
        Rule::basic_expression => parse_basic_expression(target_pair.into_inner()),
        Rule::property_access => parse_property_access(target_pair.into_inner()),
        Rule::method_call => parse_method_call(target_pair.into_inner()),
        _ => unreachable!(),
    };
    let index = parse_expression(pairs.next().unwrap().into_inner());
    Expression::Atomic(AtomicExpression::IndexAccess(IndexAccess {
        target: Box::new(target),
        index: Box::new(index),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_select_test() {
        let input = include_str!("../../examples/simple-select.nsql");
        parse_module(&input).unwrap();
    }

    #[test]
    fn simple_select_and_join_test() {
        let input = include_str!("../../examples/simple-select-and-join.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn insert_optional_test() {
        let input = include_str!("../../examples/insert-optional.nsql");
        parse_module(&input).unwrap();
    }

    #[test]
    fn insert_many_test() {
        let input = include_str!("../../examples/insert-many.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn insert_many_with_variable_test() {
        let input = include_str!("../../examples/insert-many-with-variable.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn update_test() {
        let input = include_str!("../../examples/update.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn simple_delete_test() {
        let input = include_str!("../../examples/simple-delete.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn delete_with_subquery_test() {
        let input = include_str!("../../examples/delete-with-subquery.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn dynamic_conditional_clauses_test() {
        let input = include_str!("../../examples/dynamic-conditional-clauses.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn dynamic_field_selection_test() {
        let input = include_str!("../../examples/dynamic-field-selection.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn dynamic_joins_test() {
        let input = include_str!("../../examples/dynamic-joins.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn dynamic_switch_case_test() {
        let input = include_str!("../../examples/dynamic-switch-case.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn dynamic_array_conditions_test() {
        let input = include_str!("../../examples/dynamic-array-conditions.nsql");
        dbg!("{:?}", parse_module(&input).unwrap());
    }

    #[test]
    fn comprehensive_examples_test() {
        use std::fs;
        use std::path::Path;

        // Dynamically discover all .nsql files in the examples directory
        let examples_dir = Path::new("../examples");
        let mut example_files = Vec::new();

        if examples_dir.exists() && examples_dir.is_dir() {
            match fs::read_dir(examples_dir) {
                Ok(entries) => {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            let path = entry.path();
                            if path.extension().and_then(|s| s.to_str()) == Some("nsql") {
                                if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                                    // Read file content
                                    match fs::read_to_string(&path) {
                                        Ok(content) => {
                                            example_files.push((filename.to_string(), content));
                                        }
                                        Err(e) => {
                                            panic!("Failed to read {}: {}", filename, e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    panic!("Failed to read examples directory: {}", e);
                }
            }
        } else {
            panic!("Examples directory not found or is not a directory");
        }

        // Sort files for consistent test order
        example_files.sort_by(|a, b| a.0.cmp(&b.0));

        if example_files.is_empty() {
            panic!("No .nsql example files found in examples directory");
        }

        let mut failed_files = Vec::new();
        let mut skipped_file_names = Vec::new();
        let mut total_files = 0;
        let mut successful_files = 0;
        let mut skipped_files = 0;

        println!(
            "🔍 Found {} .nsql files in examples directory",
            example_files.len()
        );

        for (filename, content) in example_files {
            total_files += 1;
            print!("Testing {}... ", filename);

            match parse_module(&content) {
                Ok(module) => {
                    successful_files += 1;

                    // Validate the module has at least one top-level item
                    assert!(
                        !module.toplevels.is_empty(),
                        "File {} should contain at least one query or mutation",
                        filename
                    );

                    // Count queries and mutations
                    let mut queries = 0;
                    let mut mutations = 0;
                    let mut with_statements = 0;
                    for toplevel in &module.toplevels {
                        match toplevel {
                            TopLevel::Query(_) => queries += 1,
                            TopLevel::Mutation(_) => mutations += 1,
                            TopLevel::With(_) => with_statements += 1,
                        }
                    }

                    println!(
                        "✅ ({} queries, {} mutations, {} with statements)",
                        queries, mutations, with_statements
                    );
                }
                Err(e) => {
                    let error_message = e.to_string();

                    // Check if this is a known unsupported feature
                    let known_unsupported_patterns = [
                        "with",
                        "groupBy",
                        "aggregate",
                        "having",
                        "union",
                        "intersect",
                        "except",
                    ];

                    let is_unsupported_feature = known_unsupported_patterns
                        .iter()
                        .any(|pattern| content.contains(pattern));

                    if is_unsupported_feature {
                        skipped_files += 1;
                        skipped_file_names.push(filename.clone());
                        println!("⚠️  SKIPPED (contains unsupported features)");
                        println!(
                            "    💡 Consider adding '{}' to unsupported_files list",
                            filename
                        );
                    } else {
                        failed_files.push((filename.clone(), error_message.clone()));
                        println!("❌ FAILED: {}", error_message);
                    }
                }
            }
        }

        // Print summary
        println!("\n📊 PARSING SUMMARY:");
        println!("Total files discovered: {}", total_files);
        println!("Successful: {} ✅", successful_files);
        println!("Skipped (unsupported): {} ⚠️", skipped_files);
        println!("Failed: {} ❌", failed_files.len());

        let testable_files = total_files - skipped_files;
        if testable_files > 0 {
            let success_rate = (successful_files as f64 / testable_files as f64) * 100.0;
            println!(
                "Success rate: {:.1}% ({}/{})",
                success_rate, successful_files, testable_files
            );
        }

        if !skipped_file_names.is_empty() {
            println!("\n⚠️  Skipped files (unsupported features):");
            for filename in &skipped_file_names {
                println!("  - {}", filename);
            }
        }

        if !failed_files.is_empty() {
            println!("\n❌ Failed files:");
            for (filename, error) in &failed_files {
                println!("  - {}: {}", filename, error);
            }
        }

        // Assert that all testable files parsed successfully
        assert!(
            failed_files.is_empty(),
            "Some example files failed to parse. See output above for details."
        );

        if skipped_files > 0 {
            println!("🎉 All supported example files parsed successfully! ({} unsupported files were skipped)", skipped_files);
        } else {
            println!("🎉 All example files parsed successfully!");
        }
    }

    #[test]
    fn test_member_expressions() {
        // Test property access on variable
        let input = "$var.length";
        let pairs = NextSqlParser::parse(Rule::expression, input).unwrap();
        let expr = parse_expression(pairs);
        match &expr {
            Expression::Atomic(AtomicExpression::PropertyAccess(PropertyAccess {
                target,
                property,
            })) => {
                assert_eq!(property, "length");
                match target.as_ref() {
                    Expression::Atomic(AtomicExpression::Variable(var)) => {
                        assert_eq!(var.name, "var");
                    }
                    _ => panic!("Expected variable as target"),
                }
            }
            _ => panic!("Expected property access, got {:?}", expr),
        }

        // Test method call on column (users.name is parsed as a column)
        let input = "users.name.toLowerCase()";
        let pairs = NextSqlParser::parse(Rule::expression, input).unwrap();
        let expr = parse_expression(pairs);
        match &expr {
            Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
                target,
                method,
                args,
            })) => {
                assert_eq!(method, "toLowerCase");
                assert!(args.is_empty());
                // users.name is parsed as ExplicitTarget column
                match target.as_ref() {
                    Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(
                        table,
                        col,
                    ))) => {
                        assert_eq!(table, "users");
                        assert_eq!(col, "name");
                    }
                    _ => panic!("Expected ExplicitTarget column as target, got {:?}", target),
                }
            }
            _ => panic!("Expected method call, got {:?}", expr),
        }

        // Test method call with arguments (users.id is parsed as a column)
        let input = "users.id.in($array)";
        let pairs = NextSqlParser::parse(Rule::expression, input).unwrap();
        let expr = parse_expression(pairs);
        match &expr {
            Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
                target,
                method,
                args,
            })) => {
                assert_eq!(method, "in");
                assert_eq!(args.len(), 1);
                // users.id is parsed as ExplicitTarget column
                match target.as_ref() {
                    Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(
                        table,
                        col,
                    ))) => {
                        assert_eq!(table, "users");
                        assert_eq!(col, "id");
                    }
                    _ => panic!("Expected ExplicitTarget column as target, got {:?}", target),
                }
                // Verify the argument is a variable
                match &args[0] {
                    Expression::Atomic(AtomicExpression::Variable(var)) => {
                        assert_eq!(var.name, "array");
                    }
                    _ => panic!("Expected variable as argument"),
                }
            }
            _ => panic!("Expected method call, got {:?}", expr),
        }

        // Test chained method calls
        let input = "$var.toUpperCase().trim()";
        let pairs = NextSqlParser::parse(Rule::expression, input).unwrap();
        let expr = parse_expression(pairs);
        match &expr {
            Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
                target,
                method,
                args,
            })) => {
                assert_eq!(method, "trim");
                assert!(args.is_empty());
                // The target should be another method call
                match target.as_ref() {
                    Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
                        target,
                        method,
                        args,
                    })) => {
                        assert_eq!(method, "toUpperCase");
                        assert!(args.is_empty());
                        // The target should be a variable
                        match target.as_ref() {
                            Expression::Atomic(AtomicExpression::Variable(var)) => {
                                assert_eq!(var.name, "var");
                            }
                            _ => panic!("Expected variable as target"),
                        }
                    }
                    _ => panic!("Expected method call as target"),
                }
            }
            _ => panic!("Expected method call, got {:?}", expr),
        }
    }
}
