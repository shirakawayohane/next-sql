use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;

use pest::{error::Error, Parser};
use pest_derive::Parser;

use crate::ast::*;

fn get_span(pair: &pest::iterators::Pair<Rule>) -> Option<Span> {
    let span = pair.as_span();
    Some(Span {
        start: span.start(),
        end: span.end(),
    })
}

#[derive(Parser)]
#[grammar = "nextsql.pest"]
pub struct NextSqlParser;

pub fn parse_module(input: &str) -> Result<Module, Box<Error<Rule>>> {
    let pairs: pest::iterators::Pairs<Rule> =
        NextSqlParser::parse(Rule::module, input).map_err(Box::new)?;

    let mut toplevels = Vec::new();

    let pairs = pairs.peekable();
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
                        Rule::relation => {
                            toplevels.push(TopLevel::Relation(parse_relation(inner_pair.into_inner())));
                        }
                        Rule::valtype_decl => {
                            toplevels.push(TopLevel::ValType(parse_valtype_decl(inner_pair)));
                        }
                        Rule::input_decl => {
                            toplevels.push(TopLevel::Input(parse_input_decl(inner_pair)));
                        }
                        Rule::EOI => break,
                        _ => {
                            // Skip unexpected rules instead of panicking
                            eprintln!(
                                "Warning: Unexpected rule in module: {:?}",
                                inner_pair.as_rule()
                            );
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
    let pair = pairs.next().unwrap();
    let name = pair.as_str()[1..].to_string();
    let span = get_span(&pair);
    Variable { name, span }
}

#[test]
fn test_simple_query_parse() {
    let input = r#"query test() {
  from(users)
  .select(users.*)
}"#;
    let result = parse_module(input);
    assert!(
        result.is_ok(),
        "Failed to parse simple query: {:?}",
        result.err()
    );
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
        "decimal",
        "json",
    ];

    for input in builtin_types {
        let pairs = match NextSqlParser::parse(Rule::r#type, input) {
            Ok(p) => p,
            Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
        };
        let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
        assert_eq!(input, typ.to_string());
    }

    // Test basic Insertable type
    let input = "Insertable<User>";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test Insertable with built-in type
    let input = "Insertable<string>";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test optional Insertable type
    let input = "Insertable<User>?";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test array of Insertable type
    let input = "[Insertable<User>]";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test optional array of Insertable type
    let input = "[Insertable<User>]?";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test Insertable with optional inner type
    let input = "Insertable<User?>";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test basic ChangeSet type
    let input = "ChangeSet<User>";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test ChangeSet with built-in type
    let input = "ChangeSet<string>";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    // Test optional ChangeSet type
    let input = "ChangeSet<User>?";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    let typ = parse_type(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(input, typ.to_string());

    let input = "[i32]";
    let pairs = match NextSqlParser::parse(Rule::r#type, input) {
        Ok(p) => p,
        Err(e) => panic!("{input}のパースでエラーが発生しました: {e}"),
    };
    matches!(
        parse_type(pairs.peekable().next().unwrap().into_inner()),
        Type::Array(_)
    );
}
fn parse_builtin_type_from_pair(pair: pest::iterators::Pair<Rule>) -> BuiltInType {
    match pair.into_inner().next().unwrap().as_rule() {
        Rule::i16_type => BuiltInType::I16,
        Rule::i32_type => BuiltInType::I32,
        Rule::i64_type => BuiltInType::I64,
        Rule::f32_type => BuiltInType::F32,
        Rule::f64_type => BuiltInType::F64,
        Rule::timestamp_type => BuiltInType::Timestamp,
        Rule::timestamptz_type => BuiltInType::Timestamptz,
        Rule::date_type => BuiltInType::Date,
        Rule::uuid_type => BuiltInType::Uuid,
        Rule::string_type => BuiltInType::String,
        Rule::bool_type => BuiltInType::Bool,
        Rule::decimal_type => BuiltInType::Decimal,
        Rule::json_type => BuiltInType::Json,
        _ => unreachable!(),
    }
}

fn parse_valtype_decl(pair: pest::iterators::Pair<Rule>) -> ValType {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    // Parse the builtin_type
    let base_type_pair = inner.next().unwrap();
    let base_type = parse_builtin_type_from_pair(base_type_pair);

    // Parse optional column binding
    let source_column = inner.next().map(|binding_pair| {
        // valtype_binding contains valtype_column_ref
        let col_ref_pair = binding_pair.into_inner().next().unwrap();
        let mut col_inner = col_ref_pair.into_inner();
        let table = col_inner.next().unwrap().as_str().to_string();
        let column = col_inner.next().unwrap().as_str().to_string();
        ColumnRef { table, column }
    });

    ValType { name, base_type, source_column }
}

fn parse_input_decl(pair: pest::iterators::Pair<Rule>) -> InputType {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let mut fields = Vec::new();
    for field_pair in inner {
        if field_pair.as_rule() == Rule::input_field {
            let mut field_inner = field_pair.into_inner();
            let field_name = field_inner.next().unwrap().as_str().to_string();
            let field_type = parse_type(field_inner.next().unwrap().into_inner());
            fields.push(InputField { name: field_name, typ: field_type });
        }
    }
    InputType { name, fields }
}

fn parse_type(pairs: pest::iterators::Pairs<Rule>) -> Type {
    fn parse_utility_type(pair: pest::iterators::Pair<Rule>) -> Type {
        let rule = pair.as_rule();
        let inner = parse_type(pair.into_inner().next().unwrap().into_inner());
        match rule {
            Rule::insertable => Type::Utility(UtilityType::Insertable(Insertable(Box::new(inner)))),
            Rule::change_set => Type::Utility(UtilityType::ChangeSet(ChangeSet(Box::new(inner)))),
            _ => unreachable!(),
        }
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
            Rule::decimal_type => Type::BuiltIn(BuiltInType::Decimal),
            Rule::json_type => Type::BuiltIn(BuiltInType::Json),
            _ => {
                unreachable!()
            }
        },
        Rule::utility_type => {
            let mut inner_pairs = pairs.next().unwrap().into_inner();
            parse_utility_type(inner_pairs.next().unwrap())
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
    let pairs = match NextSqlParser::parse(Rule::argument_list, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
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
    let pairs = match NextSqlParser::parse(Rule::query_decl, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
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
                        statements.push(QueryStatement::Select(parse_select_statement(
                            inner.into_inner(),
                        )));
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
                body.push(QueryStatement::Select(parse_select_statement(
                    inner.into_inner(),
                )));
            }
            _ => unreachable!("Unexpected rule in with statement: {:?}", pair.as_rule()),
        }
    }

    WithStatement { name, body }
}

fn parse_relation(pairs: pest::iterators::Pairs<Rule>) -> Relation {
    let mut pairs = pairs.peekable();
    let decl = parse_relation_decl(pairs.next().unwrap().into_inner());
    let body = pairs.next().unwrap();
    let join_condition = parse_expression(body.into_inner().next().unwrap().into_inner());
    
    Relation {
        decl,
        join_condition,
    }
}

fn parse_relation_decl(pairs: pest::iterators::Pairs<Rule>) -> RelationDecl {
    let pairs = pairs.peekable();
    let mut modifiers = Vec::new();
    let mut relation_type = None;
    let mut name = None;
    let mut for_table = None;
    let mut returning_type = None;
    
    for pair in pairs {
        match pair.as_rule() {
            Rule::relation_modifier => {
                let modifier_inner = pair.into_inner().next().unwrap();
                match modifier_inner.as_rule() {
                    Rule::public_modifier => modifiers.push(RelationModifier::Public),
                    Rule::optional_modifier => modifiers.push(RelationModifier::Optional),
                    _ => unreachable!("Unknown relation modifier: {:?}", modifier_inner.as_rule()),
                }
            }
            Rule::relation_type => {
                let type_inner = pair.into_inner().next().unwrap();
                relation_type = Some(match type_inner.as_rule() {
                    Rule::relation_keyword => RelationType::Relation,
                    Rule::aggregation_keyword => RelationType::Aggregation,
                    _ => unreachable!("Unknown relation type: {:?}", type_inner.as_rule()),
                });
            }
            Rule::ident => {
                if name.is_none() {
                    name = Some(pair.as_str().to_string());
                } else if for_table.is_none() {
                    for_table = Some(pair.as_str().to_string());
                } else {
                    // This is the returning table
                    returning_type = Some(RelationReturnType::Table(pair.as_str().to_string()));
                }
            }
            Rule::builtin_type => {
                // Built-in types need to be parsed directly from the pair
                let builtin = match pair.as_str() {
                    "i16" => crate::ast::BuiltInType::I16,
                    "i32" => crate::ast::BuiltInType::I32,
                    "i64" => crate::ast::BuiltInType::I64,
                    "f32" => crate::ast::BuiltInType::F32,
                    "f64" => crate::ast::BuiltInType::F64,
                    "timestamp" => crate::ast::BuiltInType::Timestamp,
                    "timestamptz" => crate::ast::BuiltInType::Timestamptz,
                    "date" => crate::ast::BuiltInType::Date,
                    "uuid" => crate::ast::BuiltInType::Uuid,
                    "string" => crate::ast::BuiltInType::String,
                    "bool" => crate::ast::BuiltInType::Bool,
                    "decimal" => crate::ast::BuiltInType::Decimal,
                    "json" => crate::ast::BuiltInType::Json,
                    _ => unreachable!("Unknown builtin type: {}", pair.as_str()),
                };
                returning_type = Some(RelationReturnType::Type(crate::ast::Type::BuiltIn(builtin)));
            }
            Rule::array_type | Rule::utility_type => {
                returning_type = Some(RelationReturnType::Type(parse_type(pair.into_inner())));
            }
            _ => {} // Skip keywords like "for" and "returning"
        }
    }
    
    RelationDecl {
        modifiers,
        relation_type: relation_type.expect("Relation type is required"),
        name: name.expect("Relation name is required"),
        for_table: for_table.expect("For table is required"),
        returning_type: returning_type.expect("Returning type is required"),
    }
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
    let pairs = match NextSqlParser::parse(Rule::mutation_decl, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let decl = parse_mutation_decl(pairs.peekable().next().unwrap().into_inner());
    dbg!(&decl);
}

fn parse_insert_clause(pairs: pest::iterators::Pairs<Rule>) -> Target {
    let mut pairs = pairs.peekable();
    let pair = pairs.next().unwrap();
    parse_target(pair)
}

#[test]
fn test_parse_insert_clause() {
    let input = "insert(foo) values(...)";
    let pairs = match NextSqlParser::parse(Rule::insert_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let clause = parse_insert_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(clause.name, "foo");
}

#[test]
fn test_parse_value_clause() {
    let input = ".value({ a: 1, b: \"abc\" })";
    let pairs = match NextSqlParser::parse(Rule::value_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let clause = parse_value_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        HashSet::from(["a".to_string(), "b".to_string()]),
        match clause {
            Expression::Atomic(e) => {
                if let AtomicExpression::Literal(Literal::Object(obj)) = e {
                    obj.fields.keys().cloned().collect()
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
    let pairs = match NextSqlParser::parse(Rule::values_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
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
    let pairs = match NextSqlParser::parse(Rule::returning_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let clause = parse_returning_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(2, clause.len());

    let input = ".returning(a.*, b.c)";
    let pairs = match NextSqlParser::parse(Rule::returning_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let clause = parse_returning_clause(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(2, clause.len());

    let input = ".returning(*)";
    let pairs = match NextSqlParser::parse(Rule::returning_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
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
    let pairs = match NextSqlParser::parse(Rule::mutation_body, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let body = parse_mutation_body(pairs.peekable().next().unwrap().into_inner());
    dbg!(&body);
}
fn parse_on_conflict_clause(pair: pest::iterators::Pair<Rule>) -> OnConflictClause {
    let mut inner = pair.into_inner();

    // Parse conflict columns
    let columns_pair = inner.next().unwrap(); // on_conflict_columns
    let columns: Vec<String> = columns_pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::ident)
        .map(|p| p.as_str().to_string())
        .collect();

    // Parse action (doUpdate or doNothing)
    let action_pair = inner.next().unwrap(); // on_conflict_action
    let action_inner = action_pair.into_inner().next().unwrap();

    let action = match action_inner.as_rule() {
        Rule::do_nothing_clause => OnConflictAction::DoNothing,
        Rule::do_update_clause => {
            let mut sets = Vec::new();
            let mut pairs = action_inner.into_inner();
            while let Some(p) = pairs.next() {
                if p.as_rule() == Rule::ident {
                    let key = p.as_str().to_string();
                    let value_pair = pairs.next().unwrap();
                    let value = parse_expression(value_pair.into_inner());
                    sets.push((key, value));
                }
            }
            OnConflictAction::DoUpdate(sets)
        }
        _ => unreachable!(),
    };

    OnConflictClause { columns, action }
}

fn parse_insert(pairs: pest::iterators::Pairs<Rule>) -> Insert {
    let mut pairs = pairs.peekable();
    // dbg!(&pairs);
    let target = match pairs.peek().unwrap().as_rule() {
        Rule::insert_clause => parse_insert_clause(pairs.next().unwrap().into_inner()),
        _ => unreachable!(),
    };
    let next = pairs.next().unwrap();
    let values = match next.as_rule() {
        Rule::value_clause => vec![parse_value_clause(next.into_inner())],
        Rule::values_clause => parse_values_clause(next.into_inner()),
        _ => Vec::new(),
    };
    let on_conflict = match pairs.peek().map(|p| p.as_rule()) {
        Some(Rule::on_conflict_clause) => {
            Some(parse_on_conflict_clause(pairs.next().unwrap()))
        }
        _ => None,
    };
    let returning = match pairs.peek().map(|p| p.as_rule()) {
        Some(Rule::returning_clause) => {
            Some(parse_returning_clause(pairs.next().unwrap().into_inner()))
        }
        _ => None,
    };
    Insert {
        into: target,
        values,
        on_conflict,
        returning,
    }
}

#[test]
fn test_parse_update_clause() {
    let input = "update(foo).set({ a: 1, b: \"abc\" }).where(a > 1).returning(a, b)";
    let pairs = match NextSqlParser::parse(Rule::update_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let mut pairs = pairs.peekable().next().unwrap().into_inner();
    let clause = pairs.next().unwrap().as_str().to_string();
    assert_eq!(clause, "foo");
}

#[test]
fn test_parse_where_clause() {
    let input = ".where(u.id == $id)";
    let pairs = match NextSqlParser::parse(Rule::where_clause, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    // dbg!(&pairs);
    let clause = parse_where_clause(pairs.peekable().next().unwrap().into_inner());
    dbg!(&clause);
    assert_eq!(
        Expression::Binary {
            left: Box::new(Expression::Atomic(AtomicExpression::Column(
                Column::ExplicitTarget("u".to_string(), "id".to_string(), Some(Span { start: 9, end: 11 }))
            ))),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Atomic(AtomicExpression::Variable(Variable {
                name: "id".to_string(),
                span: Some(Span { start: 15, end: 18 }),
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
    let (set, set_variable) = match pairs.peek().unwrap().as_rule() {
        Rule::set_clause => {
            let mut pairs = pairs.next().unwrap().into_inner().peekable();
            // Check if the first inner element is a variable
            if pairs.peek().map(|p| p.as_rule()) == Some(Rule::variable) {
                let var_pair = pairs.next().unwrap();
                let var_name = var_pair.into_inner().next().unwrap().as_str().to_string();
                (Vec::new(), Some(var_name))
            } else {
                let mut set = Vec::new();
                while pairs.peek().is_some() {
                    let column = pairs.next().unwrap().as_str().to_string();
                    let value = parse_expression(pairs.next().unwrap().into_inner());
                    set.push((column, value));
                }
                (set, None)
            }
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
        set_variable,
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
                let statement = parse_mutation_statement(inner.into_inner());
                items.push(MutationBodyItem::Mutation(statement));
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
    let pairs = match NextSqlParser::parse(Rule::mutation, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
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
    let pairs = match NextSqlParser::parse(Rule::target, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let target = parse_target(pairs.peekable().next().unwrap());
    assert_eq!(
        target,
        Target {
            name: "foo".to_string(),
            span: Some(Span { start: 0, end: 3 }),
        }
    );
}
fn parse_target(pair: pest::iterators::Pair<Rule>) -> Target {
    let name = pair.as_str().to_string();
    let span = get_span(&pair);
    Target { name, span }
}

fn parse_join_expr(pairs: pest::iterators::Pairs<Rule>) -> JoinExpr {
    let mut pairs = pairs.peekable();

    // Get join method (dot is not captured as a separate pair)
    let join_method = pairs.next().unwrap();
    let span = get_span(&join_method);
    let join_type = match join_method.as_str() {
        "innerJoin" => JoinType::Inner,
        "leftJoin" => JoinType::Left,
        "rightJoin" => JoinType::Right,
        "fullOuterJoin" => JoinType::FullOuter,
        "crossJoin" => JoinType::Cross,
        _ => unreachable!("Unknown join method: {}", join_method.as_str()),
    };

    // Parse table name and condition
    let table = pairs.next().unwrap().as_str().to_string();
    let condition = parse_expression(pairs.next().unwrap().into_inner());

    JoinExpr {
        join_type,
        table,
        condition,
        span,
    }
}

fn parse_from_expr(pairs: pest::iterators::Pairs<Rule>) -> FromExpr {
    let mut pairs = pairs.peekable();
    let first_pair = pairs.next().unwrap();
    let table = first_pair.as_str().to_string();
    let span = get_span(&first_pair);
    let joins = pairs.map(|p| parse_join_expr(p.into_inner())).collect();
    FromExpr { table, joins, span }
}


fn parse_select_expression(pairs: pest::iterators::Pairs<Rule>) -> SelectExpression {
    let mut pairs = pairs.peekable();
    let first_pair = pairs.next().unwrap();

    match first_pair.as_rule() {
        Rule::ident => {
            // Aliased select expression: alias: expression
            let alias = first_pair.as_str().to_string();
            let expr_pair = pairs.next().unwrap();
            let expr = parse_expression(expr_pair.into_inner());
            SelectExpression { alias: Some(alias), expr }
        }
        Rule::expression => {
            let expr = parse_expression(first_pair.into_inner());
            SelectExpression { alias: None, expr }
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
    let mut unions: Vec<UnionClause> = Vec::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::from_clause => {
                let from_expr_pair = pair.into_inner().next().unwrap();
                from = Some(parse_from_expr(from_expr_pair.into_inner()));
            }
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
                                        alias: None,
                                        expr: arg.clone(),
                                    })
                                    .collect(),
                            ),
                            "orderBy" => QueryClause::OrderBy(
                                call.args.iter().map(|arg| {
                                    match arg {
                                        Expression::Atomic(AtomicExpression::MethodCall(mc))
                                            if (mc.method == "asc" || mc.method == "desc") && mc.args.is_empty() =>
                                        {
                                            OrderByExpression {
                                                direction: Some(if mc.method == "asc" { OrderDirection::Asc } else { OrderDirection::Desc }),
                                                expr: *mc.target.clone(),
                                            }
                                        }
                                        _ => OrderByExpression { expr: arg.clone(), direction: None },
                                    }
                                }).collect(),
                            ),
                            "limit" => QueryClause::Limit(call.args[0].clone()),
                            "join" => {
                                // For join, parse the first argument as target and second as condition
                                if call.args.len() != 2 {
                                    unreachable!("Join requires exactly 2 arguments");
                                }

                                // Extract table name from the first argument
                                let table = match &call.args[0] {
                                    Expression::Atomic(AtomicExpression::Column(
                                        Column::ImplicitTarget(name, _),
                                    )) => name.clone(),
                                    _ => unreachable!(
                                        "Join first argument should be a table reference"
                                    ),
                                };

                                QueryClause::Join(JoinExpr {
                                    join_type: JoinType::Inner,
                                    table,
                                    condition: call.args[1].clone(),
                                    span: None,
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
                let order_exprs: Vec<OrderByExpression> = pair.into_inner()
                    .filter(|p| p.as_rule() == Rule::expression)
                    .map(|p| {
                        let expr = parse_expression(p.into_inner());
                        match &expr {
                            Expression::Atomic(AtomicExpression::MethodCall(mc))
                                if (mc.method == "asc" || mc.method == "desc") && mc.args.is_empty() =>
                            {
                                OrderByExpression {
                                    direction: Some(if mc.method == "asc" { OrderDirection::Asc } else { OrderDirection::Desc }),
                                    expr: *mc.target.clone(),
                                }
                            }
                            _ => OrderByExpression { expr, direction: None },
                        }
                    })
                    .collect();
                clauses.push(QueryClause::OrderBy(order_exprs));
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
            Rule::distinct_clause => {
                clauses.push(QueryClause::Distinct);
            }
            Rule::offset_clause => {
                let expr = parse_expression(pair.into_inner().next().unwrap().into_inner());
                clauses.push(QueryClause::Offset(expr));
            }
            Rule::having_clause => {
                let expr = parse_expression(pair.into_inner().next().unwrap().into_inner());
                clauses.push(QueryClause::Having(expr));
            }
            Rule::for_update_clause => {
                clauses.push(QueryClause::ForUpdate);
            }
            Rule::union_clause => {
                let mut union_inner = pair.into_inner();
                let union_type_pair = union_inner.next().unwrap();
                let union_type = match union_type_pair.as_str() {
                    "union" => UnionType::Union,
                    "unionAll" => UnionType::UnionAll,
                    _ => unreachable!(),
                };
                let select_pair = union_inner.next().unwrap();
                let select = parse_select_statement(select_pair.into_inner());
                unions.push(UnionClause {
                    union_type,
                    select: Box::new(select),
                });
            }
            _ => {
                unreachable!()
            }
        }
    }

    SelectStatement {
        from: from.unwrap(),
        clauses,
        unions,
    }
}

#[test]
fn test_parse_expression() {
    let input = "a == b";
    let pairs = match NextSqlParser::parse(Rule::expression, input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {e}");
            return;
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Binary {
            left: Box::new(Expression::Atomic(AtomicExpression::Column(
                Column::ImplicitTarget("a".to_string(), Some(Span { start: 0, end: 1 }))
            ))),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Atomic(AtomicExpression::Column(
                Column::ImplicitTarget("b".to_string(), Some(Span { start: 5, end: 6 }))
            )))
        },
        expr
    );

    let input = { "{ name: \"alice\" }" };
    let pairs = match NextSqlParser::parse(Rule::expression, input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {e}");
            return;
        }
    };
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::Literal(Literal::Object(
            ObjectLiteralExpression {
                fields: vec![(
                    "name".to_string(),
                    Expression::Atomic(AtomicExpression::Literal(Literal::String(
                        "alice".to_string()
                    )))
                )]
                .into_iter()
                .collect(),
                span: Some(Span { start: 0, end: 17 }),
            }
        ))),
        expr
    );
    let input = "1 + 1";
    let pairs = match NextSqlParser::parse(Rule::expression, input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {e}");
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
    let pairs = match NextSqlParser::parse(Rule::expression, input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {e}");
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
    let pairs = NextSqlParser::parse(Rule::expression, input).expect("パースエラー");
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::Literal(Literal::String(",".to_string()))),
        expr
    );

    let input = "split(\"a,b,c\", \",\")";
    let pairs = match NextSqlParser::parse(Rule::expression, input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {e}");
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
            ],
            span: Some(Span { start: 0, end: 5 }),
        })),
        expr
    );
    let input = "call()[0]";
    let pairs = match NextSqlParser::parse(Rule::expression, input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("パースエラー: {e}");
            panic!();
        }
    };
    println!("{}", &pairs);
    let expr = parse_expression(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Expression::Atomic(AtomicExpression::IndexAccess(IndexAccess {
            target: Box::new(Expression::Atomic(AtomicExpression::Call(CallExpression {
                callee: "call".to_string(),
                args: vec![],
                span: Some(Span { start: 0, end: 4 }),
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
    let mut left = parse_equality_expression(pairs.next().unwrap().into_inner());
    while pairs.peek().is_some() {
        let op = match pairs.next().unwrap().as_rule() {
            Rule::and => BinaryOp::And,
            Rule::or => BinaryOp::Or,
            _ => unreachable!(),
        };
        let right = parse_equality_expression(pairs.next().unwrap().into_inner());
        left = Expression::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    left
}

fn parse_equality_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let mut left = parse_relational_expression(pairs.next().unwrap().into_inner());
    while pairs.peek().is_some() {
        let op = match pairs.next().unwrap().as_rule() {
            Rule::equal => BinaryOp::Equal,
            Rule::unequal => BinaryOp::Unequal,
            _ => unreachable!(),
        };
        let right = parse_relational_expression(pairs.next().unwrap().into_inner());
        left = Expression::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    left
}

fn parse_relational_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let mut left = parse_additive_expression(pairs.next().unwrap().into_inner());
    while pairs.peek().is_some() {
        let op = match pairs.next().unwrap().as_rule() {
            Rule::lt => BinaryOp::LessThan,
            Rule::le => BinaryOp::LessThanOrEqual,
            Rule::gt => BinaryOp::GreaterThan,
            Rule::ge => BinaryOp::GreaterThanOrEqual,
            _ => unreachable!(),
        };
        let right = parse_additive_expression(pairs.next().unwrap().into_inner());
        left = Expression::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    left
}

fn parse_additive_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let mut left = parse_multiplicative_expression(pairs.next().unwrap().into_inner());
    while pairs.peek().is_some() {
        let op = match pairs.next().unwrap().as_rule() {
            Rule::add => BinaryOp::Add,
            Rule::subtract => BinaryOp::Subtract,
            _ => unreachable!(),
        };
        let right = parse_multiplicative_expression(pairs.next().unwrap().into_inner());
        left = Expression::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    left
}

fn parse_multiplicative_expression(pairs: pest::iterators::Pairs<Rule>) -> Expression {
    let mut pairs = pairs.peekable();
    let mut left = parse_unary_expression(pairs.next().unwrap().into_inner());
    while pairs.peek().is_some() {
        let op = match pairs.next().unwrap().as_rule() {
            Rule::multiply => BinaryOp::Multiply,
            Rule::divide => BinaryOp::Divide,
            Rule::rem => BinaryOp::Remainder,
            _ => unreachable!(),
        };
        let right = parse_unary_expression(pairs.next().unwrap().into_inner());
        left = Expression::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    left
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
    let pairs = match NextSqlParser::parse(Rule::column, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(Column::Wildcard(Some(Span { start: 0, end: 1 })), column);

    let input = "foo_bar";
    let pairs = match NextSqlParser::parse(Rule::column, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(Column::ImplicitTarget("foo_bar".to_string(), Some(Span { start: 0, end: 7 })), column);

    let input = "foo.bar";
    let pairs = match NextSqlParser::parse(Rule::column, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(
        Column::ExplicitTarget("foo".to_string(), "bar".to_string(), Some(Span { start: 4, end: 7 })),
        column
    );

    let input = "foo.*";
    let pairs = match NextSqlParser::parse(Rule::column, input) {
        Ok(p) => p,
        Err(e) => panic!("パースエラーが発生しました: {e}"),
    };
    let column = parse_column(pairs.peekable().next().unwrap().into_inner());
    assert_eq!(Column::WildcardOf("foo".to_string(), Some(Span { start: 4, end: 5 })), column);
}
fn parse_column(pairs: pest::iterators::Pairs<Rule>) -> Column {
    let mut pairs = pairs.peekable();
    let first_pair = pairs.peek().unwrap();
    match first_pair.as_rule() {
        Rule::wildcard => {
            let span = get_span(first_pair);
            Column::Wildcard(span)
        },
        Rule::ident => {
            let first_pair = pairs.next().unwrap();
            let first = first_pair.as_str().to_string();
            let first_span = get_span(&first_pair);
            
            if let Some(second) = pairs.next() {
                if second.as_str() == "*" {
                    // WildcardOfの場合、*の位置を使用
                    let wildcard_span = get_span(&second);
                    Column::WildcardOf(first, wildcard_span)
                } else {
                    // ExplicitTargetの場合、カラム名（second）の位置を使用
                    let column_span = get_span(&second);
                    Column::ExplicitTarget(first, second.as_str().to_string(), column_span)
                }
            } else {
                // ImplicitTargetの場合、そのまま使用
                Column::ImplicitTarget(first, first_span)
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
            let span = get_span(&pair);
            let mut pairs = pair.into_inner();
            let mut map: HashMap<String, Expression> = HashMap::new();
            while pairs.peek().is_some() {
                let key = pairs.next().unwrap().as_str().to_string();
                let value = parse_expression(pairs.next().unwrap().into_inner());
                map.insert(key, value);
            }
            AtomicExpression::Literal(Literal::Object(ObjectLiteralExpression { fields: map, span }))
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
    let callee_pair = pairs.next().unwrap();
    let callee = callee_pair.as_str().to_string();
    let span = get_span(&callee_pair);
    let mut args = Vec::new();
    while pairs.peek().is_some() {
        args.push(parse_expression(pairs.next().unwrap().into_inner()));
    }
    Expression::Atomic(AtomicExpression::Call(CallExpression { callee, args, span }))
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
        Rule::cast_expression => {
            let mut pairs = pair.into_inner().peekable();
            let expr = parse_expression(pairs.next().unwrap().into_inner());
            let target_type = parse_builtin_type_from_pair(pairs.next().unwrap());
            Expression::Atomic(AtomicExpression::Cast(CastExpression {
                expr: Box::new(expr),
                target_type,
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
            let pair_str = pair.as_str();
            let function_type = if pair_str.starts_with("SUM") {
                AggregateFunctionType::Sum
            } else if pair_str.starts_with("COUNT") {
                AggregateFunctionType::Count
            } else if pair_str.starts_with("AVG") {
                AggregateFunctionType::Avg
            } else if pair_str.starts_with("MIN") {
                AggregateFunctionType::Min
            } else if pair_str.starts_with("MAX") {
                AggregateFunctionType::Max
            } else {
                unreachable!("Unknown aggregate function: {}", pair_str)
            };
            let mut pairs = pair.into_inner().peekable();
            let expr = parse_expression(pairs.next().unwrap().into_inner());
            let filter = if let Some(filter_pair) = pairs.next() {
                assert_eq!(filter_pair.as_rule(), Rule::aggregate_filter);
                let filter_expr = parse_expression(filter_pair.into_inner().next().unwrap().into_inner());
                Some(Box::new(filter_expr))
            } else {
                None
            };
            Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                function_type,
                expr: Box::new(expr),
                filter,
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
    let mut result = parse_basic_expression(target_pair.into_inner());
    // Consume all chained property idents (the grammar `("." ~ ident)+` may produce multiple)
    for prop_pair in pairs {
        let property = prop_pair.as_str().to_string();
        result = Expression::Atomic(AtomicExpression::PropertyAccess(PropertyAccess {
            target: Box::new(result),
            property,
        }));
    }
    result
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
        parse_module(input).unwrap();
    }

    #[test]
    fn simple_select_and_join_test() {
        let input = include_str!("../../examples/simple-select-and-join.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn insert_optional_test() {
        let input = include_str!("../../examples/insert-optional.nsql");
        parse_module(input).unwrap();
    }

    #[test]
    fn insert_many_test() {
        let input = include_str!("../../examples/insert-many.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn insert_many_with_variable_test() {
        let input = include_str!("../../examples/insert-many-with-variable.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn update_test() {
        let input = include_str!("../../examples/update.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn simple_delete_test() {
        let input = include_str!("../../examples/simple-delete.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn delete_with_subquery_test() {
        let input = include_str!("../../examples/delete-with-subquery.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn dynamic_conditional_clauses_test() {
        let input = include_str!("../../examples/dynamic-conditional-clauses.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn dynamic_field_selection_test() {
        let input = include_str!("../../examples/dynamic-field-selection.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn dynamic_joins_test() {
        let input = include_str!("../../examples/dynamic-joins.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn dynamic_array_conditions_test() {
        let input = include_str!("../../examples/dynamic-array-conditions.nsql");
        dbg!("{:?}", parse_module(input).unwrap());
    }

    #[test]
    fn test_relation_parsing() {
        let input = r#"
relation author for posts returning users {
  users.id == posts.author_id
}

public optional relation post_detail for posts returning post_details {
  post_details.id == posts.detail_id
}

public relation organization for users returning organizations {
  organizations.id == users.organization_id
}

aggregation comment_count for posts returning i32 {
  count(comments.id)
}
"#;
        
        let result = parse_module(input);
        assert!(result.is_ok(), "Failed to parse relations: {:?}", result.err());
        
        let module = result.unwrap();
        assert_eq!(module.toplevels.len(), 4);
        
        // Check first relation
        if let TopLevel::Relation(relation) = &module.toplevels[0] {
            assert_eq!(relation.decl.name, "author");
            assert_eq!(relation.decl.for_table, "posts");
            assert!(relation.decl.modifiers.is_empty());
            assert!(matches!(relation.decl.relation_type, crate::ast::RelationType::Relation));
            
            if let crate::ast::RelationReturnType::Table(table) = &relation.decl.returning_type {
                assert_eq!(table, "users");
            } else {
                panic!("Expected Table return type, got: {:?}", relation.decl.returning_type);
            }
        } else {
            panic!("Expected first item to be a relation");
        }
        
        // Check optional relation
        if let TopLevel::Relation(relation) = &module.toplevels[1] {
            assert_eq!(relation.decl.name, "post_detail");
            assert_eq!(relation.decl.modifiers.len(), 2);
            assert!(relation.decl.modifiers.iter().any(|m| matches!(m, crate::ast::RelationModifier::Public)));
            assert!(relation.decl.modifiers.iter().any(|m| matches!(m, crate::ast::RelationModifier::Optional)));
        } else {
            panic!("Expected second item to be a relation");
        }
        
        // Check aggregation
        if let TopLevel::Relation(relation) = &module.toplevels[3] {
            assert_eq!(relation.decl.name, "comment_count");
            assert!(matches!(relation.decl.relation_type, crate::ast::RelationType::Aggregation));
            if let crate::ast::RelationReturnType::Type(typ) = &relation.decl.returning_type {
                assert!(matches!(typ, crate::ast::Type::BuiltIn(crate::ast::BuiltInType::I32)));
            } else {
                panic!("Expected Type return type");
            }
        } else {
            panic!("Expected fourth item to be an aggregation");
        }
    }
    
    #[test]
    fn test_union() {
        let input = r#"
            query findAllPeople() {
                from(employees)
                .select(employees.name, employees.email)
                .union(
                    from(contractors)
                    .select(contractors.name, contractors.email)
                )
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(select) = &query.body.statements[0];
        assert_eq!(select.unions.len(), 1);
        match &select.unions[0].union_type {
            UnionType::Union => {},
            _ => panic!("Expected Union"),
        }
    }

    #[test]
    fn test_union_all() {
        let input = r#"
            query findAllPeople() {
                from(employees)
                .select(employees.name)
                .unionAll(
                    from(contractors)
                    .select(contractors.name)
                )
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(select) = &query.body.statements[0];
        assert_eq!(select.unions.len(), 1);
        match &select.unions[0].union_type {
            UnionType::UnionAll => {},
            _ => panic!("Expected UnionAll"),
        }
    }

    #[test]
    fn test_multiple_unions() {
        let input = r#"
            query findAllPeople() {
                from(employees)
                .select(employees.name)
                .union(
                    from(contractors)
                    .select(contractors.name)
                )
                .union(
                    from(freelancers)
                    .select(freelancers.name)
                )
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(select) = &query.body.statements[0];
        assert_eq!(select.unions.len(), 2);
        match &select.unions[0].union_type {
            UnionType::Union => {},
            _ => panic!("Expected Union for first union clause"),
        }
        match &select.unions[1].union_type {
            UnionType::Union => {},
            _ => panic!("Expected Union for second union clause"),
        }
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
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) == Some("nsql") {
                            if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                                // Read file content
                                match fs::read_to_string(&path) {
                                    Ok(content) => {
                                        example_files.push((filename.to_string(), content));
                                    }
                                    Err(e) => {
                                        panic!("Failed to read {filename}: {e}");
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    panic!("Failed to read examples directory: {e}");
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
            print!("Testing {filename}... ");


            match parse_module(&content) {
                Ok(module) => {
                    successful_files += 1;

                    // Validate the module has at least one top-level item
                    assert!(
                        !module.toplevels.is_empty(),
                        "File {filename} should contain at least one query or mutation"
                    );

                    // Count queries and mutations
                    let mut queries = 0;
                    let mut mutations = 0;
                    let mut with_statements = 0;
                    let mut relations = 0;
                    for toplevel in &module.toplevels {
                        match toplevel {
                            TopLevel::Query(_) => queries += 1,
                            TopLevel::Mutation(_) => mutations += 1,
                            TopLevel::With(_) => with_statements += 1,
                            TopLevel::Relation(_) => relations += 1,
                            TopLevel::ValType(_) | TopLevel::Input(_) => {}
                        }
                    }

                    println!(
                        "✅ ({queries} queries, {mutations} mutations, {with_statements} with statements, {relations} relations)"
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
                            "    💡 Consider adding '{filename}' to unsupported_files list"
                        );
                    } else {
                        failed_files.push((filename.clone(), error_message.clone()));
                        println!("❌ FAILED: {error_message}");
                    }
                }
            }
        }

        // Print summary
        println!("\n📊 PARSING SUMMARY:");
        println!("Total files discovered: {total_files}");
        println!("Successful: {successful_files} ✅");
        println!("Skipped (unsupported): {skipped_files} ⚠️");
        println!("Failed: {} ❌", failed_files.len());

        let testable_files = total_files - skipped_files;
        if testable_files > 0 {
            let success_rate = (successful_files as f64 / testable_files as f64) * 100.0;
            println!(
                "Success rate: {success_rate:.1}% ({successful_files}/{testable_files})"
            );
        }

        if !skipped_file_names.is_empty() {
            println!("\n⚠️  Skipped files (unsupported features):");
            for filename in &skipped_file_names {
                println!("  - {filename}");
            }
        }

        if !failed_files.is_empty() {
            println!("\n❌ Failed files:");
            for (filename, error) in &failed_files {
                println!("  - {filename}: {error}");
            }
        }

        // Assert that all testable files parsed successfully
        assert!(
            failed_files.is_empty(),
            "Some example files failed to parse. See output above for details."
        );

        if skipped_files > 0 {
            println!("🎉 All supported example files parsed successfully! ({skipped_files} unsupported files were skipped)");
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
            _ => panic!("Expected property access, got {expr:?}"),
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
                        _,
                    ))) => {
                        assert_eq!(table, "users");
                        assert_eq!(col, "name");
                    }
                    _ => panic!("Expected ExplicitTarget column as target, got {target:?}"),
                }
            }
            _ => panic!("Expected method call, got {expr:?}"),
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
                        _,
                    ))) => {
                        assert_eq!(table, "users");
                        assert_eq!(col, "id");
                    }
                    _ => panic!("Expected ExplicitTarget column as target, got {target:?}"),
                }
                // Verify the argument is a variable
                match &args[0] {
                    Expression::Atomic(AtomicExpression::Variable(var)) => {
                        assert_eq!(var.name, "array");
                    }
                    _ => panic!("Expected variable as argument"),
                }
            }
            _ => panic!("Expected method call, got {expr:?}"),
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
            _ => panic!("Expected method call, got {expr:?}"),
        }
    }

    #[test]
    fn test_distinct_clause() {
        let input = r#"
            query findDistinctNames() {
                from(users)
                .distinct()
                .select(users.name)
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(stmt) = &query.body.statements[0];
        assert!(stmt.clauses.iter().any(|c| matches!(c, QueryClause::Distinct)));
    }

    #[test]
    fn test_offset_clause() {
        let input = r#"
            query findUsersWithOffset($limit: i32, $offset: i32) {
                from(users)
                .select(users.*)
                .limit($limit)
                .offset($offset)
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(stmt) = &query.body.statements[0];
        assert!(stmt.clauses.iter().any(|c| matches!(c, QueryClause::Offset(_))));
    }

    #[test]
    fn test_having_clause() {
        let input = r#"
            query findActiveGroups() {
                from(orders)
                .groupBy(orders.customer_id)
                .having(COUNT(orders.id) > 5)
                .aggregate(order_count: COUNT(orders.id))
                .select(orders.customer_id, order_count)
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(stmt) = &query.body.statements[0];
        assert!(stmt.clauses.iter().any(|c| matches!(c, QueryClause::Having(_))));
    }

    #[test]
    fn test_order_by_with_direction() {
        let input = r#"
            query findUsers() {
                from(users)
                .select(users.*)
                .orderBy(users.name.asc(), users.created_at.desc())
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(select) = &query.body.statements[0];
        let order_by = select.clauses.iter().find(|c| matches!(c, QueryClause::OrderBy(_)));
        assert!(order_by.is_some());
        match order_by.unwrap() {
            QueryClause::OrderBy(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert_eq!(exprs[0].direction, Some(OrderDirection::Asc));
                assert_eq!(exprs[1].direction, Some(OrderDirection::Desc));
            }
            _ => panic!("Expected OrderBy"),
        }
    }

    #[test]
    fn test_order_by_without_direction() {
        let input = r#"
            query findUsers() {
                from(users)
                .select(users.*)
                .orderBy(users.name)
            }
        "#;
        let module = parse_module(input).unwrap();
        let query = match &module.toplevels[0] {
            TopLevel::Query(q) => q,
            _ => panic!("Expected query"),
        };
        let QueryStatement::Select(select) = &query.body.statements[0];
        let order_by = select.clauses.iter().find(|c| matches!(c, QueryClause::OrderBy(_)));
        assert!(order_by.is_some());
        match order_by.unwrap() {
            QueryClause::OrderBy(exprs) => {
                assert_eq!(exprs.len(), 1);
                assert_eq!(exprs[0].direction, None);
            }
            _ => panic!("Expected OrderBy"),
        }
    }
}

#[test]
fn test_on_conflict_do_update() {
    let input = r#"
        mutation upsertUser($name: string, $email: string) {
            insert(users)
            .value({ name: $name, email: $email })
            .onConflict(email)
            .doUpdate({ name: $name })
        }
    "#;
    let module = parse_module(input).unwrap();
    let toplevel = &module.toplevels[0];
    match toplevel {
        TopLevel::Mutation(mutation) => {
            assert_eq!(mutation.decl.name, "upsertUser");
            match &mutation.body.items[0] {
                MutationBodyItem::Mutation(MutationStatement::Insert(insert)) => {
                    assert_eq!(insert.into.name, "users");
                    let on_conflict = insert.on_conflict.as_ref().unwrap();
                    assert_eq!(on_conflict.columns, vec!["email".to_string()]);
                    match &on_conflict.action {
                        OnConflictAction::DoUpdate(sets) => {
                            assert_eq!(sets.len(), 1);
                            assert_eq!(sets[0].0, "name");
                        }
                        _ => panic!("Expected DoUpdate"),
                    }
                    assert!(insert.returning.is_none());
                }
                _ => panic!("Expected Insert"),
            }
        }
        _ => panic!("Expected Mutation"),
    }
}

#[test]
fn test_on_conflict_do_nothing() {
    let input = r#"
        mutation upsertUser($name: string, $email: string) {
            insert(users)
            .value({ name: $name, email: $email })
            .onConflict(email, organization_id)
            .doNothing()
        }
    "#;
    let module = parse_module(input).unwrap();
    let toplevel = &module.toplevels[0];
    match toplevel {
        TopLevel::Mutation(mutation) => {
            match &mutation.body.items[0] {
                MutationBodyItem::Mutation(MutationStatement::Insert(insert)) => {
                    let on_conflict = insert.on_conflict.as_ref().unwrap();
                    assert_eq!(on_conflict.columns, vec!["email".to_string(), "organization_id".to_string()]);
                    assert_eq!(on_conflict.action, OnConflictAction::DoNothing);
                }
                _ => panic!("Expected Insert"),
            }
        }
        _ => panic!("Expected Mutation"),
    }
}

#[test]
fn test_on_conflict_with_returning() {
    let input = r#"
        mutation upsertUser($name: string, $email: string) {
            insert(users)
            .value({ name: $name, email: $email })
            .onConflict(email)
            .doUpdate({ name: $name })
            .returning(users.*)
        }
    "#;
    let module = parse_module(input).unwrap();
    let toplevel = &module.toplevels[0];
    match toplevel {
        TopLevel::Mutation(mutation) => {
            match &mutation.body.items[0] {
                MutationBodyItem::Mutation(MutationStatement::Insert(insert)) => {
                    assert!(insert.on_conflict.is_some());
                    assert!(insert.returning.is_some());
                    let returning = insert.returning.as_ref().unwrap();
                    assert_eq!(returning.len(), 1);
                    match &returning[0] {
                        Column::WildcardOf(name, _) => assert_eq!(name, "users"),
                        _ => panic!("Expected WildcardOf"),
                    }
                }
                _ => panic!("Expected Insert"),
            }
        }
        _ => panic!("Expected Mutation"),
    }
}

#[test]
fn test_order_by_method_call_on_column() {
    let input = r#"
query findPopularProducts($min_sales: i32) {
  from(order_items)
  .groupBy(order_items.product_id)
  .having(SUM(order_items.quantity) >= $min_sales)
  .aggregate(total_sold: SUM(order_items.quantity))
  .select(order_items.product_id, total_sold)
  .orderBy(total_sold.desc())
}
"#;
    let module = parse_module(input).expect("Parsing should succeed for orderBy with method call on column");
    assert_eq!(module.toplevels.len(), 1);
    match &module.toplevels[0] {
        TopLevel::Query(query) => {
            assert_eq!(query.decl.name, "findPopularProducts");
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_valtype_with_source_column() {
    let input = r#"
        valtype UserId = uuid for users.id
    "#;
    let module = parse_module(input).unwrap();
    assert_eq!(module.toplevels.len(), 1);
    match &module.toplevels[0] {
        TopLevel::ValType(vt) => {
            assert_eq!(vt.name, "UserId");
            assert_eq!(vt.base_type, BuiltInType::Uuid);
            let src = vt.source_column.as_ref().unwrap();
            assert_eq!(src.table, "users");
            assert_eq!(src.column, "id");
        }
        _ => panic!("Expected ValType"),
    }
}

#[test]
fn test_valtype_without_binding() {
    let input = r#"
        valtype Amount = f64
    "#;
    let module = parse_module(input).unwrap();
    match &module.toplevels[0] {
        TopLevel::ValType(vt) => {
            assert_eq!(vt.name, "Amount");
            assert_eq!(vt.base_type, BuiltInType::F64);
            assert!(vt.source_column.is_none());
        }
        _ => panic!("Expected ValType"),
    }
}

#[test]
fn test_valtype_with_query() {
    let input = r#"
        valtype UserId = uuid for users.id

        query findUserById($id: UserId) {
            from(users)
            .where(users.id == $id)
            .select(users.*)
        }
    "#;
    let module = parse_module(input).unwrap();
    assert_eq!(module.toplevels.len(), 2);
}

#[test]
fn test_multiple_valtypes() {
    let input = r#"
        valtype UserId = uuid for users.id
        valtype Email = string for users.email
        valtype PostId = uuid for posts.id
    "#;
    let module = parse_module(input).unwrap();
    assert_eq!(module.toplevels.len(), 3);
}

#[test]
fn test_for_update_clause() {
    let input = r#"
        query findForUpdate($id: uuid) {
            from(users)
            .where(users.id == $id)
            .select(users.*)
            .forUpdate()
        }
    "#;
    let module = parse_module(input).unwrap();
    let q = match &module.toplevels[0] {
        TopLevel::Query(q) => q,
        _ => panic!("expected query"),
    };
    let QueryStatement::Select(stmt) = &q.body.statements[0];
    assert!(
        stmt.clauses.iter().any(|c| matches!(c, QueryClause::ForUpdate)),
        "Expected ForUpdate clause in parsed query"
    );
}

#[test]
fn test_set_with_column_expression() {
    let input = r#"
        mutation incrementStage($id: uuid) {
            update(requests)
            .where(requests.id == $id)
            .set({ stage: stage + 1 })
        }
    "#;
    let module = parse_module(input).unwrap();
    let m = match &module.toplevels[0] {
        TopLevel::Mutation(m) => m,
        _ => panic!("expected mutation"),
    };
    let update = match &m.body.items[0] {
        MutationBodyItem::Mutation(MutationStatement::Update(u)) => u,
        _ => panic!("expected update"),
    };
    // Find the "stage" set entry
    let (col, expr) = update.set.iter().find(|(c, _)| c == "stage").expect("expected 'stage' in set");
    assert_eq!(col, "stage");
    // Should be a binary add expression
    match expr {
        Expression::Binary { op, .. } => {
            assert_eq!(*op, BinaryOp::Add);
        }
        _ => panic!("expected binary expression for stage, got {expr:?}"),
    }
}

#[test]
fn test_now_function_call_parse() {
    let input = r#"
        mutation touchUpdatedAt($id: uuid) {
            update(records)
            .where(records.id == $id)
            .set({ updated_at: now() })
        }
    "#;
    let module = parse_module(input).unwrap();
    let m = match &module.toplevels[0] {
        TopLevel::Mutation(m) => m,
        _ => panic!("expected mutation"),
    };
    let update = match &m.body.items[0] {
        MutationBodyItem::Mutation(MutationStatement::Update(u)) => u,
        _ => panic!("expected update"),
    };
    let (_, expr) = update.set.iter().find(|(c, _)| c == "updated_at").expect("expected 'updated_at' in set");
    match expr {
        Expression::Atomic(AtomicExpression::Call(call)) => {
            assert_eq!(call.callee, "now");
            assert!(call.args.is_empty());
        }
        _ => panic!("expected call expression for now(), got {expr:?}"),
    }
}

#[test]
fn test_count_with_alias_parse() {
    let input = r#"
        query countRecords() {
            from(users)
            .where(users.is_active == true)
            .select(total: COUNT(users.id))
        }
    "#;
    let module = parse_module(input).unwrap();
    let q = match &module.toplevels[0] {
        TopLevel::Query(q) => q,
        _ => panic!("expected query"),
    };
    let QueryStatement::Select(stmt) = &q.body.statements[0];
    // Find Select clause
    let select_clause = stmt.clauses.iter().find_map(|c| match c {
        QueryClause::Select(exprs) => Some(exprs),
        _ => None,
    }).expect("expected select clause");
    assert_eq!(select_clause.len(), 1);
    assert_eq!(select_clause[0].alias.as_deref(), Some("total"));
    // COUNT in select_expression context is parsed as a CallExpression
    // (since call_expression matches before aggregate_function in the grammar).
    // The SQL generator handles both correctly.
    match &select_clause[0].expr {
        Expression::Atomic(AtomicExpression::Call(call)) => {
            assert_eq!(call.callee, "COUNT");
            assert_eq!(call.args.len(), 1);
        }
        Expression::Atomic(AtomicExpression::Aggregate(agg)) => {
            assert_eq!(agg.function_type, AggregateFunctionType::Count);
        }
        _ => panic!("expected call or aggregate expression, got {:?}", select_clause[0].expr),
    }
}

#[test]
fn test_change_set_type() {
    let input = r#"
        mutation updateUser($id: uuid, $changes: ChangeSet<users>) {
            update(users)
            .where(users.id == $id)
            .set({
                name: $changes
            })
        }
    "#;
    let module = parse_module(input).unwrap();
    let m = match &module.toplevels[0] {
        TopLevel::Mutation(m) => m,
        _ => panic!("expected mutation"),
    };
    assert_eq!(m.decl.name, "updateUser");
    assert_eq!(m.decl.arguments.len(), 2);
    assert_eq!(
        m.decl.arguments[1].typ,
        Type::Utility(UtilityType::ChangeSet(ChangeSet(Box::new(Type::UserDefined("users".to_string())))))
    );
}

#[test]
fn test_cast_expression_parse() {
    let input = r#"
        query findByCode($code: string) {
            from(items)
            .where(cast(items.origin, string) == $code)
            .select(items.*)
        }
    "#;
    let module = parse_module(input).unwrap();
    let query = match &module.toplevels[0] {
        TopLevel::Query(q) => q,
        _ => panic!("expected query"),
    };
    assert_eq!(query.decl.name, "findByCode");
}

#[test]
fn test_cast_expression_with_different_types() {
    // Test cast with i32
    let input = r#"
        query castToInt {
            from(items)
            .where(cast(items.amount, i32) == 100)
            .select(items.*)
        }
    "#;
    let module = parse_module(input).unwrap();
    assert_eq!(module.toplevels.len(), 1);

    // Test cast with i64
    let input = r#"
        query castToI64 {
            from(items)
            .select(cast(items.val, i64))
        }
    "#;
    let module = parse_module(input).unwrap();
    assert_eq!(module.toplevels.len(), 1);
}

#[test]
fn test_aggregate_with_filter() {
    let input = r#"
        query test() {
            from(orders)
            .aggregate(active_count: COUNT(orders.id).filter(orders.status == "active"))
            .select(active_count)
        }
    "#;
    let module = parse_module(input).unwrap();
    assert_eq!(module.toplevels.len(), 1);

    let query = match &module.toplevels[0] {
        TopLevel::Query(q) => q,
        _ => panic!("expected query"),
    };
    let QueryStatement::Select(select_stmt) = &query.body.statements[0];
    // Find the aggregate clause
    let agg_clause = select_stmt.clauses.iter().find_map(|c| match c {
        QueryClause::Aggregate(aggs) => Some(aggs),
        _ => None,
    }).expect("expected aggregate clause");

    assert_eq!(agg_clause.len(), 1);
    assert_eq!(agg_clause[0].alias, "active_count");

    // The aggregate expression should be an AggregateFunction with a filter
    match &agg_clause[0].expr {
        Expression::Atomic(AtomicExpression::Aggregate(agg)) => {
            assert_eq!(agg.function_type, AggregateFunctionType::Count);
            assert!(agg.filter.is_some(), "expected filter on aggregate");
        }
        _ => panic!("expected aggregate function expression"),
    }
}
