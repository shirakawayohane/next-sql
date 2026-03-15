use std::collections::{HashMap, HashSet};
use nextsql_core::ast::*;
use nextsql_core::schema::DatabaseSchema;
use crate::sql_gen;
use crate::type_mapping::nextsql_type_to_rust;

use super::RowField;
use super::valtype::ValTypeRegistry;
use super::naming::{to_snake_case, to_pascal_case, table_name_to_model_name};
use super::type_resolve::*;
use super::emit::*;

/// Walk query clauses to find the select expressions and aggregate expressions.
pub(super) fn find_select_and_aggregate_clauses(
    clauses: &[QueryClause],
) -> (Option<&Vec<SelectExpression>>, Option<&Vec<AggregateExpression>>) {
    let mut select = None;
    let mut aggregate = None;
    for c in clauses {
        match c {
            QueryClause::Select(exprs) => select = Some(exprs),
            QueryClause::Aggregate(aggs) => aggregate = Some(aggs),
            _ => {}
        }
    }
    (select, aggregate)
}

pub(super) fn generate_query(out: &mut String, query: &Query, schema: &DatabaseSchema, model_tables: &mut HashSet<String>, registry: &ValTypeRegistry, rel_registry: &sql_gen::RelationRegistry, errors: &mut Vec<String>) {
    let name = &query.decl.name;
    let fn_name = to_snake_case(name);
    let pascal = to_pascal_case(name);
    let params_struct = format!("{}Params", pascal);
    let row_struct = format!("{}Row", pascal);

    // We only handle the first statement in the body for code generation.
    let stmt = match query.body.statements.first() {
        Some(QueryStatement::Select(s)) => s,
        None => return,
    };

    // Generate SQL
    let gen = sql_gen::generate_select_sql_with_relations(stmt, rel_registry);

    // Params struct (skip if no arguments)
    let has_params = !query.decl.arguments.is_empty();
    if has_params {
        emit_params_struct(out, &params_struct, &query.decl.arguments, &gen.params, registry);
    }

    // Determine row fields
    let (select_clause, aggregate_clause) = find_select_and_aggregate_clauses(&stmt.clauses);

    // Check for simple wildcard pattern
    let use_model = is_simple_wildcard_select(stmt, select_clause, aggregate_clause);
    let effective_row_struct = if let Some(ref tbl) = use_model {
        model_tables.insert(tbl.clone());
        table_name_to_model_name(tbl)
    } else {
        // Generate row struct as before
        let mut row_fields: Vec<RowField> = Vec::new();

        // Build a map of aggregate alias -> RowField so we can:
        // 1) Use correct types for aggregate references in .select()
        // 2) Deduplicate when both .select() and .aggregate() define the same name
        let aggregate_field_map: HashMap<String, RowField> = if let Some(aggs) = aggregate_clause {
            resolve_aggregate_clause_fields(aggs, &stmt.from.table, schema)
                .into_iter()
                .map(|f| (f.name.clone(), f))
                .collect()
        } else {
            HashMap::new()
        };

        if let Some(sel) = select_clause {
            let mut select_fields = resolve_select_fields(sel, &stmt.from.table, schema, registry, rel_registry);
            // Replace fallback fields that match aggregate aliases with properly typed versions
            for field in &mut select_fields {
                if let Some(agg_field) = aggregate_field_map.get(&field.name) {
                    *field = agg_field.clone();
                }
            }
            row_fields.extend(select_fields);
        }

        if let Some(aggs) = aggregate_clause {
            // Only add aggregate fields whose names are not already present from select
            let seen: HashSet<String> = row_fields.iter().map(|f| f.name.clone()).collect();
            let agg_fields = resolve_aggregate_clause_fields(aggs, &stmt.from.table, schema);
            for f in agg_fields {
                if !seen.contains(&f.name) {
                    row_fields.push(f);
                }
            }
        }

        // If no select/aggregate clauses found, expand wildcard of from table
        if row_fields.is_empty() {
            if let Some(ts) = schema.get_table(&stmt.from.table) {
                for c in &ts.columns {
                    let typ = effective_type(&c.column_type, c.nullable);
                    row_fields.push(make_row_field(&c.name, &stmt.from.table, &typ, registry));
                }
            }
        }

        // Check for duplicate field names (Bug 1 fix)
        let mut seen_names: HashMap<String, usize> = HashMap::new();
        let mut duplicates: Vec<String> = Vec::new();
        for field in &row_fields {
            let count = seen_names.entry(field.name.clone()).or_insert(0);
            *count += 1;
            if *count == 2 {
                duplicates.push(field.name.clone());
            }
        }
        if !duplicates.is_empty() {
            for dup in &duplicates {
                errors.push(format!(
                    "Duplicate field name '{}' in query '{}'. Use an alias to resolve: e.g., select({table}.*, {dup}_alias: {table}.relation.{dup})",
                    dup, name, table = stmt.from.table,
                ));
            }
            // Skip generating this query entirely
            return;
        }

        emit_row_struct(out, &row_struct, &row_fields);
        row_struct.clone()
    };

    // Executor function
    if has_params {
        let conversion = generate_params_with_conversions(&gen.params, &query.decl.arguments, registry);
        let conv_ref = if conversion.0.is_empty() { None } else { Some(&conversion) };
        emit_query_fn(out, &fn_name, &params_struct, &effective_row_struct, &gen.sql, conv_ref);
    } else {
        emit_query_fn_no_params(out, &fn_name, &effective_row_struct, &gen.sql);
    }
}

pub(super) fn generate_mutation(out: &mut String, mutation: &Mutation, schema: &DatabaseSchema, model_tables: &mut HashSet<String>, registry: &ValTypeRegistry, rel_registry: &sql_gen::RelationRegistry) {
    let name = &mutation.decl.name;
    let fn_name = to_snake_case(name);
    let pascal = to_pascal_case(name);
    let params_struct = format!("{}Params", pascal);
    let row_struct = format!("{}Row", pascal);

    for item in &mutation.body.items {
        match item {
            MutationBodyItem::Mutation(stmt) => {
                match stmt {
                    MutationStatement::Insert(insert) => {
                        let gen = sql_gen::generate_insert_sql_with_relations(insert, rel_registry);
                        let has_params = !mutation.decl.arguments.is_empty();
                        if has_params {
                            emit_params_struct(
                                out,
                                &params_struct,
                                &mutation.decl.arguments,
                                &gen.params,
                                registry,
                            );
                        }

                        let conversion = if has_params {
                            let c = generate_params_with_conversions(&gen.params, &mutation.decl.arguments, registry);
                            if c.0.is_empty() { None } else { Some(c) }
                        } else { None };

                        if let Some(ref returning) = insert.returning {
                            let use_model = is_simple_wildcard_returning(returning, &insert.into);
                            let effective_row = if let Some(ref tbl) = use_model {
                                model_tables.insert(tbl.clone());
                                table_name_to_model_name(tbl)
                            } else {
                                let fields =
                                    resolve_returning_fields(returning, &insert.into, schema, registry);
                                emit_row_struct(out, &row_struct, &fields);
                                row_struct.clone()
                            };
                            if has_params {
                                emit_mutation_query_fn(
                                    out,
                                    &fn_name,
                                    &params_struct,
                                    &effective_row,
                                    &gen.sql,
                                    conversion.as_ref(),
                                );
                            } else {
                                emit_query_fn_no_params(out, &fn_name, &effective_row, &gen.sql);
                            }
                        } else if has_params {
                            emit_execute_fn(out, &fn_name, &params_struct, &gen.sql, conversion.as_ref());
                        } else {
                            emit_execute_fn_no_params(out, &fn_name, &gen.sql);
                        }
                    }
                    MutationStatement::Update(update) => {
                        // Check if this is an Updatable (dynamic SET) mutation
                        let updatable_info = find_updatable_param(
                            &mutation.decl.arguments,
                            update.set_variable.as_deref(),
                        );

                        if let Some((updatable_var_name, updatable_table_name)) = updatable_info {
                            generate_updatable_mutation(
                                out,
                                mutation,
                                update,
                                &updatable_var_name,
                                &updatable_table_name,
                                schema,
                                model_tables,
                                registry,
                                rel_registry,
                            );
                        } else {
                            let gen = sql_gen::generate_update_sql_with_relations(update, rel_registry);
                            let has_params = !mutation.decl.arguments.is_empty();
                            if has_params {
                                emit_params_struct(
                                    out,
                                    &params_struct,
                                    &mutation.decl.arguments,
                                    &gen.params,
                                    registry,
                                );
                            }

                            let conversion = if has_params {
                                let c = generate_params_with_conversions(&gen.params, &mutation.decl.arguments, registry);
                                if c.0.is_empty() { None } else { Some(c) }
                            } else { None };

                            if let Some(ref returning) = update.returning {
                                let use_model = is_simple_wildcard_returning(returning, &update.target.name);
                                let effective_row = if let Some(ref tbl) = use_model {
                                    model_tables.insert(tbl.clone());
                                    table_name_to_model_name(tbl)
                                } else {
                                    let fields = resolve_returning_fields(
                                        returning,
                                        &update.target.name,
                                        schema,
                                        registry,
                                    );
                                    emit_row_struct(out, &row_struct, &fields);
                                    row_struct.clone()
                                };
                                if has_params {
                                    emit_mutation_query_fn(
                                        out,
                                        &fn_name,
                                        &params_struct,
                                        &effective_row,
                                        &gen.sql,
                                        conversion.as_ref(),
                                    );
                                } else {
                                    emit_query_fn_no_params(out, &fn_name, &effective_row, &gen.sql);
                                }
                            } else if has_params {
                                emit_execute_fn(out, &fn_name, &params_struct, &gen.sql, conversion.as_ref());
                            } else {
                                emit_execute_fn_no_params(out, &fn_name, &gen.sql);
                            }
                        }
                    }
                    MutationStatement::Delete(delete) => {
                        let gen = sql_gen::generate_delete_sql_with_relations(delete, rel_registry);
                        let has_params = !mutation.decl.arguments.is_empty();
                        if has_params {
                            emit_params_struct(
                                out,
                                &params_struct,
                                &mutation.decl.arguments,
                                &gen.params,
                                registry,
                            );
                        }

                        let conversion = if has_params {
                            let c = generate_params_with_conversions(&gen.params, &mutation.decl.arguments, registry);
                            if c.0.is_empty() { None } else { Some(c) }
                        } else { None };

                        if let Some(ref returning) = delete.returning {
                            let use_model = is_simple_wildcard_returning(returning, &delete.target.name);
                            let effective_row = if let Some(ref tbl) = use_model {
                                model_tables.insert(tbl.clone());
                                table_name_to_model_name(tbl)
                            } else {
                                let fields = resolve_returning_fields(
                                    returning,
                                    &delete.target.name,
                                    schema,
                                    registry,
                                );
                                emit_row_struct(out, &row_struct, &fields);
                                row_struct.clone()
                            };
                            if has_params {
                                emit_mutation_query_fn(
                                    out,
                                    &fn_name,
                                    &params_struct,
                                    &effective_row,
                                    &gen.sql,
                                    conversion.as_ref(),
                                );
                            } else {
                                emit_query_fn_no_params(out, &fn_name, &effective_row, &gen.sql);
                            }
                        } else if has_params {
                            emit_execute_fn(out, &fn_name, &params_struct, &gen.sql, conversion.as_ref());
                        } else {
                            emit_execute_fn_no_params(out, &fn_name, &gen.sql);
                        }
                    }
                }
            }
        }
    }
}

// ── Updatable (dynamic SET) codegen ─────────────────────────────────────

/// Check if any parameter is `Updatable<table>` and matches the set_variable.
/// Returns Some((variable_name, table_name)) if found.
pub(super) fn find_updatable_param(
    args: &[Argument],
    set_variable: Option<&str>,
) -> Option<(String, String)> {
    // If there's an explicit set_variable, find the matching Updatable param
    if let Some(var_name) = set_variable {
        for arg in args {
            if arg.name == var_name {
                if let Type::Utility(UtilityType::Updatable(Updatable(inner))) = &arg.typ {
                    if let Type::UserDefined(table_name) = inner.as_ref() {
                        return Some((var_name.to_string(), table_name.clone()));
                    }
                }
            }
        }
    }
    // Also check if any param is Updatable even without set_variable
    // (for cases where the set clause is a variable reference)
    for arg in args {
        if let Type::Utility(UtilityType::Updatable(Updatable(inner))) = &arg.typ {
            if let Type::UserDefined(table_name) = inner.as_ref() {
                return Some((arg.name.clone(), table_name.clone()));
            }
        }
    }
    None
}

/// Generate the Changes struct, Default impl, Params struct, and dynamic UPDATE function
/// for an Updatable mutation.
pub(super) fn generate_updatable_mutation(
    out: &mut String,
    mutation: &Mutation,
    update: &Update,
    updatable_var_name: &str,
    updatable_table_name: &str,
    schema: &DatabaseSchema,
    model_tables: &mut HashSet<String>,
    registry: &ValTypeRegistry,
    _rel_registry: &sql_gen::RelationRegistry,
) {
    let name = &mutation.decl.name;
    let fn_name = to_snake_case(name);
    let pascal = to_pascal_case(name);
    let changes_struct = format!("{}Changes", pascal);
    let params_struct = format!("{}Params", pascal);
    let row_struct = format!("{}Row", pascal);

    let table = match schema.get_table(updatable_table_name) {
        Some(t) => t,
        None => return, // table not found in schema
    };

    // Get non-PK columns for the Changes struct
    let updatable_columns: Vec<&nextsql_core::schema::ColumnSchema> = table
        .columns
        .iter()
        .filter(|c| !c.primary_key)
        .collect();

    // ── Changes struct ──
    out.push_str(&format!("pub struct {} {{\n", changes_struct));
    for col in &updatable_columns {
        let inner_type = column_to_rust_type(col, &table.name, registry);
        out.push_str(&format!(
            "    pub {}: super::runtime::UpdateField<{}>,\n",
            to_snake_case(&col.name),
            inner_type,
        ));
    }
    out.push_str("}\n\n");

    // ── Default impl ──
    out.push_str(&format!("impl Default for {} {{\n", changes_struct));
    out.push_str("    fn default() -> Self {\n");
    out.push_str("        Self {\n");
    for col in &updatable_columns {
        out.push_str(&format!(
            "            {}: super::runtime::UpdateField::Unchanged,\n",
            to_snake_case(&col.name),
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    // ── Params struct ──
    // Non-updatable args get normal types, the updatable arg gets the Changes struct type.
    out.push_str(&format!("pub struct {} {{\n", params_struct));
    for arg in &mutation.decl.arguments {
        if arg.name == updatable_var_name {
            out.push_str(&format!(
                "    pub {}: {},\n",
                to_snake_case(&arg.name),
                changes_struct,
            ));
        } else {
            let rust_type = match &arg.typ {
                Type::UserDefined(name) if registry.is_valtype(name) => name.clone(),
                other => nextsql_type_to_rust(other),
            };
            out.push_str(&format!(
                "    pub {}: {},\n",
                to_snake_case(&arg.name),
                rust_type,
            ));
        }
    }
    out.push_str("}\n\n");

    // ── Determine return type ──
    let has_returning = update.returning.is_some();
    let effective_row = if let Some(ref returning) = update.returning {
        let use_model = is_simple_wildcard_returning(returning, &update.target.name);
        if let Some(ref tbl) = use_model {
            model_tables.insert(tbl.clone());
            Some(table_name_to_model_name(tbl))
        } else {
            let fields = resolve_returning_fields(returning, &update.target.name, schema, registry);
            emit_row_struct(out, &row_struct, &fields);
            Some(row_struct.clone())
        }
    } else {
        None
    };

    // ── Dynamic UPDATE function ──
    if has_returning {
        let row_type = effective_row.as_ref().unwrap();
        out.push_str(&format!(
            "pub async fn {}(\n    client: &(impl super::runtime::Client + ?Sized),\n    params: &{},\n) -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
            fn_name, params_struct, row_type,
        ));
    } else {
        out.push_str(&format!(
            "pub async fn {}(\n    client: &(impl super::runtime::Client + ?Sized),\n    params: &{},\n) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {{\n",
            fn_name, params_struct,
        ));
    }

    out.push_str("    let mut set_parts: Vec<String> = Vec::new();\n");
    out.push_str("    let mut bind_params: Vec<&dyn super::runtime::ToSqlParam> = Vec::new();\n");
    out.push_str("    let mut idx = 1usize;\n\n");

    // Collect non-updatable params that appear in WHERE clause.
    // We need to determine which arguments are used in the WHERE clause (non-updatable params).
    let non_updatable_args: Vec<&Argument> = mutation
        .decl
        .arguments
        .iter()
        .filter(|a| a.name != updatable_var_name)
        .collect();

    // Bind non-updatable params first and track their indices
    for arg in &non_updatable_args {
        let snake_name = to_snake_case(&arg.name);
        out.push_str(&format!("    bind_params.push(&params.{});\n", snake_name));
        out.push_str(&format!("    let {}_idx = idx;\n", snake_name));
        out.push_str("    idx += 1;\n\n");
    }

    // Dynamic SET fields
    let updatable_snake = to_snake_case(updatable_var_name);
    for col in &updatable_columns {
        let col_snake = to_snake_case(&col.name);
        out.push_str(&format!(
            "    if params.{}.{}.is_set() {{\n",
            updatable_snake, col_snake,
        ));
        out.push_str(&format!(
            "        set_parts.push(format!(\"{} = ${{}}\", idx));\n",
            col.name,
        ));
        out.push_str(&format!(
            "        bind_params.push(&params.{}.{});\n",
            updatable_snake, col_snake,
        ));
        out.push_str("        idx += 1;\n");
        out.push_str("    }\n");
    }

    // Early return if nothing to update
    out.push_str("\n    if set_parts.is_empty() {\n");
    if has_returning {
        out.push_str("        return Ok(Vec::new());\n");
    } else {
        out.push_str("        return Ok(0);\n");
    }
    out.push_str("    }\n\n");

    // Build the SQL dynamically
    // Generate the WHERE clause using the same SQL gen logic to get the template
    let where_sql = if let Some(ref where_clause) = update.where_clause {
        let mut where_parts = String::new();
        generate_where_sql_template(&mut where_parts, where_clause, &non_updatable_args);
        Some(where_parts)
    } else {
        None
    };

    // Build returning columns string
    let returning_sql = if let Some(ref returning) = update.returning {
        let cols: Vec<String> = returning.iter().map(|c| format_column_sql(c)).collect();
        Some(cols.join(", "))
    } else {
        None
    };

    // Emit the format! call
    let mut format_args = Vec::new();
    out.push_str("    let sql = format!(\n        \"UPDATE ");
    out.push_str(&update.target.name);
    out.push_str(" SET {}");

    if let Some(ref where_sql) = where_sql {
        out.push_str(&format!(" WHERE {}", where_sql));
    }

    if let Some(ref ret_sql) = returning_sql {
        out.push_str(&format!(" RETURNING {}", ret_sql));
    }

    out.push_str("\",\n        set_parts.join(\", \"),\n");

    // Add format args for WHERE clause placeholders
    for arg in &non_updatable_args {
        let snake_name = to_snake_case(&arg.name);
        format_args.push(format!("        {}_idx", snake_name));
    }
    if !format_args.is_empty() {
        out.push_str(&format_args.join(",\n"));
        out.push('\n');
    }

    out.push_str("    );\n");

    // Execute
    if has_returning {
        let row_type = effective_row.as_ref().unwrap();
        out.push_str("    let rows = client.query(&sql, &bind_params).await?;\n");
        out.push_str(&format!(
            "    Ok(rows.iter().map(|row| {}::from_row(row)).collect())\n",
            row_type,
        ));
    } else {
        out.push_str("    let count = client.execute(&sql, &bind_params).await?;\n");
        out.push_str("    Ok(count)\n");
    }
    out.push_str("}\n\n");
}

/// Generate a WHERE clause SQL template with `${}` placeholders for format! args.
/// Each variable reference becomes `${}`  which will be filled by `{var}_idx` at runtime.
pub(super) fn generate_where_sql_template(
    out: &mut String,
    expr: &Expression,
    non_updatable_args: &[&Argument],
) {
    match expr {
        Expression::Binary { left, op, right } => {
            generate_where_sql_template(out, left, non_updatable_args);
            let op_str = match op {
                BinaryOp::Equal => " = ",
                BinaryOp::Unequal => " != ",
                BinaryOp::LessThan => " < ",
                BinaryOp::LessThanOrEqual => " <= ",
                BinaryOp::GreaterThan => " > ",
                BinaryOp::GreaterThanOrEqual => " >= ",
                BinaryOp::And => " AND ",
                BinaryOp::Or => " OR ",
                BinaryOp::Add => " + ",
                BinaryOp::Subtract => " - ",
                BinaryOp::Multiply => " * ",
                BinaryOp::Divide => " / ",
                BinaryOp::Remainder => " % ",
            };
            out.push_str(op_str);
            generate_where_sql_template(out, right, non_updatable_args);
        }
        Expression::Unary { op, expr } => {
            match op {
                UnaryOp::Not => out.push_str("NOT "),
            }
            generate_where_sql_template(out, expr, non_updatable_args);
        }
        Expression::Atomic(atomic) => {
            match atomic {
                AtomicExpression::Column(col) => {
                    out.push_str(&format_column_sql(col));
                }
                AtomicExpression::Variable(_var) => {
                    // Use ${} placeholder for format! substitution
                    out.push_str("${}");
                }
                AtomicExpression::Literal(lit) => {
                    match lit {
                        Literal::Numeric(n) => out.push_str(&n.to_string()),
                        Literal::String(s) => out.push_str(&format!("'{}'", s)),
                        Literal::Boolean(b) => out.push_str(if *b { "TRUE" } else { "FALSE" }),
                        Literal::Null => out.push_str("NULL"),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
}

/// Format a Column AST node to SQL string.
pub(super) fn format_column_sql(col: &Column) -> String {
    match col {
        Column::ExplicitTarget(table, column, _) => format!("{}.{}", table, column),
        Column::ImplicitTarget(column, _) => column.clone(),
        Column::WildcardOf(table, _) => format!("{}.*", table),
        Column::Wildcard(_) => "*".to_string(),
    }
}
