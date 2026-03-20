use std::collections::{HashMap, HashSet};
use nextsql_core::ast::*;
use nextsql_core::schema::DatabaseSchema;
use crate::sql_gen;
use crate::type_mapping::nextsql_type_to_rust;

use super::RowField;
use super::NamingConfig;
use super::InputTypeRegistry;
use super::valtype::ValTypeRegistry;
use super::naming::{to_snake_case, to_pascal_case, table_name_to_model_name};
use super::type_resolve::*;
use super::emit::*;

/// How parameters are passed to generated functions.
enum ParamStyle {
    /// No parameters
    None,
    /// All args are plain types — use individual function parameters
    Individual,
    /// Has Insertable<T> — keep existing struct generation
    Insertable,
    /// Has ChangeSet<T> — keep existing struct generation
    ChangeSet,
}

/// Determine the parameter style for a query/mutation.
fn determine_param_style(args: &[Argument]) -> ParamStyle {
    if args.is_empty() {
        return ParamStyle::None;
    }
    for arg in args {
        match &arg.typ {
            Type::Utility(UtilityType::Insertable(_)) => return ParamStyle::Insertable,
            Type::Utility(UtilityType::ChangeSet(_)) => return ParamStyle::ChangeSet,
            _ => {}
        }
    }
    ParamStyle::Individual
}

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

pub(super) fn generate_query(out: &mut String, query: &Query, schema: &DatabaseSchema, model_tables: &mut HashSet<String>, registry: &ValTypeRegistry, rel_registry: &sql_gen::RelationRegistry, errors: &mut Vec<String>, input_registry: &InputTypeRegistry) {
    let name = &query.decl.name;
    let fn_name = to_snake_case(name);
    let pascal = to_pascal_case(name);
    let row_struct = format!("{}Row", pascal);

    // We only handle the first statement in the body for code generation.
    let stmt = match query.body.statements.first() {
        Some(QueryStatement::Select(s)) => s,
        None => return,
    };

    // Generate SQL
    let gen = sql_gen::generate_select_sql_with_relations(stmt, rel_registry);

    let param_style = determine_param_style(&query.decl.arguments);

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
                    row_fields.push(make_row_field(&c.name, &stmt.from.table, &typ, registry, schema));
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
    match param_style {
        ParamStyle::None => {
            emit_query_fn_no_params(out, &fn_name, &effective_row_struct, &gen.sql);
        }
        ParamStyle::Individual => {
            if gen.is_dynamic && !gen.when_clauses.is_empty() {
                emit_dynamic_query_fn_individual_params(out, &fn_name, &query.decl.arguments, &effective_row_struct, &gen, registry, input_registry, schema);
            } else {
                emit_query_fn_individual_params(out, &fn_name, &query.decl.arguments, &gen.params, &effective_row_struct, &gen.sql, registry, input_registry, schema);
            }
        }
        _ => {
            // Insertable/ChangeSet shouldn't appear in queries, but handle gracefully
            emit_query_fn_no_params(out, &fn_name, &effective_row_struct, &gen.sql);
        }
    }
}

pub(super) fn generate_mutation(out: &mut String, mutation: &Mutation, schema: &DatabaseSchema, model_tables: &mut HashSet<String>, registry: &ValTypeRegistry, rel_registry: &sql_gen::RelationRegistry, naming: &NamingConfig, input_registry: &InputTypeRegistry) {
    let name = &mutation.decl.name;
    let fn_name = to_snake_case(name);
    let pascal = to_pascal_case(name);
    let row_struct = format!("{}Row", pascal);
    let param_style = determine_param_style(&mutation.decl.arguments);

    for item in &mutation.body.items {
        match item {
            MutationBodyItem::Mutation(stmt) => {
                match stmt {
                    MutationStatement::Insert(insert) => {
                        // Check if this is an Insertable<T> mutation
                        let insertable_info = find_insertable_param(&mutation.decl.arguments);
                        if let Some((var_name, table_name)) = insertable_info {
                            generate_insertable_mutation(
                                out, mutation, insert, &var_name, &table_name,
                                schema, model_tables, registry, rel_registry, naming,
                            );
                            continue;
                        }

                        let gen = sql_gen::generate_insert_sql_with_relations(insert, rel_registry);

                        if let Some(ref returning) = insert.returning {
                            let use_model = is_simple_wildcard_returning(returning, &insert.into.name);
                            let effective_row = if let Some(ref tbl) = use_model {
                                model_tables.insert(tbl.clone());
                                table_name_to_model_name(tbl)
                            } else {
                                let fields =
                                    resolve_returning_fields(returning, &insert.into.name, schema, registry);
                                emit_row_struct(out, &row_struct, &fields);
                                row_struct.clone()
                            };
                            match param_style {
                                ParamStyle::None => {
                                    emit_query_fn_no_params(out, &fn_name, &effective_row, &gen.sql);
                                }
                                ParamStyle::Individual => {
                                    emit_query_fn_individual_params(out, &fn_name, &mutation.decl.arguments, &gen.params, &effective_row, &gen.sql, registry, input_registry, schema);
                                }
                                _ => unreachable!(),
                            }
                        } else {
                            match param_style {
                                ParamStyle::None => {
                                    emit_execute_fn_no_params(out, &fn_name, &gen.sql);
                                }
                                ParamStyle::Individual => {
                                    emit_execute_fn_individual_params(out, &fn_name, &mutation.decl.arguments, &gen.params, &gen.sql, registry, input_registry, schema);
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                    MutationStatement::Update(update) => {
                        // Check if this is a ChangeSet (dynamic SET) mutation
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
                                naming,
                            );
                        } else {
                            let gen = sql_gen::generate_update_sql_with_relations(update, rel_registry);

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
                                match param_style {
                                    ParamStyle::None => {
                                        emit_query_fn_no_params(out, &fn_name, &effective_row, &gen.sql);
                                    }
                                    ParamStyle::Individual => {
                                        emit_query_fn_individual_params(out, &fn_name, &mutation.decl.arguments, &gen.params, &effective_row, &gen.sql, registry, input_registry, schema);
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                match param_style {
                                    ParamStyle::None => {
                                        emit_execute_fn_no_params(out, &fn_name, &gen.sql);
                                    }
                                    ParamStyle::Individual => {
                                        emit_execute_fn_individual_params(out, &fn_name, &mutation.decl.arguments, &gen.params, &gen.sql, registry, input_registry, schema);
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                    }
                    MutationStatement::Delete(delete) => {
                        let gen = sql_gen::generate_delete_sql_with_relations(delete, rel_registry);

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
                            match param_style {
                                ParamStyle::None => {
                                    emit_query_fn_no_params(out, &fn_name, &effective_row, &gen.sql);
                                }
                                ParamStyle::Individual => {
                                    emit_query_fn_individual_params(out, &fn_name, &mutation.decl.arguments, &gen.params, &effective_row, &gen.sql, registry, input_registry, schema);
                                }
                                _ => unreachable!(),
                            }
                        } else {
                            match param_style {
                                ParamStyle::None => {
                                    emit_execute_fn_no_params(out, &fn_name, &gen.sql);
                                }
                                ParamStyle::Individual => {
                                    emit_execute_fn_individual_params(out, &fn_name, &mutation.decl.arguments, &gen.params, &gen.sql, registry, input_registry, schema);
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
            }
        }
    }
}

// ── Insertable (dynamic INSERT) codegen ─────────────────────────────────

/// Check if any parameter is `Insertable<table>`.
/// Returns Some((variable_name, table_name)) if found.
pub(super) fn find_insertable_param(args: &[Argument]) -> Option<(String, String)> {
    for arg in args {
        if let Type::Utility(UtilityType::Insertable(Insertable(inner))) = &arg.typ {
            if let Type::UserDefined(table_name) = inner.as_ref() {
                return Some((arg.name.clone(), table_name.clone()));
            }
        }
    }
    None
}

/// Generate the Insertable params struct, builder, and dynamic INSERT function.
pub(super) fn generate_insertable_mutation(
    out: &mut String,
    mutation: &Mutation,
    insert: &Insert,
    insertable_var_name: &str,
    insertable_table_name: &str,
    schema: &DatabaseSchema,
    model_tables: &mut HashSet<String>,
    registry: &ValTypeRegistry,
    _rel_registry: &sql_gen::RelationRegistry,
    naming: &NamingConfig,
) {
    let name = &mutation.decl.name;
    let fn_name = to_snake_case(name);
    let pascal = to_pascal_case(name);
    let row_struct = format!("{}Row", pascal);

    let table = match schema.get_table(insertable_table_name) {
        Some(t) => t,
        None => return,
    };

    let table_pascal = table_name_to_model_name(insertable_table_name);
    let insertable_struct = naming.insert_struct_name(&table_pascal);

    // Determine insertable columns (exclude auto-generated PKs)
    let insertable_columns: Vec<&nextsql_core::schema::ColumnSchema> = table
        .columns
        .iter()
        .filter(|c| !(c.primary_key && c.has_default))
        .collect();

    // ── Insertable struct ──
    out.push_str(&format!("pub struct {} {{\n", insertable_struct));
    for col in &insertable_columns {
        let base_type = column_to_rust_type(col, &table.name, registry, schema);
        // Fields with defaults or nullable get Option<T>
        let is_optional = col.nullable || col.has_default;
        if is_optional {
            // If column_to_rust_type already wrapped in Option (nullable), use as-is
            // Otherwise wrap in Option
            if base_type.starts_with("Option<") {
                out.push_str(&format!("    pub {}: {},\n", to_snake_case(&col.name), base_type));
            } else {
                out.push_str(&format!("    pub {}: Option<{}>,\n", to_snake_case(&col.name), base_type));
            }
        } else {
            out.push_str(&format!("    pub {}: {},\n", to_snake_case(&col.name), base_type));
        }
    }
    out.push_str("}\n\n");

    // ── Builder struct ──
    let builder_struct = format!("{}Builder", insertable_struct);
    out.push_str(&format!("pub struct {} {{\n", builder_struct));
    for col in &insertable_columns {
        let base_type = column_to_rust_type(col, &table.name, registry, schema);
        // All builder fields are Option
        if base_type.starts_with("Option<") {
            // For nullable fields, the builder stores Option<Option<T>>
            // but we simplify: builder stores the same type as struct
            out.push_str(&format!("    {}: {},\n", to_snake_case(&col.name), base_type));
        } else {
            out.push_str(&format!("    {}: Option<{}>,\n", to_snake_case(&col.name), base_type));
        }
    }
    out.push_str("}\n\n");

    // ── builder() constructor ──
    out.push_str(&format!("impl {} {{\n", insertable_struct));
    out.push_str(&format!("    pub fn builder() -> {} {{\n", builder_struct));
    out.push_str(&format!("        {} {{\n", builder_struct));
    for col in &insertable_columns {
        out.push_str(&format!("            {}: None,\n", to_snake_case(&col.name)));
    }
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    // ── Builder impl with setter methods ──
    out.push_str(&format!("impl {} {{\n", builder_struct));
    for col in &insertable_columns {
        let col_snake = to_snake_case(&col.name);
        let base_type = column_to_rust_type(col, &table.name, registry, schema);
        // Setter takes the inner value (not Option)
        let inner_type = if base_type.starts_with("Option<") {
            // For nullable fields, setter takes the inner type
            base_type[7..base_type.len()-1].to_string()
        } else {
            base_type.clone()
        };
        out.push_str(&format!(
            "    pub fn {}(mut self, value: {}) -> Self {{\n        self.{} = Some(value);\n        self\n    }}\n",
            col_snake, inner_type, col_snake,
        ));
    }

    // ── build() method ──
    out.push_str(&format!("    pub fn build(self) -> Result<{}, String> {{\n", insertable_struct));
    out.push_str(&format!("        Ok({} {{\n", insertable_struct));
    for col in &insertable_columns {
        let col_snake = to_snake_case(&col.name);
        let base_type = column_to_rust_type(col, &table.name, registry, schema);
        let is_optional = col.nullable || col.has_default;
        if is_optional {
            if base_type.starts_with("Option<") {
                // nullable field: builder's None → None, Some(v) → Some(v)
                out.push_str(&format!("            {}: self.{},\n", col_snake, col_snake));
            } else {
                // has_default but not nullable: builder's Option becomes struct's Option
                out.push_str(&format!("            {}: self.{},\n", col_snake, col_snake));
            }
        } else {
            // Required field: must be set
            out.push_str(&format!(
                "            {}: self.{}.ok_or_else(|| \"{} is required\".to_string())?,\n",
                col_snake, col_snake, col_snake,
            ));
        }
    }
    out.push_str("        })\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    // ── Params struct ──
    // If there are other params besides the insertable, create a wrapper
    let other_args: Vec<&Argument> = mutation.decl.arguments.iter()
        .filter(|a| a.name != insertable_var_name)
        .collect();
    let params_struct = format!("{}Params", pascal);
    let has_other_params = !other_args.is_empty();

    if has_other_params {
        out.push_str(&format!("pub struct {} {{\n", params_struct));
        for arg in &mutation.decl.arguments {
            if arg.name == insertable_var_name {
                out.push_str(&format!(
                    "    pub {}: {},\n",
                    to_snake_case(&arg.name), insertable_struct,
                ));
            } else {
                let rust_type = match &arg.typ {
                    Type::UserDefined(name) if registry.is_valtype(name) => name.clone(),
                    other => nextsql_type_to_rust(other),
                };
                out.push_str(&format!(
                    "    pub {}: {},\n",
                    to_snake_case(&arg.name), rust_type,
                ));
            }
        }
        out.push_str("}\n\n");
    }

    // ── Determine return type ──
    let has_returning = insert.returning.is_some();
    let effective_row = if let Some(ref returning) = insert.returning {
        let use_model = is_simple_wildcard_returning(returning, &insert.into.name);
        if let Some(ref tbl) = use_model {
            model_tables.insert(tbl.clone());
            Some(table_name_to_model_name(tbl))
        } else {
            let fields = resolve_returning_fields(returning, &insert.into.name, schema, registry);
            emit_row_struct(out, &row_struct, &fields);
            Some(row_struct.clone())
        }
    } else {
        None
    };

    // ── Dynamic INSERT function ──
    let actual_params_struct = if has_other_params { &params_struct } else { &insertable_struct };
    let insertable_access = if has_other_params {
        format!("params.{}", to_snake_case(insertable_var_name))
    } else {
        "params".to_string()
    };

    if has_returning {
        let row_type = effective_row.as_ref().unwrap();
        out.push_str(&format!(
            "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n    params: &{},\n) -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
            fn_name, actual_params_struct, row_type,
        ));
    } else {
        out.push_str(&format!(
            "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n    params: &{},\n) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {{\n",
            fn_name, actual_params_struct,
        ));
    }

    // Build columns and params dynamically
    out.push_str("    let mut columns: Vec<&str> = Vec::new();\n");
    out.push_str("    let mut bind_params: Vec<&dyn nextsql_backend_rust_runtime::ToSqlParam> = Vec::new();\n\n");

    for col in &insertable_columns {
        let col_snake = to_snake_case(&col.name);
        let is_optional = col.nullable || col.has_default;
        if is_optional {
            out.push_str(&format!(
                "    if let Some(ref v) = {}.{} {{\n",
                insertable_access, col_snake,
            ));
            out.push_str(&format!("        columns.push(\"{}\");\n", col.name));
            out.push_str("        bind_params.push(v);\n");
            out.push_str("    }\n");
        } else {
            out.push_str(&format!("    columns.push(\"{}\");\n", col.name));
            out.push_str(&format!(
                "    bind_params.push(&{}.{});\n",
                insertable_access, col_snake,
            ));
        }
    }

    // Build SQL
    out.push_str("\n    let placeholders: Vec<String> = (1..=bind_params.len()).map(|i| format!(\"${}\", i)).collect();\n");

    // Build the returning clause
    let returning_sql = if let Some(ref returning) = insert.returning {
        let cols: Vec<String> = returning.iter().map(|c| format_column_sql(c)).collect();
        Some(cols.join(", "))
    } else {
        None
    };

    if let Some(ref ret_sql) = returning_sql {
        out.push_str(&format!(
            "    let sql = format!(\"INSERT INTO {} ({{}}) VALUES ({{}}) RETURNING {}\", columns.join(\", \"), placeholders.join(\", \"));\n",
            insertable_table_name, ret_sql,
        ));
    } else {
        out.push_str(&format!(
            "    let sql = format!(\"INSERT INTO {} ({{}}) VALUES ({{}})\", columns.join(\", \"), placeholders.join(\", \"));\n",
            insertable_table_name,
        ));
    }

    // Execute
    if has_returning {
        let row_type = effective_row.as_ref().unwrap();
        out.push_str("    let rows = client.query(&sql, &bind_params).await?;\n");
        out.push_str(&format!(
            "    rows.iter().map(|row| {}::from_row(row)).collect()\n",
            row_type,
        ));
    } else {
        out.push_str("    let count = client.execute(&sql, &bind_params).await?;\n");
        out.push_str("    Ok(count)\n");
    }
    out.push_str("}\n\n");
}

// ── ChangeSet (dynamic SET) codegen ─────────────────────────────────────

/// Check if any parameter is `ChangeSet<table>` and matches the set_variable.
/// Returns Some((variable_name, table_name)) if found.
pub(super) fn find_updatable_param(
    args: &[Argument],
    set_variable: Option<&str>,
) -> Option<(String, String)> {
    // If there's an explicit set_variable, find the matching ChangeSet param
    if let Some(var_name) = set_variable {
        for arg in args {
            if arg.name == var_name {
                if let Type::Utility(UtilityType::ChangeSet(ChangeSet(inner))) = &arg.typ {
                    if let Type::UserDefined(table_name) = inner.as_ref() {
                        return Some((var_name.to_string(), table_name.clone()));
                    }
                }
            }
        }
    }
    // Also check if any param is ChangeSet even without set_variable
    for arg in args {
        if let Type::Utility(UtilityType::ChangeSet(ChangeSet(inner))) = &arg.typ {
            if let Type::UserDefined(table_name) = inner.as_ref() {
                return Some((arg.name.clone(), table_name.clone()));
            }
        }
    }
    None
}

/// Generate the Changes struct, Default impl, Params struct, and dynamic UPDATE function
/// for a ChangeSet mutation.
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
    naming: &NamingConfig,
) {
    let name = &mutation.decl.name;
    let fn_name = to_snake_case(name);
    let pascal = to_pascal_case(name);
    let row_struct = format!("{}Row", pascal);

    let table_pascal = table_name_to_model_name(updatable_table_name);
    let changes_struct = naming.update_struct_name(&table_pascal);
    let params_struct = {
        let candidate = format!("{}Params", pascal);
        if candidate == changes_struct {
            format!("{}Input", pascal)
        } else {
            candidate
        }
    };

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
        let inner_type = column_to_rust_type(col, &table.name, registry, schema);
        out.push_str(&format!(
            "    pub {}: nextsql_backend_rust_runtime::UpdateField<{}>,\n",
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
            "            {}: nextsql_backend_rust_runtime::UpdateField::Unchanged,\n",
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
            "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n    params: &{},\n) -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
            fn_name, params_struct, row_type,
        ));
    } else {
        out.push_str(&format!(
            "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n    params: &{},\n) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {{\n",
            fn_name, params_struct,
        ));
    }

    out.push_str("    let mut set_parts: Vec<String> = Vec::new();\n");
    out.push_str("    let mut bind_params: Vec<&dyn nextsql_backend_rust_runtime::ToSqlParam> = Vec::new();\n");
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
            "    rows.iter().map(|row| {}::from_row(row)).collect()\n",
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
                AtomicExpression::MethodCall(mc) => {
                    let mut target_sql = String::new();
                    generate_where_sql_template(&mut target_sql, &mc.target, non_updatable_args);
                    match mc.method.as_str() {
                        "isNull" => out.push_str(&format!("{} IS NULL", target_sql)),
                        "isNotNull" => out.push_str(&format!("{} IS NOT NULL", target_sql)),
                        "like" => {
                            let mut arg_sql = String::new();
                            generate_where_sql_template(&mut arg_sql, &mc.args[0], non_updatable_args);
                            out.push_str(&format!("{} LIKE {}", target_sql, arg_sql));
                        }
                        "ilike" => {
                            let mut arg_sql = String::new();
                            generate_where_sql_template(&mut arg_sql, &mc.args[0], non_updatable_args);
                            out.push_str(&format!("{} ILIKE {}", target_sql, arg_sql));
                        }
                        "between" => {
                            let mut lo = String::new();
                            let mut hi = String::new();
                            generate_where_sql_template(&mut lo, &mc.args[0], non_updatable_args);
                            generate_where_sql_template(&mut hi, &mc.args[1], non_updatable_args);
                            out.push_str(&format!("{} BETWEEN {} AND {}", target_sql, lo, hi));
                        }
                        "eqAny" => {
                            let mut arg_sql = String::new();
                            generate_where_sql_template(&mut arg_sql, &mc.args[0], non_updatable_args);
                            out.push_str(&format!("{} = ANY({})", target_sql, arg_sql));
                        }
                        "contains" => {
                            let mut arg_sql = String::new();
                            generate_where_sql_template(&mut arg_sql, &mc.args[0], non_updatable_args);
                            out.push_str(&format!("{} @> {}", target_sql, arg_sql));
                        }
                        other => {
                            // Generic fallback: METHOD(target, args...)
                            let mut all_args = vec![target_sql];
                            for arg in &mc.args {
                                let mut arg_sql = String::new();
                                generate_where_sql_template(&mut arg_sql, arg, non_updatable_args);
                                all_args.push(arg_sql);
                            }
                            out.push_str(&format!("{}({})", other, all_args.join(", ")));
                        }
                    }
                }
                AtomicExpression::Call(call) => {
                    let mut args = Vec::new();
                    for arg in &call.args {
                        let mut arg_sql = String::new();
                        generate_where_sql_template(&mut arg_sql, arg, non_updatable_args);
                        args.push(arg_sql);
                    }
                    out.push_str(&format!("{}({})", call.callee, args.join(", ")));
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
