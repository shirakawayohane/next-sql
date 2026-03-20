use std::collections::HashMap;
use nextsql_core::ast::*;
use nextsql_core::schema::DatabaseSchema;
use crate::sql_gen::{GeneratedSql, WhenClauseType};
use crate::type_mapping::{nextsql_type_to_rust, is_enum_type, enum_rust_type};

use super::RowField;
use super::InputTypeRegistry;
use super::valtype::ValTypeRegistry;
use super::naming::to_snake_case;

/// Resolve the Rust type for a parameter, handling ValType wrappers, enums, input types, Array and Optional.
fn resolve_param_rust_type(typ: &Type, registry: &ValTypeRegistry, input_registry: &InputTypeRegistry, schema: &DatabaseSchema) -> String {
    match typ {
        Type::UserDefined(name) if registry.is_valtype(name) => name.clone(),
        Type::UserDefined(name) if is_enum_type(name, schema) => enum_rust_type(name),
        Type::UserDefined(name) if input_registry.get(name).is_some() => name.clone(),
        Type::Array(inner) => {
            let inner_type = resolve_param_rust_type(inner, registry, input_registry, schema);
            format!("Vec<{}>", inner_type)
        }
        Type::Optional(inner) => {
            let inner_type = resolve_param_rust_type(inner, registry, input_registry, schema);
            format!("Option<{}>", inner_type)
        }
        other => nextsql_type_to_rust(other),
    }
}

/// Kind of valtype unwrapping needed for a parameter.
enum ValtypeParamKind {
    /// Vec<ValType> -> Vec<Inner>
    Vec,
    /// Option<ValType> -> Option<Inner>
    Optional,
    /// ValType -> Inner (pass .0 directly)
    Plain,
}

/// Information about a ValType param that needs unwrapping.
pub(super) struct ValtypeParam {
    /// The snake_case field name in the Params struct
    field_name: String,
    /// The Rust type of the inner value (e.g., "uuid::Uuid")
    inner_rust_type: String,
    /// What kind of unwrapping is needed
    kind: ValtypeParamKind,
}

/// Check if any argument uses a ValType and collect info for code generation.
pub(super) fn find_valtype_params(args: &[Argument], param_order: &[String], registry: &ValTypeRegistry) -> Vec<ValtypeParam> {
    let mut result = Vec::new();
    let arg_map: HashMap<String, &Argument> = args.iter().map(|a| (a.name.clone(), a)).collect();
    for pname in param_order {
        if let Some(arg) = arg_map.get(pname) {
            match &arg.typ {
                Type::Array(inner) => {
                    if let Type::UserDefined(name) = inner.as_ref() {
                        if let Some(base_type) = registry.valtypes.get(name) {
                            result.push(ValtypeParam {
                                field_name: to_snake_case(&arg.name),
                                inner_rust_type: nextsql_type_to_rust(&Type::BuiltIn(base_type.clone())),
                                kind: ValtypeParamKind::Vec,
                            });
                        }
                    }
                }
                Type::Optional(inner) => {
                    if let Type::UserDefined(name) = inner.as_ref() {
                        if let Some(base_type) = registry.valtypes.get(name) {
                            result.push(ValtypeParam {
                                field_name: to_snake_case(&arg.name),
                                inner_rust_type: nextsql_type_to_rust(&Type::BuiltIn(base_type.clone())),
                                kind: ValtypeParamKind::Optional,
                            });
                        }
                    }
                }
                Type::UserDefined(name) => {
                    if let Some(base_type) = registry.valtypes.get(name) {
                        result.push(ValtypeParam {
                            field_name: to_snake_case(&arg.name),
                            inner_rust_type: nextsql_type_to_rust(&Type::BuiltIn(base_type.clone())),
                            kind: ValtypeParamKind::Plain,
                        });
                    }
                }
                _ => {}
            }
        }
    }
    result
}

/// Emit a Row struct + its `from_row` impl.
pub(super) fn emit_row_struct(out: &mut String, struct_name: &str, fields: &[RowField]) {
    out.push_str(&format!("pub struct {} {{\n", struct_name));
    for f in fields {
        out.push_str(&format!("    pub {}: {},\n", to_snake_case(&f.name), f.rust_type));
    }
    out.push_str("}\n\n");

    out.push_str(&format!("impl {} {{\n", struct_name));
    out.push_str("    pub fn from_row(row: &dyn nextsql_backend_rust_runtime::Row) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {\n");
    out.push_str("        Ok(Self {\n");
    for (i, f) in fields.iter().enumerate() {
        let getter_expr = if f.enum_parse {
            // Enum field: parse from string
            if f.getter.contains("get_opt_") {
                format!("row.{}({}).map(|s| s.parse()).transpose()?", f.getter, i)
            } else {
                format!("row.{}({}).parse()?", f.getter, i)
            }
        } else if let Some(ref wrapper) = f.valtype_wrapper {
            // Check if the getter is an optional variant
            if f.getter.contains("get_opt_") {
                format!("row.{}({}).map({})", f.getter, i, wrapper)
            } else {
                format!("{}(row.{}({}))", wrapper, f.getter, i)
            }
        } else {
            format!("row.{}({})", f.getter, i)
        };
        out.push_str(&format!(
            "            {}: {},\n",
            to_snake_case(&f.name),
            getter_expr,
        ));
    }
    out.push_str("        })\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");
}

/// Emit an async executor function that calls `client.query()` without params.
pub(super) fn emit_query_fn_no_params(
    out: &mut String,
    fn_name: &str,
    row_struct: &str,
    sql: &str,
) {
    out.push_str(&format!(
        "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n) -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
        fn_name, row_struct
    ));
    out.push_str(&format!(
        "    let rows = client.query(\n        \"{}\",\n        &[],\n    ).await?;\n",
        sql.replace('\"', "\\\"")
    ));
    out.push_str(&format!(
        "    rows.iter().map(|row| {}::from_row(row)).collect()\n",
        row_struct
    ));
    out.push_str("}\n\n");
}

/// Emit an async executor function that calls `client.execute()` without params.
pub(super) fn emit_execute_fn_no_params(
    out: &mut String,
    fn_name: &str,
    sql: &str,
) {
    out.push_str(&format!(
        "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {{\n",
        fn_name
    ));
    out.push_str(&format!(
        "    let count = client.execute(\n        \"{}\",\n        &[],\n    ).await?;\n",
        sql.replace('\"', "\\\"")
    ));
    out.push_str("    Ok(count)\n");
    out.push_str("}\n\n");
}

// ── Input struct emission ──────────────────────────────────────────────

/// Emit a struct definition for an `input` type declaration.
pub(super) fn emit_input_struct(out: &mut String, input: &InputType, registry: &ValTypeRegistry, input_registry: &InputTypeRegistry, schema: &DatabaseSchema) {
    out.push_str(&format!("pub struct {} {{\n", input.name));
    for field in &input.fields {
        let rust_type = resolve_param_rust_type(&field.typ, registry, input_registry, schema);
        out.push_str(&format!("    pub {}: {},\n", to_snake_case(&field.name), rust_type));
    }
    out.push_str("}\n\n");
}

// ── Individual parameters emission ─────────────────────────────────────

/// Determine if a parameter key refers to an input field (contains a dot).
fn is_input_param(param_key: &str) -> bool {
    param_key.contains('.')
}

/// Split a dotted param key like "input.id" into (var_name, field_name).
fn split_input_param(param_key: &str) -> (&str, &str) {
    let dot = param_key.find('.').unwrap();
    (&param_key[..dot], &param_key[dot + 1..])
}

/// Generate a single parameter binding expression for use in the param array.
/// For plain args: `&arg_name as &dyn nextsql_backend_rust_runtime::ToSqlParam`
/// For input field args: `&input_name.field_name as &dyn nextsql_backend_rust_runtime::ToSqlParam`
/// Handles ValType unwrapping inline.
fn gen_param_binding(
    param_key: &str,
    args: &[Argument],
    registry: &ValTypeRegistry,
    input_registry: &InputTypeRegistry,
) -> String {
    if is_input_param(param_key) {
        let (var_name, field_name) = split_input_param(param_key);
        let var_snake = to_snake_case(var_name);
        let field_snake = to_snake_case(field_name);
        // Look up input type to check for ValType wrapping on the field
        let needs_valtype_unwrap = args.iter()
            .find(|a| a.name == var_name)
            .and_then(|a| match &a.typ {
                Type::UserDefined(name) => input_registry.get(name),
                _ => None,
            })
            .and_then(|input_type| input_type.fields.iter().find(|f| f.name == field_name))
            .map(|f| match &f.typ {
                Type::UserDefined(name) => registry.is_valtype(name),
                _ => false,
            })
            .unwrap_or(false);

        if needs_valtype_unwrap {
            format!("&{}.{}.0 as &dyn nextsql_backend_rust_runtime::ToSqlParam", var_snake, field_snake)
        } else {
            format!("&{}.{} as &dyn nextsql_backend_rust_runtime::ToSqlParam", var_snake, field_snake)
        }
    } else {
        let arg = args.iter().find(|a| a.name == param_key);
        let field = to_snake_case(param_key);
        // Note: individual params are already references (&T), so we don't add & again.
        // Exception: ValType .0 access needs & because it dereferences into the inner value.
        match arg.map(|a| &a.typ) {
            Some(Type::UserDefined(name)) if registry.is_valtype(name) => {
                format!("&{}.0 as &dyn nextsql_backend_rust_runtime::ToSqlParam", field)
            }
            Some(Type::Array(inner)) if matches!(inner.as_ref(), Type::UserDefined(name) if registry.is_valtype(name)) => {
                // Vec<ValType> needs conversion — handled via prelude
                format!("&__{}_inner as &dyn nextsql_backend_rust_runtime::ToSqlParam", field)
            }
            Some(Type::Optional(inner)) if matches!(inner.as_ref(), Type::UserDefined(name) if registry.is_valtype(name)) => {
                format!("&__{}_inner as &dyn nextsql_backend_rust_runtime::ToSqlParam", field)
            }
            _ => {
                format!("{} as &dyn nextsql_backend_rust_runtime::ToSqlParam", field)
            }
        }
    }
}

/// Generate ValType conversion prelude for individual params (Vec/Optional ValType unwrapping).
fn gen_individual_valtype_prelude(
    args: &[Argument],
    param_order: &[String],
    registry: &ValTypeRegistry,
) -> String {
    let mut prelude = String::new();
    let vt_params = find_valtype_params(args, param_order, registry);
    for vtp in &vt_params {
        match vtp.kind {
            ValtypeParamKind::Vec => {
                prelude.push_str(&format!(
                    "    let __{}_inner: Vec<{}> = {}.iter().map(|v| v.0.clone()).collect();\n",
                    vtp.field_name, vtp.inner_rust_type, vtp.field_name
                ));
            }
            ValtypeParamKind::Optional => {
                prelude.push_str(&format!(
                    "    let __{}_inner: Option<{}> = {}.as_ref().map(|v| v.0.clone());\n",
                    vtp.field_name, vtp.inner_rust_type, vtp.field_name
                ));
            }
            ValtypeParamKind::Plain => {} // handled inline
        }
    }
    prelude
}

/// Emit an async query function with individual parameters (no Params struct).
pub(super) fn emit_query_fn_individual_params(
    out: &mut String,
    fn_name: &str,
    args: &[Argument],
    param_order: &[String],
    row_struct: &str,
    sql: &str,
    registry: &ValTypeRegistry,
    input_registry: &InputTypeRegistry,
    schema: &DatabaseSchema,
) {
    // Function signature
    out.push_str(&format!(
        "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n",
        fn_name,
    ));
    emit_individual_fn_params(out, args, registry, input_registry, schema);
    out.push_str(&format!(
        ") -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
        row_struct,
    ));

    // ValType conversion prelude
    let prelude = gen_individual_valtype_prelude(args, param_order, registry);
    out.push_str(&prelude);

    // Build params array
    let bindings: Vec<String> = param_order.iter()
        .map(|p| gen_param_binding(p, args, registry, input_registry))
        .collect();

    out.push_str(&format!(
        "    let rows = client.query(\n        \"{}\",\n        &[{}],\n    ).await?;\n",
        sql.replace('\"', "\\\""),
        bindings.join(", "),
    ));
    out.push_str(&format!(
        "    rows.iter().map(|row| {}::from_row(row)).collect()\n",
        row_struct,
    ));
    out.push_str("}\n\n");
}

/// Emit an async execute function with individual parameters (no Params struct).
pub(super) fn emit_execute_fn_individual_params(
    out: &mut String,
    fn_name: &str,
    args: &[Argument],
    param_order: &[String],
    sql: &str,
    registry: &ValTypeRegistry,
    input_registry: &InputTypeRegistry,
    schema: &DatabaseSchema,
) {
    out.push_str(&format!(
        "pub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n",
        fn_name,
    ));
    emit_individual_fn_params(out, args, registry, input_registry, schema);
    out.push_str(") -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {\n");

    let prelude = gen_individual_valtype_prelude(args, param_order, registry);
    out.push_str(&prelude);

    let bindings: Vec<String> = param_order.iter()
        .map(|p| gen_param_binding(p, args, registry, input_registry))
        .collect();

    out.push_str(&format!(
        "    let count = client.execute(\n        \"{}\",\n        &[{}],\n    ).await?;\n",
        sql.replace('\"', "\\\""),
        bindings.join(", "),
    ));
    out.push_str("    Ok(count)\n");
    out.push_str("}\n\n");
}

/// Emit an async query function with dynamic SQL building for when-clause queries,
/// using individual parameters instead of a Params struct.
pub(super) fn emit_dynamic_query_fn_individual_params(
    out: &mut String,
    fn_name: &str,
    args: &[Argument],
    row_struct: &str,
    gen: &GeneratedSql,
    registry: &ValTypeRegistry,
    input_registry: &InputTypeRegistry,
    schema: &DatabaseSchema,
) {
    // Function signature
    out.push_str(&format!(
        "#[allow(unused_assignments)]\npub async fn {}(\n    client: &(impl nextsql_backend_rust_runtime::QueryExecutor + ?Sized),\n",
        fn_name,
    ));
    emit_individual_fn_params(out, args, registry, input_registry, schema);
    out.push_str(&format!(
        ") -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
        row_struct,
    ));

    // Build a map of arg name -> Argument for type lookups
    let arg_map: HashMap<String, &Argument> = args.iter().map(|a| (a.name.clone(), a)).collect();

    // Separate when-clauses by type
    let when_wheres: Vec<_> = gen.when_clauses.iter()
        .filter(|wc| wc.clause_type == WhenClauseType::Where)
        .collect();
    let when_order_bys: Vec<_> = gen.when_clauses.iter()
        .filter(|wc| wc.clause_type == WhenClauseType::OrderBy)
        .collect();

    // Static params - always bound
    out.push_str("    let mut bind_params: Vec<&dyn nextsql_backend_rust_runtime::ToSqlParam> = Vec::new();\n");

    // ValType conversion prelude
    let prelude = gen_individual_valtype_prelude(args, &gen.params, registry);
    out.push_str(&prelude);

    // Bind static params
    let static_param_count = gen.params.len();
    for pname in &gen.params {
        let binding = gen_param_binding(pname, args, registry, input_registry);
        out.push_str(&format!("    bind_params.push({});\n", binding));
    }

    out.push_str(&format!("    let mut idx = {}usize;\n\n", static_param_count + 1));

    // Dynamic WHERE conditions
    if !when_wheres.is_empty() {
        out.push_str("    let mut conditions: Vec<String> = Vec::new();\n");

        for wc in &when_wheres {
            let condition_var = match &wc.condition_var {
                Some(v) => to_snake_case(v),
                None => continue,
            };

            let is_optional = if let Some(arg) = arg_map.get(wc.condition_var.as_deref().unwrap_or("")) {
                matches!(&arg.typ, Type::Optional(_))
            } else {
                true
            };

            if is_optional {
                out.push_str(&format!("    if {}.is_some() {{\n", condition_var));
            } else {
                out.push_str("    {\n");
            }

            let param_count = wc.params_in_clause.len();
            if param_count == 0 {
                out.push_str(&format!(
                    "        conditions.push(\"{}\".to_string());\n",
                    wc.clause_sql,
                ));
            } else if param_count == 1 {
                out.push_str(&format!(
                    "        conditions.push(format!(\"{}\", idx));\n",
                    wc.clause_sql,
                ));
                let binding = gen_param_binding(&wc.params_in_clause[0], args, registry, input_registry);
                out.push_str(&format!("        bind_params.push({});\n", binding));
                out.push_str("        idx += 1;\n");
            } else {
                out.push_str(&format!(
                    "        conditions.push(format!(\"{}\"",
                    wc.clause_sql,
                ));
                for i in 0..param_count {
                    if i == 0 {
                        out.push_str(", idx");
                    } else {
                        out.push_str(&format!(", idx + {}", i));
                    }
                }
                out.push_str("));\n");
                for p in &wc.params_in_clause {
                    let binding = gen_param_binding(p, args, registry, input_registry);
                    out.push_str(&format!("        bind_params.push({});\n", binding));
                }
                out.push_str(&format!("        idx += {};\n", param_count));
            }

            out.push_str("    }\n");
        }
    }

    // Build the SQL string dynamically (same logic as emit_dynamic_query_fn)
    let static_sql = &gen.sql;
    let where_pos = static_sql.find(" WHERE ");
    let order_by_pos = static_sql.find(" ORDER BY ");
    let group_by_pos = static_sql.find(" GROUP BY ");
    let limit_pos = static_sql.find(" LIMIT ");
    let offset_pos = static_sql.find(" OFFSET ");
    let for_update_pos = static_sql.find(" FOR UPDATE");

    let tail_start = [order_by_pos, group_by_pos, limit_pos, offset_pos, for_update_pos]
        .iter()
        .filter_map(|p| *p)
        .min();

    let (before_where, static_where, after_where) = if let Some(wp) = where_pos {
        let before = &static_sql[..wp];
        if let Some(ts) = tail_start {
            if ts > wp {
                let where_content = &static_sql[wp + 7..ts];
                let after = &static_sql[ts..];
                (before, Some(where_content.to_string()), after.to_string())
            } else {
                let where_content = &static_sql[wp + 7..];
                (before, Some(where_content.to_string()), String::new())
            }
        } else {
            let where_content = &static_sql[wp + 7..];
            (before, Some(where_content.to_string()), String::new())
        }
    } else if let Some(ts) = tail_start {
        let before = &static_sql[..ts];
        let after = &static_sql[ts..];
        (before, None, after.to_string())
    } else {
        (static_sql.as_str(), None, String::new())
    };

    out.push_str("\n    // Build WHERE clause\n");
    let has_static_where = static_where.is_some();
    let has_dynamic_wheres = !when_wheres.is_empty();

    if has_static_where || has_dynamic_wheres {
        out.push_str("    let where_clause = {\n");
        out.push_str("        let mut parts: Vec<String> = Vec::new();\n");
        if let Some(ref sw) = static_where {
            out.push_str(&format!(
                "        parts.push(\"{}\".to_string());\n",
                sw.replace('\"', "\\\""),
            ));
        }
        if has_dynamic_wheres {
            out.push_str("        parts.extend(conditions);\n");
        }
        out.push_str("        if parts.is_empty() {\n");
        out.push_str("            String::new()\n");
        out.push_str("        } else {\n");
        out.push_str("            format!(\" WHERE {}\", parts.join(\" AND \"))\n");
        out.push_str("        }\n");
        out.push_str("    };\n");
    }

    let has_static_order_by = order_by_pos.is_some();
    if !when_order_bys.is_empty() {
        out.push_str("\n    // Build ORDER BY clause\n");
        out.push_str("    let order_by_clause = {\n");
        if has_static_order_by {
            let ob_start = order_by_pos.unwrap() + 10;
            let ob_end = [group_by_pos, limit_pos, offset_pos, for_update_pos]
                .iter()
                .filter_map(|p| *p)
                .filter(|p| *p > order_by_pos.unwrap())
                .min()
                .unwrap_or(static_sql.len());
            let static_ob = &static_sql[ob_start..ob_end];
            out.push_str(&format!(
                "        let mut ob = \"{}\".to_string();\n",
                static_ob.trim().replace('\"', "\\\""),
            ));
        } else {
            out.push_str("        let mut ob = String::new();\n");
        }
        for wc in &when_order_bys {
            if let Some(ref cv) = wc.condition_var {
                let cond_field = to_snake_case(cv);
                let is_optional = if let Some(arg) = arg_map.get(cv.as_str()) {
                    matches!(&arg.typ, Type::Optional(_))
                } else {
                    true
                };
                if is_optional {
                    out.push_str(&format!("        if {}.is_some() {{\n", cond_field));
                } else {
                    out.push_str("        {\n");
                }
                out.push_str(&format!(
                    "            ob = \"{}\".to_string();\n",
                    wc.clause_sql.replace('\"', "\\\""),
                ));
                out.push_str("        }\n");
            }
        }
        out.push_str("        if ob.is_empty() { String::new() } else { format!(\" ORDER BY {}\", ob) }\n");
        out.push_str("    };\n");
    }

    // Build the final SQL string
    out.push_str("\n    let sql = format!(\n        \"{}");
    if has_static_where || has_dynamic_wheres {
        out.push_str("{}");
    }
    let mut tail = after_where.clone();
    if !when_order_bys.is_empty() {
        if let Some(ob_pos_in_tail) = tail.find(" ORDER BY ") {
            let ob_end_in_tail = [
                tail.find(" GROUP BY ").filter(|p| *p > ob_pos_in_tail),
                tail.find(" LIMIT ").filter(|p| *p > ob_pos_in_tail),
                tail.find(" OFFSET ").filter(|p| *p > ob_pos_in_tail),
                tail.find(" FOR UPDATE").filter(|p| *p > ob_pos_in_tail),
            ]
            .iter()
            .filter_map(|p| *p)
            .min()
            .unwrap_or(tail.len());
            tail = format!("{}{}", &tail[..ob_pos_in_tail], &tail[ob_end_in_tail..]);
        }
        out.push_str("{}");
    }
    out.push_str(&tail.replace('\"', "\\\"").replace('{', "{{").replace('}', "}}"));
    out.push_str("\",\n");
    out.push_str(&format!("        \"{}\",\n", before_where.replace('\"', "\\\"")));
    if has_static_where || has_dynamic_wheres {
        out.push_str("        where_clause,\n");
    }
    if !when_order_bys.is_empty() {
        out.push_str("        order_by_clause,\n");
    }
    out.push_str("    );\n");

    out.push_str("    let rows = client.query(&sql, &bind_params).await?;\n");
    out.push_str(&format!(
        "    rows.iter().map(|row| {}::from_row(row)).collect()\n",
        row_struct
    ));
    out.push_str("}\n\n");
}

/// Helper to emit individual function parameter declarations.
fn emit_individual_fn_params(
    out: &mut String,
    args: &[Argument],
    registry: &ValTypeRegistry,
    input_registry: &InputTypeRegistry,
    schema: &DatabaseSchema,
) {
    for arg in args {
        let rust_type = resolve_param_rust_type(&arg.typ, registry, input_registry, schema);
        // Check if this is an input type (pass by reference)
        let is_input = matches!(&arg.typ, Type::UserDefined(name) if input_registry.get(name).is_some());
        if is_input {
            out.push_str(&format!("    {}: &{},\n", to_snake_case(&arg.name), rust_type));
        } else {
            out.push_str(&format!("    {}: &{},\n", to_snake_case(&arg.name), rust_type));
        }
    }
}
