use std::collections::{HashMap, HashSet};
use nextsql_core::ast::*;
use crate::sql_gen::{GeneratedSql, WhenClauseType};
use crate::type_mapping::nextsql_type_to_rust;

use super::RowField;
use super::valtype::ValTypeRegistry;
use super::naming::to_snake_case;

/// Resolve the Rust type for a parameter, handling ValType wrappers in Array and Optional.
fn resolve_param_rust_type(typ: &Type, registry: &ValTypeRegistry) -> String {
    match typ {
        Type::UserDefined(name) if registry.is_valtype(name) => name.clone(),
        Type::Array(inner) => {
            let inner_type = resolve_param_rust_type(inner, registry);
            format!("Vec<{}>", inner_type)
        }
        Type::Optional(inner) => {
            let inner_type = resolve_param_rust_type(inner, registry);
            format!("Option<{}>", inner_type)
        }
        other => nextsql_type_to_rust(other),
    }
}

/// Emit a Params struct + its `to_params` impl.
pub(super) fn emit_params_struct(
    out: &mut String,
    struct_name: &str,
    args: &[Argument],
    param_order: &[String],
    registry: &ValTypeRegistry,
) {
    // Struct definition
    out.push_str(&format!("pub struct {} {{\n", struct_name));
    for arg in args {
        let rust_type = resolve_param_rust_type(&arg.typ, registry);
        out.push_str(&format!(
            "    pub {}: {},\n",
            to_snake_case(&arg.name),
            rust_type
        ));
    }
    out.push_str("}\n\n");

    // to_params impl – order must match SQL $1, $2, ...
    out.push_str(&format!("impl {} {{\n", struct_name));
    out.push_str("    pub fn to_params(&self) -> Vec<&dyn super::runtime::ToSqlParam> {\n");
    out.push_str("        vec![");
    for (i, pname) in param_order.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&format!("&self.{}", to_snake_case(pname)));
    }
    out.push_str("]\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");
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

/// Generate the parameter list expression, handling ValType conversions.
/// When there are ValType params, generates local variables for the conversions
/// and returns code that must be placed before the query call + the param refs expression.
pub(super) fn generate_params_with_conversions(
    param_order: &[String],
    args: &[Argument],
    registry: &ValTypeRegistry,
) -> (String, String) {
    let vt_params = find_valtype_params(args, param_order, registry);
    if vt_params.is_empty() {
        return (String::new(), "params.to_params()".to_string());
    }

    // Generate local conversion variables
    let mut prelude = String::new();
    for vtp in &vt_params {
        match vtp.kind {
            ValtypeParamKind::Vec => {
                prelude.push_str(&format!(
                    "    let __{}_inner: Vec<{}> = params.{}.iter().map(|v| v.0.clone()).collect();\n",
                    vtp.field_name, vtp.inner_rust_type, vtp.field_name
                ));
            }
            ValtypeParamKind::Optional => {
                prelude.push_str(&format!(
                    "    let __{}_inner: Option<{}> = params.{}.as_ref().map(|v| v.0.clone());\n",
                    vtp.field_name, vtp.inner_rust_type, vtp.field_name
                ));
            }
            ValtypeParamKind::Plain => {
                // No prelude needed, we reference .0 inline
            }
        }
    }

    // Build the param list inline
    let mut refs = Vec::new();
    for pname in param_order {
        let field = to_snake_case(pname);
        if let Some(vtp) = vt_params.iter().find(|v| v.field_name == field) {
            match vtp.kind {
                ValtypeParamKind::Vec | ValtypeParamKind::Optional => {
                    refs.push(format!("&__{}_inner as &dyn super::runtime::ToSqlParam", field));
                }
                ValtypeParamKind::Plain => {
                    refs.push(format!("&params.{}.0 as &dyn super::runtime::ToSqlParam", field));
                }
            }
        } else {
            refs.push(format!("&params.{} as &dyn super::runtime::ToSqlParam", field));
        }
    }

    let params_expr = format!("vec![{}]", refs.join(", "));
    (prelude, params_expr)
}

/// Emit a Row struct + its `from_row` impl.
pub(super) fn emit_row_struct(out: &mut String, struct_name: &str, fields: &[RowField]) {
    out.push_str(&format!("pub struct {} {{\n", struct_name));
    for f in fields {
        out.push_str(&format!("    pub {}: {},\n", to_snake_case(&f.name), f.rust_type));
    }
    out.push_str("}\n\n");

    out.push_str(&format!("impl {} {{\n", struct_name));
    out.push_str("    pub fn from_row(row: &dyn super::runtime::Row) -> Self {\n");
    out.push_str("        Self {\n");
    for (i, f) in fields.iter().enumerate() {
        let getter_expr = if let Some(ref wrapper) = f.valtype_wrapper {
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
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");
}

/// Emit an async executor function that calls `client.query()` and returns `Vec<Row>`.
/// If `param_conversion` is Some, it contains (prelude_code, params_expression) for
/// handling Vec<ValType> conversions. Otherwise uses `params.to_params()`.
pub(super) fn emit_query_fn(
    out: &mut String,
    fn_name: &str,
    params_struct: &str,
    row_struct: &str,
    sql: &str,
    param_conversion: Option<&(String, String)>,
) {
    out.push_str(&format!(
        "pub async fn {}(\n    client: &(impl super::runtime::Client + ?Sized),\n    params: &{},\n) -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
        fn_name, params_struct, row_struct
    ));
    if let Some((prelude, params_expr)) = param_conversion {
        out.push_str(prelude);
        out.push_str(&format!(
            "    let rows = client.query(\n        \"{}\",\n        &{},\n    ).await?;\n",
            sql.replace('\"', "\\\""), params_expr
        ));
    } else {
        out.push_str(&format!(
            "    let rows = client.query(\n        \"{}\",\n        &params.to_params(),\n    ).await?;\n",
            sql.replace('\"', "\\\"")
        ));
    }
    out.push_str(&format!(
        "    Ok(rows.iter().map(|row| {}::from_row(row)).collect())\n",
        row_struct
    ));
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
        "pub async fn {}(\n    client: &(impl super::runtime::Client + ?Sized),\n) -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
        fn_name, row_struct
    ));
    out.push_str(&format!(
        "    let rows = client.query(\n        \"{}\",\n        &[],\n    ).await?;\n",
        sql.replace('\"', "\\\"")
    ));
    out.push_str(&format!(
        "    Ok(rows.iter().map(|row| {}::from_row(row)).collect())\n",
        row_struct
    ));
    out.push_str("}\n\n");
}

/// Emit an async executor function that calls `client.execute()` and returns `u64`.
pub(super) fn emit_execute_fn(
    out: &mut String,
    fn_name: &str,
    params_struct: &str,
    sql: &str,
    param_conversion: Option<&(String, String)>,
) {
    out.push_str(&format!(
        "pub async fn {}(\n    client: &(impl super::runtime::Client + ?Sized),\n    params: &{},\n) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {{\n",
        fn_name, params_struct
    ));
    if let Some((prelude, params_expr)) = param_conversion {
        out.push_str(prelude);
        out.push_str(&format!(
            "    let count = client.execute(\n        \"{}\",\n        &{},\n    ).await?;\n",
            sql.replace('\"', "\\\""), params_expr
        ));
    } else {
        out.push_str(&format!(
            "    let count = client.execute(\n        \"{}\",\n        &params.to_params(),\n    ).await?;\n",
            sql.replace('\"', "\\\"")
        ));
    }
    out.push_str("    Ok(count)\n");
    out.push_str("}\n\n");
}

/// Emit an async executor function that calls `client.execute()` without params.
pub(super) fn emit_execute_fn_no_params(
    out: &mut String,
    fn_name: &str,
    sql: &str,
) {
    out.push_str(&format!(
        "pub async fn {}(\n    client: &(impl super::runtime::Client + ?Sized),\n) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {{\n",
        fn_name
    ));
    out.push_str(&format!(
        "    let count = client.execute(\n        \"{}\",\n        &[],\n    ).await?;\n",
        sql.replace('\"', "\\\"")
    ));
    out.push_str("    Ok(count)\n");
    out.push_str("}\n\n");
}

/// Emit an async query function with dynamic SQL building for when-clause queries.
/// This generates runtime SQL construction where:
/// - Static params always get indices $1..$M
/// - Dynamic (when-clause) params get indices $M+1.. based on which clauses are active
pub(super) fn emit_dynamic_query_fn(
    out: &mut String,
    fn_name: &str,
    params_struct: &str,
    row_struct: &str,
    gen: &GeneratedSql,
    args: &[Argument],
    registry: &ValTypeRegistry,
) {
    // Function signature
    out.push_str(&format!(
        "#[allow(unused_assignments)]\npub async fn {}(\n    client: &(impl super::runtime::Client + ?Sized),\n    params: &{},\n) -> Result<Vec<{}>, Box<dyn std::error::Error + Send + Sync>> {{\n",
        fn_name, params_struct, row_struct
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
    out.push_str("    let mut bind_params: Vec<&dyn super::runtime::ToSqlParam> = Vec::new();\n");

    // Check if any static param needs ValType conversion
    let vt_params = find_valtype_params(args, &gen.params, registry);
    let vt_fields: HashSet<String> = vt_params.iter().map(|v| v.field_name.clone()).collect();

    // Generate ValType conversion prelude for static params
    for vtp in &vt_params {
        match vtp.kind {
            ValtypeParamKind::Vec => {
                out.push_str(&format!(
                    "    let __{}_inner: Vec<{}> = params.{}.iter().map(|v| v.0.clone()).collect();\n",
                    vtp.field_name, vtp.inner_rust_type, vtp.field_name
                ));
            }
            ValtypeParamKind::Optional => {
                out.push_str(&format!(
                    "    let __{}_inner: Option<{}> = params.{}.as_ref().map(|v| v.0.clone());\n",
                    vtp.field_name, vtp.inner_rust_type, vtp.field_name
                ));
            }
            ValtypeParamKind::Plain => {
                // No prelude needed
            }
        }
    }

    // Bind static params
    let static_param_count = gen.params.len();
    for pname in &gen.params {
        let field = to_snake_case(pname);
        if let Some(vtp) = vt_params.iter().find(|v| v.field_name == field) {
            match vtp.kind {
                ValtypeParamKind::Vec | ValtypeParamKind::Optional => {
                    out.push_str(&format!("    bind_params.push(&__{}_inner as &dyn super::runtime::ToSqlParam);\n", field));
                }
                ValtypeParamKind::Plain => {
                    out.push_str(&format!("    bind_params.push(&params.{}.0 as &dyn super::runtime::ToSqlParam);\n", field));
                }
            }
        } else {
            out.push_str(&format!("    bind_params.push(&params.{});\n", field));
        }
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

            // Check if the condition variable's type is Optional
            let is_optional = if let Some(arg) = arg_map.get(wc.condition_var.as_deref().unwrap_or("")) {
                matches!(&arg.typ, Type::Optional(_))
            } else {
                true // assume optional if not found
            };

            if is_optional {
                out.push_str(&format!("    if params.{}.is_some() {{\n", condition_var));
            } else {
                // For non-optional types, always include
                out.push_str("    {\n");
            }

            // Build the condition SQL with dynamic param indices
            // The template has `${}` placeholders that need to be replaced with `idx`
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
                // Bind the param
                let param_field = to_snake_case(&wc.params_in_clause[0]);
                // Check if this param needs ValType conversion
                if vt_fields.contains(&param_field) {
                    if let Some(vtp) = vt_params.iter().find(|v| v.field_name == param_field) {
                        match vtp.kind {
                            ValtypeParamKind::Vec | ValtypeParamKind::Optional => {
                                out.push_str(&format!("        bind_params.push(&__{}_inner as &dyn super::runtime::ToSqlParam);\n", param_field));
                            }
                            ValtypeParamKind::Plain => {
                                out.push_str(&format!("        bind_params.push(&params.{}.0 as &dyn super::runtime::ToSqlParam);\n", param_field));
                            }
                        }
                    }
                } else {
                    out.push_str(&format!("        bind_params.push(&params.{});\n", param_field));
                }
                out.push_str("        idx += 1;\n");
            } else {
                // Multiple params in clause
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
                // Bind all params
                for p in &wc.params_in_clause {
                    let param_field = to_snake_case(p);
                    if vt_fields.contains(&param_field) {
                        if let Some(vtp) = vt_params.iter().find(|v| v.field_name == param_field) {
                            match vtp.kind {
                                ValtypeParamKind::Vec | ValtypeParamKind::Optional => {
                                    out.push_str(&format!("        bind_params.push(&__{}_inner as &dyn super::runtime::ToSqlParam);\n", param_field));
                                }
                                ValtypeParamKind::Plain => {
                                    out.push_str(&format!("        bind_params.push(&params.{}.0 as &dyn super::runtime::ToSqlParam);\n", param_field));
                                }
                            }
                        }
                    } else {
                        out.push_str(&format!("        bind_params.push(&params.{});\n", param_field));
                    }
                }
                out.push_str(&format!("        idx += {};\n", param_count));
            }

            out.push_str("    }\n");
        }
    }

    // Build the SQL string dynamically
    // Extract the static SQL parts. The gen.sql has the complete static SQL
    // (without when-clause contributions). For dynamic queries, we need to
    // reconstruct the SQL with dynamic WHERE conditions.
    //
    // Strategy: Split the static SQL at " WHERE " and " ORDER BY " to insert
    // dynamic conditions.
    let static_sql = &gen.sql;

    // Find the positions of WHERE and ORDER BY in the static SQL
    let where_pos = static_sql.find(" WHERE ");
    let order_by_pos = static_sql.find(" ORDER BY ");
    let group_by_pos = static_sql.find(" GROUP BY ");
    let limit_pos = static_sql.find(" LIMIT ");
    let offset_pos = static_sql.find(" OFFSET ");
    let for_update_pos = static_sql.find(" FOR UPDATE");

    // Find the first "tail" keyword position after WHERE
    let tail_start = [order_by_pos, group_by_pos, limit_pos, offset_pos, for_update_pos]
        .iter()
        .filter_map(|p| *p)
        .min();

    // Extract parts
    let (before_where, static_where, after_where) = if let Some(wp) = where_pos {
        let before = &static_sql[..wp];
        if let Some(ts) = tail_start {
            if ts > wp {
                let where_content = &static_sql[wp + 7..ts]; // skip " WHERE "
                let after = &static_sql[ts..];
                (before, Some(where_content.to_string()), after.to_string())
            } else {
                // tail is before WHERE - shouldn't happen normally
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

    // Build the WHERE clause
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

    // Handle dynamic ORDER BY (when clauses with orderBy type)
    let has_static_order_by = order_by_pos.is_some();
    if !when_order_bys.is_empty() {
        // If there are dynamic order-by when-clauses, we need to handle them
        // For now, extract the static ORDER BY and make it conditional
        out.push_str("\n    // Build ORDER BY clause\n");
        out.push_str("    let order_by_clause = {\n");

        if has_static_order_by {
            // Extract the static ORDER BY content
            let ob_start = order_by_pos.unwrap() + 10; // skip " ORDER BY "
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
                    out.push_str(&format!("        if params.{}.is_some() {{\n", cond_field));
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

    // The format string: base_sql + {} for where + {} for order_by + rest
    if has_static_where || has_dynamic_wheres {
        out.push_str("{}"); // where_clause placeholder
    }

    // Handle the tail (ORDER BY, LIMIT, etc.) from the static SQL
    // We need to exclude ORDER BY if it's being handled dynamically
    let mut tail = after_where.clone();
    if !when_order_bys.is_empty() {
        // Remove the static ORDER BY from the tail - we handle it dynamically
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
        out.push_str("{}"); // order_by_clause placeholder
    }

    // Add remaining static tail
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

    // Execute query
    out.push_str("    let rows = client.query(&sql, &bind_params).await?;\n");
    out.push_str(&format!(
        "    Ok(rows.iter().map(|row| {}::from_row(row)).collect())\n",
        row_struct
    ));
    out.push_str("}\n\n");
}

/// Emit an async mutation function that calls `client.query()` with returning and returns `Vec<Row>`.
pub(super) fn emit_mutation_query_fn(
    out: &mut String,
    fn_name: &str,
    params_struct: &str,
    row_struct: &str,
    sql: &str,
    param_conversion: Option<&(String, String)>,
) {
    emit_query_fn(out, fn_name, params_struct, row_struct, sql, param_conversion);
}
