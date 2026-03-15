use std::collections::{HashMap, HashSet};
use nextsql_core::ast::*;
use crate::type_mapping::nextsql_type_to_rust;

use super::RowField;
use super::valtype::ValTypeRegistry;
use super::naming::to_snake_case;

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
        let rust_type = match &arg.typ {
            Type::UserDefined(name) if registry.is_valtype(name) => name.clone(),
            other => nextsql_type_to_rust(other),
        };
        out.push_str(&format!(
            "    pub {}: {},\n",
            to_snake_case(&arg.name),
            rust_type
        ));
    }
    out.push_str("}\n\n");

    // to_params impl – order must match SQL $1, $2, ...
    out.push_str(&format!("impl {} {{\n", struct_name));
    out.push_str("    fn to_params(&self) -> Vec<&dyn super::runtime::ToSqlParam> {\n");
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

/// Information about a Vec<ValType> param that needs unwrapping.
pub(super) struct VecValtypeParam {
    /// The snake_case field name in the Params struct
    field_name: String,
    /// The Rust type of the inner value (e.g., "uuid::Uuid")
    inner_rust_type: String,
}

/// Check if any argument has type Vec<ValType> and collect info for code generation.
pub(super) fn find_vec_valtype_params(args: &[Argument], param_order: &[String], registry: &ValTypeRegistry) -> Vec<VecValtypeParam> {
    let mut result = Vec::new();
    let arg_map: HashMap<String, &Argument> = args.iter().map(|a| (a.name.clone(), a)).collect();
    for pname in param_order {
        if let Some(arg) = arg_map.get(pname) {
            if let Type::Array(inner) = &arg.typ {
                if let Type::UserDefined(name) = inner.as_ref() {
                    if let Some(base_type) = registry.valtypes.get(name) {
                        result.push(VecValtypeParam {
                            field_name: to_snake_case(&arg.name),
                            inner_rust_type: nextsql_type_to_rust(&Type::BuiltIn(base_type.clone())),
                        });
                    }
                }
            }
        }
    }
    result
}

/// Generate the parameter list expression, handling Vec<ValType> conversions.
/// When there are Vec<ValType> params, generates local variables for the conversions
/// and returns code that must be placed before the query call + the param refs expression.
pub(super) fn generate_params_with_conversions(
    param_order: &[String],
    args: &[Argument],
    registry: &ValTypeRegistry,
) -> (String, String) {
    let vec_vt_params = find_vec_valtype_params(args, param_order, registry);
    if vec_vt_params.is_empty() {
        return (String::new(), "params.to_params()".to_string());
    }

    // Generate local conversion variables
    let mut prelude = String::new();
    let vt_fields: HashSet<String> = vec_vt_params.iter().map(|v| v.field_name.clone()).collect();
    for vtp in &vec_vt_params {
        prelude.push_str(&format!(
            "    let __{}_inner: Vec<{}> = params.{}.iter().map(|v| v.0).collect();\n",
            vtp.field_name, vtp.inner_rust_type, vtp.field_name
        ));
    }

    // Build the param list inline
    let mut refs = Vec::new();
    for pname in param_order {
        let field = to_snake_case(pname);
        if vt_fields.contains(&field) {
            refs.push(format!("&__{}_inner as &dyn super::runtime::ToSqlParam", field));
        } else {
            refs.push(format!("&params.{}", field));
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
    out.push_str("    fn from_row(row: &dyn super::runtime::Row) -> Self {\n");
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
