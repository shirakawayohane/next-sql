use nextsql_core::ast::*;
use nextsql_core::schema::DatabaseSchema;
use crate::sql_gen;
use crate::type_mapping::{nextsql_type_to_rust, nextsql_type_to_row_getter, is_enum_type, enum_rust_type};

use super::RowField;
use super::valtype::ValTypeRegistry;
use super::emit::emit_row_struct;

/// Resolve the type of a PropertyAccess expression by looking up relations.
/// Returns (rust_type, getter, valtype_wrapper) if resolution succeeds.
pub(super) fn resolve_property_access_type(
    pa: &PropertyAccess,
    _from_table: &str,
    schema: &DatabaseSchema,
    registry: &ValTypeRegistry,
    rel_registry: &sql_gen::RelationRegistry,
) -> Option<(String, String, Option<String>)> {
    let segments = decompose_property_chain_for_type(pa)?;
    if segments.len() < 3 {
        return None;
    }
    // segments: [table, relation, column] (possibly more for nested)
    let source_table = &segments[0];
    let relation_name = &segments[1];
    let column_name = &segments[2];

    let rel_info = rel_registry.lookup(source_table, relation_name)?;
    let target_table = &rel_info.returning_table;

    let ts = schema.get_table(target_table)?;
    let cs = ts.get_column(column_name)?;
    let typ = effective_type(&cs.column_type, cs.nullable);
    let field = make_row_field(column_name, target_table, &typ, registry, schema);
    Some((field.rust_type, field.getter, field.valtype_wrapper))
}

/// Try to resolve the type of an expression via relation access.
/// This handles PropertyAccess expressions that resolve through relations.
pub(super) fn resolve_expr_type_via_relation(
    expr: &Expression,
    from_table: &str,
    schema: &DatabaseSchema,
    registry: &ValTypeRegistry,
    rel_registry: &sql_gen::RelationRegistry,
) -> Option<(String, String, Option<String>)> {
    match expr {
        Expression::Atomic(AtomicExpression::PropertyAccess(pa)) => {
            resolve_property_access_type(pa, from_table, schema, registry, rel_registry)
        }
        Expression::Atomic(AtomicExpression::Column(col)) => {
            match col {
                Column::ExplicitTarget(table, column, _) => {
                    let rf = field_from_schema(table, column, schema, registry)?;
                    Some((rf.rust_type, rf.getter, rf.valtype_wrapper))
                }
                Column::ImplicitTarget(column, _) => {
                    let rf = field_from_schema(from_table, column, schema, registry)?;
                    Some((rf.rust_type, rf.getter, rf.valtype_wrapper))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Resolve the list of `RowField`s produced by a select clause.
pub(super) fn resolve_select_fields(
    select_exprs: &[SelectExpression],
    from_table: &str,
    schema: &DatabaseSchema,
    registry: &ValTypeRegistry,
    rel_registry: &sql_gen::RelationRegistry,
) -> Vec<RowField> {
    let mut fields = Vec::new();
    let mut col_idx: usize = 0;

    for sel in select_exprs {
        // If the select expression has an explicit alias, use it for any expression type
        if let Some(ref alias) = sel.alias {
            let name = alias.clone();
            if let Some(func_type) = infer_aggregate_function_type(&sel.expr) {
                let inner_expr = extract_aggregate_inner_expr(&sel.expr);
                let typ = aggregate_return_type_with_schema(&func_type, inner_expr, Some(schema));
                fields.push(RowField {
                    name,
                    rust_type: nextsql_type_to_rust(&typ),
                    getter: nextsql_type_to_row_getter(&typ),
                    valtype_wrapper: None,
                    enum_parse: false,
                });
            } else {
                // Try to resolve aliased PropertyAccess via relation
                let resolved = resolve_expr_type_via_relation(&sel.expr, from_table, schema, registry, rel_registry);
                let (rust_type, getter, valtype_wrapper) = resolved.unwrap_or_else(|| {
                    ("String".to_string(), "get_string".to_string(), None)
                });
                fields.push(RowField {
                    name,
                    rust_type,
                    getter,
                    valtype_wrapper,
                    enum_parse: false,
                });
            }
            col_idx += 1;
            continue;
        }

        match &sel.expr {
            Expression::Atomic(AtomicExpression::Column(col)) => {
                match col {
                    Column::WildcardOf(tbl, _) => {
                        let tbl_name = tbl.as_str();
                        if let Some(table_schema) = schema.get_table(tbl_name) {
                            for c in &table_schema.columns {
                                let typ = effective_type(&c.column_type, c.nullable);
                                fields.push(make_row_field(&c.name, tbl_name, &typ, registry, schema));
                                col_idx += 1;
                            }
                        }
                    }
                    Column::Wildcard(Some(..)) => {
                        let tbl_name = from_table;
                        if let Some(table_schema) = schema.get_table(tbl_name) {
                            for c in &table_schema.columns {
                                let typ = effective_type(&c.column_type, c.nullable);
                                fields.push(make_row_field(&c.name, tbl_name, &typ, registry, schema));
                                col_idx += 1;
                            }
                        }
                    }
                    Column::Wildcard(None) => {
                        // bare * – expand from_table
                        if let Some(table_schema) = schema.get_table(from_table) {
                            for c in &table_schema.columns {
                                let typ = effective_type(&c.column_type, c.nullable);
                                fields.push(make_row_field(&c.name, from_table, &typ, registry, schema));
                                col_idx += 1;
                            }
                        }
                    }
                    Column::ExplicitTarget(table, column, _) => {
                        if let Some(rf) = field_from_schema(table, column, schema, registry) {
                            fields.push(rf);
                        } else {
                            fields.push(fallback_field(column));
                        }
                        col_idx += 1;
                    }
                    Column::ImplicitTarget(column, _) => {
                        if let Some(rf) = field_from_schema(from_table, column, schema, registry) {
                            fields.push(rf);
                        } else {
                            fields.push(fallback_field(column));
                        }
                        col_idx += 1;
                    }
                }
            }
            // Aggregate expressions get default types
            Expression::Atomic(AtomicExpression::Aggregate(agg)) => {
                let (name, typ) = aggregate_field_info(agg, col_idx, Some(schema));
                fields.push(RowField {
                    name,
                    rust_type: nextsql_type_to_rust(&typ),
                    getter: nextsql_type_to_row_getter(&typ),
                    valtype_wrapper: None,
                    enum_parse: false,
                });
                col_idx += 1;
            }
            // PropertyAccess: resolve via relation if possible
            Expression::Atomic(AtomicExpression::PropertyAccess(pa)) => {
                let name = pa.property.clone();
                let resolved = resolve_property_access_type(pa, from_table, schema, registry, rel_registry);
                let (rust_type, getter, valtype_wrapper) = resolved.unwrap_or_else(|| {
                    ("String".to_string(), "get_string".to_string(), None)
                });
                fields.push(RowField {
                    name,
                    rust_type,
                    getter,
                    valtype_wrapper,
                    enum_parse: false,
                });
                col_idx += 1;
            }
            // MethodCall: derive name from the method or target
            Expression::Atomic(AtomicExpression::MethodCall(_mc)) => {
                let name = extract_name_from_expr(&sel.expr)
                    .unwrap_or_else(|| format!("col_{col_idx}"));
                fields.push(RowField {
                    name,
                    rust_type: "String".to_string(),
                    getter: "get_string".to_string(),
                    valtype_wrapper: None,
                    enum_parse: false,
                });
                col_idx += 1;
            }
            // Call expressions: check if it's an aggregate function call
            Expression::Atomic(AtomicExpression::Call(_call)) => {
                let name = extract_name_from_expr(&sel.expr)
                    .unwrap_or_else(|| format!("col_{col_idx}"));
                if let Some(func_type) = infer_aggregate_function_type(&sel.expr) {
                    let inner_expr = extract_aggregate_inner_expr(&sel.expr);
                    let typ = aggregate_return_type_with_schema(&func_type, inner_expr, Some(schema));
                    fields.push(RowField {
                        name,
                        rust_type: nextsql_type_to_rust(&typ),
                        getter: nextsql_type_to_row_getter(&typ),
                        valtype_wrapper: None,
                        enum_parse: false,
                    });
                } else {
                    fields.push(RowField {
                        name,
                        rust_type: "String".to_string(),
                        getter: "get_string".to_string(),
                        valtype_wrapper: None,
                        enum_parse: false,
                    });
                }
                col_idx += 1;
            }
            _ => {
                // Unknown expression – fallback
                let name = extract_name_from_expr(&sel.expr)
                    .unwrap_or_else(|| format!("col_{col_idx}"));
                fields.push(RowField {
                    name,
                    rust_type: "String".to_string(),
                    getter: "get_string".to_string(),
                    valtype_wrapper: None,
                    enum_parse: false,
                });
                col_idx += 1;
            }
        }
    }

    fields
}

/// Resolve Row fields for a RETURNING clause (Vec<Column>).
pub(super) fn resolve_returning_fields(
    returning: &[Column],
    table_name: &str,
    schema: &DatabaseSchema,
    registry: &ValTypeRegistry,
) -> Vec<RowField> {
    let mut fields = Vec::new();
    for col in returning {
        match col {
            Column::WildcardOf(table, _) => {
                if let Some(ts) = schema.get_table(table) {
                    for c in &ts.columns {
                        let typ = effective_type(&c.column_type, c.nullable);
                        fields.push(make_row_field(&c.name, table, &typ, registry, schema));
                    }
                }
            }
            Column::Wildcard(_) => {
                if let Some(ts) = schema.get_table(table_name) {
                    for c in &ts.columns {
                        let typ = effective_type(&c.column_type, c.nullable);
                        fields.push(make_row_field(&c.name, table_name, &typ, registry, schema));
                    }
                }
            }
            Column::ExplicitTarget(table, column, _) => {
                if let Some(rf) = field_from_schema(table, column, schema, registry) {
                    fields.push(rf);
                } else {
                    fields.push(fallback_field(column));
                }
            }
            Column::ImplicitTarget(column, _) => {
                if let Some(rf) = field_from_schema(table_name, column, schema, registry) {
                    fields.push(rf);
                } else {
                    fields.push(fallback_field(column));
                }
            }
        }
    }
    fields
}

/// Resolve aggregate select-clause fields (the `.aggregate(...)` query clause).
pub(super) fn resolve_aggregate_clause_fields(
    agg_exprs: &[AggregateExpression],
    _from_table: &str,
    schema: &DatabaseSchema,
) -> Vec<RowField> {
    let mut fields = Vec::new();
    for agg in agg_exprs {
        // The alias becomes the field name.
        // Try to infer type from the inner expression.
        if let Some(func_type) = infer_aggregate_function_type(&agg.expr) {
            let inner_expr = extract_aggregate_inner_expr(&agg.expr);
            let typ = aggregate_return_type_with_schema(&func_type, inner_expr, Some(schema));
            fields.push(RowField {
                name: agg.alias.clone(),
                rust_type: nextsql_type_to_rust(&typ),
                getter: nextsql_type_to_row_getter(&typ),
                valtype_wrapper: None,
                enum_parse: false,
            });
        } else {
            fields.push(RowField {
                name: agg.alias.clone(),
                rust_type: "String".to_string(),
                getter: "get_string".to_string(),
                valtype_wrapper: None,
                enum_parse: false,
            });
        }
    }
    fields
}

/// Try to extract the aggregate function type from an expression.
/// Handles both the proper AggregateFunction AST node and CallExpression
/// with aggregate function names (COUNT, SUM, AVG, MIN, MAX), since the
/// parser may produce either form depending on grammar ordering.
pub(super) fn infer_aggregate_function_type(expr: &Expression) -> Option<AggregateFunctionType> {
    match expr {
        Expression::Atomic(AtomicExpression::Aggregate(af)) => {
            Some(af.function_type.clone())
        }
        Expression::Atomic(AtomicExpression::Call(call)) => {
            match call.callee.as_str() {
                "COUNT" => Some(AggregateFunctionType::Count),
                "SUM" => Some(AggregateFunctionType::Sum),
                "AVG" => Some(AggregateFunctionType::Avg),
                "MIN" => Some(AggregateFunctionType::Min),
                "MAX" => Some(AggregateFunctionType::Max),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Extract the inner expression from an aggregate function expression.
/// Works for both AggregateFunction AST nodes and Call expressions with aggregate names.
pub(super) fn extract_aggregate_inner_expr(expr: &Expression) -> Option<&Expression> {
    match expr {
        Expression::Atomic(AtomicExpression::Aggregate(af)) => Some(&af.expr),
        Expression::Atomic(AtomicExpression::Call(call)) => {
            match call.callee.as_str() {
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => call.args.first(),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Decompose a PropertyAccess chain into segments, mirroring the SQL gen logic.
pub(super) fn decompose_property_chain_for_type(pa: &PropertyAccess) -> Option<Vec<String>> {
    let mut segments = Vec::new();
    segments.push(pa.property.clone());

    let mut current = &*pa.target;
    loop {
        match current {
            Expression::Atomic(AtomicExpression::PropertyAccess(inner_pa)) => {
                segments.push(inner_pa.property.clone());
                current = &*inner_pa.target;
            }
            Expression::Atomic(AtomicExpression::Column(Column::ImplicitTarget(name, _))) => {
                segments.push(name.clone());
                break;
            }
            Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(table, col, _))) => {
                segments.push(col.clone());
                segments.push(table.clone());
                break;
            }
            _ => return None,
        }
    }
    segments.reverse();
    Some(segments)
}

/// Try to extract a meaningful field name from an expression.
/// Returns None if no sensible name can be derived.
pub(super) fn extract_name_from_expr(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Atomic(atomic) => extract_name_from_atomic(atomic),
        _ => None,
    }
}

pub(super) fn extract_name_from_atomic(atomic: &AtomicExpression) -> Option<String> {
    match atomic {
        AtomicExpression::PropertyAccess(pa) => Some(pa.property.clone()),
        AtomicExpression::Column(col) => match col {
            Column::ExplicitTarget(_, name, _) => Some(name.clone()),
            Column::ImplicitTarget(name, _) => Some(name.clone()),
            _ => None,
        },
        AtomicExpression::MethodCall(mc) => {
            // Try to get the name from the target (e.g., users.name.toLowerCase() -> name)
            extract_name_from_expr(&mc.target)
        }
        AtomicExpression::Call(call) => {
            // For when(cond, expr), try to extract name from the result expression
            if call.callee == "when" && call.args.len() == 2 {
                extract_name_from_expr(&call.args[1])
            } else {
                None
            }
        }
        AtomicExpression::Aggregate(agg) => {
            let prefix = match agg.function_type {
                AggregateFunctionType::Count => "count",
                AggregateFunctionType::Sum => "sum",
                AggregateFunctionType::Avg => "avg",
                AggregateFunctionType::Min => "min",
                AggregateFunctionType::Max => "max",
            };
            if let Some(inner_name) = extract_name_from_expr(&agg.expr) {
                Some(format!("{prefix}_{inner_name}"))
            } else {
                Some(prefix.to_string())
            }
        }
        _ => None,
    }
}

pub(super) fn effective_type(col_type: &Type, nullable: bool) -> Type {
    if nullable {
        Type::Optional(Box::new(col_type.clone()))
    } else {
        col_type.clone()
    }
}

pub(super) fn field_from_schema(table: &str, column: &str, schema: &DatabaseSchema, registry: &ValTypeRegistry) -> Option<RowField> {
    let ts = schema.get_table(table)?;
    let cs = ts.get_column(column)?;
    let typ = effective_type(&cs.column_type, cs.nullable);
    Some(make_row_field(column, table, &typ, registry, schema))
}

/// Create a RowField, checking the valtype registry for type overrides
/// and the schema for enum type resolution.
pub(super) fn make_row_field(column: &str, table: &str, typ: &Type, registry: &ValTypeRegistry, schema: &DatabaseSchema) -> RowField {
    if let Some((vt_name, _vt_base)) = registry.lookup(table, column) {
        // Use the valtype wrapper. The getter is still the raw type getter.
        let getter = nextsql_type_to_row_getter(typ);
        let (rust_type, wrapper) = match typ {
            Type::Optional(_) => (format!("Option<{vt_name}>"), Some(vt_name.clone())),
            _ => (vt_name.clone(), Some(vt_name.clone())),
        };
        RowField {
            name: column.to_string(),
            rust_type,
            getter,
            valtype_wrapper: wrapper,
            enum_parse: false,
        }
    } else if let Some(enum_name) = extract_enum_type_name(typ, schema) {
        // This is a PostgreSQL enum type - use the generated Rust enum
        let rust_enum = enum_rust_type(&enum_name);
        let is_optional = matches!(typ, Type::Optional(_));
        let getter = if is_optional {
            "get_opt_string".to_string()
        } else {
            "get_string".to_string()
        };
        let rust_type = if is_optional {
            format!("Option<{rust_enum}>")
        } else {
            rust_enum
        };
        RowField {
            name: column.to_string(),
            rust_type,
            getter,
            valtype_wrapper: None,
            enum_parse: true,
        }
    } else {
        RowField {
            name: column.to_string(),
            rust_type: nextsql_type_to_rust(typ),
            getter: nextsql_type_to_row_getter(typ),
            valtype_wrapper: None,
            enum_parse: false,
        }
    }
}

/// Extract the enum type name from a Type if it's a UserDefined type that corresponds
/// to a known enum in the schema. Handles Optional wrapping.
fn extract_enum_type_name(typ: &Type, schema: &DatabaseSchema) -> Option<String> {
    match typ {
        Type::UserDefined(name) if is_enum_type(name, schema) => Some(name.clone()),
        Type::Optional(inner) => extract_enum_type_name(inner, schema),
        _ => None,
    }
}

pub(super) fn fallback_field(name: &str) -> RowField {
    RowField {
        name: name.to_string(),
        rust_type: "String".to_string(), // type could not be inferred
        getter: "get_string".to_string(),
        valtype_wrapper: None,
        enum_parse: false,
    }
}

/// Infer whether an expression involves a float type by inspecting column references
/// against the schema. Returns true if any column in the expression is f32 or f64.
pub(super) fn expr_involves_float(expr: &Expression, schema: &DatabaseSchema) -> bool {
    match expr {
        Expression::Atomic(AtomicExpression::Column(col)) => {
            let (table, column) = match col {
                Column::ExplicitTarget(t, c, _) => (t.as_str(), c.as_str()),
                _ => return false,
            };
            if let Some(ts) = schema.get_table(table) {
                if let Some(cs) = ts.get_column(column) {
                    return matches!(cs.column_type, Type::BuiltIn(BuiltInType::F32) | Type::BuiltIn(BuiltInType::F64));
                }
            }
            false
        }
        Expression::Binary { left, right, .. } => {
            expr_involves_float(left, schema) || expr_involves_float(right, schema)
        }
        _ => false,
    }
}

pub(super) fn aggregate_return_type_with_schema(ft: &AggregateFunctionType, inner_expr: Option<&Expression>, schema: Option<&DatabaseSchema>) -> Type {
    match ft {
        AggregateFunctionType::Count => Type::BuiltIn(BuiltInType::I64),
        AggregateFunctionType::Sum => {
            // If the inner expression involves float columns, SUM returns f64
            if let (Some(expr), Some(s)) = (inner_expr, schema) {
                if expr_involves_float(expr, s) {
                    return Type::BuiltIn(BuiltInType::F64);
                }
            }
            Type::BuiltIn(BuiltInType::I64)
        }
        AggregateFunctionType::Avg => Type::BuiltIn(BuiltInType::F64),
        AggregateFunctionType::Min | AggregateFunctionType::Max => {
            // Without knowing the column type we default to f64
            Type::BuiltIn(BuiltInType::F64)
        }
    }
}

pub(super) fn aggregate_field_info(agg: &AggregateFunction, idx: usize, schema: Option<&DatabaseSchema>) -> (String, Type) {
    let name = match &agg.function_type {
        AggregateFunctionType::Count => "count",
        AggregateFunctionType::Sum => "sum",
        AggregateFunctionType::Avg => "avg",
        AggregateFunctionType::Min => "min",
        AggregateFunctionType::Max => "max",
    };
    let typ = aggregate_return_type_with_schema(&agg.function_type, Some(&agg.expr), schema);
    (format!("{name}_{idx}"), typ)
}

/// Check if a query's SELECT clause is a simple `table.*` on a single table (no joins, no aggregates).
/// Returns Some(table_name) if the pattern matches.
pub(super) fn is_simple_wildcard_select(stmt: &SelectStatement, clauses_select: Option<&Vec<SelectExpression>>, clauses_aggregate: Option<&Vec<AggregateExpression>>) -> Option<String> {
    // Must have no joins
    if !stmt.from.joins.is_empty() {
        return None;
    }
    // Must have no aggregate clause
    if clauses_aggregate.is_some() {
        return None;
    }
    // Must have exactly one select expression that is WildcardOf
    let sel = clauses_select?;
    if sel.len() != 1 {
        return None;
    }
    match &sel[0].expr {
        Expression::Atomic(AtomicExpression::Column(Column::WildcardOf(tbl, _))) => {
            // The wildcard table must match the from table
            if tbl == &stmt.from.table {
                Some(tbl.clone())
            } else {
                None
            }
        }
        Expression::Atomic(AtomicExpression::Column(Column::Wildcard(_))) => {
            // bare * on a single-table query
            Some(stmt.from.table.clone())
        }
        _ => None,
    }
}

/// Check if a RETURNING clause is a simple wildcard on the given table.
/// Returns Some(table_name) if pattern matches.
pub(super) fn is_simple_wildcard_returning(returning: &[Column], table_name: &str) -> Option<String> {
    if returning.len() != 1 {
        return None;
    }
    match &returning[0] {
        Column::WildcardOf(tbl, _) if tbl == table_name => Some(tbl.clone()),
        Column::Wildcard(_) => Some(table_name.to_string()),
        _ => None,
    }
}

/// Generate a model struct (and from_row impl) for a table schema.
pub(super) fn generate_model_struct(out: &mut String, table: &nextsql_core::schema::TableSchema, registry: &ValTypeRegistry, schema: &DatabaseSchema) {
    let model_name = super::naming::table_name_to_model_name(&table.name);
    let fields: Vec<RowField> = table
        .columns
        .iter()
        .map(|c| {
            let typ = effective_type(&c.column_type, c.nullable);
            make_row_field(&c.name, &table.name, &typ, registry, schema)
        })
        .collect();
    emit_row_struct(out, &model_name, &fields);
}

/// Generate a Rust type string for a column, respecting nullability, valtype overrides,
/// and enum type resolution.
pub(super) fn column_to_rust_type(
    col: &nextsql_core::schema::ColumnSchema,
    table_name: &str,
    registry: &ValTypeRegistry,
    schema: &DatabaseSchema,
) -> String {
    let typ = effective_type(&col.column_type, col.nullable);
    if let Some((vt_name, _)) = registry.lookup(table_name, &col.name) {
        match &typ {
            Type::Optional(_) => format!("Option<{vt_name}>"),
            _ => vt_name.clone(),
        }
    } else if let Some(enum_name) = extract_enum_type_name(&typ, schema) {
        let rust_enum = enum_rust_type(&enum_name);
        match &typ {
            Type::Optional(_) => format!("Option<{rust_enum}>"),
            _ => rust_enum,
        }
    } else {
        nextsql_type_to_rust(&typ)
    }
}
