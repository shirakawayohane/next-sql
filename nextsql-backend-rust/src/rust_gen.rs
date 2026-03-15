use std::collections::{HashMap, HashSet};
use nextsql_core::ast::*;
use nextsql_core::schema::{DatabaseSchema, TableSchema};
use crate::sql_gen;
use crate::type_mapping::{nextsql_type_to_rust, nextsql_type_to_row_getter};

// ── ValType Registry ────────────────────────────────────────────────────

/// Registry holding valtype mappings from columns to newtype wrappers.
struct ValTypeRegistry {
    /// Map from (table, column) -> (valtype name, base type)
    column_to_valtype: HashMap<(String, String), (String, BuiltInType)>,
    /// All defined valtypes: name -> base type
    valtypes: HashMap<String, BuiltInType>,
}

impl ValTypeRegistry {
    fn new() -> Self {
        Self {
            column_to_valtype: HashMap::new(),
            valtypes: HashMap::new(),
        }
    }

    /// Build the registry from all TopLevel items in a module.
    fn from_module(module: &Module) -> Self {
        let mut reg = Self::new();

        // First pass: collect all ValType definitions
        for tl in &module.toplevels {
            if let TopLevel::ValType(vt) = tl {
                reg.valtypes.insert(vt.name.clone(), vt.base_type.clone());
                if let Some(ref col_ref) = vt.source_column {
                    let key = (col_ref.table.clone(), col_ref.column.clone());
                    reg.column_to_valtype.entry(key)
                        .or_insert_with(|| (vt.name.clone(), vt.base_type.clone()));
                }
            }
        }

        // Second pass: infer valtypes from Relation join conditions
        for tl in &module.toplevels {
            if let TopLevel::Relation(rel) = tl {
                let pairs = extract_equality_pairs(&rel.join_condition);
                for (left, right) in pairs {
                    let left_key = (left.table.clone(), left.column.clone());
                    let right_key = (right.table.clone(), right.column.clone());

                    if let Some((vt_name, vt_base)) = reg.column_to_valtype.get(&left_key).cloned() {
                        reg.column_to_valtype.entry(right_key).or_insert((vt_name, vt_base));
                    } else if let Some((vt_name, vt_base)) = reg.column_to_valtype.get(&right_key).cloned() {
                        reg.column_to_valtype.entry(left_key).or_insert((vt_name, vt_base));
                    }
                }
            }
        }

        reg
    }

    /// Look up the valtype for a (table, column) pair.
    fn lookup(&self, table: &str, column: &str) -> Option<&(String, BuiltInType)> {
        self.column_to_valtype.get(&(table.to_string(), column.to_string()))
    }

    /// Check if a user-defined type name is a known valtype.
    fn is_valtype(&self, name: &str) -> bool {
        self.valtypes.contains_key(name)
    }
}

/// Extract equality pairs from a join condition expression.
fn extract_equality_pairs(expr: &Expression) -> Vec<(ColumnRef, ColumnRef)> {
    match expr {
        Expression::Binary { left, op: BinaryOp::Equal, right } => {
            if let (Some(left_col), Some(right_col)) = (extract_column_ref(left), extract_column_ref(right)) {
                vec![(left_col, right_col)]
            } else {
                vec![]
            }
        }
        Expression::Binary { left, op: BinaryOp::And, right } => {
            let mut pairs = extract_equality_pairs(left);
            pairs.extend(extract_equality_pairs(right));
            pairs
        }
        _ => vec![]
    }
}

/// Try to extract a ColumnRef from an expression.
fn extract_column_ref(expr: &Expression) -> Option<ColumnRef> {
    match expr {
        Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(table, col, _))) => {
            Some(ColumnRef { table: table.clone(), column: col.clone() })
        }
        _ => None
    }
}

/// Extract all ValType definitions from a module (for CLI use).
pub fn extract_valtypes(module: &Module) -> Vec<&ValType> {
    module.toplevels.iter().filter_map(|tl| {
        match tl {
            TopLevel::ValType(vt) => Some(vt),
            _ => None
        }
    }).collect()
}

/// Generated Rust code for a single .nsql file
#[derive(Debug)]
pub struct GeneratedRustFile {
    pub filename: String,
    pub content: String,
    pub errors: Vec<String>,
}

/// A single field in a generated Row struct.
#[derive(Clone)]
struct RowField {
    name: String,
    rust_type: String,
    getter: String,
    /// If set, wrap the getter call with this valtype constructor.
    valtype_wrapper: Option<String>,
}

// ── Naming helpers ──────────────────────────────────────────────────────

/// Convert camelCase to snake_case.
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Convert camelCase to PascalCase (just capitalise the first letter).
fn to_pascal_case(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => {
            let mut out = c.to_uppercase().to_string();
            out.extend(chars);
            out
        }
    }
}

// ── Singularize / model-struct helpers ───────────────────────────────────

/// Simple singularization: strip trailing 's'.
fn singularize(name: &str) -> String {
    if name.ends_with('s') && name.len() > 2 {
        name[..name.len() - 1].to_string()
    } else {
        name.to_string()
    }
}

/// Convert a table name like "users" or "application_headers" to PascalCase model name.
/// e.g. "users" -> "User", "application_headers" -> "ApplicationHeader"
fn table_name_to_model_name(table_name: &str) -> String {
    let singular = singularize(table_name);
    // Convert snake_case to PascalCase
    singular
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => {
                    let mut out = c.to_uppercase().to_string();
                    out.extend(chars);
                    out
                }
            }
        })
        .collect()
}

/// Generate a model struct (and from_row impl) for a table schema.
fn generate_model_struct(out: &mut String, table: &TableSchema, registry: &ValTypeRegistry) {
    let model_name = table_name_to_model_name(&table.name);
    let fields: Vec<RowField> = table
        .columns
        .iter()
        .map(|c| {
            let typ = effective_type(&c.column_type, c.nullable);
            make_row_field(&c.name, &table.name, &typ, registry)
        })
        .collect();
    emit_row_struct(out, &model_name, &fields);
}

/// Check if a query's SELECT clause is a simple `table.*` on a single table (no joins, no aggregates).
/// Returns Some(table_name) if the pattern matches.
fn is_simple_wildcard_select(stmt: &SelectStatement, clauses_select: Option<&Vec<SelectExpression>>, clauses_aggregate: Option<&Vec<AggregateExpression>>) -> Option<String> {
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
fn is_simple_wildcard_returning(returning: &[Column], table_name: &str) -> Option<String> {
    if returning.len() != 1 {
        return None;
    }
    match &returning[0] {
        Column::WildcardOf(tbl, _) if tbl == table_name => Some(tbl.clone()),
        Column::Wildcard(_) => Some(table_name.to_string()),
        _ => None,
    }
}

// ── Row-field resolution ────────────────────────────────────────────────

/// Extract the inner expression from an aggregate function expression.
/// Works for both AggregateFunction AST nodes and Call expressions with aggregate names.
fn extract_aggregate_inner_expr(expr: &Expression) -> Option<&Expression> {
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
fn decompose_property_chain_for_type(pa: &PropertyAccess) -> Option<Vec<String>> {
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

/// Resolve the type of a PropertyAccess expression by looking up relations.
/// Returns (rust_type, getter, valtype_wrapper) if resolution succeeds.
fn resolve_property_access_type(
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
    let field = make_row_field(column_name, target_table, &typ, registry);
    Some((field.rust_type, field.getter, field.valtype_wrapper))
}

/// Try to resolve the type of an expression via relation access.
/// This handles PropertyAccess expressions that resolve through relations.
fn resolve_expr_type_via_relation(
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
fn resolve_select_fields(
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
                                fields.push(make_row_field(&c.name, tbl_name, &typ, registry));
                                col_idx += 1;
                            }
                        }
                    }
                    Column::Wildcard(Some(..)) => {
                        let tbl_name = from_table;
                        if let Some(table_schema) = schema.get_table(tbl_name) {
                            for c in &table_schema.columns {
                                let typ = effective_type(&c.column_type, c.nullable);
                                fields.push(make_row_field(&c.name, tbl_name, &typ, registry));
                                col_idx += 1;
                            }
                        }
                    }
                    Column::Wildcard(None) => {
                        // bare * – expand from_table
                        if let Some(table_schema) = schema.get_table(from_table) {
                            for c in &table_schema.columns {
                                let typ = effective_type(&c.column_type, c.nullable);
                                fields.push(make_row_field(&c.name, from_table, &typ, registry));
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
                });
                col_idx += 1;
            }
            // MethodCall: derive name from the method or target
            Expression::Atomic(AtomicExpression::MethodCall(_mc)) => {
                let name = extract_name_from_expr(&sel.expr)
                    .unwrap_or_else(|| format!("col_{}", col_idx));
                fields.push(RowField {
                    name,
                    rust_type: "String".to_string(),
                    getter: "get_string".to_string(),
                    valtype_wrapper: None,
                });
                col_idx += 1;
            }
            // Call expressions: check if it's an aggregate function call
            Expression::Atomic(AtomicExpression::Call(_call)) => {
                let name = extract_name_from_expr(&sel.expr)
                    .unwrap_or_else(|| format!("col_{}", col_idx));
                if let Some(func_type) = infer_aggregate_function_type(&sel.expr) {
                    let inner_expr = extract_aggregate_inner_expr(&sel.expr);
                    let typ = aggregate_return_type_with_schema(&func_type, inner_expr, Some(schema));
                    fields.push(RowField {
                        name,
                        rust_type: nextsql_type_to_rust(&typ),
                        getter: nextsql_type_to_row_getter(&typ),
                        valtype_wrapper: None,
                    });
                } else {
                    fields.push(RowField {
                        name,
                        rust_type: "String".to_string(),
                        getter: "get_string".to_string(),
                        valtype_wrapper: None,
                    });
                }
                col_idx += 1;
            }
            _ => {
                // Unknown expression – fallback
                let name = extract_name_from_expr(&sel.expr)
                    .unwrap_or_else(|| format!("col_{}", col_idx));
                fields.push(RowField {
                    name,
                    rust_type: "String".to_string(),
                    getter: "get_string".to_string(),
                    valtype_wrapper: None,
                });
                col_idx += 1;
            }
        }
    }

    fields
}

/// Resolve Row fields for a RETURNING clause (Vec<Column>).
fn resolve_returning_fields(
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
                        fields.push(make_row_field(&c.name, table, &typ, registry));
                    }
                }
            }
            Column::Wildcard(_) => {
                if let Some(ts) = schema.get_table(table_name) {
                    for c in &ts.columns {
                        let typ = effective_type(&c.column_type, c.nullable);
                        fields.push(make_row_field(&c.name, table_name, &typ, registry));
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
/// Try to extract the aggregate function type from an expression.
/// Handles both the proper AggregateFunction AST node and CallExpression
/// with aggregate function names (COUNT, SUM, AVG, MIN, MAX), since the
/// parser may produce either form depending on grammar ordering.
fn infer_aggregate_function_type(expr: &Expression) -> Option<AggregateFunctionType> {
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

fn resolve_aggregate_clause_fields(
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
            });
        } else {
            fields.push(RowField {
                name: agg.alias.clone(),
                rust_type: "String".to_string(),
                getter: "get_string".to_string(),
                valtype_wrapper: None,
            });
        }
    }
    fields
}

/// Try to extract a meaningful field name from an expression.
/// Returns None if no sensible name can be derived.
fn extract_name_from_expr(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Atomic(atomic) => extract_name_from_atomic(atomic),
        _ => None,
    }
}

fn extract_name_from_atomic(atomic: &AtomicExpression) -> Option<String> {
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
        AtomicExpression::When(when) => {
            extract_name_from_expr(&when.then_expr)
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
                Some(format!("{}_{}", prefix, inner_name))
            } else {
                Some(prefix.to_string())
            }
        }
        _ => None,
    }
}

fn effective_type(col_type: &Type, nullable: bool) -> Type {
    if nullable {
        Type::Optional(Box::new(col_type.clone()))
    } else {
        col_type.clone()
    }
}

fn field_from_schema(table: &str, column: &str, schema: &DatabaseSchema, registry: &ValTypeRegistry) -> Option<RowField> {
    let ts = schema.get_table(table)?;
    let cs = ts.get_column(column)?;
    let typ = effective_type(&cs.column_type, cs.nullable);
    Some(make_row_field(column, table, &typ, registry))
}

/// Create a RowField, checking the valtype registry for type overrides.
fn make_row_field(column: &str, table: &str, typ: &Type, registry: &ValTypeRegistry) -> RowField {
    if let Some((vt_name, _vt_base)) = registry.lookup(table, column) {
        // Use the valtype wrapper. The getter is still the raw type getter.
        let getter = nextsql_type_to_row_getter(typ);
        let (rust_type, wrapper) = match typ {
            Type::Optional(_) => (format!("Option<{}>", vt_name), Some(vt_name.clone())),
            _ => (vt_name.clone(), Some(vt_name.clone())),
        };
        RowField {
            name: column.to_string(),
            rust_type,
            getter,
            valtype_wrapper: wrapper,
        }
    } else {
        RowField {
            name: column.to_string(),
            rust_type: nextsql_type_to_rust(typ),
            getter: nextsql_type_to_row_getter(typ),
            valtype_wrapper: None,
        }
    }
}

fn fallback_field(name: &str) -> RowField {
    RowField {
        name: name.to_string(),
        rust_type: "String".to_string(), // type could not be inferred
        getter: "get_string".to_string(),
        valtype_wrapper: None,
    }
}

/// Infer whether an expression involves a float type by inspecting column references
/// against the schema. Returns true if any column in the expression is f32 or f64.
fn expr_involves_float(expr: &Expression, schema: &DatabaseSchema) -> bool {
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

fn aggregate_return_type_with_schema(ft: &AggregateFunctionType, inner_expr: Option<&Expression>, schema: Option<&DatabaseSchema>) -> Type {
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

fn aggregate_field_info(agg: &AggregateFunction, idx: usize, schema: Option<&DatabaseSchema>) -> (String, Type) {
    let name = match &agg.function_type {
        AggregateFunctionType::Count => "count",
        AggregateFunctionType::Sum => "sum",
        AggregateFunctionType::Avg => "avg",
        AggregateFunctionType::Min => "min",
        AggregateFunctionType::Max => "max",
    };
    let typ = aggregate_return_type_with_schema(&agg.function_type, Some(&agg.expr), schema);
    (format!("{}_{}", name, idx), typ)
}

// ── Code emitters ───────────────────────────────────────────────────────

/// Emit a Params struct + its `to_params` impl.
fn emit_params_struct(
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
struct VecValtypeParam {
    /// The snake_case field name in the Params struct
    field_name: String,
    /// The Rust type of the inner value (e.g., "uuid::Uuid")
    inner_rust_type: String,
}

/// Check if any argument has type Vec<ValType> and collect info for code generation.
fn find_vec_valtype_params(args: &[Argument], param_order: &[String], registry: &ValTypeRegistry) -> Vec<VecValtypeParam> {
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
fn generate_params_with_conversions(
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
fn emit_row_struct(out: &mut String, struct_name: &str, fields: &[RowField]) {
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
fn emit_query_fn(
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
fn emit_query_fn_no_params(
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
fn emit_execute_fn(
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
fn emit_execute_fn_no_params(
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
fn emit_mutation_query_fn(
    out: &mut String,
    fn_name: &str,
    params_struct: &str,
    row_struct: &str,
    sql: &str,
    param_conversion: Option<&(String, String)>,
) {
    emit_query_fn(out, fn_name, params_struct, row_struct, sql, param_conversion);
}

// ── Extracting select fields from query clauses ─────────────────────────

/// Walk query clauses to find the select expressions and aggregate expressions.
fn find_select_and_aggregate_clauses(
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

// ── Top-level generators ────────────────────────────────────────────────

fn generate_query(out: &mut String, query: &Query, schema: &DatabaseSchema, model_tables: &mut HashSet<String>, registry: &ValTypeRegistry, rel_registry: &sql_gen::RelationRegistry, errors: &mut Vec<String>) {
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

fn generate_mutation(out: &mut String, mutation: &Mutation, schema: &DatabaseSchema, model_tables: &mut HashSet<String>, registry: &ValTypeRegistry, rel_registry: &sql_gen::RelationRegistry) {
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
fn find_updatable_param(
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
fn generate_updatable_mutation(
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

/// Generate a Rust type string for a column, respecting nullability and valtype overrides.
fn column_to_rust_type(
    col: &nextsql_core::schema::ColumnSchema,
    table_name: &str,
    registry: &ValTypeRegistry,
) -> String {
    let typ = effective_type(&col.column_type, col.nullable);
    if let Some((vt_name, _)) = registry.lookup(table_name, &col.name) {
        match &typ {
            Type::Optional(_) => format!("Option<{}>", vt_name),
            _ => vt_name.clone(),
        }
    } else {
        nextsql_type_to_rust(&typ)
    }
}

/// Generate a WHERE clause SQL template with `${}` placeholders for format! args.
/// Each variable reference becomes `${}`  which will be filled by `{var}_idx` at runtime.
fn generate_where_sql_template(
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
fn format_column_sql(col: &Column) -> String {
    match col {
        Column::ExplicitTarget(table, column, _) => format!("{}.{}", table, column),
        Column::ImplicitTarget(column, _) => column.clone(),
        Column::WildcardOf(table, _) => format!("{}.*", table),
        Column::Wildcard(_) => "*".to_string(),
    }
}

// ── Public entry points ─────────────────────────────────────────────────

/// Generate Rust code from a parsed NextSQL module.
pub fn generate_rust_module(
    module: &Module,
    schema: &DatabaseSchema,
    module_name: &str,
) -> GeneratedRustFile {
    let result = generate_rust_file(module, schema);
    GeneratedRustFile {
        filename: format!("{}.rs", module_name),
        content: result.content,
        errors: result.errors,
    }
}

/// Generate a shared types.rs file containing all valtype definitions from multiple modules.
/// This avoids duplicating valtype structs across generated files.
pub fn generate_valtype_file(modules: &[&Module]) -> GeneratedRustFile {
    let mut out = String::new();
    out.push_str("// AUTO-GENERATED by nextsql. Do not edit.\n\n");

    // Collect all valtypes across modules, deduplicating by name
    let mut all_valtypes: HashMap<String, BuiltInType> = HashMap::new();
    for module in modules {
        for tl in &module.toplevels {
            if let TopLevel::ValType(vt) = tl {
                all_valtypes.entry(vt.name.clone())
                    .or_insert_with(|| vt.base_type.clone());
            }
        }
    }

    // Sort by name for deterministic output
    let mut sorted: Vec<(&String, &BuiltInType)> = all_valtypes.iter().collect();
    sorted.sort_by_key(|(name, _)| (*name).clone());

    for (name, base_type) in sorted {
        let rust_base = nextsql_type_to_rust(&Type::BuiltIn(base_type.clone()));
        out.push_str(&format!("pub struct {}(pub {});\n\n", name, rust_base));
        out.push_str(&format!("impl super::runtime::ToSqlParam for {} {{\n", name));
        out.push_str("    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {\n");
        out.push_str("        &self.0\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");
    }

    GeneratedRustFile {
        filename: "types.rs".to_string(),
        content: out,
        errors: Vec::new(),
    }
}

/// Generate the full .rs file content from a module and schema.
/// Includes valtype definitions inline (for backward compatibility / single-file use).
pub fn generate_rust_file(module: &Module, schema: &DatabaseSchema) -> GeneratedRustFile {
    generate_rust_file_with_options(module, schema, false)
}

/// Generate the full .rs file content from a module and schema.
/// When `skip_valtype_generation` is true, valtype structs are not emitted inline
/// (they should be generated in a shared types.rs instead).
pub fn generate_rust_file_with_options(module: &Module, schema: &DatabaseSchema, skip_valtype_generation: bool) -> GeneratedRustFile {
    let mut out = String::new();
    let mut errors: Vec<String> = Vec::new();
    out.push_str("// AUTO-GENERATED by nextsql. Do not edit.\n\n");

    if skip_valtype_generation {
        out.push_str("use super::types::*;\n\n");
    }

    // Build the valtype registry from the module
    let registry = ValTypeRegistry::from_module(module);

    // Build the relation registry from the module
    let rel_registry = sql_gen::RelationRegistry::from_module(module);

    // Generate valtype newtype structs (only if not skipping)
    if !skip_valtype_generation {
        generate_valtype_structs(&mut out, &registry);
    }

    // First pass: generate queries/mutations into a buffer, collecting model table names
    let mut body = String::new();
    let mut model_tables: HashSet<String> = HashSet::new();

    for toplevel in &module.toplevels {
        match toplevel {
            TopLevel::Query(q) => generate_query(&mut body, q, schema, &mut model_tables, &registry, &rel_registry, &mut errors),
            TopLevel::Mutation(m) => generate_mutation(&mut body, m, schema, &mut model_tables, &registry, &rel_registry),
            // With and Relation are not directly translated to Rust code
            TopLevel::With(_) | TopLevel::Relation(_) | TopLevel::ValType(_) => {}
        }
    }

    // Generate model structs at the top for tables that need them
    let mut sorted_tables: Vec<&String> = model_tables.iter().collect();
    sorted_tables.sort();
    for table_name in sorted_tables {
        if let Some(table_schema) = schema.get_table(table_name) {
            generate_model_struct(&mut out, table_schema, &registry);
        }
    }

    // Append the query/mutation code
    out.push_str(&body);

    GeneratedRustFile {
        filename: String::new(),
        content: out,
        errors,
    }
}

/// Generate newtype struct definitions and ToSqlParam impls for all valtypes.
fn generate_valtype_structs(out: &mut String, registry: &ValTypeRegistry) {
    // Sort by name for deterministic output
    let mut valtypes: Vec<(&String, &BuiltInType)> = registry.valtypes.iter().collect();
    valtypes.sort_by_key(|(name, _)| (*name).clone());

    for (name, base_type) in valtypes {
        let rust_base = nextsql_type_to_rust(&Type::BuiltIn(base_type.clone()));
        out.push_str(&format!("pub struct {}(pub {});\n\n", name, rust_base));
        out.push_str(&format!("impl super::runtime::ToSqlParam for {} {{\n", name));
        out.push_str("    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {\n");
        out.push_str("        &self.0\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nextsql_core::schema::{DatabaseSchema, TableSchema, ColumnSchema};

    /// Helper: build a minimal "users" schema.
    fn users_schema() -> DatabaseSchema {
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("users".to_string());
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        table.add_column(ColumnSchema {
            name: "name".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        table.add_column(ColumnSchema {
            name: "email".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(table);
        schema
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("findUserById"), "find_user_by_id");
        assert_eq!(to_snake_case("insertUser"), "insert_user");
        assert_eq!(to_snake_case("listAll"), "list_all");
        assert_eq!(to_snake_case("a"), "a");
    }

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("findUserById"), "FindUserById");
        assert_eq!(to_pascal_case("insertUser"), "InsertUser");
    }

    #[test]
    fn test_generate_simple_query() {
        let schema = users_schema();

        // query findUserById($id: uuid) {
        //   from(users)
        //   .where(users.id == $id)
        //   .select(users.id, users.name, users.email)
        // }
        let module = Module {
            toplevels: vec![TopLevel::Query(Query {
                decl: QueryDecl {
                    name: "findUserById".to_string(),
                    arguments: vec![Argument {
                        name: "id".to_string(),
                        typ: Type::BuiltIn(BuiltInType::Uuid),
                    }],
                },
                body: QueryBody {
                    statements: vec![QueryStatement::Select(SelectStatement {
                        from: FromExpr {
                            table: "users".to_string(),
                            joins: vec![],
                            span: None,
                        },
                        clauses: vec![
                            QueryClause::Where(Expression::Binary {
                                left: Box::new(Expression::Atomic(AtomicExpression::Column(
                                    Column::ExplicitTarget(
                                        "users".to_string(),
                                        "id".to_string(),
                                        None,
                                    ),
                                ))),
                                op: BinaryOp::Equal,
                                right: Box::new(Expression::Atomic(AtomicExpression::Variable(
                                    Variable {
                                        name: "id".to_string(),
                                        span: None,
                                    },
                                ))),
                            }),
                            QueryClause::Select(vec![
                                SelectExpression {
                                    expr: Expression::Atomic(AtomicExpression::Column(
                                        Column::ExplicitTarget(
                                            "users".to_string(),
                                            "id".to_string(),
                                            None,
                                        ),
                                    )),
                                    alias: None,
                                },
                                SelectExpression {
                                    expr: Expression::Atomic(AtomicExpression::Column(
                                        Column::ExplicitTarget(
                                            "users".to_string(),
                                            "name".to_string(),
                                            None,
                                        ),
                                    )),
                                    alias: None,
                                },
                                SelectExpression {
                                    expr: Expression::Atomic(AtomicExpression::Column(
                                        Column::ExplicitTarget(
                                            "users".to_string(),
                                            "email".to_string(),
                                            None,
                                        ),
                                    )),
                                    alias: None,
                                },
                            ]),
                        ],
                        unions: vec![],
                    })],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // Verify header
        assert!(result.contains("// AUTO-GENERATED by nextsql. Do not edit."));

        // Verify Params struct
        assert!(result.contains("pub struct FindUserByIdParams {"));
        assert!(result.contains("pub id: uuid::Uuid,"));
        assert!(result.contains("fn to_params(&self) -> Vec<&dyn super::runtime::ToSqlParam>"));
        assert!(result.contains("&self.id"));

        // Verify Row struct
        assert!(result.contains("pub struct FindUserByIdRow {"));
        assert!(result.contains("pub id: uuid::Uuid,"));
        assert!(result.contains("pub name: String,"));
        assert!(result.contains("pub email: String,"));

        // Verify from_row
        assert!(result.contains("fn from_row(row: &dyn super::runtime::Row) -> Self"));
        assert!(result.contains("row.get_uuid(0)"));
        assert!(result.contains("row.get_string(1)"));
        assert!(result.contains("row.get_string(2)"));

        // Verify executor function
        assert!(result.contains("pub async fn find_user_by_id("));
        assert!(result.contains("client.query("));
        assert!(result.contains("FindUserByIdRow::from_row(row)"));
    }

    #[test]
    fn test_generate_insert_mutation_with_returning() {
        let schema = users_schema();

        // mutation insertUser($name: string, $email: string) {
        //   insert(users)
        //   .value({ name: $name, email: $email })
        //   .returning(users.*)
        // }
        let mut values = std::collections::HashMap::new();
        values.insert(
            "name".to_string(),
            Expression::Atomic(AtomicExpression::Variable(Variable {
                name: "name".to_string(),
                span: None,
            })),
        );
        values.insert(
            "email".to_string(),
            Expression::Atomic(AtomicExpression::Variable(Variable {
                name: "email".to_string(),
                span: None,
            })),
        );

        let module = Module {
            toplevels: vec![TopLevel::Mutation(Mutation {
                decl: MutationDecl {
                    name: "insertUser".to_string(),
                    arguments: vec![
                        Argument {
                            name: "name".to_string(),
                            typ: Type::BuiltIn(BuiltInType::String),
                        },
                        Argument {
                            name: "email".to_string(),
                            typ: Type::BuiltIn(BuiltInType::String),
                        },
                    ],
                },
                body: MutationBody {
                    items: vec![MutationBodyItem::Mutation(MutationStatement::Insert(
                        Insert {
                            into: "users".to_string(),
                            values: vec![Expression::Atomic(AtomicExpression::Literal(
                                Literal::Object(ObjectLiteralExpression(values)),
                            ))],
                            on_conflict: None,
                            returning: Some(vec![Column::WildcardOf(
                                "users".to_string(),
                                None,
                            )]),
                        },
                    ))],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // Verify Params struct
        assert!(result.contains("pub struct InsertUserParams {"));
        assert!(result.contains("pub name: String,"));
        assert!(result.contains("pub email: String,"));

        // Verify model struct is used (RETURNING users.* reuses User model)
        assert!(result.contains("pub struct User {"));
        assert!(!result.contains("pub struct InsertUserRow {"));

        // Verify executor uses client.query (because RETURNING is present)
        assert!(result.contains("pub async fn insert_user("));
        assert!(result.contains("client.query("));
        assert!(result.contains("User::from_row(row)"));
    }

    #[test]
    fn test_generate_delete_without_returning() {
        let schema = users_schema();

        // mutation deleteUser($id: uuid) {
        //   delete(users)
        //   .where(users.id == $id)
        // }
        let module = Module {
            toplevels: vec![TopLevel::Mutation(Mutation {
                decl: MutationDecl {
                    name: "deleteUser".to_string(),
                    arguments: vec![Argument {
                        name: "id".to_string(),
                        typ: Type::BuiltIn(BuiltInType::Uuid),
                    }],
                },
                body: MutationBody {
                    items: vec![MutationBodyItem::Mutation(MutationStatement::Delete(
                        Delete {
                            target: Target {
                                name: "users".to_string(),
                                span: None,
                            },
                            where_clause: Some(Expression::Binary {
                                left: Box::new(Expression::Atomic(AtomicExpression::Column(
                                    Column::ExplicitTarget(
                                        "users".to_string(),
                                        "id".to_string(),
                                        None,
                                    ),
                                ))),
                                op: BinaryOp::Equal,
                                right: Box::new(Expression::Atomic(AtomicExpression::Variable(
                                    Variable {
                                        name: "id".to_string(),
                                        span: None,
                                    },
                                ))),
                            }),
                            returning: None,
                        },
                    ))],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // Verify Params struct
        assert!(result.contains("pub struct DeleteUserParams {"));
        assert!(result.contains("pub id: uuid::Uuid,"));

        // No Row struct for DELETE without RETURNING
        assert!(!result.contains("pub struct DeleteUserRow {"));

        // Verify executor uses client.execute (no RETURNING)
        assert!(result.contains("pub async fn delete_user("));
        assert!(result.contains("client.execute("));
        assert!(result.contains("Result<u64,"));
    }

    #[test]
    fn test_param_ordering_matches_sql() {
        let schema = users_schema();

        // Build a query with 2 params to verify ordering
        let module = Module {
            toplevels: vec![TopLevel::Query(Query {
                decl: QueryDecl {
                    name: "findUser".to_string(),
                    arguments: vec![
                        Argument {
                            name: "name".to_string(),
                            typ: Type::BuiltIn(BuiltInType::String),
                        },
                        Argument {
                            name: "email".to_string(),
                            typ: Type::BuiltIn(BuiltInType::String),
                        },
                    ],
                },
                body: QueryBody {
                    statements: vec![QueryStatement::Select(SelectStatement {
                        from: FromExpr {
                            table: "users".to_string(),
                            joins: vec![],
                            span: None,
                        },
                        clauses: vec![
                            QueryClause::Where(Expression::Binary {
                                left: Box::new(Expression::Binary {
                                    left: Box::new(Expression::Atomic(
                                        AtomicExpression::Column(Column::ExplicitTarget(
                                            "users".to_string(),
                                            "name".to_string(),
                                            None,
                                        )),
                                    )),
                                    op: BinaryOp::Equal,
                                    right: Box::new(Expression::Atomic(
                                        AtomicExpression::Variable(Variable {
                                            name: "name".to_string(),
                                            span: None,
                                        }),
                                    )),
                                }),
                                op: BinaryOp::And,
                                right: Box::new(Expression::Binary {
                                    left: Box::new(Expression::Atomic(
                                        AtomicExpression::Column(Column::ExplicitTarget(
                                            "users".to_string(),
                                            "email".to_string(),
                                            None,
                                        )),
                                    )),
                                    op: BinaryOp::Equal,
                                    right: Box::new(Expression::Atomic(
                                        AtomicExpression::Variable(Variable {
                                            name: "email".to_string(),
                                            span: None,
                                        }),
                                    )),
                                }),
                            }),
                            QueryClause::Select(vec![SelectExpression {
                                expr: Expression::Atomic(AtomicExpression::Column(
                                    Column::WildcardOf("users".to_string(), None),
                                )),
                                alias: None,
                            }]),
                        ],
                        unions: vec![],
                    })],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // The SQL should have $1 for name and $2 for email
        // The to_params() should list them in the same order
        assert!(result.contains("&self.name"));
        assert!(result.contains("&self.email"));

        // Verify the SQL contains both placeholders
        assert!(result.contains("$1"));
        assert!(result.contains("$2"));

        // This query uses users.* on a single table, so it should use User model
        assert!(result.contains("pub struct User {"));
        assert!(!result.contains("pub struct FindUserRow {"));
        assert!(result.contains("User::from_row(row)"));
    }

    #[test]
    fn test_simple_wildcard_uses_model_struct() {
        let schema = users_schema();

        // query findAllUsers() { from(users).select(users.*) }
        let module = Module {
            toplevels: vec![TopLevel::Query(Query {
                decl: QueryDecl {
                    name: "findAllUsers".to_string(),
                    arguments: vec![],
                },
                body: QueryBody {
                    statements: vec![QueryStatement::Select(SelectStatement {
                        from: FromExpr {
                            table: "users".to_string(),
                            joins: vec![],
                            span: None,
                        },
                        clauses: vec![QueryClause::Select(vec![SelectExpression {
                            expr: Expression::Atomic(AtomicExpression::Column(
                                Column::WildcardOf("users".to_string(), None),
                            )),
                            alias: None,
                        }])],
                        unions: vec![],
                    })],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // Should generate User model struct, not FindAllUsersRow
        assert!(result.contains("pub struct User {"));
        assert!(!result.contains("pub struct FindAllUsersRow {"));
        assert!(result.contains("User::from_row(row)"));

        // Model struct should have all columns
        assert!(result.contains("pub id: uuid::Uuid,"));
        assert!(result.contains("pub name: String,"));
        assert!(result.contains("pub email: String,"));
    }

    #[test]
    fn test_partial_select_uses_row_struct() {
        let schema = users_schema();

        // query findUserNames() { from(users).select(users.name) }
        let module = Module {
            toplevels: vec![TopLevel::Query(Query {
                decl: QueryDecl {
                    name: "findUserNames".to_string(),
                    arguments: vec![],
                },
                body: QueryBody {
                    statements: vec![QueryStatement::Select(SelectStatement {
                        from: FromExpr {
                            table: "users".to_string(),
                            joins: vec![],
                            span: None,
                        },
                        clauses: vec![QueryClause::Select(vec![SelectExpression {
                            expr: Expression::Atomic(AtomicExpression::Column(
                                Column::ExplicitTarget(
                                    "users".to_string(),
                                    "name".to_string(),
                                    None,
                                ),
                            )),
                            alias: None,
                        }])],
                        unions: vec![],
                    })],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // Should generate FindUserNamesRow, not User
        assert!(result.contains("pub struct FindUserNamesRow {"));
        assert!(!result.contains("pub struct User {"));
        assert!(result.contains("FindUserNamesRow::from_row(row)"));
    }

    #[test]
    fn test_join_query_uses_row_struct() {
        let mut schema = users_schema();
        // Add posts table
        let mut posts = TableSchema::new("posts".to_string());
        posts.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: None,
        });
        posts.add_column(ColumnSchema {
            name: "title".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        posts.add_column(ColumnSchema {
            name: "user_id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(posts);

        // query findUserWithPosts($id: uuid) {
        //   from(users.innerJoin(posts, users.id == posts.user_id))
        //   .where(users.id == $id)
        //   .select(users.*)
        // }
        let module = Module {
            toplevels: vec![TopLevel::Query(Query {
                decl: QueryDecl {
                    name: "findUserWithPosts".to_string(),
                    arguments: vec![Argument {
                        name: "id".to_string(),
                        typ: Type::BuiltIn(BuiltInType::Uuid),
                    }],
                },
                body: QueryBody {
                    statements: vec![QueryStatement::Select(SelectStatement {
                        from: FromExpr {
                            table: "users".to_string(),
                            joins: vec![JoinExpr {
                                join_type: JoinType::Inner,
                                table: "posts".to_string(),
                                condition: Expression::Binary {
                                    left: Box::new(Expression::Atomic(
                                        AtomicExpression::Column(Column::ExplicitTarget(
                                            "users".to_string(),
                                            "id".to_string(),
                                            None,
                                        )),
                                    )),
                                    op: BinaryOp::Equal,
                                    right: Box::new(Expression::Atomic(
                                        AtomicExpression::Column(Column::ExplicitTarget(
                                            "posts".to_string(),
                                            "user_id".to_string(),
                                            None,
                                        )),
                                    )),
                                },
                                span: None,
                            }],
                            span: None,
                        },
                        clauses: vec![
                            QueryClause::Where(Expression::Binary {
                                left: Box::new(Expression::Atomic(AtomicExpression::Column(
                                    Column::ExplicitTarget(
                                        "users".to_string(),
                                        "id".to_string(),
                                        None,
                                    ),
                                ))),
                                op: BinaryOp::Equal,
                                right: Box::new(Expression::Atomic(
                                    AtomicExpression::Variable(Variable {
                                        name: "id".to_string(),
                                        span: None,
                                    }),
                                )),
                            }),
                            QueryClause::Select(vec![SelectExpression {
                                expr: Expression::Atomic(AtomicExpression::Column(
                                    Column::WildcardOf("users".to_string(), None),
                                )),
                                alias: None,
                            }]),
                        ],
                        unions: vec![],
                    })],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // Even though it's users.*, the query has a JOIN so it should use Row struct
        assert!(result.contains("pub struct FindUserWithPostsRow {"));
        assert!(!result.contains("pub struct User {"));
        assert!(result.contains("FindUserWithPostsRow::from_row(row)"));
    }

    #[test]
    fn test_mutation_returning_wildcard_uses_model() {
        let schema = users_schema();

        // mutation deleteUserReturning($id: uuid) {
        //   delete(users)
        //   .where(users.id == $id)
        //   .returning(users.*)
        // }
        let module = Module {
            toplevels: vec![TopLevel::Mutation(Mutation {
                decl: MutationDecl {
                    name: "deleteUserReturning".to_string(),
                    arguments: vec![Argument {
                        name: "id".to_string(),
                        typ: Type::BuiltIn(BuiltInType::Uuid),
                    }],
                },
                body: MutationBody {
                    items: vec![MutationBodyItem::Mutation(MutationStatement::Delete(
                        Delete {
                            target: Target {
                                name: "users".to_string(),
                                span: None,
                            },
                            where_clause: Some(Expression::Binary {
                                left: Box::new(Expression::Atomic(AtomicExpression::Column(
                                    Column::ExplicitTarget(
                                        "users".to_string(),
                                        "id".to_string(),
                                        None,
                                    ),
                                ))),
                                op: BinaryOp::Equal,
                                right: Box::new(Expression::Atomic(AtomicExpression::Variable(
                                    Variable {
                                        name: "id".to_string(),
                                        span: None,
                                    },
                                ))),
                            }),
                            returning: Some(vec![Column::WildcardOf(
                                "users".to_string(),
                                None,
                            )]),
                        },
                    ))],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema).content;

        // Should use User model struct, not DeleteUserReturningRow
        assert!(result.contains("pub struct User {"));
        assert!(!result.contains("pub struct DeleteUserReturningRow {"));
        assert!(result.contains("User::from_row(row)"));
    }

    #[test]
    fn test_table_name_to_model_name() {
        assert_eq!(table_name_to_model_name("users"), "User");
        assert_eq!(table_name_to_model_name("posts"), "Post");
        assert_eq!(table_name_to_model_name("application_headers"), "ApplicationHeader");
        assert_eq!(table_name_to_model_name("statuses"), "Statuse");
    }

    #[test]
    fn test_singularize() {
        assert_eq!(singularize("users"), "user");
        assert_eq!(singularize("posts"), "post");
        assert_eq!(singularize("s"), "s");
        assert_eq!(singularize("data"), "data");
    }

    #[test]
    fn test_valtype_struct_generation() {
        let input = r#"
            valtype UserId = uuid for users.id

            query findAllUsers() {
                from(users)
                .select(users.*)
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).unwrap();
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("users".to_string());
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        table.add_column(ColumnSchema {
            name: "name".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(table);

        let result = generate_rust_file(&module, &schema).content;
        // Verify valtype struct is generated
        assert!(result.contains("pub struct UserId(pub uuid::Uuid)"), "missing valtype struct: {}", result);
        // Verify ToSqlParam impl
        assert!(result.contains("impl super::runtime::ToSqlParam for UserId"), "missing ToSqlParam impl: {}", result);
        // Verify User model uses UserId for id field
        assert!(result.contains("pub id: UserId"), "id should be UserId: {}", result);
        // Verify from_row wraps with UserId
        assert!(result.contains("UserId(row.get_uuid(0))"), "from_row should wrap with UserId: {}", result);
    }

    #[test]
    fn test_valtype_relation_inference() {
        let input = r#"
            valtype UserId = uuid for users.id

            relation author for posts returning users {
                posts.author_id == users.id
            }

            query findAllPosts() {
                from(posts)
                .select(posts.*)
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).unwrap();
        let mut schema = DatabaseSchema::new();
        // Add users table
        let mut users = TableSchema::new("users".to_string());
        users.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        schema.add_table(users);
        // Add posts table
        let mut posts = TableSchema::new("posts".to_string());
        posts.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        posts.add_column(ColumnSchema {
            name: "author_id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        posts.add_column(ColumnSchema {
            name: "title".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(posts);

        let result = generate_rust_file(&module, &schema).content;
        // posts.author_id should be inferred as UserId from the relation
        assert!(result.contains("pub author_id: UserId"), "author_id should be UserId: {}", result);
    }

    #[test]
    fn test_valtype_in_params() {
        let input = r#"
            valtype UserId = uuid for users.id

            query findUserById($id: UserId) {
                from(users)
                .where(users.id == $id)
                .select(users.*)
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).unwrap();
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("users".to_string());
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: None,
        });
        table.add_column(ColumnSchema {
            name: "name".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(table);

        let result = generate_rust_file(&module, &schema).content;
        // Params should use UserId
        assert!(result.contains("pub id: UserId"), "params id should be UserId: {}", result);
    }

    #[test]
    fn test_aggregate_fields_use_correct_types() {
        let input = r#"
query countOrdersByStatus() {
  from(orders)
  .groupBy(orders.status)
  .aggregate(
    order_count: COUNT(orders.id),
    total_amount: SUM(orders.amount),
    avg_amount: AVG(orders.amount),
    min_amount: MIN(orders.amount),
    max_amount: MAX(orders.amount)
  )
  .select(orders.status, order_count, total_amount, avg_amount, min_amount, max_amount)
}
"#;
        let module = nextsql_core::parser::parse_module(input).expect("parse failed");
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("orders".to_string());
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false, primary_key: true, has_default: true, default_value: None,
        });
        table.add_column(ColumnSchema {
            name: "status".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false, primary_key: false, has_default: false, default_value: None,
        });
        table.add_column(ColumnSchema {
            name: "amount".to_string(),
            column_type: Type::BuiltIn(BuiltInType::F64),
            nullable: false, primary_key: false, has_default: false, default_value: None,
        });
        schema.add_table(table);

        let result = generate_rust_file(&module, &schema);
        let content = &result.content;

        // COUNT should produce i64
        assert!(content.contains("pub order_count: i64"), "COUNT should produce i64, got:\n{}", content);
        // SUM over f64 column should produce f64
        assert!(content.contains("pub total_amount: f64"), "SUM over f64 should produce f64, got:\n{}", content);
        // AVG should produce f64
        assert!(content.contains("pub avg_amount: f64"), "AVG should produce f64, got:\n{}", content);
        // MIN should produce f64
        assert!(content.contains("pub min_amount: f64"), "MIN should produce f64, got:\n{}", content);
        // MAX should produce f64
        assert!(content.contains("pub max_amount: f64"), "MAX should produce f64, got:\n{}", content);
        // status should remain String (from schema)
        assert!(content.contains("pub status: String"), "status should remain String");
    }

    #[test]
    fn test_updatable_codegen() {
        let input = r#"
            valtype AddressId = uuid for addresses.id

            mutation updateAddress($id: AddressId, $changes: Updatable<addresses>) {
                update(addresses)
                .where(addresses.id == $id)
                .set($changes)
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).unwrap();
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("addresses".to_string());
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: None,
        });
        table.add_column(ColumnSchema {
            name: "city".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        table.add_column(ColumnSchema {
            name: "notes".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: true,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(table);

        let result = generate_rust_file(&module, &schema);
        let content = &result.content;

        // Should have UpdateField in the Changes struct
        assert!(content.contains("UpdateField<String>"), "city should be UpdateField<String>: {}", content);
        assert!(content.contains("UpdateField<Option<String>>"), "notes should be UpdateField<Option<String>>: {}", content);
        // Should have Default impl
        assert!(content.contains("impl Default for"), "Should have Default impl: {}", content);
        assert!(content.contains("Unchanged"), "Default should use Unchanged: {}", content);
        // Should have dynamic SQL building
        assert!(content.contains("set_parts"), "Should build SET parts dynamically: {}", content);
        assert!(content.contains("is_set()"), "Should check is_set(): {}", content);
        // Should have Changes struct
        assert!(content.contains("pub struct UpdateAddressChanges"), "Should have Changes struct: {}", content);
        // Should have Params struct with changes field
        assert!(content.contains("pub changes: UpdateAddressChanges"), "Should have changes field in Params: {}", content);
        // Should have id param with valtype
        assert!(content.contains("pub id: AddressId"), "Should have id with AddressId type: {}", content);
        // Should return Result<u64>
        assert!(content.contains("Result<u64,"), "Should return u64 for non-returning: {}", content);
    }

    #[test]
    fn test_updatable_codegen_with_returning() {
        let input = r#"
            mutation updateAddress($id: uuid, $changes: Updatable<addresses>) {
                update(addresses)
                .where(addresses.id == $id)
                .set($changes)
                .returning(addresses.*)
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).unwrap();
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("addresses".to_string());
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: None,
        });
        table.add_column(ColumnSchema {
            name: "city".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(table);

        let result = generate_rust_file(&module, &schema);
        let content = &result.content;

        // Should have RETURNING with model struct
        assert!(content.contains("pub struct Addresse {") || content.contains("pub struct Address {"),
            "Should have model struct for returning: {}", content);
        // Should return Vec, not u64
        assert!(content.contains("Result<Vec<"), "Should return Vec for returning: {}", content);
        // Should use client.query
        assert!(content.contains("client.query(&sql"), "Should use client.query for returning: {}", content);
    }

    #[test]
    fn test_aliased_select_type_from_schema() {
        // Schema: payees table with expense_id as uuid
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("payees".to_string());
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        table.add_column(ColumnSchema {
            name: "expense_id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        schema.add_table(table);

        // query getPayee() {
        //   from(payees)
        //   .select(app_id: payees.expense_id)
        // }
        let module = Module {
            toplevels: vec![TopLevel::Query(Query {
                decl: QueryDecl {
                    name: "getPayee".to_string(),
                    arguments: vec![],
                },
                body: QueryBody {
                    statements: vec![QueryStatement::Select(SelectStatement {
                        from: FromExpr {
                            table: "payees".to_string(),
                            joins: vec![],
                            span: None,
                        },
                        clauses: vec![
                            QueryClause::Select(vec![
                                SelectExpression {
                                    expr: Expression::Atomic(AtomicExpression::Column(
                                        Column::ExplicitTarget(
                                            "payees".to_string(),
                                            "expense_id".to_string(),
                                            None,
                                        ),
                                    )),
                                    alias: Some("app_id".to_string()),
                                },
                            ]),
                        ],
                        unions: vec![],
                    })],
                },
            })],
        };

        let result = generate_rust_file(&module, &schema);
        let content = &result.content;

        // Should have app_id as uuid::Uuid, NOT String
        assert!(
            content.contains("pub app_id: uuid::Uuid"),
            "Aliased column should resolve to uuid::Uuid, not String. Got: {}",
            content
        );
        assert!(
            !content.contains("pub app_id: String"),
            "Aliased column should NOT be String. Got: {}",
            content
        );
    }
}
