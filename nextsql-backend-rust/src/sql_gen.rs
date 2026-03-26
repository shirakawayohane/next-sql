use std::collections::{HashMap, HashSet};
use nextsql_core::ast::*;
use nextsql_core::schema::DatabaseSchema;

fn builtin_type_to_pg_type(typ: &BuiltInType) -> &'static str {
    match typ {
        BuiltInType::I16 => "SMALLINT",
        BuiltInType::I32 => "INTEGER",
        BuiltInType::I64 => "BIGINT",
        BuiltInType::F32 => "REAL",
        BuiltInType::F64 => "DOUBLE PRECISION",
        BuiltInType::String => "TEXT",
        BuiltInType::Bool => "BOOLEAN",
        BuiltInType::Uuid => "UUID",
        BuiltInType::Timestamp => "TIMESTAMP",
        BuiltInType::Timestamptz => "TIMESTAMPTZ",
        BuiltInType::Date => "DATE",
        BuiltInType::Decimal => "NUMERIC",
        BuiltInType::Json => "JSON",
    }
}

/// Information about a single `when` clause for dynamic SQL generation.
#[derive(Debug, Clone)]
pub struct WhenClauseInfo {
    /// The variable name used in the condition (e.g., "name_like").
    /// When this is `Some`, the Rust code checks `params.<var>.is_some()`.
    pub condition_var: Option<String>,
    /// The type of inner clause (e.g., "where", "orderBy").
    pub clause_type: WhenClauseType,
    /// The SQL fragment for this clause body, with `${}` format placeholders
    /// for parameter indices that will be filled at runtime.
    pub clause_sql: String,
    /// Variable names used inside this clause body (in order).
    pub params_in_clause: Vec<String>,
}

/// The type of clause inside a `when`.
#[derive(Debug, Clone, PartialEq)]
pub enum WhenClauseType {
    Where,
    OrderBy,
    Limit,
    Other,
}

/// Result of SQL generation - contains the SQL string and parameter mapping
#[derive(Debug)]
pub struct GeneratedSql {
    /// The SQL string with $1, $2, ... placeholders
    pub sql: String,
    /// Ordered list of parameter variable names (without $ prefix).
    /// For dynamic queries, this only contains static (non-when) params.
    pub params: Vec<String>,
    /// Whether this query has dynamic (when) clauses
    pub is_dynamic: bool,
    /// Information about each `when` clause for dynamic code generation.
    pub when_clauses: Vec<WhenClauseInfo>,
    /// All parameter variable names (static + when), in declaration order.
    /// Used to generate the Params struct with all fields.
    pub all_params: Vec<String>,
    /// The static WHERE clause SQL (without "WHERE " prefix), if any.
    /// Used by dynamic queries to always include the static condition.
    pub static_where_sql: Option<String>,
}

/// Information about a single relation definition.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub name: String,
    pub for_table: String,
    pub returning_table: String,
    pub join_condition: Expression,
    pub is_optional: bool,
}

/// Registry of all relation definitions, keyed by (for_table, relation_name).
#[derive(Debug, Clone)]
pub struct RelationRegistry {
    relations: HashMap<(String, String), RelationInfo>,
}

impl RelationRegistry {
    /// Create an empty registry (backward-compatible default).
    pub fn empty() -> Self {
        RelationRegistry {
            relations: HashMap::new(),
        }
    }

    /// Build a registry from all TopLevel items in a Module.
    pub fn from_module(module: &Module) -> Self {
        let mut relations = HashMap::new();
        for tl in &module.toplevels {
            if let TopLevel::Relation(rel) = tl {
                let returning_table = match &rel.decl.returning_type {
                    RelationReturnType::Table(t) => t.clone(),
                    _ => continue,
                };
                let is_optional = rel.decl.modifiers.contains(&RelationModifier::Optional);
                let info = RelationInfo {
                    name: rel.decl.name.clone(),
                    for_table: rel.decl.for_table.clone(),
                    returning_table,
                    join_condition: rel.join_condition.clone(),
                    is_optional,
                };
                relations.insert((rel.decl.for_table.clone(), rel.decl.name.clone()), info);
            }
        }
        RelationRegistry { relations }
    }

    /// Look up a relation by (table, relation_name).
    pub fn lookup(&self, table: &str, relation_name: &str) -> Option<&RelationInfo> {
        self.relations
            .get(&(table.to_string(), relation_name.to_string()))
    }
}

/// Generate SQL from a SelectStatement
pub fn generate_select_sql(stmt: &SelectStatement) -> GeneratedSql {
    generate_select_sql_with_relations(stmt, &RelationRegistry::empty())
}

/// Generate SQL from a SelectStatement with relation resolution
pub fn generate_select_sql_with_relations(
    stmt: &SelectStatement,
    relations: &RelationRegistry,
) -> GeneratedSql {
    let mut ctx = SqlGenContext::new(relations);
    let sql = ctx.gen_select(stmt);
    let all_params = ctx.all_params_ordered();
    GeneratedSql {
        sql,
        params: ctx.params,
        is_dynamic: ctx.is_dynamic,
        when_clauses: ctx.when_clauses,
        all_params,
        static_where_sql: ctx.static_where_sql,
    }
}

/// Generate SQL from an Insert statement
pub fn generate_insert_sql(insert: &Insert) -> GeneratedSql {
    generate_insert_sql_with_relations(insert, &RelationRegistry::empty())
}

/// Generate SQL from an Insert statement with relation resolution
pub fn generate_insert_sql_with_relations(
    insert: &Insert,
    relations: &RelationRegistry,
) -> GeneratedSql {
    generate_insert_sql_with_schema(insert, relations, None)
}

/// Generate SQL from an Insert statement with relation resolution and schema for enum casts
pub fn generate_insert_sql_with_schema(
    insert: &Insert,
    relations: &RelationRegistry,
    schema: Option<&DatabaseSchema>,
) -> GeneratedSql {
    let mut ctx = match schema {
        Some(s) => SqlGenContext::with_schema(relations, s),
        None => SqlGenContext::new(relations),
    };
    let sql = ctx.gen_insert(insert);
    let all_params = ctx.all_params_ordered();
    GeneratedSql {
        sql,
        params: ctx.params,
        is_dynamic: ctx.is_dynamic,
        when_clauses: ctx.when_clauses,
        all_params,
        static_where_sql: ctx.static_where_sql,
    }
}

/// Generate SQL from an Update statement
pub fn generate_update_sql(update: &Update) -> GeneratedSql {
    generate_update_sql_with_relations(update, &RelationRegistry::empty())
}

/// Generate SQL from an Update statement with relation resolution
pub fn generate_update_sql_with_relations(
    update: &Update,
    relations: &RelationRegistry,
) -> GeneratedSql {
    generate_update_sql_with_schema(update, relations, None)
}

/// Generate SQL from an Update statement with relation resolution and schema for enum casts
pub fn generate_update_sql_with_schema(
    update: &Update,
    relations: &RelationRegistry,
    schema: Option<&DatabaseSchema>,
) -> GeneratedSql {
    let mut ctx = match schema {
        Some(s) => SqlGenContext::with_schema(relations, s),
        None => SqlGenContext::new(relations),
    };
    let sql = ctx.gen_update(update);
    let all_params = ctx.all_params_ordered();
    GeneratedSql {
        sql,
        params: ctx.params,
        is_dynamic: ctx.is_dynamic,
        when_clauses: ctx.when_clauses,
        all_params,
        static_where_sql: ctx.static_where_sql,
    }
}

/// Generate SQL from a Delete statement
pub fn generate_delete_sql(delete: &Delete) -> GeneratedSql {
    generate_delete_sql_with_relations(delete, &RelationRegistry::empty())
}

/// Generate SQL from a Delete statement with relation resolution
pub fn generate_delete_sql_with_relations(
    delete: &Delete,
    relations: &RelationRegistry,
) -> GeneratedSql {
    let mut ctx = SqlGenContext::new(relations);
    let sql = ctx.gen_delete(delete);
    let all_params = ctx.all_params_ordered();
    GeneratedSql {
        sql,
        params: ctx.params,
        is_dynamic: ctx.is_dynamic,
        when_clauses: ctx.when_clauses,
        all_params,
        static_where_sql: ctx.static_where_sql,
    }
}

struct SqlGenContext<'a> {
    params: Vec<String>,
    param_counter: usize,
    is_dynamic: bool,
    relation_registry: &'a RelationRegistry,
    /// JOIN clauses injected by relation resolution, in insertion order.
    injected_joins: Vec<String>,
    /// Keys to avoid duplicate JOIN injection.
    injected_join_keys: HashSet<String>,
    /// Counts for alias deduplication when the same table is joined multiple times.
    /// Maps table_name -> (count, Vec<(alias_key, alias_name)>)
    table_alias_counts: HashMap<String, usize>,
    /// Maps a relation path key (e.g. "posts.author") to the SQL alias used for
    /// the joined table. This allows subsequent accesses to the same relation
    /// to use the correct alias.
    relation_alias_map: HashMap<String, String>,
    /// Collected when clause info for dynamic SQL generation.
    when_clauses: Vec<WhenClauseInfo>,
    /// Static WHERE clause SQL (set during gen_select for dynamic queries).
    static_where_sql: Option<String>,
    /// Optional schema reference for enum type casting in INSERT/UPDATE.
    schema: Option<&'a DatabaseSchema>,
    /// The target table name for INSERT/UPDATE, used to look up column types for enum casts.
    enum_cast_table: Option<String>,
}

impl<'a> SqlGenContext<'a> {
    fn new(relation_registry: &'a RelationRegistry) -> Self {
        Self {
            params: Vec::new(),
            param_counter: 0,
            is_dynamic: false,
            relation_registry,
            injected_joins: Vec::new(),
            injected_join_keys: HashSet::new(),
            table_alias_counts: HashMap::new(),
            relation_alias_map: HashMap::new(),
            when_clauses: Vec::new(),
            static_where_sql: None,
            schema: None,
            enum_cast_table: None,
        }
    }

    fn with_schema(relation_registry: &'a RelationRegistry, schema: &'a DatabaseSchema) -> Self {
        Self {
            schema: Some(schema),
            ..Self::new(relation_registry)
        }
    }

    /// If the column is a PostgreSQL enum type and the value is a literal (not a parameter
    /// placeholder like `$1`), wrap the value expression with a type cast.
    /// e.g. `'approved'` becomes `'approved'::status_type`
    ///
    /// Parameter placeholders (`$N`) are NOT cast because tokio-postgres validates parameter
    /// types and rejects String parameters cast to custom enum types. PostgreSQL handles
    /// implicit text-to-enum conversion for parameters in INSERT/UPDATE contexts.
    fn maybe_enum_cast(&self, column_name: &str, val: String) -> String {
        // Skip cast for parameter placeholders ($1, $2, etc.) — PG handles implicit conversion
        if val.starts_with('$') && val[1..].chars().all(|c| c.is_ascii_digit()) {
            return val;
        }
        if let (Some(schema), Some(table_name)) = (self.schema, &self.enum_cast_table) {
            if let Some(table) = schema.get_table(table_name) {
                if let Some(col) = table.columns.iter().find(|c| c.name == column_name) {
                    if let Type::UserDefined(enum_name) = &col.column_type {
                        if schema.enums.contains_key(enum_name) {
                            return format!("{val}::{enum_name}");
                        }
                    }
                }
            }
        }
        val
    }

    /// Return all params in order: static params first, then when-clause params.
    /// Deduplicates while preserving first-seen order.
    fn all_params_ordered(&self) -> Vec<String> {
        let mut result = self.params.clone();
        let mut seen: HashSet<String> = result.iter().cloned().collect();
        for wc in &self.when_clauses {
            for p in &wc.params_in_clause {
                if seen.insert(p.clone()) {
                    result.push(p.clone());
                }
            }
        }
        result
    }

    fn add_param(&mut self, name: &str) -> String {
        if let Some(idx) = self.params.iter().position(|p| p == name) {
            return format!("${}", idx + 1);
        }
        self.param_counter += 1;
        self.params.push(name.to_string());
        format!("${}", self.param_counter)
    }

    /// Extract the condition variable name from a when condition expression.
    /// Handles patterns like:
    ///   - `$var != null` -> Some("var")
    ///   - `$var.isNotNull()` -> Some("var")
    ///   - Complex expressions -> None (falls back to always-include)
    fn extract_condition_var(expr: &Expression) -> Option<String> {
        match expr {
            // $var != null
            Expression::Binary { left, op: BinaryOp::Unequal, right } => {
                // Check left is variable, right is null (or vice versa)
                if let Expression::Atomic(AtomicExpression::Variable(var)) = left.as_ref() {
                    if matches!(right.as_ref(), Expression::Atomic(AtomicExpression::Literal(Literal::Null))) {
                        return Some(var.name.clone());
                    }
                }
                if let Expression::Atomic(AtomicExpression::Variable(var)) = right.as_ref() {
                    if matches!(left.as_ref(), Expression::Atomic(AtomicExpression::Literal(Literal::Null))) {
                        return Some(var.name.clone());
                    }
                }
                None
            }
            // $var.isNotNull()
            Expression::Atomic(AtomicExpression::MethodCall(mc)) => {
                if mc.method == "isNotNull" {
                    if let Expression::Atomic(AtomicExpression::Variable(var)) = mc.target.as_ref() {
                        return Some(var.name.clone());
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Generate the SQL fragment for a when-clause body.
    /// Returns (clause_type, sql_fragment, params_used).
    /// The sql_fragment uses `${}` placeholders for parameter positions
    /// that will be filled at runtime with dynamic indices.
    fn gen_when_clause_body(&mut self, clause: &QueryClause) -> (WhenClauseType, String, Vec<String>) {
        // Use a sub-context to generate the SQL without polluting the main params.
        let mut sub_ctx = SqlGenContext::new(self.relation_registry);
        // Copy injected join state so relation resolution works
        sub_ctx.injected_join_keys = self.injected_join_keys.clone();
        sub_ctx.table_alias_counts = self.table_alias_counts.clone();
        sub_ctx.relation_alias_map = self.relation_alias_map.clone();

        match clause {
            QueryClause::Where(expr) => {
                let sql = sub_ctx.gen_expr(expr);
                // Replace $N placeholders with ${} for format! substitution
                let params = sub_ctx.params.clone();
                let mut template = sql;
                // Replace $N with ${} in reverse order to avoid replacing $1 inside $10
                for i in (1..=params.len()).rev() {
                    template = template.replace(
                        &format!("${i}"),
                        "${}",
                    );
                }
                // Merge any injected joins back
                for join in sub_ctx.injected_joins {
                    if !self.injected_join_keys.contains(&join) {
                        self.injected_joins.push(join);
                    }
                }
                (WhenClauseType::Where, template, params)
            }
            QueryClause::OrderBy(exprs) => {
                let ob: Vec<String> = exprs
                    .iter()
                    .map(|e| {
                        let expr_sql = sub_ctx.gen_expr(&e.expr);
                        match &e.direction {
                            Some(OrderDirection::Asc) => format!("{expr_sql} ASC"),
                            Some(OrderDirection::Desc) => format!("{expr_sql} DESC"),
                            None => expr_sql,
                        }
                    })
                    .collect();
                let sql = ob.join(", ");
                let params = sub_ctx.params.clone();
                let mut template = sql;
                for i in (1..=params.len()).rev() {
                    template = template.replace(&format!("${i}"), "${}");
                }
                (WhenClauseType::OrderBy, template, params)
            }
            QueryClause::Limit(expr) => {
                let sql = sub_ctx.gen_expr(expr);
                let params = sub_ctx.params.clone();
                let mut template = sql;
                for i in (1..=params.len()).rev() {
                    template = template.replace(&format!("${i}"), "${}");
                }
                (WhenClauseType::Limit, template, params)
            }
            _ => {
                // Fallback for other clause types
                (WhenClauseType::Other, String::new(), Vec::new())
            }
        }
    }


    // ── Expression generation ───────────────────────────────────────────

    fn gen_expr(&mut self, expr: &Expression) -> String {
        match expr {
            Expression::Binary { left, op, right } => {
                let l = self.gen_expr_wrapped(left);
                let r = self.gen_expr_wrapped(right);
                let op_str = match op {
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Equal => "=",
                    BinaryOp::Unequal => "!=",
                    BinaryOp::LessThan => "<",
                    BinaryOp::LessThanOrEqual => "<=",
                    BinaryOp::GreaterThan => ">",
                    BinaryOp::GreaterThanOrEqual => ">=",
                    BinaryOp::Add => "+",
                    BinaryOp::Subtract => "-",
                    BinaryOp::Multiply => "*",
                    BinaryOp::Divide => "/",
                    BinaryOp::Remainder => "%",
                };
                format!("{l} {op_str} {r}")
            }
            Expression::Unary { op, expr } => {
                let e = self.gen_expr(expr);
                match op {
                    UnaryOp::Not => format!("NOT {e}"),
                }
            }
            Expression::Atomic(atomic) => self.gen_atomic(atomic),
        }
    }

    /// Wrap nested binary expressions in parentheses
    fn gen_expr_wrapped(&mut self, expr: &Expression) -> String {
        match expr {
            Expression::Binary { .. } => {
                let s = self.gen_expr(expr);
                format!("({s})")
            }
            _ => self.gen_expr(expr),
        }
    }

    fn gen_atomic(&mut self, atomic: &AtomicExpression) -> String {
        match atomic {
            AtomicExpression::Column(col) => self.gen_column(col),
            AtomicExpression::Literal(lit) => self.gen_literal(lit),
            AtomicExpression::Variable(var) => self.add_param(&var.name),
            AtomicExpression::Call(call) => {
                if call.callee == "excluded" && call.args.len() == 1 {
                    // Special handling for EXCLUDED reference in ON CONFLICT DO UPDATE
                    if let Expression::Atomic(AtomicExpression::Column(col)) = &call.args[0] {
                        match col {
                            Column::ImplicitTarget(name, _) => format!("EXCLUDED.{name}"),
                            Column::ExplicitTarget(_, name, _) => format!("EXCLUDED.{name}"),
                            _ => "EXCLUDED.*".to_string(),
                        }
                    } else {
                        let arg = self.gen_expr(&call.args[0]);
                        format!("EXCLUDED.{arg}")
                    }
                } else {
                    let args: Vec<String> = call.args.iter().map(|a| self.gen_expr(a)).collect();
                    format!("{}({})", call.callee, args.join(", "))
                }
            }
            AtomicExpression::MethodCall(mc) => self.gen_method_call(mc),
            AtomicExpression::PropertyAccess(pa) => {
                if let Expression::Atomic(AtomicExpression::Variable(var)) = &*pa.target {
                    self.add_param(&format!("{}.{}", var.name, pa.property))
                } else {
                    self.resolve_property_access(pa)
                }
            }
            AtomicExpression::IndexAccess(ia) => {
                let target = self.gen_expr(&ia.target);
                let index = self.gen_expr(&ia.index);
                format!("{target}[{index}]")
            }
            AtomicExpression::SubQuery(select) => {
                let sql = self.gen_select(select);
                format!("({sql})")
            }
            AtomicExpression::Aggregate(agg) => {
                let func = match agg.function_type {
                    AggregateFunctionType::Sum => "SUM",
                    AggregateFunctionType::Count => "COUNT",
                    AggregateFunctionType::Avg => "AVG",
                    AggregateFunctionType::Min => "MIN",
                    AggregateFunctionType::Max => "MAX",
                };
                let expr = self.gen_expr(&agg.expr);
                if let Some(filter_expr) = &agg.filter {
                    let filter = self.gen_expr(filter_expr);
                    format!("{func}({expr}) FILTER (WHERE {filter})")
                } else {
                    format!("{func}({expr})")
                }
            }
            AtomicExpression::Exists(select) => {
                let sql = self.gen_select(select);
                format!("EXISTS ({sql})")
            }
            AtomicExpression::Cast(cast) => {
                let expr = self.gen_expr(&cast.expr);
                let pg_type = builtin_type_to_pg_type(&cast.target_type);
                format!("CAST({expr} AS {pg_type})")
            }
        }
    }

    fn gen_column(&self, col: &Column) -> String {
        match col {
            Column::ImplicitTarget(name, _) => name.clone(),
            Column::ExplicitTarget(table, col_name, _) => format!("{table}.{col_name}"),
            Column::WildcardOf(table, _) => format!("{table}.*"),
            Column::Wildcard(_) => "*".to_string(),
        }
    }

    fn gen_literal(&mut self, lit: &Literal) -> String {
        match lit {
            Literal::Numeric(n) => {
                if *n == (*n as i64) as f64 {
                    format!("{}", *n as i64)
                } else {
                    format!("{n}")
                }
            }
            Literal::String(s) => {
                let escaped = s.replace('\'', "''");
                format!("'{escaped}'")
            }
            Literal::Boolean(true) => "TRUE".to_string(),
            Literal::Boolean(false) => "FALSE".to_string(),
            Literal::Null => "NULL".to_string(),
            Literal::Object(obj) => {
                // Object literals in expression context: generate as ROW(...) constructor
                // This handles cases where an object literal appears outside of INSERT context
                let mut keys: Vec<&String> = obj.fields.keys().collect();
                keys.sort();
                let values: Vec<String> = keys
                    .iter()
                    .map(|k| self.gen_expr(obj.fields.get(*k).unwrap()))
                    .collect();
                format!("ROW({})", values.join(", "))
            }
            Literal::Array(arr) => {
                let items: Vec<String> = arr.0.iter().map(|e| self.gen_expr(e)).collect();
                format!("ARRAY[{}]", items.join(", "))
            }
        }
    }

    fn gen_method_call(&mut self, mc: &MethodCall) -> String {
        let target = self.gen_expr(&mc.target);
        match mc.method.as_str() {
            "isNull" => format!("{target} IS NULL"),
            "isNotNull" => format!("{target} IS NOT NULL"),
            "like" => {
                let pattern = self.gen_expr(&mc.args[0]);
                format!("{target} LIKE {pattern}")
            }
            "ilike" => {
                let pattern = self.gen_expr(&mc.args[0]);
                format!("{target} ILIKE {pattern}")
            }
            "between" => {
                let a = self.gen_expr(&mc.args[0]);
                let b = self.gen_expr(&mc.args[1]);
                format!("{target} BETWEEN {a} AND {b}")
            }
            "eqAny" => {
                let arr = self.gen_expr(&mc.args[0]);
                format!("{target} = ANY({arr})")
            }
            "neAny" if mc.args.len() == 1 => {
                let arg = self.gen_expr(&mc.args[0]);
                format!("NOT ({target} = ANY({arg}))")
            }
            "in" => {
                let items: Vec<String> = mc.args.iter().map(|a| self.gen_expr(a)).collect();
                format!("{} IN ({})", target, items.join(", "))
            }
            "asc" => format!("{target} ASC"),
            "desc" => format!("{target} DESC"),
            "contains" => {
                let val = self.gen_expr(&mc.args[0]);
                format!("{target} @> {val}")
            }
            "toLowerCase" => format!("LOWER({target})"),
            "toUpperCase" => format!("UPPER({target})"),
            "trim" => format!("TRIM({target})"),
            _ => {
                let args: Vec<String> = mc.args.iter().map(|a| self.gen_expr(a)).collect();
                format!("{}.{}({})", target, mc.method, args.join(", "))
            }
        }
    }

    // ── Relation resolution ────────────────────────────────────────────

    /// Decompose a PropertyAccess chain into a list of segments.
    /// E.g. `posts.author.name` → ["posts", "author", "name"]
    /// Returns None if the chain doesn't start with a simple identifier (Column).
    fn decompose_property_chain(pa: &PropertyAccess) -> Option<Vec<String>> {
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
                Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(
                    table, col, _,
                ))) => {
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

    /// Resolve a PropertyAccess, injecting JOINs for relations as needed.
    /// Falls back to simple `target.property` if no relation matches.
    fn resolve_property_access(&mut self, pa: &PropertyAccess) -> String {
        let segments = match Self::decompose_property_chain(pa) {
            Some(s) if s.len() >= 3 => s,
            _ => {
                // Not a chain or too short to be a relation access — fall back
                let target = self.gen_expr(&pa.target);
                return format!("{}.{}", target, pa.property);
            }
        };

        // Try to resolve a relation chain starting from segments[0] (table).
        // segments = ["posts", "author", "name"] or
        //            ["posts", "author", "organization", "name"]
        let mut current_table = segments[0].clone();
        let mut resolved_any = false;

        // Walk intermediate segments (all except first and last)
        // to see if they are relations.
        for i in 1..segments.len() - 1 {
            let relation_name = &segments[i];
            if let Some(info) = self.relation_registry.lookup(&current_table, relation_name) {
                resolved_any = true;
                let join_key = segments[0..=i].join(".");

                if !self.injected_join_keys.contains(&join_key) {
                    // Determine alias for the target table
                    let target_table = info.returning_table.clone();
                    let source_table = info.for_table.clone();
                    let is_optional = info.is_optional;
                    let join_condition = info.join_condition.clone();
                    let alias = self.allocate_table_alias(&target_table, &join_key);
                    let join_type = if is_optional {
                        "LEFT JOIN"
                    } else {
                        "INNER JOIN"
                    };

                    // Render the join condition, replacing table references
                    // with appropriate aliases.
                    let condition_sql = self.gen_join_condition_expr(
                        &join_condition,
                        &source_table,
                        &current_table,
                        &target_table,
                        &alias,
                    );

                    let join_sql = if alias == target_table {
                        format!("{join_type} {target_table} ON {condition_sql}")
                    } else {
                        format!(
                            "{join_type} {target_table} AS {alias} ON {condition_sql}"
                        )
                    };

                    self.injected_joins.push(join_sql);
                    self.injected_join_keys.insert(join_key.clone());
                }

                // The current table for the next iteration is the alias we used
                current_table = self
                    .relation_alias_map
                    .get(&join_key)
                    .cloned()
                    .unwrap_or_else(|| info.returning_table.clone());
            } else if resolved_any {
                // We already resolved part of the chain as relations but this
                // segment doesn't match — treat the rest as column access.
                // e.g., resolved "posts.author" → "users", now "users.address.street"
                // where address is a JSONB column, not a relation.
                let remaining: Vec<&str> =
                    segments[i..].iter().map(|s| s.as_str()).collect();
                return format!("{}.{}", current_table, remaining.join("."));
            } else {
                // No relation matched from the start — fall back to default
                let target = self.gen_expr(&pa.target);
                return format!("{}.{}", target, pa.property);
            }
        }

        if resolved_any {
            // The last segment is the column on the resolved table.
            let col = segments.last().unwrap();
            format!("{current_table}.{col}")
        } else {
            // No relations resolved — fall back
            let target = self.gen_expr(&pa.target);
            format!("{}.{}", target, pa.property)
        }
    }

    /// Allocate a table alias, deduplicating when the same table is joined
    /// multiple times. Returns the alias to use.
    fn allocate_table_alias(&mut self, table: &str, join_key: &str) -> String {
        // Check if we already have an alias for this exact join_key
        if let Some(alias) = self.relation_alias_map.get(join_key) {
            return alias.clone();
        }

        let count = self.table_alias_counts.entry(table.to_string()).or_insert(0);
        *count += 1;

        let alias = if *count == 1 {
            table.to_string()
        } else {
            format!("{table}_{count}")
        };

        self.relation_alias_map
            .insert(join_key.to_string(), alias.clone());
        alias
    }

    /// Render a join condition expression, replacing column table references as needed.
    /// `source_alias` is the alias being used for the source (for_table) side,
    /// `target_table` is the original returning table name,
    /// `target_alias` is the alias being used for the returning table.
    fn gen_join_condition_expr(
        &mut self,
        expr: &Expression,
        source_table: &str,
        source_alias: &str,
        target_table: &str,
        target_alias: &str,
    ) -> String {
        match expr {
            Expression::Binary { left, op, right } => {
                let l = self.gen_join_condition_expr(left, source_table, source_alias, target_table, target_alias);
                let r = self.gen_join_condition_expr(right, source_table, source_alias, target_table, target_alias);
                let op_str = match op {
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Equal => "=",
                    BinaryOp::Unequal => "!=",
                    BinaryOp::LessThan => "<",
                    BinaryOp::LessThanOrEqual => "<=",
                    BinaryOp::GreaterThan => ">",
                    BinaryOp::GreaterThanOrEqual => ">=",
                    BinaryOp::Add => "+",
                    BinaryOp::Subtract => "-",
                    BinaryOp::Multiply => "*",
                    BinaryOp::Divide => "/",
                    BinaryOp::Remainder => "%",
                };
                format!("{l} {op_str} {r}")
            }
            Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(tbl, col, _))) => {
                let alias = if tbl == target_table {
                    target_alias
                } else if tbl == source_table {
                    source_alias
                } else {
                    tbl.as_str()
                };
                format!("{alias}.{col}")
            }
            _ => self.gen_expr(expr),
        }
    }

    // ── SELECT generation ───────────────────────────────────────────────

    fn gen_select(&mut self, stmt: &SelectStatement) -> String {
        let mut parts: Vec<String> = Vec::new();

        // Extract clause data
        let mut select_exprs: Vec<&SelectExpression> = Vec::new();
        let mut where_clause: Option<&Expression> = None;
        let mut group_by: Option<&Vec<Expression>> = None;
        let mut having: Option<&Expression> = None;
        let mut order_by: Option<&Vec<OrderByExpression>> = None;
        let mut limit: Option<&Expression> = None;
        let mut offset: Option<&Expression> = None;
        let mut is_distinct = false;
        let mut extra_joins: Vec<&JoinExpr> = Vec::new();
        let mut aggregate_exprs: Vec<&AggregateExpression> = Vec::new();
        let mut has_for_update = false;

        for clause in &stmt.clauses {
            self.process_clause(
                clause,
                &mut select_exprs,
                &mut where_clause,
                &mut group_by,
                &mut having,
                &mut order_by,
                &mut limit,
                &mut offset,
                &mut is_distinct,
                &mut extra_joins,
                &mut aggregate_exprs,
                &mut has_for_update,
            );
        }

        // SELECT [DISTINCT]
        let mut select_part = "SELECT".to_string();
        if is_distinct {
            select_part.push_str(" DISTINCT");
        }

        // Collect aggregate alias names so we can replace bare references in
        // .select() with the full aggregate expression (Bug 3 fix: avoid
        // emitting both a bare reference AND the aggregate definition).
        let aggregate_alias_set: HashSet<&str> = aggregate_exprs
            .iter()
            .map(|ae| ae.alias.as_str())
            .collect();

        // select expressions – skip any that are a bare reference to an
        // aggregate alias; the aggregate loop below will emit the full
        // definition with AS.
        let mut columns: Vec<String> = Vec::new();
        for se in &select_exprs {
            let is_aggregate_ref = match &se.expr {
                Expression::Atomic(AtomicExpression::Column(
                    Column::ImplicitTarget(name, _),
                )) => aggregate_alias_set.contains(name.as_str()),
                _ => false,
            };
            if is_aggregate_ref {
                // Will be emitted below as the full aggregate expression
                continue;
            }
            let col_sql = self.gen_expr(&se.expr);
            // If the select expression has an explicit alias, emit it
            if let Some(ref alias) = se.alias {
                columns.push(format!("{col_sql} AS {alias}"));
            } else {
                columns.push(col_sql);
            }
        }
        for ae in &aggregate_exprs {
            let expr_sql = self.gen_expr(&ae.expr);
            columns.push(format!("{} AS {}", expr_sql, ae.alias));
        }

        if columns.is_empty() {
            select_part.push_str(" *");
        } else {
            select_part.push(' ');
            select_part.push_str(&columns.join(", "));
        }
        parts.push(select_part);

        // Generate WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET SQL fragments
        // before assembling, so that all relation JOINs are collected first.
        let where_sql = where_clause.map(|cond| {
            let w = self.gen_expr(cond);
            // For dynamic queries, save the static WHERE condition separately.
            if self.is_dynamic {
                self.static_where_sql = Some(w.clone());
            }
            format!("WHERE {w}")
        });

        let group_by_sql = group_by.map(|exprs| {
            let gb: Vec<String> = exprs.iter().map(|e| self.gen_expr(e)).collect();
            format!("GROUP BY {}", gb.join(", "))
        });

        let having_sql = having.map(|cond| {
            let h = self.gen_expr(cond);
            format!("HAVING {h}")
        });

        let order_by_sql = order_by.map(|exprs| {
            let ob: Vec<String> = exprs
                .iter()
                .map(|e| {
                    let expr_sql = self.gen_expr(&e.expr);
                    match &e.direction {
                        Some(OrderDirection::Asc) => format!("{expr_sql} ASC"),
                        Some(OrderDirection::Desc) => format!("{expr_sql} DESC"),
                        None => expr_sql,
                    }
                })
                .collect();
            format!("ORDER BY {}", ob.join(", "))
        });

        let limit_sql = limit.map(|expr| {
            let l = self.gen_expr(expr);
            format!("LIMIT {l}")
        });

        let offset_sql = offset.map(|expr| {
            let o = self.gen_expr(expr);
            format!("OFFSET {o}")
        });

        // FROM
        parts.push(format!("FROM {}", stmt.from.table));

        // JOINs from FromExpr
        for join in &stmt.from.joins {
            parts.push(self.gen_join(join));
        }

        // Extra joins from clauses
        for join in &extra_joins {
            parts.push(self.gen_join(join));
        }

        // Relation-injected JOINs (collected during expression generation above)
        for join_sql in std::mem::take(&mut self.injected_joins) {
            parts.push(join_sql);
        }

        if let Some(s) = where_sql {
            parts.push(s);
        }
        if let Some(s) = group_by_sql {
            parts.push(s);
        }
        if let Some(s) = having_sql {
            parts.push(s);
        }
        if let Some(s) = order_by_sql {
            parts.push(s);
        }
        if let Some(s) = limit_sql {
            parts.push(s);
        }
        if let Some(s) = offset_sql {
            parts.push(s);
        }
        if has_for_update {
            parts.push("FOR UPDATE".to_string());
        }

        let mut sql = parts.join(" ");

        // UNION clauses
        for union in &stmt.unions {
            let union_sql = self.gen_select(&union.select);
            match union.union_type {
                UnionType::Union => {
                    sql.push_str(&format!(" UNION {union_sql}"));
                }
                UnionType::UnionAll => {
                    sql.push_str(&format!(" UNION ALL {union_sql}"));
                }
            }
        }

        sql
    }

    #[allow(clippy::too_many_arguments)]
    fn process_clause<'b>(
        &mut self,
        clause: &'b QueryClause,
        select_exprs: &mut Vec<&'b SelectExpression>,
        where_clause: &mut Option<&'b Expression>,
        group_by: &mut Option<&'b Vec<Expression>>,
        having: &mut Option<&'b Expression>,
        order_by: &mut Option<&'b Vec<OrderByExpression>>,
        limit: &mut Option<&'b Expression>,
        offset: &mut Option<&'b Expression>,
        is_distinct: &mut bool,
        extra_joins: &mut Vec<&'b JoinExpr>,
        aggregate_exprs: &mut Vec<&'b AggregateExpression>,
        has_for_update: &mut bool,
    ) {
        match clause {
            QueryClause::Select(exprs) => {
                select_exprs.extend(exprs.iter());
            }
            QueryClause::Where(expr) => {
                *where_clause = Some(expr);
            }
            QueryClause::GroupBy(exprs) => {
                *group_by = Some(exprs);
            }
            QueryClause::Having(expr) => {
                *having = Some(expr);
            }
            QueryClause::OrderBy(exprs) => {
                *order_by = Some(exprs);
            }
            QueryClause::Limit(expr) => {
                *limit = Some(expr);
            }
            QueryClause::Offset(expr) => {
                *offset = Some(expr);
            }
            QueryClause::Distinct => {
                *is_distinct = true;
            }
            QueryClause::Join(join) => {
                extra_joins.push(join);
            }
            QueryClause::Aggregate(exprs) => {
                aggregate_exprs.extend(exprs.iter());
            }
            QueryClause::ForUpdate => {
                *has_for_update = true;
            }
            QueryClause::When(when_clause) => {
                self.is_dynamic = true;
                // Extract the condition variable name from the when condition.
                let condition_var = Self::extract_condition_var(&when_clause.condition);

                // Generate the SQL fragment for the inner clause body using
                // a separate sub-context so params are tracked independently.
                let (clause_type, clause_sql, params_in_clause) =
                    self.gen_when_clause_body(&when_clause.clause);

                self.when_clauses.push(WhenClauseInfo {
                    condition_var,
                    clause_type,
                    clause_sql,
                    params_in_clause,
                });
                // Do NOT process the inner clause into the regular slots -
                // that was the bug causing overwriting.
            }
        }
    }

    fn gen_join(&mut self, join: &JoinExpr) -> String {
        let join_type = match join.join_type {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::FullOuter => "FULL OUTER JOIN",
            JoinType::Cross => "CROSS JOIN",
        };
        let cond = self.gen_expr(&join.condition);
        format!("{} {} ON {}", join_type, join.table, cond)
    }

    // ── INSERT generation ───────────────────────────────────────────────

    fn gen_insert_object(
        &mut self,
        obj: &ObjectLiteralExpression,
        all_columns: &mut Vec<String>,
    ) -> Vec<String> {
        // Use sorted keys for deterministic output
        let mut keys: Vec<&String> = obj.fields.keys().collect();
        keys.sort();

        if all_columns.is_empty() {
            *all_columns = keys.iter().map(|k| k.to_string()).collect();
        }

        keys.iter()
            .map(|k| {
                let val = self.gen_expr(obj.fields.get(*k).unwrap());
                self.maybe_enum_cast(k, val)
            })
            .collect()
    }

    fn gen_insert(&mut self, insert: &Insert) -> String {
        // Set the target table for enum type casting
        self.enum_cast_table = Some(insert.into.name.clone());

        // Collect columns and values from the value expressions.
        // Values are expected to be Object literals, arrays of Object literals,
        // or variables (for Insertable<T> pattern).
        let mut all_columns: Vec<String> = Vec::new();
        let mut all_value_rows: Vec<Vec<String>> = Vec::new();

        for value_expr in &insert.values {
            match value_expr {
                Expression::Atomic(AtomicExpression::Literal(Literal::Object(obj))) => {
                    let values = self.gen_insert_object(obj, &mut all_columns);
                    all_value_rows.push(values);
                }
                Expression::Atomic(AtomicExpression::Literal(Literal::Array(arr))) => {
                    // Array of object literals: .values([{...}, {...}])
                    for item in &arr.0 {
                        match item {
                            Expression::Atomic(AtomicExpression::Literal(Literal::Object(
                                obj,
                            ))) => {
                                let values = self.gen_insert_object(obj, &mut all_columns);
                                all_value_rows.push(values);
                            }
                            _ => {
                                // Non-object array item: generate the expression directly
                                all_value_rows.push(vec![self.gen_expr(item)]);
                            }
                        }
                    }
                }
                _ => {
                    // Non-object values (e.g., variables for Insertable<T>): generate directly
                    all_value_rows.push(vec![self.gen_expr(value_expr)]);
                }
            }
        }

        let cols_str = if all_columns.is_empty() {
            String::new()
        } else {
            format!(" ({})", all_columns.join(", "))
        };

        let rows: Vec<String> = all_value_rows
            .iter()
            .map(|row| format!("({})", row.join(", ")))
            .collect();

        let mut sql = format!(
            "INSERT INTO {}{} VALUES {}",
            insert.into.name,
            cols_str,
            rows.join(", ")
        );

        // ON CONFLICT
        if let Some(on_conflict) = &insert.on_conflict {
            let conflict_cols = on_conflict.columns.join(", ");
            match &on_conflict.action {
                OnConflictAction::DoNothing => {
                    sql.push_str(&format!(" ON CONFLICT ({conflict_cols}) DO NOTHING"));
                }
                OnConflictAction::DoUpdate(sets) => {
                    let set_parts: Vec<String> = sets
                        .iter()
                        .map(|(col, expr)| {
                            let val = self.gen_expr(expr);
                            let val = self.maybe_enum_cast(col, val);
                            format!("{col} = {val}")
                        })
                        .collect();
                    sql.push_str(&format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        conflict_cols,
                        set_parts.join(", ")
                    ));
                }
            }
        }

        // RETURNING
        if let Some(returning) = &insert.returning {
            let cols: Vec<String> = returning.iter().map(|c| self.gen_column(c)).collect();
            sql.push_str(&format!(" RETURNING {}", cols.join(", ")));
        }

        sql
    }

    // ── UPDATE generation ───────────────────────────────────────────────

    fn gen_update(&mut self, update: &Update) -> String {
        // Set the target table for enum type casting
        self.enum_cast_table = Some(update.target.name.clone());

        let set_parts: Vec<String> = update
            .set
            .iter()
            .map(|(col, expr)| {
                let val = self.gen_expr(expr);
                let val = self.maybe_enum_cast(col, val);
                format!("{col} = {val}")
            })
            .collect();

        let mut sql = format!(
            "UPDATE {} SET {}",
            update.target.name,
            set_parts.join(", ")
        );

        if let Some(where_clause) = &update.where_clause {
            let w = self.gen_expr(where_clause);
            sql.push_str(&format!(" WHERE {w}"));
        }

        if let Some(returning) = &update.returning {
            let cols: Vec<String> = returning.iter().map(|c| self.gen_column(c)).collect();
            sql.push_str(&format!(" RETURNING {}", cols.join(", ")));
        }

        sql
    }

    // ── DELETE generation ───────────────────────────────────────────────

    fn gen_delete(&mut self, delete: &Delete) -> String {
        let mut sql = format!("DELETE FROM {}", delete.target.name);

        if let Some(where_clause) = &delete.where_clause {
            let w = self.gen_expr(where_clause);
            sql.push_str(&format!(" WHERE {w}"));
        }

        if let Some(returning) = &delete.returning {
            let cols: Vec<String> = returning.iter().map(|c| self.gen_column(c)).collect();
            sql.push_str(&format!(" RETURNING {}", cols.join(", ")));
        }

        sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── Helper constructors ─────────────────────────────────────────────

    fn col_explicit(table: &str, col: &str) -> Expression {
        Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(
            table.to_string(),
            col.to_string(),
            None,
        )))
    }

    fn col_implicit(name: &str) -> Expression {
        Expression::Atomic(AtomicExpression::Column(Column::ImplicitTarget(
            name.to_string(),
            None,
        )))
    }

    fn col_wildcard() -> Expression {
        Expression::Atomic(AtomicExpression::Column(Column::Wildcard(None)))
    }

    fn col_wildcard_of(table: &str) -> Expression {
        Expression::Atomic(AtomicExpression::Column(Column::WildcardOf(
            table.to_string(),
            None,
        )))
    }

    fn var(name: &str) -> Expression {
        Expression::Atomic(AtomicExpression::Variable(Variable {
            name: name.to_string(),
            span: None,
        }))
    }

    fn num(n: f64) -> Expression {
        Expression::Atomic(AtomicExpression::Literal(Literal::Numeric(n)))
    }

    fn str_lit(s: &str) -> Expression {
        Expression::Atomic(AtomicExpression::Literal(Literal::String(s.to_string())))
    }

    fn bool_lit(b: bool) -> Expression {
        Expression::Atomic(AtomicExpression::Literal(Literal::Boolean(b)))
    }

    fn null_lit() -> Expression {
        Expression::Atomic(AtomicExpression::Literal(Literal::Null))
    }

    fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
        Expression::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn select_expr(expr: Expression) -> SelectExpression {
        SelectExpression { expr, alias: None }
    }

    fn simple_from(table: &str) -> FromExpr {
        FromExpr {
            table: table.to_string(),
            joins: vec![],
            span: None,
        }
    }

    // ── Expression tests ────────────────────────────────────────────────

    #[test]
    fn test_binary_expression() {
        let expr = binary(col_explicit("u", "age"), BinaryOp::GreaterThan, num(18.0));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&expr), "u.age > 18");
    }

    #[test]
    fn test_nested_binary_wraps_parens() {
        let inner = binary(num(1.0), BinaryOp::Add, num(2.0));
        let outer = binary(inner, BinaryOp::Multiply, num(3.0));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&outer), "(1 + 2) * 3");
    }

    #[test]
    fn test_and_or() {
        let left = binary(col_implicit("a"), BinaryOp::Equal, num(1.0));
        let right = binary(col_implicit("b"), BinaryOp::Equal, num(2.0));
        let expr = binary(left, BinaryOp::And, right);
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&expr), "(a = 1) AND (b = 2)");
    }

    #[test]
    fn test_unary_not() {
        let expr = Expression::Unary {
            op: UnaryOp::Not,
            expr: Box::new(bool_lit(true)),
        };
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&expr), "NOT TRUE");
    }

    #[test]
    fn test_all_binary_ops() {
        let ops = vec![
            (BinaryOp::Equal, "="),
            (BinaryOp::Unequal, "!="),
            (BinaryOp::LessThan, "<"),
            (BinaryOp::LessThanOrEqual, "<="),
            (BinaryOp::GreaterThan, ">"),
            (BinaryOp::GreaterThanOrEqual, ">="),
            (BinaryOp::Add, "+"),
            (BinaryOp::Subtract, "-"),
            (BinaryOp::Multiply, "*"),
            (BinaryOp::Divide, "/"),
            (BinaryOp::Remainder, "%"),
            (BinaryOp::Or, "OR"),
        ];
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        for (op, expected) in ops {
            let expr = binary(num(1.0), op, num(2.0));
            let sql = ctx.gen_expr(&expr);
            assert!(sql.contains(expected), "Expected '{expected}' in '{sql}'");
        }
    }

    // ── Column tests ────────────────────────────────────────────────────

    #[test]
    fn test_columns() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&col_implicit("name")), "name");
        assert_eq!(ctx.gen_expr(&col_explicit("users", "id")), "users.id");
        assert_eq!(ctx.gen_expr(&col_wildcard()), "*");
        assert_eq!(ctx.gen_expr(&col_wildcard_of("users")), "users.*");
    }

    // ── Literal tests ───────────────────────────────────────────────────

    #[test]
    fn test_literals() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&num(42.0)), "42");
        assert_eq!(ctx.gen_expr(&num(3.15)), "3.15");
        assert_eq!(ctx.gen_expr(&str_lit("hello")), "'hello'");
        assert_eq!(ctx.gen_expr(&str_lit("it's")), "'it''s'");
        assert_eq!(ctx.gen_expr(&bool_lit(true)), "TRUE");
        assert_eq!(ctx.gen_expr(&bool_lit(false)), "FALSE");
        assert_eq!(ctx.gen_expr(&null_lit()), "NULL");
    }

    #[test]
    fn test_array_literal() {
        let arr = Expression::Atomic(AtomicExpression::Literal(Literal::Array(
            ArrayExpression(vec![num(1.0), num(2.0), num(3.0)]),
        )));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&arr), "ARRAY[1, 2, 3]");
    }

    #[test]
    fn test_object_literal_generates_row() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), str_lit("Alice"));
        map.insert("age".to_string(), num(30.0));
        let obj = Expression::Atomic(AtomicExpression::Literal(Literal::Object(
            ObjectLiteralExpression { fields: map, span: None },
        )));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        let result = ctx.gen_expr(&obj);
        // Keys are sorted, so "age" comes before "name"
        assert_eq!(result, "ROW(30, 'Alice')");
    }

    #[test]
    fn test_empty_object_literal_generates_row() {
        let obj = Expression::Atomic(AtomicExpression::Literal(Literal::Object(
            ObjectLiteralExpression { fields: HashMap::new(), span: None },
        )));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&obj), "ROW()");
    }

    // ── Variable / parameter tests ──────────────────────────────────────

    #[test]
    fn test_variable_params() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&var("id")), "$1");
        assert_eq!(ctx.gen_expr(&var("name")), "$2");
        // Reuse existing param
        assert_eq!(ctx.gen_expr(&var("id")), "$1");
        assert_eq!(ctx.params, vec!["id", "name"]);
    }

    // ── Call expression test ────────────────────────────────────────────

    #[test]
    fn test_call_expression() {
        let call = Expression::Atomic(AtomicExpression::Call(CallExpression {
            callee: "COALESCE".to_string(),
            args: vec![col_implicit("name"), str_lit("unknown")],
            span: None,
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&call), "COALESCE(name, 'unknown')");
    }

    // ── Method call tests ───────────────────────────────────────────────

    #[test]
    fn test_method_is_null() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("email")),
            method: "isNull".to_string(),
            args: vec![],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "email IS NULL");
    }

    #[test]
    fn test_method_is_not_null() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("email")),
            method: "isNotNull".to_string(),
            args: vec![],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "email IS NOT NULL");
    }

    #[test]
    fn test_method_like() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("name")),
            method: "like".to_string(),
            args: vec![str_lit("%foo%")],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "name LIKE '%foo%'");
    }

    #[test]
    fn test_method_ilike() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("name")),
            method: "ilike".to_string(),
            args: vec![str_lit("%foo%")],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "name ILIKE '%foo%'");
    }

    #[test]
    fn test_method_between() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("age")),
            method: "between".to_string(),
            args: vec![num(18.0), num(65.0)],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "age BETWEEN 18 AND 65");
    }

    #[test]
    fn test_method_eq_any() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("id")),
            method: "eqAny".to_string(),
            args: vec![var("ids")],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "id = ANY($1)");
    }

    #[test]
    fn test_method_ne_any() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("id")),
            method: "neAny".to_string(),
            args: vec![var("ids")],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "NOT (id = ANY($1))");
    }

    #[test]
    fn test_excluded_in_on_conflict() {
        let mut map = HashMap::new();
        map.insert("email".to_string(), var("email"));
        map.insert("name".to_string(), var("name"));

        // excluded(name) → Call { callee: "excluded", args: [Column(ImplicitTarget("name"))] }
        let excluded_name = Expression::Atomic(AtomicExpression::Call(CallExpression {
            callee: "excluded".to_string(),
            args: vec![col_implicit("name")],
            span: None,
        }));

        let insert = Insert {
            into: Target { name: "users".to_string(), span: None },
            values: vec![Expression::Atomic(AtomicExpression::Literal(
                Literal::Object(ObjectLiteralExpression { fields: map, span: None }),
            ))],
            on_conflict: Some(OnConflictClause {
                columns: vec!["email".to_string()],
                action: OnConflictAction::DoUpdate(vec![(
                    "name".to_string(),
                    excluded_name,
                )]),
            }),
            returning: None,
        };
        let result = generate_insert_sql(&insert);
        assert!(
            result.sql.contains("ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name"),
            "Expected 'EXCLUDED.name' in: {}",
            result.sql
        );
    }

    #[test]
    fn test_method_in() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("status")),
            method: "in".to_string(),
            args: vec![str_lit("active"), str_lit("pending")],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "status IN ('active', 'pending')");
    }

    #[test]
    fn test_method_asc_desc() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);

        let asc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("name")),
            method: "asc".to_string(),
            args: vec![],
        }));
        assert_eq!(ctx.gen_expr(&asc), "name ASC");

        let desc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("name")),
            method: "desc".to_string(),
            args: vec![],
        }));
        assert_eq!(ctx.gen_expr(&desc), "name DESC");
    }

    #[test]
    fn test_method_contains() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("tags")),
            method: "contains".to_string(),
            args: vec![var("tag")],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "tags @> $1");
    }

    #[test]
    fn test_method_string_functions() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);

        let lower = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("name")),
            method: "toLowerCase".to_string(),
            args: vec![],
        }));
        assert_eq!(ctx.gen_expr(&lower), "LOWER(name)");

        let upper = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("name")),
            method: "toUpperCase".to_string(),
            args: vec![],
        }));
        assert_eq!(ctx.gen_expr(&upper), "UPPER(name)");

        let trim = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("name")),
            method: "trim".to_string(),
            args: vec![],
        }));
        assert_eq!(ctx.gen_expr(&trim), "TRIM(name)");
    }

    #[test]
    fn test_method_generic_fallback() {
        let mc = Expression::Atomic(AtomicExpression::MethodCall(MethodCall {
            target: Box::new(col_implicit("geom")),
            method: "st_distance".to_string(),
            args: vec![var("point")],
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&mc), "geom.st_distance($1)");
    }

    // ── PropertyAccess / IndexAccess ────────────────────────────────────

    #[test]
    fn test_property_access() {
        let pa = Expression::Atomic(AtomicExpression::PropertyAccess(PropertyAccess {
            target: Box::new(col_implicit("data")),
            property: "name".to_string(),
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&pa), "data.name");
    }

    #[test]
    fn test_index_access() {
        let ia = Expression::Atomic(AtomicExpression::IndexAccess(IndexAccess {
            target: Box::new(col_implicit("arr")),
            index: Box::new(num(0.0)),
        }));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&ia), "arr[0]");
    }

    // ── Aggregate tests ─────────────────────────────────────────────────

    #[test]
    fn test_aggregates() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        let types = vec![
            (AggregateFunctionType::Sum, "SUM(amount)"),
            (AggregateFunctionType::Count, "COUNT(id)"),
            (AggregateFunctionType::Avg, "AVG(score)"),
            (AggregateFunctionType::Min, "MIN(price)"),
            (AggregateFunctionType::Max, "MAX(price)"),
        ];
        let cols = ["amount", "id", "score", "price", "price"];
        for ((func_type, expected), col) in types.into_iter().zip(cols.iter()) {
            let agg = Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                function_type: func_type,
                expr: Box::new(col_implicit(col)),
                filter: None,
            }));
            assert_eq!(ctx.gen_expr(&agg), expected);
        }
    }

    #[test]
    fn test_aggregate_with_filter() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        let agg = Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
            function_type: AggregateFunctionType::Count,
            expr: Box::new(col_implicit("id")),
            filter: Some(Box::new(binary(
                col_implicit("status"),
                BinaryOp::Equal,
                str_lit("active"),
            ))),
        }));
        assert_eq!(
            ctx.gen_expr(&agg),
            "COUNT(id) FILTER (WHERE status = 'active')"
        );
    }

    // ── Exists ──────────────────────────────────────────────────────────

    #[test]
    fn test_exists() {
        let exists = Expression::Atomic(AtomicExpression::Exists(Box::new(SelectStatement {
            from: simple_from("orders"),
            clauses: vec![
                QueryClause::Where(binary(
                    col_explicit("orders", "user_id"),
                    BinaryOp::Equal,
                    col_explicit("users", "id"),
                )),
                QueryClause::Select(vec![select_expr(num(1.0))]),
            ],
            unions: vec![],
        })));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(
            ctx.gen_expr(&exists),
            "EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
        );
    }

    // ── SubQuery ────────────────────────────────────────────────────────

    #[test]
    fn test_subquery() {
        let sub = Expression::Atomic(AtomicExpression::SubQuery(Box::new(SelectStatement {
            from: simple_from("users"),
            clauses: vec![QueryClause::Select(vec![select_expr(col_implicit("id"))])],
            unions: vec![],
        })));
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        assert_eq!(ctx.gen_expr(&sub), "(SELECT id FROM users)");
    }

    // ── SELECT statement tests ──────────────────────────────────────────

    #[test]
    fn test_simple_select() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![QueryClause::Select(vec![
                select_expr(col_wildcard_of("users")),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(result.sql, "SELECT users.* FROM users");
        assert!(result.params.is_empty());
    }

    #[test]
    fn test_select_with_where() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![
                QueryClause::Where(binary(
                    col_explicit("users", "id"),
                    BinaryOp::Equal,
                    var("id"),
                )),
                QueryClause::Select(vec![select_expr(col_wildcard_of("users"))]),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(result.sql, "SELECT users.* FROM users WHERE users.id = $1");
        assert_eq!(result.params, vec!["id"]);
    }

    #[test]
    fn test_select_distinct() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![
                QueryClause::Distinct,
                QueryClause::Select(vec![select_expr(col_implicit("name"))]),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(result.sql, "SELECT DISTINCT name FROM users");
    }

    #[test]
    fn test_select_with_join() {
        let stmt = SelectStatement {
            from: FromExpr {
                table: "users".to_string(),
                joins: vec![JoinExpr {
                    join_type: JoinType::Inner,
                    table: "posts".to_string(),
                    condition: binary(
                        col_explicit("users", "id"),
                        BinaryOp::Equal,
                        col_explicit("posts", "user_id"),
                    ),
                    span: None,
                }],
                span: None,
            },
            clauses: vec![QueryClause::Select(vec![
                select_expr(col_explicit("users", "name")),
                select_expr(col_explicit("posts", "title")),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(
            result.sql,
            "SELECT users.name, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id"
        );
    }

    #[test]
    fn test_select_all_join_types() {
        let join_types = vec![
            (JoinType::Left, "LEFT JOIN"),
            (JoinType::Right, "RIGHT JOIN"),
            (JoinType::FullOuter, "FULL OUTER JOIN"),
            (JoinType::Cross, "CROSS JOIN"),
        ];
        for (jt, expected) in join_types {
            let stmt = SelectStatement {
                from: FromExpr {
                    table: "a".to_string(),
                    joins: vec![JoinExpr {
                        join_type: jt,
                        table: "b".to_string(),
                        condition: binary(
                            col_explicit("a", "id"),
                            BinaryOp::Equal,
                            col_explicit("b", "a_id"),
                        ),
                        span: None,
                    }],
                    span: None,
                },
                clauses: vec![QueryClause::Select(vec![select_expr(col_wildcard())])],
                unions: vec![],
            };
            let result = generate_select_sql(&stmt);
            assert!(
                result.sql.contains(expected),
                "Expected '{}' in '{}'",
                expected,
                result.sql
            );
        }
    }

    #[test]
    fn test_select_with_group_by_having() {
        let stmt = SelectStatement {
            from: simple_from("orders"),
            clauses: vec![
                QueryClause::Select(vec![select_expr(col_implicit("status"))]),
                QueryClause::Aggregate(vec![AggregateExpression {
                    alias: "total".to_string(),
                    expr: Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                        function_type: AggregateFunctionType::Count,
                        expr: Box::new(col_wildcard()),
                        filter: None,
                    })),
                }]),
                QueryClause::GroupBy(vec![col_implicit("status")]),
                QueryClause::Having(binary(
                    Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                        function_type: AggregateFunctionType::Count,
                        expr: Box::new(col_wildcard()),
                        filter: None,
                    })),
                    BinaryOp::GreaterThan,
                    num(5.0),
                )),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(
            result.sql,
            "SELECT status, COUNT(*) AS total FROM orders GROUP BY status HAVING COUNT(*) > 5"
        );
    }

    #[test]
    fn test_select_with_order_by() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![
                QueryClause::Select(vec![select_expr(col_wildcard())]),
                QueryClause::OrderBy(vec![
                    OrderByExpression {
                        expr: col_implicit("name"),
                        direction: Some(OrderDirection::Asc),
                    },
                    OrderByExpression {
                        expr: col_implicit("age"),
                        direction: Some(OrderDirection::Desc),
                    },
                    OrderByExpression {
                        expr: col_implicit("id"),
                        direction: None,
                    },
                ]),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(
            result.sql,
            "SELECT * FROM users ORDER BY name ASC, age DESC, id"
        );
    }

    #[test]
    fn test_select_with_limit_offset() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![
                QueryClause::Select(vec![select_expr(col_wildcard())]),
                QueryClause::Limit(num(10.0)),
                QueryClause::Offset(num(20.0)),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(result.sql, "SELECT * FROM users LIMIT 10 OFFSET 20");
    }

    #[test]
    fn test_select_with_union() {
        let stmt = SelectStatement {
            from: simple_from("active_users"),
            clauses: vec![QueryClause::Select(vec![select_expr(col_implicit("name"))])],
            unions: vec![UnionClause {
                union_type: UnionType::Union,
                select: Box::new(SelectStatement {
                    from: simple_from("archived_users"),
                    clauses: vec![QueryClause::Select(vec![select_expr(col_implicit("name"))])],
                    unions: vec![],
                }),
            }],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(
            result.sql,
            "SELECT name FROM active_users UNION SELECT name FROM archived_users"
        );
    }

    #[test]
    fn test_select_with_union_all() {
        let stmt = SelectStatement {
            from: simple_from("a"),
            clauses: vec![QueryClause::Select(vec![select_expr(col_wildcard())])],
            unions: vec![UnionClause {
                union_type: UnionType::UnionAll,
                select: Box::new(SelectStatement {
                    from: simple_from("b"),
                    clauses: vec![QueryClause::Select(vec![select_expr(col_wildcard())])],
                    unions: vec![],
                }),
            }],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(result.sql, "SELECT * FROM a UNION ALL SELECT * FROM b");
    }

    #[test]
    fn test_select_no_select_clause_defaults_to_star() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(result.sql, "SELECT * FROM users");
    }

    #[test]
    fn test_select_with_when_clause_sets_dynamic() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![
                QueryClause::Select(vec![select_expr(col_wildcard())]),
                QueryClause::When(WhenClause {
                    condition: Box::new(binary(
                        var("name"),
                        BinaryOp::Unequal,
                        null_lit(),
                    )),
                    clause: Box::new(QueryClause::Where(binary(
                        col_implicit("name"),
                        BinaryOp::Equal,
                        var("name"),
                    ))),
                }),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert!(result.is_dynamic);
        // When clauses should NOT be in the static SQL
        assert!(!result.sql.contains("WHERE"), "static SQL should not contain WHERE for when-only queries");
        // Params should be in when_clauses, not in the static params
        assert!(result.params.is_empty(), "static params should be empty, got: {:?}", result.params);
        assert_eq!(result.when_clauses.len(), 1);
        assert!(result.when_clauses[0].params_in_clause.contains(&"name".to_string()));
        assert_eq!(result.when_clauses[0].condition_var, Some("name".to_string()));
        // all_params should contain the when-clause params
        assert!(result.all_params.contains(&"name".to_string()), "all_params should contain 'name', got: {:?}", result.all_params);
    }

    #[test]
    fn test_when_clause_params_from_multiple_whens() {
        // Simulates: .when($name != null, where(name == $name)).when($age != null, where(age >= $age))
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![
                QueryClause::Select(vec![select_expr(col_wildcard())]),
                QueryClause::When(WhenClause {
                    condition: Box::new(binary(var("name"), BinaryOp::Unequal, null_lit())),
                    clause: Box::new(QueryClause::Where(binary(
                        col_implicit("name"),
                        BinaryOp::Equal,
                        var("name"),
                    ))),
                }),
                QueryClause::When(WhenClause {
                    condition: Box::new(binary(var("age"), BinaryOp::Unequal, null_lit())),
                    clause: Box::new(QueryClause::Where(binary(
                        col_implicit("age"),
                        BinaryOp::GreaterThanOrEqual,
                        var("age"),
                    ))),
                }),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert!(result.is_dynamic);
        // Both when clauses should be captured
        assert_eq!(result.when_clauses.len(), 2);
        assert!(result.when_clauses[0].params_in_clause.contains(&"name".to_string()));
        assert!(result.when_clauses[1].params_in_clause.contains(&"age".to_string()));
        // all_params should contain both
        assert!(result.all_params.contains(&"name".to_string()), "all_params should contain 'name', got: {:?}", result.all_params);
        assert!(result.all_params.contains(&"age".to_string()), "all_params should contain 'age', got: {:?}", result.all_params);
    }

    // ── INSERT tests ────────────────────────────────────────────────────

    #[test]
    fn test_simple_insert() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), var("name"));
        map.insert("email".to_string(), var("email"));

        let insert = Insert {
            into: Target { name: "users".to_string(), span: None },
            values: vec![Expression::Atomic(AtomicExpression::Literal(
                Literal::Object(ObjectLiteralExpression { fields: map, span: None }),
            ))],
            on_conflict: None,
            returning: None,
        };
        let result = generate_insert_sql(&insert);
        // Keys are sorted, so "email" comes before "name"
        assert_eq!(
            result.sql,
            "INSERT INTO users (email, name) VALUES ($1, $2)"
        );
        assert_eq!(result.params, vec!["email", "name"]);
    }

    #[test]
    fn test_insert_with_returning() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), var("name"));

        let insert = Insert {
            into: Target { name: "users".to_string(), span: None },
            values: vec![Expression::Atomic(AtomicExpression::Literal(
                Literal::Object(ObjectLiteralExpression { fields: map, span: None }),
            ))],
            on_conflict: None,
            returning: Some(vec![
                Column::Wildcard(None),
            ]),
        };
        let result = generate_insert_sql(&insert);
        assert!(result.sql.contains("RETURNING *"));
    }

    #[test]
    fn test_insert_on_conflict_do_nothing() {
        let mut map = HashMap::new();
        map.insert("email".to_string(), var("email"));

        let insert = Insert {
            into: Target { name: "users".to_string(), span: None },
            values: vec![Expression::Atomic(AtomicExpression::Literal(
                Literal::Object(ObjectLiteralExpression { fields: map, span: None }),
            ))],
            on_conflict: Some(OnConflictClause {
                columns: vec!["email".to_string()],
                action: OnConflictAction::DoNothing,
            }),
            returning: None,
        };
        let result = generate_insert_sql(&insert);
        assert!(result.sql.contains("ON CONFLICT (email) DO NOTHING"));
    }

    #[test]
    fn test_insert_on_conflict_do_update() {
        let mut map = HashMap::new();
        map.insert("email".to_string(), var("email"));
        map.insert("name".to_string(), var("name"));

        let insert = Insert {
            into: Target { name: "users".to_string(), span: None },
            values: vec![Expression::Atomic(AtomicExpression::Literal(
                Literal::Object(ObjectLiteralExpression { fields: map, span: None }),
            ))],
            on_conflict: Some(OnConflictClause {
                columns: vec!["email".to_string()],
                action: OnConflictAction::DoUpdate(vec![(
                    "name".to_string(),
                    var("name"),
                )]),
            }),
            returning: None,
        };
        let result = generate_insert_sql(&insert);
        assert!(result.sql.contains("ON CONFLICT (email) DO UPDATE SET name = $2"));
    }

    // ── UPDATE tests ────────────────────────────────────────────────────

    #[test]
    fn test_simple_update() {
        let update = Update {
            target: Target {
                name: "users".to_string(),
                span: None,
            },
            set: vec![
                ("name".to_string(), var("name")),
                ("email".to_string(), var("email")),
            ],
            set_variable: None,
            where_clause: Some(binary(
                col_explicit("users", "id"),
                BinaryOp::Equal,
                var("id"),
            )),
            returning: None,
        };
        let result = generate_update_sql(&update);
        assert_eq!(
            result.sql,
            "UPDATE users SET name = $1, email = $2 WHERE users.id = $3"
        );
        assert_eq!(result.params, vec!["name", "email", "id"]);
    }

    #[test]
    fn test_update_no_where() {
        let update = Update {
            target: Target {
                name: "users".to_string(),
                span: None,
            },
            set: vec![("active".to_string(), bool_lit(false))],
            set_variable: None,
            where_clause: None,
            returning: None,
        };
        let result = generate_update_sql(&update);
        assert_eq!(result.sql, "UPDATE users SET active = FALSE");
    }

    #[test]
    fn test_update_with_returning() {
        let update = Update {
            target: Target {
                name: "users".to_string(),
                span: None,
            },
            set: vec![("name".to_string(), var("name"))],
            set_variable: None,
            where_clause: Some(binary(
                col_implicit("id"),
                BinaryOp::Equal,
                var("id"),
            )),
            returning: Some(vec![Column::Wildcard(None)]),
        };
        let result = generate_update_sql(&update);
        assert!(result.sql.contains("RETURNING *"));
    }

    // ── DELETE tests ────────────────────────────────────────────────────

    #[test]
    fn test_simple_delete() {
        let delete = Delete {
            target: Target {
                name: "users".to_string(),
                span: None,
            },
            where_clause: Some(binary(
                col_explicit("users", "id"),
                BinaryOp::Equal,
                var("id"),
            )),
            returning: None,
        };
        let result = generate_delete_sql(&delete);
        assert_eq!(result.sql, "DELETE FROM users WHERE users.id = $1");
        assert_eq!(result.params, vec!["id"]);
    }

    #[test]
    fn test_delete_no_where() {
        let delete = Delete {
            target: Target {
                name: "users".to_string(),
                span: None,
            },
            where_clause: None,
            returning: None,
        };
        let result = generate_delete_sql(&delete);
        assert_eq!(result.sql, "DELETE FROM users");
    }

    #[test]
    fn test_delete_with_returning() {
        let delete = Delete {
            target: Target {
                name: "users".to_string(),
                span: None,
            },
            where_clause: Some(binary(
                col_implicit("id"),
                BinaryOp::Equal,
                var("id"),
            )),
            returning: Some(vec![
                Column::ImplicitTarget("id".to_string(), None),
                Column::ImplicitTarget("name".to_string(), None),
            ]),
        };
        let result = generate_delete_sql(&delete);
        assert_eq!(
            result.sql,
            "DELETE FROM users WHERE id = $1 RETURNING id, name"
        );
    }

    // ── Complex / integration tests ─────────────────────────────────────

    #[test]
    fn test_complex_select_with_subquery_in_where() {
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![
                QueryClause::Select(vec![select_expr(col_wildcard_of("users"))]),
                QueryClause::Where(binary(
                    col_explicit("users", "id"),
                    BinaryOp::Equal,
                    Expression::Atomic(AtomicExpression::SubQuery(Box::new(SelectStatement {
                        from: simple_from("orders"),
                        clauses: vec![
                            QueryClause::Select(vec![select_expr(col_implicit("user_id"))]),
                            QueryClause::Limit(num(1.0)),
                        ],
                        unions: vec![],
                    }))),
                )),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(
            result.sql,
            "SELECT users.* FROM users WHERE users.id = (SELECT user_id FROM orders LIMIT 1)"
        );
    }

    #[test]
    fn test_param_reuse_across_statements() {
        let reg = RelationRegistry::empty();
        let mut ctx = SqlGenContext::new(&reg);
        // Same param used twice
        let p1 = ctx.add_param("id");
        let p2 = ctx.add_param("name");
        let p3 = ctx.add_param("id");
        assert_eq!(p1, "$1");
        assert_eq!(p2, "$2");
        assert_eq!(p3, "$1");
        assert_eq!(ctx.params.len(), 2);
    }

    #[test]
    fn test_full_select_all_clauses() {
        let stmt = SelectStatement {
            from: FromExpr {
                table: "orders".to_string(),
                joins: vec![JoinExpr {
                    join_type: JoinType::Left,
                    table: "users".to_string(),
                    condition: binary(
                        col_explicit("orders", "user_id"),
                        BinaryOp::Equal,
                        col_explicit("users", "id"),
                    ),
                    span: None,
                }],
                span: None,
            },
            clauses: vec![
                QueryClause::Distinct,
                QueryClause::Select(vec![
                    select_expr(col_explicit("users", "name")),
                ]),
                QueryClause::Aggregate(vec![AggregateExpression {
                    alias: "order_count".to_string(),
                    expr: Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                        function_type: AggregateFunctionType::Count,
                        expr: Box::new(col_wildcard()),
                        filter: None,
                    })),
                }]),
                QueryClause::Where(binary(
                    col_explicit("orders", "status"),
                    BinaryOp::Equal,
                    str_lit("completed"),
                )),
                QueryClause::GroupBy(vec![col_explicit("users", "name")]),
                QueryClause::Having(binary(
                    Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                        function_type: AggregateFunctionType::Count,
                        expr: Box::new(col_wildcard()),
                        filter: None,
                    })),
                    BinaryOp::GreaterThanOrEqual,
                    num(3.0),
                )),
                QueryClause::OrderBy(vec![OrderByExpression {
                    expr: Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                        function_type: AggregateFunctionType::Count,
                        expr: Box::new(col_wildcard()),
                        filter: None,
                    })),
                    direction: Some(OrderDirection::Desc),
                }]),
                QueryClause::Limit(num(10.0)),
                QueryClause::Offset(num(0.0)),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert_eq!(
            result.sql,
            "SELECT DISTINCT users.name, COUNT(*) AS order_count \
             FROM orders \
             LEFT JOIN users ON orders.user_id = users.id \
             WHERE orders.status = 'completed' \
             GROUP BY users.name \
             HAVING COUNT(*) >= 3 \
             ORDER BY COUNT(*) DESC \
             LIMIT 10 \
             OFFSET 0"
        );
    }

    #[test]
    fn test_having_with_variable_params() {
        // Reproduce Bug 2 scenario: HAVING with a variable should register params
        let stmt = SelectStatement {
            from: FromExpr {
                table: "reviews".to_string(),
                joins: vec![],
                span: None,
            },
            clauses: vec![
                QueryClause::Select(vec![
                    select_expr(col_explicit("reviews", "product_id")),
                ]),
                QueryClause::Aggregate(vec![AggregateExpression {
                    alias: "avg_rating".to_string(),
                    expr: Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                        function_type: AggregateFunctionType::Avg,
                        expr: Box::new(col_explicit("reviews", "rating")),
                        filter: None,
                    })),
                }]),
                QueryClause::GroupBy(vec![col_explicit("reviews", "product_id")]),
                QueryClause::Having(binary(
                    Expression::Atomic(AtomicExpression::Aggregate(AggregateFunction {
                        function_type: AggregateFunctionType::Avg,
                        expr: Box::new(col_explicit("reviews", "rating")),
                        filter: None,
                    })),
                    BinaryOp::GreaterThanOrEqual,
                    var("min_rating"),
                )),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert!(result.params.contains(&"min_rating".to_string()),
            "HAVING variable should be registered as param, got: {:?}", result.params);
        assert!(result.sql.contains("HAVING AVG(reviews.rating) >= $1"),
            "HAVING clause should contain comparison with param, got: {}", result.sql);
    }

    #[test]
    fn test_where_with_comparison_and_variable() {
        // Reproduce Bug 2 scenario: WHERE with >= and variable
        let stmt = SelectStatement {
            from: FromExpr {
                table: "products".to_string(),
                joins: vec![],
                span: None,
            },
            clauses: vec![
                QueryClause::Select(vec![select_expr(col_wildcard_of("products"))]),
                QueryClause::Where(binary(
                    binary(
                        col_explicit("products", "stock"),
                        BinaryOp::LessThanOrEqual,
                        var("threshold"),
                    ),
                    BinaryOp::And,
                    binary(
                        col_explicit("products", "is_active"),
                        BinaryOp::Equal,
                        bool_lit(true),
                    ),
                )),
            ],
            unions: vec![],
        };
        let result = generate_select_sql(&stmt);
        assert!(result.params.contains(&"threshold".to_string()),
            "WHERE variable should be registered as param, got: {:?}", result.params);
        assert!(result.sql.contains("WHERE (products.stock <= $1) AND (products.is_active = TRUE)"),
            "WHERE clause should contain comparison with param, got: {}", result.sql);
    }

    // ── Relation resolution tests ──────────────────────────────────────

    /// Helper: build a RelationRegistry from relation definitions
    fn make_registry(relations: Vec<RelationInfo>) -> RelationRegistry {
        let mut map = HashMap::new();
        for info in relations {
            map.insert((info.for_table.clone(), info.name.clone()), info);
        }
        RelationRegistry { relations: map }
    }

    /// Helper: build a PropertyAccess expression chain from segments.
    /// E.g. `["posts", "author", "name"]` → `posts.author.name` as a PropertyAccess chain.
    fn prop_chain(segments: &[&str]) -> Expression {
        assert!(segments.len() >= 2);
        let mut expr = Expression::Atomic(AtomicExpression::Column(Column::ImplicitTarget(
            segments[0].to_string(),
            None,
        )));
        for seg in &segments[1..] {
            expr = Expression::Atomic(AtomicExpression::PropertyAccess(PropertyAccess {
                target: Box::new(expr),
                property: seg.to_string(),
            }));
        }
        expr
    }

    fn author_relation() -> RelationInfo {
        RelationInfo {
            name: "author".to_string(),
            for_table: "posts".to_string(),
            returning_table: "users".to_string(),
            join_condition: binary(
                col_explicit("users", "id"),
                BinaryOp::Equal,
                col_explicit("posts", "author_id"),
            ),
            is_optional: false,
        }
    }

    fn org_relation() -> RelationInfo {
        RelationInfo {
            name: "organization".to_string(),
            for_table: "users".to_string(),
            returning_table: "organizations".to_string(),
            join_condition: binary(
                col_explicit("organizations", "id"),
                BinaryOp::Equal,
                col_explicit("users", "organization_id"),
            ),
            is_optional: false,
        }
    }

    #[test]
    fn test_relation_access_basic() {
        // posts.author.name → users.name with INNER JOIN users ON users.id = posts.author_id
        let reg = make_registry(vec![author_relation()]);
        let stmt = SelectStatement {
            from: simple_from("posts"),
            clauses: vec![QueryClause::Select(vec![
                select_expr(col_explicit("posts", "title")),
                select_expr(prop_chain(&["posts", "author", "name"])),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);
        assert_eq!(
            result.sql,
            "SELECT posts.title, users.name FROM posts INNER JOIN users ON users.id = posts.author_id"
        );
    }

    #[test]
    fn test_relation_access_nested() {
        // posts.author.organization.name → organizations.name
        // with two JOINs
        let reg = make_registry(vec![author_relation(), org_relation()]);
        let stmt = SelectStatement {
            from: simple_from("posts"),
            clauses: vec![QueryClause::Select(vec![
                select_expr(prop_chain(&["posts", "author", "organization", "name"])),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);
        assert_eq!(
            result.sql,
            "SELECT organizations.name FROM posts \
             INNER JOIN users ON users.id = posts.author_id \
             INNER JOIN organizations ON organizations.id = users.organization_id"
        );
    }

    #[test]
    fn test_optional_relation_uses_left_join() {
        let reg = make_registry(vec![RelationInfo {
            name: "featured_image".to_string(),
            for_table: "posts".to_string(),
            returning_table: "images".to_string(),
            join_condition: binary(
                col_explicit("images", "id"),
                BinaryOp::Equal,
                col_explicit("posts", "featured_image_id"),
            ),
            is_optional: true,
        }]);
        let stmt = SelectStatement {
            from: simple_from("posts"),
            clauses: vec![QueryClause::Select(vec![
                select_expr(col_explicit("posts", "title")),
                select_expr(prop_chain(&["posts", "featured_image", "url"])),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);
        assert_eq!(
            result.sql,
            "SELECT posts.title, images.url FROM posts LEFT JOIN images ON images.id = posts.featured_image_id"
        );
    }

    #[test]
    fn test_relation_in_where_clause() {
        // from(posts).where(posts.author.name == $name).select(posts.*)
        let reg = make_registry(vec![author_relation()]);
        let stmt = SelectStatement {
            from: simple_from("posts"),
            clauses: vec![
                QueryClause::Where(binary(
                    prop_chain(&["posts", "author", "name"]),
                    BinaryOp::Equal,
                    var("name"),
                )),
                QueryClause::Select(vec![select_expr(col_wildcard_of("posts"))]),
            ],
            unions: vec![],
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);
        assert_eq!(
            result.sql,
            "SELECT posts.* FROM posts INNER JOIN users ON users.id = posts.author_id WHERE users.name = $1"
        );
    }

    #[test]
    fn test_no_duplicate_joins() {
        // Accessing posts.author.name and posts.author.email should only inject one JOIN
        let reg = make_registry(vec![author_relation()]);
        let stmt = SelectStatement {
            from: simple_from("posts"),
            clauses: vec![QueryClause::Select(vec![
                select_expr(prop_chain(&["posts", "author", "name"])),
                select_expr(prop_chain(&["posts", "author", "email"])),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);
        assert_eq!(
            result.sql,
            "SELECT users.name, users.email FROM posts INNER JOIN users ON users.id = posts.author_id"
        );
    }

    #[test]
    fn test_non_relation_property_access_unchanged() {
        // When no relation matches, fall back to normal property access
        let reg = RelationRegistry::empty();
        let stmt = SelectStatement {
            from: simple_from("users"),
            clauses: vec![QueryClause::Select(vec![
                select_expr(prop_chain(&["data", "name"])),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);
        assert_eq!(result.sql, "SELECT data.name FROM users");
    }

    #[test]
    fn test_relation_with_alias_for_duplicate_table() {
        // Two different relations from different source tables both joining to 'users'
        // e.g., comments.commenter -> users AND comments.post.author -> users
        let reg = make_registry(vec![
            RelationInfo {
                name: "commenter".to_string(),
                for_table: "comments".to_string(),
                returning_table: "users".to_string(),
                join_condition: binary(
                    col_explicit("users", "id"),
                    BinaryOp::Equal,
                    col_explicit("comments", "user_id"),
                ),
                is_optional: false,
            },
            RelationInfo {
                name: "post".to_string(),
                for_table: "comments".to_string(),
                returning_table: "posts".to_string(),
                join_condition: binary(
                    col_explicit("posts", "id"),
                    BinaryOp::Equal,
                    col_explicit("comments", "post_id"),
                ),
                is_optional: false,
            },
            author_relation(),
        ]);
        let stmt = SelectStatement {
            from: simple_from("comments"),
            clauses: vec![QueryClause::Select(vec![
                select_expr(prop_chain(&["comments", "commenter", "name"])),
                select_expr(prop_chain(&["comments", "post", "title"])),
                select_expr(prop_chain(&["comments", "post", "author", "name"])),
            ])],
            unions: vec![],
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);
        // First users join uses 'users', second uses 'users_2' alias
        assert!(result.sql.contains("INNER JOIN users ON users.id = comments.user_id"));
        assert!(result.sql.contains("INNER JOIN posts ON posts.id = comments.post_id"));
        assert!(result.sql.contains("INNER JOIN users AS users_2 ON users_2.id = posts.author_id"));
        assert!(result.sql.contains("users.name"));
        assert!(result.sql.contains("posts.title"));
        assert!(result.sql.contains("users_2.name"));
    }

    // ── Parse-and-generate integration tests ─────────────────────────────

    fn parse_first_select(input: &str) -> SelectStatement {
        let module = nextsql_core::parser::parse_module(input).expect("parse failed");
        let q = match &module.toplevels[0] {
            nextsql_core::ast::TopLevel::Query(q) => q,
            _ => panic!("expected query"),
        };
        match &q.body.statements[0] {
            nextsql_core::ast::QueryStatement::Select(s) => s.clone(),
        }
    }

    #[test]
    fn test_comparison_operators_in_where_clause() {
        // Test all comparison operators: >, >=, <, <=
        let cases = vec![
            (">=", "reviews.rating >= $1"),
            (">", "reviews.rating > $1"),
            ("<=", "reviews.rating <= $1"),
            ("<", "reviews.rating < $1"),
        ];
        for (op, expected_fragment) in cases {
            let input = format!(
                r#"
query test($min: i32) {{
  from(reviews)
  .where(reviews.rating {op} $min)
  .select(reviews.rating)
}}
"#
            );
            let stmt = parse_first_select(&input);
            let result = generate_select_sql(&stmt);
            assert!(
                result.sql.contains(expected_fragment),
                "For operator '{}': expected '{}' in '{}'",
                op, expected_fragment, result.sql
            );
        }
    }

    #[test]
    fn test_comparison_with_and_conjunction() {
        // Test: reviews.rating >= $min && reviews.rating <= $max
        let input = r#"
query test($min: i32, $max: i32) {
  from(reviews)
  .where(reviews.rating >= $min && reviews.rating <= $max)
  .select(reviews.rating)
}
"#;
        let stmt = parse_first_select(input);
        let result = generate_select_sql(&stmt);
        assert!(
            result.sql.contains("reviews.rating >= $1") && result.sql.contains("reviews.rating <= $2"),
            "Expected both >= and <= in WHERE clause, got: {}",
            result.sql
        );
    }

    #[test]
    fn test_nested_relation_parsed_from_nsql() {
        // End-to-end test: parse .nsql with nested relation access and verify SQL.
        // reviews.product.category.name has 4 levels:
        //   reviews -> product (relation) -> category (relation) -> name (column)
        let input = r#"
relation product for reviews returning products {
  products.id == reviews.product_id
}
relation category for products returning categories {
  categories.id == products.category_id
}
query findHighRatedReviews($min_rating: i32) {
  from(reviews)
  .where(reviews.rating >= $min_rating)
  .select(reviews.rating, product_name: reviews.product.name, category_name: reviews.product.category.name)
  .orderBy(reviews.rating.desc())
}
"#;
        let module = nextsql_core::parser::parse_module(input).expect("parse failed");
        let reg = RelationRegistry::from_module(&module);
        let q = match &module.toplevels.iter().find(|tl| matches!(tl, nextsql_core::ast::TopLevel::Query(_))).unwrap() {
            nextsql_core::ast::TopLevel::Query(q) => q,
            _ => unreachable!(),
        };
        let stmt = match &q.body.statements[0] {
            nextsql_core::ast::QueryStatement::Select(s) => s.clone(),
        };
        let result = generate_select_sql_with_relations(&stmt, &reg);

        // Should join both products and categories
        assert!(
            result.sql.contains("INNER JOIN products ON products.id = reviews.product_id"),
            "Should join products table, got: {}", result.sql
        );
        assert!(
            result.sql.contains("INNER JOIN categories ON categories.id = products.category_id"),
            "Should join categories table, got: {}", result.sql
        );
        // Should resolve to categories.name, not products.category
        assert!(
            result.sql.contains("categories.name"),
            "Should resolve to categories.name, got: {}", result.sql
        );
        // The SELECT clause should NOT contain "products.category " (with trailing space,
        // i.e., an unresolved column reference). Note: "products.category_id" in the JOIN
        // ON clause is expected and correct.
        let select_part = result.sql.split(" FROM ").next().unwrap();
        assert!(
            !select_part.contains("products.category"),
            "SELECT clause should not have unresolved products.category, got: {}", result.sql
        );
    }

    #[test]
    fn test_having_with_comparison() {
        let input = r#"
query test($min_orders: i64) {
  from(orders)
  .groupBy(orders.status)
  .having(COUNT(orders.id) >= $min_orders)
  .aggregate(order_count: COUNT(orders.id))
  .select(orders.status, order_count)
}
"#;
        let stmt = parse_first_select(input);
        let result = generate_select_sql(&stmt);
        assert!(
            result.sql.contains("HAVING COUNT(orders.id) >= $1"),
            "Expected 'HAVING COUNT(orders.id) >= $1' in: {}",
            result.sql
        );
    }

    // ── Feature tests: FOR UPDATE, column refs in SET, now(), COUNT alias ─

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
        let stmt = parse_first_select(input);
        let result = generate_select_sql(&stmt);
        assert!(
            result.sql.ends_with("FOR UPDATE"),
            "Expected SQL to end with 'FOR UPDATE', got: {}",
            result.sql
        );
        assert!(
            result.sql.contains("SELECT users.*"),
            "Expected 'SELECT users.*' in: {}",
            result.sql
        );
    }

    #[test]
    fn test_set_with_column_reference() {
        let input = r#"
            mutation incrementStage($id: uuid) {
                update(requests)
                .where(requests.id == $id)
                .set({ stage: stage + 1 })
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).expect("parse failed");
        let m = match &module.toplevels[0] {
            nextsql_core::ast::TopLevel::Mutation(m) => m,
            _ => panic!("expected mutation"),
        };
        let update = match &m.body.items[0] {
            nextsql_core::ast::MutationBodyItem::Mutation(
                nextsql_core::ast::MutationStatement::Update(u),
            ) => u,
            _ => panic!("expected update"),
        };
        let result = generate_update_sql(update);
        assert!(
            result.sql.contains("SET stage = stage + 1"),
            "Expected 'SET stage = stage + 1' in: {}",
            result.sql
        );
        assert!(
            result.sql.contains("WHERE requests.id = $1"),
            "Expected 'WHERE requests.id = $1' in: {}",
            result.sql
        );
    }

    #[test]
    fn test_now_function_in_set() {
        let input = r#"
            mutation touchUpdatedAt($id: uuid) {
                update(records)
                .where(records.id == $id)
                .set({ updated_at: now() })
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).expect("parse failed");
        let m = match &module.toplevels[0] {
            nextsql_core::ast::TopLevel::Mutation(m) => m,
            _ => panic!("expected mutation"),
        };
        let update = match &m.body.items[0] {
            nextsql_core::ast::MutationBodyItem::Mutation(
                nextsql_core::ast::MutationStatement::Update(u),
            ) => u,
            _ => panic!("expected update"),
        };
        let result = generate_update_sql(update);
        assert!(
            result.sql.contains("SET updated_at = now()"),
            "Expected 'SET updated_at = now()' in: {}",
            result.sql
        );
    }

    #[test]
    fn test_now_and_string_in_set() {
        let input = r#"
            mutation shipOrder($id: uuid) {
                update(orders)
                .where(orders.id == $id)
                .set({ shipped_at: now(), status: "shipped" })
            }
        "#;
        let module = nextsql_core::parser::parse_module(input).expect("parse failed");
        let m = match &module.toplevels[0] {
            nextsql_core::ast::TopLevel::Mutation(m) => m,
            _ => panic!("expected mutation"),
        };
        let update = match &m.body.items[0] {
            nextsql_core::ast::MutationBodyItem::Mutation(
                nextsql_core::ast::MutationStatement::Update(u),
            ) => u,
            _ => panic!("expected update"),
        };
        let result = generate_update_sql(update);
        assert!(
            result.sql.contains("shipped_at = now()"),
            "Expected 'shipped_at = now()' in: {}",
            result.sql
        );
        assert!(
            result.sql.contains("status = 'shipped'"),
            "Expected \"status = 'shipped'\" in: {}",
            result.sql
        );
    }

    #[test]
    fn test_count_with_alias() {
        let input = r#"
            query countRecords() {
                from(users)
                .where(users.is_active == true)
                .select(total: COUNT(users.id))
            }
        "#;
        let stmt = parse_first_select(input);
        let result = generate_select_sql(&stmt);
        assert!(
            result.sql.contains("COUNT(users.id) AS total"),
            "Expected 'COUNT(users.id) AS total' in: {}",
            result.sql
        );
    }

    #[test]
    fn test_cast_sql_generation() {
        // cast(col, string) -> CAST(col AS TEXT)
        let input = r#"
            query castToString {
                from(items)
                .where(cast(items.origin, string) == "foo")
                .select(items.*)
            }
        "#;
        let stmt = parse_first_select(input);
        let result = generate_select_sql(&stmt);
        assert!(
            result.sql.contains("CAST(items.origin AS TEXT)"),
            "Expected 'CAST(items.origin AS TEXT)' in: {}",
            result.sql
        );

        // cast(col, i32) -> CAST(col AS INTEGER)
        let input = r#"
            query castToInt {
                from(items)
                .select(cast(items.amount, i32))
            }
        "#;
        let stmt = parse_first_select(input);
        let result = generate_select_sql(&stmt);
        assert!(
            result.sql.contains("CAST(items.amount AS INTEGER)"),
            "Expected 'CAST(items.amount AS INTEGER)' in: {}",
            result.sql
        );
    }

    #[test]
    fn test_three_conditions_with_literal_boolean() {
        // Bug: 3 conditions joined with && drops the last literal condition.
        let input = r#"
            query findUserByEmail($organizationId: i64, $email: string) {
                from(users)
                .where(users.organization_id == $organizationId && users.email == $email && users.is_active == true)
                .select(users.*)
            }
        "#;
        let stmt = parse_first_select(input);
        let result = generate_select_sql(&stmt);
        assert!(
            result.sql.contains("users.is_active = TRUE"),
            "Expected 'users.is_active = TRUE' in WHERE clause, got: {}",
            result.sql
        );
        assert!(
            result.sql.contains("users.organization_id = $1"),
            "Expected 'users.organization_id = $1' in WHERE clause, got: {}",
            result.sql
        );
        assert!(
            result.sql.contains("users.email = $2"),
            "Expected 'users.email = $2' in WHERE clause, got: {}",
            result.sql
        );
    }
}
