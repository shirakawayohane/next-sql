use crate::ast::{Module, MutationBody, MutationBodyItem, MutationStatement, Expression, Type, BuiltInType, Literal, CallExpression, AtomicExpression, Column, PropertyAccess, FromExpr, Span, RelationReturnType, UtilityType, Target, InputType};
use crate::schema::DatabaseSchema;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
    pub span: Option<Span>,
}

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub for_table: String,
    pub returning_type: RelationReturnType,
    pub join_condition: Expression,
    pub is_optional: bool,
}

pub struct TypeValidator<'a> {
    schema: &'a DatabaseSchema,
    variables: HashMap<String, Type>,
    valid_tables: HashSet<String>,  // 有効なテーブル名
    errors: Vec<ValidationError>,
    relations: HashMap<String, RelationInfo>,  // relation name -> relation info
    valtypes: HashMap<String, BuiltInType>,  // valtype name -> base type
    input_types: HashMap<String, InputType>,  // input type name -> input type definition
}

impl<'a> TypeValidator<'a> {
    pub fn new(schema: &'a DatabaseSchema) -> Self {
        let mut valid_tables = HashSet::new();
        // 実際のテーブル名を追加
        for table_name in schema.tables.keys() {
            valid_tables.insert(table_name.clone());
        }
        
        Self {
            schema,
            variables: HashMap::new(),
            valid_tables,
            errors: Vec::new(),
            relations: HashMap::new(),
            valtypes: HashMap::new(),
            input_types: HashMap::new(),
        }
    }

    pub fn register_valtype(&mut self, name: String, base_type: BuiltInType) {
        self.valtypes.insert(name, base_type);
    }

    pub fn validate_module(&mut self, module: &Module) -> Vec<ValidationError> {
        self.errors.clear();
        self.variables.clear();
        self.relations.clear();
        
        // valid_tablesを実テーブル名で初期化
        self.valid_tables.clear();
        for table_name in self.schema.tables.keys() {
            self.valid_tables.insert(table_name.clone());
        }
        
        // Collect valtype definitions (keep externally registered ones)
        // Collect input type definitions
        for toplevel in &module.toplevels {
            match toplevel {
                crate::ast::TopLevel::ValType(vt) => {
                    self.valtypes.insert(vt.name.clone(), vt.base_type.clone());
                }
                crate::ast::TopLevel::Input(input) => {
                    self.input_types.insert(input.name.clone(), input.clone());
                }
                _ => {}
            }
        }

        // First pass: collect all function signatures, variables, and relations
        for toplevel in &module.toplevels {
            match toplevel {
                crate::ast::TopLevel::Query(query) => {
                    for arg in &query.decl.arguments {
                        self.variables.insert(arg.name.clone(), arg.typ.clone());
                    }
                }
                crate::ast::TopLevel::Mutation(mutation) => {
                    for arg in &mutation.decl.arguments {
                        self.variables.insert(arg.name.clone(), arg.typ.clone());
                    }
                }
                crate::ast::TopLevel::Relation(relation) => {
                    let is_optional = relation.decl.modifiers.iter()
                        .any(|m| matches!(m, crate::ast::RelationModifier::Optional));
                    
                    self.relations.insert(
                        relation.decl.name.clone(),
                        RelationInfo {
                            for_table: relation.decl.for_table.clone(),
                            returning_type: relation.decl.returning_type.clone(),
                            join_condition: relation.join_condition.clone(),
                            is_optional,
                        }
                    );
                }
                _ => {}
            }
        }

        // Second pass: validate function bodies
        for toplevel in &module.toplevels {
            match toplevel {
                crate::ast::TopLevel::Query(query) => {
                    self.validate_query_body(&query.body);
                }
                crate::ast::TopLevel::Mutation(mutation) => {
                    self.validate_mutation_body(&mutation.body);
                }
                crate::ast::TopLevel::Relation(relation) => {
                    self.validate_relation(relation);
                }
                _ => {}
            }
        }

        self.errors.clone()
    }

    fn validate_expression(&mut self, expr: &Expression) {
        match expr {
            Expression::Atomic(AtomicExpression::Variable(var)) => {
                if !self.variables.contains_key(&var.name) {
                    self.errors.push(ValidationError {
                        message: format!("Undefined variable: ${}", var.name),
                        span: var.span.clone(),
                    });
                }
            }
            Expression::Atomic(AtomicExpression::Call(call)) => {
                self.validate_function_call(call);
            }
            Expression::Atomic(AtomicExpression::Literal(Literal::Object(_obj))) => {
                // Object literals are validated in context
            }
            Expression::Atomic(AtomicExpression::PropertyAccess(property_access)) => {
                self.validate_property_access(property_access);
            }
            Expression::Atomic(AtomicExpression::Column(column)) => {
                self.validate_column_reference(column);
            }
            Expression::Binary { left, right, .. } => {
                self.validate_expression(left);
                self.validate_expression(right);
            }
            _ => {}
        }
    }

    /// Try to resolve a property access chain to the table it refers to.
    /// e.g., `orders.customer` → Some("customers") if `customer` is a relation on orders returning customers.
    /// e.g., `reviews.product.category` → Some("categories") via nested relations.
    fn resolve_property_access_table(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(table_name, relation_name, _))) => {
                // table.relation → check if relation_name is a relation for table_name
                if let Some(relation_info) = self.relations.get(relation_name) {
                    if relation_info.for_table == *table_name {
                        match &relation_info.returning_type {
                            RelationReturnType::Table(ret_table) => Some(ret_table.clone()),
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Expression::Atomic(AtomicExpression::PropertyAccess(inner)) => {
                // Nested: resolve inner first, then check if property is a relation on the resolved table
                let inner_table = self.resolve_property_access_table(&inner.target)?;
                if let Some(relation_info) = self.relations.get(&inner.property) {
                    if relation_info.for_table == inner_table {
                        match &relation_info.returning_type {
                            RelationReturnType::Table(ret_table) => Some(ret_table.clone()),
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn validate_property_access(&mut self, property_access: &PropertyAccess) {
        // Try to resolve the target as a relation chain
        let resolved_table = self.resolve_property_access_table(&property_access.target);

        if let Some(ref table_name) = resolved_table {
            // The target resolves to a table via relation(s).
            // Validate that property is a column of that table, or another relation.
            if let Some(table) = self.schema.get_table(table_name) {
                if !table.has_column(&property_access.property)
                    && !self.is_relation_for_table(&property_access.property, table_name)
                {
                    self.errors.push(ValidationError {
                        message: format!(
                            "Column '{}' not found in table '{}'",
                            property_access.property, table_name
                        ),
                        span: None,
                    });
                }
            }
            // Validate the rest of the chain (the base expression, skipping the relation part)
            // We validate the root table.relation column reference (which is valid)
            self.validate_property_access_base(&property_access.target);
        } else {
            // Not a relation access — validate normally
            self.validate_expression(&property_access.target);

            match property_access.target.as_ref() {
                Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(table_name, _column_name, _))) => {
                    // target is table.column, property is a method or subfield
                    // If it's not a known column, validate_column_reference already handles it
                    // Check if property is a column (for table.column.property pattern that's not a relation)
                    if let Some(table) = self.schema.get_table(table_name) {
                        if !table.has_column(&property_access.property)
                            && !self.is_relation_for_table(&property_access.property, table_name)
                        {
                            // This could be a method call like .desc(), .asc(), .isNull() etc.
                            // Don't report as error — these are handled elsewhere
                        }
                    }
                }
                Expression::Atomic(AtomicExpression::Variable(var)) => {
                    // Check if the variable's type is a known input type
                    if let Some(var_type) = self.variables.get(&var.name) {
                        if let Type::UserDefined(type_name) = var_type {
                            if let Some(input_type) = self.input_types.get(type_name).cloned() {
                                if !input_type.fields.iter().any(|f| f.name == property_access.property) {
                                    self.errors.push(ValidationError {
                                        message: format!(
                                            "Field '{}' not found in input type '{}'",
                                            property_access.property, type_name
                                        ),
                                        span: var.span.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Other types of property access
                }
            }
        }
    }

    /// Validate the base of a property access chain without reporting
    /// relation names as missing columns.
    fn validate_property_access_base(&mut self, expr: &Expression) {
        match expr {
            Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(table_name, _name, span))) => {
                // This is table.something — check table exists
                if !self.schema.tables.contains_key(table_name) && !self.valid_tables.contains(table_name) {
                    self.errors.push(ValidationError {
                        message: format!("Table '{}' not found", table_name),
                        span: span.clone(),
                    });
                }
                // name could be a column or a relation — both are fine here
            }
            Expression::Atomic(AtomicExpression::PropertyAccess(inner)) => {
                self.validate_property_access_base(&inner.target);
            }
            _ => {
                self.validate_expression(expr);
            }
        }
    }

    /// Check if a name is a relation defined for a given table.
    fn is_relation_for_table(&self, name: &str, table_name: &str) -> bool {
        if let Some(relation_info) = self.relations.get(name) {
            relation_info.for_table == table_name
        } else {
            false
        }
    }

    fn validate_column_reference(&mut self, column: &Column) {
        match column {
            Column::ExplicitTarget(table_name, column_name, span) => {
                if let Some(table) = self.schema.get_table(table_name) {
                    // It's a valid column, or it could be a relation name
                    if !table.has_column(column_name) && !self.is_relation_for_table(column_name, table_name) {
                        self.errors.push(ValidationError {
                            message: format!("Column '{}' not found in table '{}'", column_name, table_name),
                            span: span.clone(),
                        });
                    }
                } else {
                    self.errors.push(ValidationError {
                        message: format!("Table '{}' not found", table_name),
                        span: span.clone(),
                    });
                }
            }
            Column::ImplicitTarget(_column_name, _span) => {
                // For implicit targets, we'd need to search all available tables
                // For now, we'll skip this validation as it requires more context
            }
            Column::WildcardOf(table_name, span) => {
                if !self.schema.tables.contains_key(table_name) {
                    self.errors.push(ValidationError {
                        message: format!("Table '{}' not found", table_name),
                        span: span.clone(),
                    });
                }
            }
            Column::Wildcard(_span) => {
                // Wildcard is always valid if there are tables in the query
            }
        }
    }

    fn validate_query_body(&mut self, query_body: &crate::ast::QueryBody) {
        for statement in &query_body.statements {
            match statement {
                crate::ast::QueryStatement::Select(select) => {
                    self.validate_select_statement(select);
                }
            }
        }
    }

    fn validate_select_statement(&mut self, select: &crate::ast::SelectStatement) {
        // Validate FROM clause
        self.validate_from_clause(&select.from);

        // Validate query clauses
        for clause in &select.clauses {
            self.validate_query_clause(clause);
        }

        // Validate union clauses
        for union_clause in &select.unions {
            self.validate_select_statement(&union_clause.select);
        }
    }

    fn validate_query_clause(&mut self, clause: &crate::ast::QueryClause) {
        match clause {
            crate::ast::QueryClause::Where(condition) => {
                self.validate_expression(condition);
            }
            crate::ast::QueryClause::Select(select_expressions) => {
                for select_expr in select_expressions {
                    self.validate_expression(&select_expr.expr);
                }
            }
            crate::ast::QueryClause::Limit(limit_expr) => {
                self.validate_expression(limit_expr);
            }
            crate::ast::QueryClause::OrderBy(order_exprs) => {
                for order_expr in order_exprs {
                    self.validate_expression(&order_expr.expr);
                }
            }
            crate::ast::QueryClause::GroupBy(expressions) => {
                for expr in expressions {
                    self.validate_expression(expr);
                }
            }
            crate::ast::QueryClause::When(when_clause) => {
                self.validate_expression(&when_clause.condition);
                self.validate_query_clause(&when_clause.clause);
            }
            crate::ast::QueryClause::Join(join) => {
                if !self.schema.tables.contains_key(&join.table) {
                    self.errors.push(ValidationError {
                        message: format!("Table '{}' not found in join", join.table),
                        span: join.span.clone(),
                    });
                }
                self.validate_expression(&join.condition);
            }
            crate::ast::QueryClause::Aggregate(aggregates) => {
                for aggregate in aggregates {
                    self.validate_expression(&aggregate.expr);
                }
            }
            crate::ast::QueryClause::Distinct => {
                // No expression to validate
            }
            crate::ast::QueryClause::Offset(offset_expr) => {
                self.validate_expression(offset_expr);
            }
            crate::ast::QueryClause::Having(having_expr) => {
                self.validate_expression(having_expr);
            }
            crate::ast::QueryClause::ForUpdate => {
                // No expression to validate
            }
        }
    }

    fn validate_from_clause(&mut self, from: &FromExpr) {
        // エイリアスまたは実テーブル名をチェック
        if !self.valid_tables.contains(&from.table) && !self.schema.tables.contains_key(&from.table) {
            self.errors.push(ValidationError {
                message: format!("Table '{}' not found", from.table),
                span: from.span.clone(),
            });
        }
        
        // Validate joins
        for join in &from.joins {
            if !self.valid_tables.contains(&join.table) && !self.schema.tables.contains_key(&join.table) {
                self.errors.push(ValidationError {
                    message: format!("Table '{}' not found in join", join.table),
                    span: join.span.clone(),
                });
            }
            self.validate_expression(&join.condition);
        }
    }

    fn validate_mutation_body(&mut self, mutation_body: &MutationBody) {
        for item in &mutation_body.items {
            match item {
                MutationBodyItem::Mutation(statement) => {
                    match statement {
                        MutationStatement::Insert(insert) => {
                            self.validate_insert(&insert.into.name, &insert.values, insert.into.span.as_ref());
                            if let Some(on_conflict) = &insert.on_conflict {
                                if let crate::ast::OnConflictAction::DoUpdate(sets) = &on_conflict.action {
                                    for (_key, value) in sets {
                                        self.validate_expression(value);
                                    }
                                }
                            }
                        }
                        MutationStatement::Update(update) => {
                            self.validate_update(&update.target, &update.set, update.set_variable.as_deref(), update.where_clause.as_ref());
                        }
                        MutationStatement::Delete(delete) => {
                            self.validate_delete(&delete.target, delete.where_clause.as_ref());
                        }
                    }
                }
            }
        }
    }

    fn validate_insert(&mut self, target: &str, values: &[Expression], target_span: Option<&Span>) {
        let table = match self.schema.get_table(target) {
            Some(table) => table,
            None => {
                // 実テーブル名をチェック
                if !self.valid_tables.contains(target) {
                    self.errors.push(ValidationError {
                        message: format!("Table '{}' not found", target),
                        span: target_span.cloned(),
                    });
                }
                return;
            }
        };

        for value_expr in values {
            match value_expr {
                Expression::Atomic(AtomicExpression::Literal(Literal::Object(obj))) => {
                // Check for unknown fields
                    let table_columns: HashSet<_> = table.columns.iter().map(|c| &c.name).collect();
                    for (field_name, field_value) in &obj.fields {
                        if !table_columns.contains(field_name) {
                            self.errors.push(ValidationError {
                                message: format!("Unknown field '{}' in table '{}'", field_name, target),
                                span: Self::extract_span(field_value),
                            });
                        }
                    }

                    // Check required fields
                    let provided_fields: HashSet<_> = obj.fields.keys().collect();
                    for column in table.get_required_columns() {
                        if !provided_fields.contains(&column.name) {
                            self.errors.push(ValidationError {
                                message: format!("Required field '{}' is missing in table '{}'", column.name, target),
                                span: obj.span.clone().or_else(|| target_span.cloned()),
                            });
                        }
                    }

                    // Validate field types and expressions
                    for (field_name, field_value) in &obj.fields {
                        // First validate the expression itself (for undefined variables, etc)
                        self.validate_expression(field_value);

                        // Then validate the type if the column exists
                        if let Some(column) = table.get_column(field_name) {
                            self.validate_value_type(field_value, &column.column_type, field_name);
                        }
                    }
                }
                Expression::Atomic(AtomicExpression::Variable(var)) => {
                    // Check if the variable exists
                    if !self.variables.contains_key(&var.name) {
                        self.errors.push(ValidationError {
                            message: format!("Undefined variable: ${}", var.name),
                            span: var.span.clone(),
                        });
                    } else if let Some(var_type) = self.variables.get(&var.name) {
                        // Allow Insertable<T> typed variables where T matches the target table
                        match var_type {
                            Type::Utility(UtilityType::Insertable(insertable)) => {
                                if let Type::UserDefined(table_name) = insertable.0.as_ref() {
                                    if table_name != target {
                                        self.errors.push(ValidationError {
                                            message: format!(
                                                "Type mismatch: variable ${} is Insertable<{}>, but inserting into '{}'",
                                                var.name, table_name, target
                                            ),
                                            span: var.span.clone(),
                                        });
                                    }
                                }
                            }
                            _ => {
                                self.errors.push(ValidationError {
                                    message: format!(
                                        "INSERT values must be object literals or Insertable<T> typed variables, but ${} has type {:?}",
                                        var.name, var_type
                                    ),
                                    span: var.span.clone(),
                                });
                            }
                        }
                    }
                }
                _ => {
                    self.errors.push(ValidationError {
                        message: "INSERT values must be object literals or Insertable<T> typed variables".to_string(),
                        span: target_span.cloned(),
                    });
                }
            }
        }
    }

    fn validate_update(&mut self, target: &Target, set: &[(String, Expression)], set_variable: Option<&str>, where_clause: Option<&Expression>) {
        let target_span = target.span.as_ref();
        let target_name = &target.name;
        let table = match self.schema.get_table(target_name) {
            Some(table) => table,
            None => {
                // 実テーブル名をチェック
                if !self.valid_tables.contains(target_name) {
                    self.errors.push(ValidationError {
                        message: format!("Table '{}' not found", target_name),
                        span: target_span.cloned(),
                    });
                }
                return;
            }
        };

        // Validate set_variable (ChangeSet<T> typed variable)
        if let Some(var_name) = set_variable {
            if !self.variables.contains_key(var_name) {
                self.errors.push(ValidationError {
                    message: format!("Undefined variable: ${}", var_name),
                    span: target_span.cloned(),
                });
            } else if let Some(var_type) = self.variables.get(var_name) {
                match var_type {
                    Type::Utility(UtilityType::ChangeSet(change_set)) => {
                        if let Type::UserDefined(table_name) = change_set.0.as_ref() {
                            if table_name != target_name {
                                self.errors.push(ValidationError {
                                    message: format!(
                                        "Type mismatch: variable ${} is ChangeSet<{}>, but updating '{}'",
                                        var_name, table_name, target_name
                                    ),
                                    span: target_span.cloned(),
                                });
                            }
                        }
                    }
                    _ => {
                        self.errors.push(ValidationError {
                            message: format!(
                                "UPDATE .set() variable must be ChangeSet<T> typed, but ${} has type {:?}",
                                var_name, var_type
                            ),
                            span: target_span.cloned(),
                        });
                    }
                }
            }
        }

        // Validate SET clause (object literal fields)
        for (field_name, field_value) in set {
            match table.get_column(field_name) {
                Some(column) => {
                    self.validate_value_type(field_value, &column.column_type, field_name);
                }
                None => {
                    self.errors.push(ValidationError {
                        message: format!("Unknown field '{}' in table '{}'", field_name, target_name),
                        span: target_span.cloned(),
                    });
                }
            }
        }

        // Validate WHERE clause
        if let Some(where_expr) = where_clause {
            self.validate_expression(where_expr);
        }
    }

    fn validate_delete(&mut self, target: &Target, where_clause: Option<&Expression>) {
        if !self.schema.tables.contains_key(&target.name) && !self.valid_tables.contains(&target.name) {
            self.errors.push(ValidationError {
                message: format!("Table '{}' not found", target.name),
                span: target.span.clone(),
            });
        }

        if let Some(where_expr) = where_clause {
            self.validate_expression(where_expr);
        }
    }

    fn validate_function_call(&mut self, call: &CallExpression) {
        match call.callee.as_str() {
            "insert" => {
                if call.args.len() != 1 {
                    self.errors.push(ValidationError {
                        message: "insert() requires exactly one argument".to_string(),
                        span: None,
                    });
                }
            }
            "update" => {
                if call.args.len() != 1 {
                    self.errors.push(ValidationError {
                        message: "update() requires exactly one argument".to_string(),
                        span: None,
                    });
                }
            }
            "delete" => {
                if call.args.len() != 1 {
                    self.errors.push(ValidationError {
                        message: "delete() requires exactly one argument".to_string(),
                        span: None,
                    });
                }
            }
            "value" | "values" => {
                if call.args.len() != 1 {
                    self.errors.push(ValidationError {
                        message: format!("{}() requires exactly one argument", call.callee),
                        span: None,
                    });
                }
            }
            _ => {
                // Validate method chain arguments
                for arg in &call.args {
                    self.validate_expression(arg);
                }
            }
        }
    }

    fn validate_value_type(&mut self, value: &Expression, expected_type: &Type, field_name: &str) {
        match value {
            Expression::Atomic(AtomicExpression::Variable(var)) => {
                if let Some(var_type) = self.variables.get(&var.name) {
                    if !self.types_compatible(var_type, expected_type) {
                        self.errors.push(ValidationError {
                            message: format!(
                                "Type mismatch for field '{}': expected {:?}, but variable ${} has type {:?}",
                                field_name, expected_type, var.name, var_type
                            ),
                            span: var.span.clone(),
                        });
                    }
                }
            }
            Expression::Atomic(AtomicExpression::Literal(lit)) => {
                let literal_type = self.infer_literal_type(lit);
                if !self.types_compatible(&literal_type, expected_type) {
                    self.errors.push(ValidationError {
                        message: format!(
                            "Type mismatch for field '{}': expected {:?}, got {:?}",
                            field_name, expected_type, literal_type
                        ),
                        span: None,
                    });
                }
            }
            _ => {
                // For complex expressions, we'd need more sophisticated type inference
                self.validate_expression(value);
            }
        }
    }

    fn infer_literal_type(&self, lit: &Literal) -> Type {
        match lit {
            Literal::String(_) => Type::BuiltIn(BuiltInType::String),
            Literal::Numeric(n) => {
                if n.fract() != 0.0 {
                    Type::BuiltIn(BuiltInType::F64)
                } else {
                    Type::BuiltIn(BuiltInType::I32)
                }
            }
            Literal::Boolean(_) => Type::BuiltIn(BuiltInType::Bool),
            Literal::Null => Type::Optional(Box::new(Type::UserDefined("null".to_string()))),
            _ => Type::UserDefined("unknown".to_string()),
        }
    }

    fn extract_span(expr: &Expression) -> Option<Span> {
        match expr {
            Expression::Atomic(AtomicExpression::Variable(var)) => var.span.clone(),
            Expression::Atomic(AtomicExpression::Column(Column::ExplicitTarget(_, _, span))) => span.clone(),
            Expression::Atomic(AtomicExpression::Column(Column::ImplicitTarget(_, span))) => span.clone(),
            Expression::Atomic(AtomicExpression::Column(Column::WildcardOf(_, span))) => span.clone(),
            Expression::Atomic(AtomicExpression::Column(Column::Wildcard(span))) => span.clone(),
            _ => None,
        }
    }

    fn resolve_to_builtin(&self, typ: &Type) -> Option<BuiltInType> {
        match typ {
            Type::BuiltIn(b) => Some(b.clone()),
            Type::UserDefined(name) => self.valtypes.get(name).cloned(),
            Type::Optional(inner) => self.resolve_to_builtin(inner),
            _ => None,
        }
    }

    fn types_compatible(&self, actual: &Type, expected: &Type) -> bool {
        match (actual, expected) {
            // null is compatible with any type
            (Type::Optional(inner), _) if matches!(inner.as_ref(), Type::UserDefined(n) if n == "null") => true,
            (Type::Optional(inner), expected) => {
                self.types_compatible(inner, expected)
            }
            (actual, Type::Optional(inner)) => {
                self.types_compatible(actual, inner)
            }
            (Type::UserDefined(a), Type::UserDefined(e)) => {
                // Same name → compatible
                if a == e { return true; }
                // Both are ValTypes → compare base types
                if let (Some(a_base), Some(e_base)) = (self.valtypes.get(a), self.valtypes.get(e)) {
                    return a_base == e_base
                        || (self.is_numeric_builtin_type(a_base) && self.is_numeric_builtin_type(e_base));
                }
                false
            }
            // ValType (registered UserDefined) vs BuiltIn → resolve and compare base types
            (Type::UserDefined(name), _) if self.valtypes.contains_key(name) => {
                if let (Some(a), Some(e)) = (self.resolve_to_builtin(actual), self.resolve_to_builtin(expected)) {
                    a == e || (self.is_numeric_builtin_type(&a) && self.is_numeric_builtin_type(&e))
                } else {
                    false
                }
            }
            (_, Type::UserDefined(name)) if self.valtypes.contains_key(name) => {
                if let (Some(a), Some(e)) = (self.resolve_to_builtin(actual), self.resolve_to_builtin(expected)) {
                    a == e || (self.is_numeric_builtin_type(&a) && self.is_numeric_builtin_type(&e))
                } else {
                    false
                }
            }
            // PostgreSQL enum types (unknown UserDefined, not a ValType) are compatible with String
            (Type::BuiltIn(BuiltInType::String), Type::UserDefined(_))
            | (Type::UserDefined(_), Type::BuiltIn(BuiltInType::String)) => true,
            _ => {
                // Resolve both sides to built-in types and compare
                if let (Some(a), Some(e)) = (self.resolve_to_builtin(actual), self.resolve_to_builtin(expected)) {
                    a == e || (self.is_numeric_builtin_type(&a) && self.is_numeric_builtin_type(&e))
                } else {
                    false
                }
            }
        }
    }

    fn is_numeric_builtin_type(&self, builtin: &BuiltInType) -> bool {
        matches!(builtin,
            BuiltInType::I16 | BuiltInType::I32 | BuiltInType::I64 |
            BuiltInType::F32 | BuiltInType::F64 |
            BuiltInType::Decimal
        )
    }

    fn validate_relation(&mut self, relation: &crate::ast::Relation) {
        // Validate that the for_table exists
        if !self.schema.tables.contains_key(&relation.decl.for_table) {
            self.errors.push(ValidationError {
                message: format!("Table '{}' not found for relation '{}'", 
                    relation.decl.for_table, relation.decl.name),
                span: None,
            });
            return;
        }
        
        // Validate the returning type
        match &relation.decl.returning_type {
            RelationReturnType::Table(table_name) => {
                if !self.schema.tables.contains_key(table_name) {
                    self.errors.push(ValidationError {
                        message: format!("Returning table '{}' not found for relation '{}'", 
                            table_name, relation.decl.name),
                        span: None,
                    });
                }
            }
            RelationReturnType::Type(_) => {
                // Type validation for aggregation return types
            }
        }
        
        // TODO: Validate join condition expression
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TableSchema, ColumnSchema};
    use crate::ast::ObjectLiteralExpression;
    use std::collections::HashMap;

    #[test]
    fn test_valtype_compatible_with_builtin() {
        let schema = DatabaseSchema::new();
        let mut validator = TypeValidator::new(&schema);
        validator.register_valtype("OrganizationId".to_string(), BuiltInType::I64);

        // UserDefined(ValType) vs BuiltIn should be compatible when base types match
        assert!(validator.types_compatible(
            &Type::UserDefined("OrganizationId".to_string()),
            &Type::BuiltIn(BuiltInType::I64),
        ));

        // BuiltIn vs UserDefined(ValType) should also work
        assert!(validator.types_compatible(
            &Type::BuiltIn(BuiltInType::I64),
            &Type::UserDefined("OrganizationId".to_string()),
        ));

        // Mismatched base type should fail
        assert!(!validator.types_compatible(
            &Type::UserDefined("OrganizationId".to_string()),
            &Type::BuiltIn(BuiltInType::String),
        ));

        // Optional column with ValType should work
        assert!(validator.types_compatible(
            &Type::UserDefined("OrganizationId".to_string()),
            &Type::Optional(Box::new(Type::BuiltIn(BuiltInType::I64))),
        ));
    }

    #[test]
    fn test_validate_insert_missing_required_field() {
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
            name: "email".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        
        schema.add_table(table);
        
        let mut validator = TypeValidator::new(&schema);
        
        let mut obj_fields = HashMap::new();
        obj_fields.insert("name".to_string(), Expression::Atomic(AtomicExpression::Literal(Literal::String("test".to_string()))));
        let insert_values = vec![Expression::Atomic(AtomicExpression::Literal(Literal::Object(ObjectLiteralExpression { fields: obj_fields, span: None })))];
        
        validator.validate_insert("users", &insert_values, None);
        
        assert_eq!(validator.errors.len(), 2);
        assert!(validator.errors.iter().any(|e| e.message.contains("Unknown field 'name'")));
        assert!(validator.errors.iter().any(|e| e.message.contains("Required field 'email' is missing")));
    }
}