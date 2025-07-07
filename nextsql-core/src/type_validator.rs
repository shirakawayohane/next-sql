use crate::ast::{Module, MutationBody, MutationBodyItem, MutationStatement, Expression, Type, BuiltInType, Literal, CallExpression, AtomicExpression};
use crate::schema::DatabaseSchema;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
    pub span: Option<Span>,
}

#[derive(Debug, Clone)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

pub struct TypeValidator<'a> {
    schema: &'a DatabaseSchema,
    variables: HashMap<String, Type>,
    table_aliases: HashMap<String, String>,
    errors: Vec<ValidationError>,
}

impl<'a> TypeValidator<'a> {
    pub fn new(schema: &'a DatabaseSchema) -> Self {
        Self {
            schema,
            variables: HashMap::new(),
            table_aliases: HashMap::new(),
            errors: Vec::new(),
        }
    }

    pub fn validate_module(&mut self, module: &Module) -> Vec<ValidationError> {
        self.errors.clear();
        
        // First pass: collect all function signatures and variables
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
                _ => {}
            }
        }

        // Second pass: validate function bodies
        for toplevel in &module.toplevels {
            match toplevel {
                crate::ast::TopLevel::Mutation(mutation) => {
                    self.validate_mutation_body(&mutation.body);
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
                        span: None,
                    });
                }
            }
            Expression::Atomic(AtomicExpression::Call(call)) => {
                self.validate_function_call(call);
            }
            Expression::Atomic(AtomicExpression::Literal(Literal::Object(_obj))) => {
                // Object literals are validated in context
            }
            Expression::Binary { left, right, .. } => {
                self.validate_expression(left);
                self.validate_expression(right);
            }
            _ => {}
        }
    }

    fn validate_mutation_body(&mut self, mutation_body: &MutationBody) {
        for item in &mutation_body.items {
            match item {
                MutationBodyItem::Alias(alias) => {
                    // Add alias to scope
                    self.table_aliases.insert(alias.alias.clone(), alias.target.clone());
                }
                MutationBodyItem::Mutation(statement) => {
                    match statement {
                        MutationStatement::Insert(insert) => {
                            self.validate_insert(&insert.into, &insert.values);
                        }
                        MutationStatement::Update(update) => {
                            self.validate_update(&update.target.name, &update.set, update.where_clause.as_ref());
                        }
                        MutationStatement::Delete(delete) => {
                            self.validate_delete(&delete.target.name, delete.where_clause.as_ref());
                        }
                    }
                }
            }
        }
    }

    fn validate_insert(&mut self, target: &str, values: &[Expression]) {
        let table = match self.schema.get_table(target) {
            Some(table) => table,
            None => {
                self.errors.push(ValidationError {
                    message: format!("Table '{}' not found", target),
                    span: None,
                });
                return;
            }
        };

        for value_expr in values {
            match value_expr {
                Expression::Atomic(AtomicExpression::Literal(Literal::Object(obj))) => {
                // Check for unknown fields
                    let table_columns: HashSet<_> = table.columns.iter().map(|c| &c.name).collect();
                    for (field_name, _) in &obj.0 {
                        if !table_columns.contains(field_name) {
                            self.errors.push(ValidationError {
                                message: format!("Unknown field '{}' in table '{}'", field_name, target),
                                span: None,
                            });
                        }
                    }

                    // Check required fields
                    let provided_fields: HashSet<_> = obj.0.keys().collect();
                    for column in table.get_required_columns() {
                        if !provided_fields.contains(&column.name) {
                            self.errors.push(ValidationError {
                                message: format!("Required field '{}' is missing in table '{}'", column.name, target),
                                span: None,
                            });
                        }
                    }

                    // Validate field types and expressions
                    for (field_name, field_value) in &obj.0 {
                        // First validate the expression itself (for undefined variables, etc)
                        self.validate_expression(field_value);
                        
                        // Then validate the type if the column exists
                        if let Some(column) = table.get_column(field_name) {
                            self.validate_value_type(field_value, &column.column_type, field_name);
                        }
                    }
                }
                _ => {
                    self.errors.push(ValidationError {
                        message: "INSERT values must be object literals".to_string(),
                        span: None,
                    });
                }
            }
        }
    }

    fn validate_update(&mut self, target: &str, set: &[(String, Expression)], where_clause: Option<&Expression>) {
        let table = match self.schema.get_table(target) {
            Some(table) => table,
            None => {
                self.errors.push(ValidationError {
                    message: format!("Table '{}' not found", target),
                    span: None,
                });
                return;
            }
        };

        // Validate SET clause
        for (field_name, field_value) in set {
            match table.get_column(field_name) {
                Some(column) => {
                    self.validate_value_type(field_value, &column.column_type, field_name);
                }
                None => {
                    self.errors.push(ValidationError {
                        message: format!("Unknown field '{}' in table '{}'", field_name, target),
                        span: None,
                    });
                }
            }
        }

        // Validate WHERE clause
        if let Some(where_expr) = where_clause {
            self.validate_expression(where_expr);
        }
    }

    fn validate_delete(&mut self, target: &str, where_clause: Option<&Expression>) {
        if !self.schema.tables.contains_key(target) {
            self.errors.push(ValidationError {
                message: format!("Table '{}' not found", target),
                span: None,
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
                            span: None,
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

    fn types_compatible(&self, actual: &Type, expected: &Type) -> bool {
        match (actual, expected) {
            (Type::BuiltIn(a), Type::BuiltIn(e)) => {
                // Basic type compatibility
                a == e || 
                // Allow numeric conversions
                (self.is_numeric_builtin_type(a) && self.is_numeric_builtin_type(e))
            }
            (Type::Optional(inner), expected) => {
                self.types_compatible(inner, expected)
            }
            (actual, Type::Optional(inner)) => {
                self.types_compatible(actual, inner)
            }
            (Type::UserDefined(a), Type::UserDefined(e)) => a == e,
            _ => false,
        }
    }

    fn is_numeric_builtin_type(&self, builtin: &BuiltInType) -> bool {
        matches!(builtin, 
            BuiltInType::I16 | BuiltInType::I32 | BuiltInType::I64 | 
            BuiltInType::F32 | BuiltInType::F64
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TableSchema, ColumnSchema};
    use crate::ast::ObjectLiteralExpression;
    use std::collections::HashMap;

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
        let insert_values = vec![Expression::Atomic(AtomicExpression::Literal(Literal::Object(ObjectLiteralExpression(obj_fields))))];
        
        validator.validate_insert("users", &insert_values);
        
        assert_eq!(validator.errors.len(), 2);
        assert!(validator.errors.iter().any(|e| e.message.contains("Unknown field 'name'")));
        assert!(validator.errors.iter().any(|e| e.message.contains("Required field 'email' is missing")));
    }
}