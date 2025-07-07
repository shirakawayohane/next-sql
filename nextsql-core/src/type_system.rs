use crate::ast::Type;
use crate::UtilityType;
use crate::schema::{DatabaseSchema, TableSchema};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TypeDefinition {
    pub name: String,
    pub fields: Vec<FieldDefinition>,
}

#[derive(Debug, Clone)]
pub struct FieldDefinition {
    pub name: String,
    pub field_type: Type,
    pub optional: bool,
}

pub struct TypeSystem {
    pub types: HashMap<String, TypeDefinition>,
}

impl TypeSystem {
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }

    pub fn derive_from_schema(&mut self, schema: &DatabaseSchema) {
        for (table_name, table) in &schema.tables {
            // Generate the base type for the table
            let base_type = self.create_table_type(table_name, table);
            self.types.insert(table_name.clone(), base_type);

            // Generate Insertable<T> type
            let insertable_type = self.create_insertable_type(table_name, table);
            self.types.insert(format!("Insertable<{}>", table_name), insertable_type);
        }
    }

    fn create_table_type(&self, name: &str, table: &TableSchema) -> TypeDefinition {
        let fields = table.columns.iter().map(|col| {
            FieldDefinition {
                name: col.name.clone(),
                field_type: col.column_type.clone(),
                optional: col.nullable,
            }
        }).collect();

        TypeDefinition {
            name: name.to_string(),
            fields,
        }
    }

    fn create_insertable_type(&self, table_name: &str, table: &TableSchema) -> TypeDefinition {
        let fields = table.columns.iter()
            .filter_map(|col| {
                // Skip auto-generated primary keys
                if col.primary_key && col.has_default {
                    return None;
                }
                
                Some(FieldDefinition {
                    name: col.name.clone(),
                    field_type: col.column_type.clone(),
                    // Fields with defaults are optional in Insertable<T>
                    optional: col.nullable || col.has_default,
                })
            })
            .collect();

        TypeDefinition {
            name: format!("Insertable<{}>", table_name),
            fields,
        }
    }

    pub fn get_type(&self, name: &str) -> Option<&TypeDefinition> {
        self.types.get(name)
    }

    pub fn resolve_utility_type(&self, utility_type: &str, type_arg: &str) -> Option<TypeDefinition> {
        match utility_type {
            "Insertable" => {
                self.types.get(&format!("Insertable<{}>", type_arg)).cloned()
            }
            _ => None,
        }
    }

    pub fn is_assignable(&self, from_type: &Type, to_type: &Type) -> bool {
        match (from_type, to_type) {
            // Check utility types
            (Type::Utility(UtilityType::Insertable(insertable)), to) => {
                if let Type::UserDefined(table_name) = &*insertable.0 {
                    if let Some(insertable_def) = self.resolve_utility_type("Insertable", &table_name) {
                        // Check if the Insertable type is assignable to the target
                        return self.check_object_assignability(&insertable_def, to);
                    }
                }
                false
            }
            _ => {
                // Use existing type compatibility logic
                self.basic_type_compatibility(from_type, to_type)
            }
        }
    }

    fn check_object_assignability(&self, from_def: &TypeDefinition, to_type: &Type) -> bool {
        // This would be used to check if an object type (like Insertable<User>)
        // is assignable to another type
        match to_type {
            Type::UserDefined(type_name) => {
                // For now, just check if the names match the pattern
                from_def.name == format!("Insertable<{}>", type_name)
            }
            _ => false,
        }
    }

    fn basic_type_compatibility(&self, from: &Type, to: &Type) -> bool {
        match (from, to) {
            (Type::BuiltIn(a), Type::BuiltIn(b)) => a == b,
            (Type::UserDefined(a), Type::UserDefined(b)) => a == b,
            (Type::Optional(a), Type::Optional(b)) => self.basic_type_compatibility(a, b),
            (a, Type::Optional(b)) => self.basic_type_compatibility(a, b),
            (Type::Array(a), Type::Array(b)) => self.basic_type_compatibility(a, b),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema};
    use crate::ast::BuiltInType;

    #[test]
    fn test_insertable_type_derivation() {
        let mut schema = DatabaseSchema::new();
        let mut table = TableSchema::new("users".to_string());
        
        // Auto-generated ID - should not be in Insertable
        table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        
        // Required field
        table.add_column(ColumnSchema {
            name: "email".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        
        // Optional field (nullable)
        table.add_column(ColumnSchema {
            name: "name".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: true,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        
        // Field with default
        table.add_column(ColumnSchema {
            name: "created_at".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Timestamp),
            nullable: false,
            primary_key: false,
            has_default: true,
            default_value: Some("CURRENT_TIMESTAMP".to_string()),
        });
        
        schema.add_table(table);
        
        let mut type_system = TypeSystem::new();
        type_system.derive_from_schema(&schema);
        
        let insertable = type_system.get_type("Insertable<users>").unwrap();
        
        // Should have 3 fields (not id)
        assert_eq!(insertable.fields.len(), 3);
        
        // Check field properties
        let email_field = insertable.fields.iter().find(|f| f.name == "email").unwrap();
        assert!(!email_field.optional); // Required
        
        let name_field = insertable.fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name_field.optional); // Optional because nullable
        
        let created_field = insertable.fields.iter().find(|f| f.name == "created_at").unwrap();
        assert!(created_field.optional); // Optional because has default
    }
}