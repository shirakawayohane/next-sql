use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use crate::ast::Type;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    pub tables: BTreeMap<String, TableSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub column_type: Type,
    pub nullable: bool,
    pub primary_key: bool,
    pub has_default: bool,
    pub default_value: Option<String>,
}

impl DatabaseSchema {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    pub fn add_table(&mut self, table: TableSchema) {
        self.tables.insert(table.name.clone(), table);
    }
}

impl TableSchema {
    pub fn new(name: String) -> Self {
        Self {
            name,
            columns: Vec::new(),
        }
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|col| col.name == name)
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.get_column(name).is_some()
    }

    pub fn add_column(&mut self, column: ColumnSchema) {
        self.columns.push(column);
    }

    pub fn get_insertable_columns(&self) -> Vec<&ColumnSchema> {
        self.columns
            .iter()
            .filter(|col| !col.primary_key || !col.has_default)
            .collect()
    }

    pub fn get_required_columns(&self) -> Vec<&ColumnSchema> {
        self.columns
            .iter()
            .filter(|col| !col.nullable && !col.has_default && !col.primary_key)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub enum SchemaError {
    TableNotFound(String),
    ColumnNotFound { table: String, column: String },
    TypeMismatch { expected: Type, actual: Type },
    RequiredFieldMissing { table: String, field: String },
    UnknownField { table: String, field: String },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::TableNotFound(table) => {
                write!(f, "Table '{}' not found", table)
            }
            SchemaError::ColumnNotFound { table, column } => {
                write!(f, "Column '{}' not found in table '{}'", column, table)
            }
            SchemaError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {:?}, got {:?}", expected, actual)
            }
            SchemaError::RequiredFieldMissing { table, field } => {
                write!(f, "Required field '{}' is missing in table '{}'", field, table)
            }
            SchemaError::UnknownField { table, field } => {
                write!(f, "Unknown field '{}' in table '{}'", field, table)
            }
        }
    }
}

impl std::error::Error for SchemaError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Type, BuiltInType};

    #[test]
    fn test_insertable_columns() {
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
            name: "created_at".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Timestamp),
            nullable: false,
            primary_key: false,
            has_default: true,
            default_value: Some("CURRENT_TIMESTAMP".to_string()),
        });
        
        let insertable = table.get_insertable_columns();
        assert_eq!(insertable.len(), 2);
        
        let required = table.get_required_columns();
        assert_eq!(required.len(), 1);
        assert_eq!(required[0].name, "name");
    }
}