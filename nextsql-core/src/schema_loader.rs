use crate::schema::{DatabaseSchema, TableSchema, ColumnSchema};
use crate::ast::{Type, BuiltInType};
use postgres::{Client, Error};
use std::collections::HashMap;

pub struct SchemaLoader;

impl SchemaLoader {
    pub fn load_from_database(client: &mut Client) -> Result<DatabaseSchema, Error> {
        let mut schema = DatabaseSchema::new();

        let query = r#"
            SELECT
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                COALESCE(
                    (SELECT true FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage kcu
                     ON tc.constraint_name = kcu.constraint_name
                     WHERE tc.table_schema = c.table_schema
                     AND tc.table_name = c.table_name
                     AND kcu.column_name = c.column_name
                     AND tc.constraint_type = 'PRIMARY KEY'
                    ), false
                ) as is_primary_key,
                c.udt_name
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
            ORDER BY c.table_name, c.ordinal_position
        "#;

        let rows = client.query(query, &[])?;

        let mut tables: HashMap<String, TableSchema> = HashMap::new();

        for row in rows {
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);
            let data_type: String = row.get(2);
            let is_nullable: String = row.get(3);
            let column_default: Option<String> = row.get(4);
            let is_primary_key: bool = row.get(5);
            let udt_name: String = row.get(6);

            let table = tables.entry(table_name.clone())
                .or_insert_with(|| TableSchema::new(table_name));

            let column_type = Self::postgres_type_to_nextsql_type(&data_type, &udt_name);
            
            table.add_column(ColumnSchema {
                name: column_name,
                column_type,
                nullable: is_nullable == "YES",
                primary_key: is_primary_key,
                has_default: column_default.is_some(),
                default_value: column_default,
            });
        }

        for (_, table) in tables {
            schema.add_table(table);
        }

        Ok(schema)
    }

    fn postgres_type_to_nextsql_type(pg_type: &str, udt_name: &str) -> Type {
        match pg_type {
            "smallint" => Type::BuiltIn(BuiltInType::I16),
            "integer" => Type::BuiltIn(BuiltInType::I32),
            "bigint" => Type::BuiltIn(BuiltInType::I64),
            "real" => Type::BuiltIn(BuiltInType::F32),
            "double precision" => Type::BuiltIn(BuiltInType::F64),
            "text" | "character varying" | "character" => Type::BuiltIn(BuiltInType::String),
            "boolean" => Type::BuiltIn(BuiltInType::Bool),
            "uuid" => Type::BuiltIn(BuiltInType::Uuid),
            "timestamp without time zone" => Type::BuiltIn(BuiltInType::Timestamp),
            "timestamp with time zone" => Type::BuiltIn(BuiltInType::Timestamptz),
            "date" => Type::BuiltIn(BuiltInType::Date),
            "numeric" | "decimal" => Type::BuiltIn(BuiltInType::Decimal),
            "jsonb" | "json" => Type::BuiltIn(BuiltInType::Json),
            "ARRAY" => Type::Array(Box::new(Self::postgres_type_to_nextsql_type(
                &udt_name.trim_start_matches('_'), ""
            ))),
            "USER-DEFINED" => Type::UserDefined(udt_name.to_string()),
            _ => Type::UserDefined(pg_type.to_string()),
        }
    }

    pub fn load_from_json(json_str: &str) -> Result<DatabaseSchema, serde_json::Error> {
        serde_json::from_str(json_str)
    }

    pub fn save_to_json(schema: &DatabaseSchema) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_type_mapping() {
        assert!(matches!(
            SchemaLoader::postgres_type_to_nextsql_type("integer", "int4"),
            Type::BuiltIn(BuiltInType::I32)
        ));
        assert!(matches!(
            SchemaLoader::postgres_type_to_nextsql_type("text", "text"),
            Type::BuiltIn(BuiltInType::String)
        ));
        assert!(matches!(
            SchemaLoader::postgres_type_to_nextsql_type("USER-DEFINED", "my_enum"),
            Type::UserDefined(s) if s == "my_enum"
        ));
        assert!(matches!(
            SchemaLoader::postgres_type_to_nextsql_type("custom_type", "custom_type"),
            Type::UserDefined(s) if s == "custom_type"
        ));
    }
}