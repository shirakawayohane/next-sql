use crate::schema::{DatabaseSchema, TableSchema, ColumnSchema, EnumSchema};
use crate::ast::{Type, BuiltInType};
use postgres::{Client, Error};
use std::collections::BTreeMap;

pub struct SchemaLoader;

impl SchemaLoader {
    pub fn load_from_database(client: &mut Client) -> Result<DatabaseSchema, Error> {
        let mut schema = DatabaseSchema::new();

        // Use pg_catalog directly instead of information_schema views for performance.
        // pg_catalog tables are physical tables with proper indexes, while
        // information_schema consists of complex views that are significantly slower.
        let query = r#"
            SELECT
                cls.relname AS table_name,
                att.attname AS column_name,
                typ.typname AS type_name,
                NOT att.attnotnull AS is_nullable,
                pg_get_expr(def.adbin, def.adrelid) AS column_default,
                COALESCE(idx.indisprimary, false) AS is_primary_key,
                CASE
                    WHEN typ.typtype = 'e' THEN 'USER-DEFINED'
                    WHEN typ.typlen = -1 AND typ.typelem != 0 THEN 'ARRAY'
                    ELSE typ.typname
                END AS resolved_type,
                CASE
                    WHEN typ.typlen = -1 AND typ.typelem != 0 THEN elem.typname
                    ELSE typ.typname
                END AS elem_type
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
            JOIN pg_catalog.pg_attribute att ON att.attrelid = cls.oid
            JOIN pg_catalog.pg_type typ ON typ.oid = att.atttypid
            LEFT JOIN pg_catalog.pg_type elem ON elem.oid = typ.typelem
            LEFT JOIN pg_catalog.pg_attrdef def
                ON def.adrelid = cls.oid AND def.adnum = att.attnum
            LEFT JOIN pg_catalog.pg_index idx
                ON idx.indrelid = cls.oid AND idx.indisprimary AND att.attnum = ANY(idx.indkey)
            WHERE nsp.nspname = 'public'
                AND cls.relkind = 'r'
                AND att.attnum > 0
                AND NOT att.attisdropped
            ORDER BY cls.relname, att.attnum
        "#;

        let rows = client.query(query, &[])?;

        let mut tables: BTreeMap<String, TableSchema> = BTreeMap::new();

        for row in rows {
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);
            let _type_name: String = row.get(2);
            let is_nullable: bool = row.get(3);
            let column_default: Option<String> = row.get(4);
            let is_primary_key: bool = row.get(5);
            let resolved_type: String = row.get(6);
            let elem_type: String = row.get(7);

            let table = tables.entry(table_name.clone())
                .or_insert_with(|| TableSchema::new(table_name));

            let column_type = Self::pg_typname_to_nextsql_type(&resolved_type, &elem_type);

            table.add_column(ColumnSchema {
                name: column_name,
                column_type,
                nullable: is_nullable,
                primary_key: is_primary_key,
                has_default: column_default.is_some(),
                default_value: column_default,
            });
        }

        for (_, table) in tables {
            schema.add_table(table);
        }

        // Load enum types
        let enum_query = r#"
            SELECT t.typname, e.enumlabel
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = 'public'
            ORDER BY t.typname, e.enumsortorder
        "#;

        let enum_rows = client.query(enum_query, &[])?;

        let mut enums: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in enum_rows {
            let type_name: String = row.get(0);
            let variant: String = row.get(1);
            enums.entry(type_name).or_default().push(variant);
        }

        for (name, variants) in enums {
            schema.add_enum(EnumSchema { name, variants });
        }

        Ok(schema)
    }

    /// Map pg_catalog typname to NextSQL types.
    fn pg_typname_to_nextsql_type(typname: &str, elem_type: &str) -> Type {
        match typname {
            "int2" | "smallint" => Type::BuiltIn(BuiltInType::I16),
            "int4" | "integer" => Type::BuiltIn(BuiltInType::I32),
            "int8" | "bigint" => Type::BuiltIn(BuiltInType::I64),
            "float4" | "real" => Type::BuiltIn(BuiltInType::F32),
            "float8" | "double precision" => Type::BuiltIn(BuiltInType::F64),
            "text" | "varchar" | "bpchar" | "name"
            | "character varying" | "character" => Type::BuiltIn(BuiltInType::String),
            "bool" | "boolean" => Type::BuiltIn(BuiltInType::Bool),
            "uuid" => Type::BuiltIn(BuiltInType::Uuid),
            "timestamp" | "timestamp without time zone" => Type::BuiltIn(BuiltInType::Timestamp),
            "timestamptz" | "timestamp with time zone" => Type::BuiltIn(BuiltInType::Timestamptz),
            "date" => Type::BuiltIn(BuiltInType::Date),
            "numeric" | "decimal" => Type::BuiltIn(BuiltInType::Decimal),
            "jsonb" | "json" => Type::BuiltIn(BuiltInType::Json),
            "ARRAY" => Type::Array(Box::new(Self::pg_typname_to_nextsql_type(elem_type, ""))),
            "USER-DEFINED" => Type::UserDefined(elem_type.to_string()),
            _ => Type::UserDefined(typname.to_string()),
        }
    }

    /// Fetch a lightweight fingerprint of the public schema.
    /// Uses pg_catalog directly for minimal overhead.
    /// Returns a hash string that changes when any table/column DDL changes.
    pub fn fetch_schema_fingerprint(client: &mut Client) -> Result<String, Error> {
        let query = r#"
            SELECT md5(
                COALESCE((
                    SELECT string_agg(
                        cls.relname || '.' || att.attname || ':' || typ.typname || ':' || att.attnotnull::text,
                        ',' ORDER BY cls.relname, att.attnum
                    )
                    FROM pg_catalog.pg_class cls
                    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
                    JOIN pg_catalog.pg_attribute att ON att.attrelid = cls.oid
                    JOIN pg_catalog.pg_type typ ON typ.oid = att.atttypid
                    WHERE nsp.nspname = 'public'
                        AND cls.relkind = 'r'
                        AND att.attnum > 0
                        AND NOT att.attisdropped
                ), '')
                || '|' ||
                COALESCE((
                    SELECT string_agg(
                        t.typname || ':' || e.enumlabel,
                        ',' ORDER BY t.typname, e.enumsortorder
                    )
                    FROM pg_catalog.pg_type t
                    JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
                    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                    WHERE n.nspname = 'public'
                ), '')
            )
        "#;
        let row = client.query_one(query, &[])?;
        let hash: Option<String> = row.get(0);
        Ok(hash.unwrap_or_default())
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
            SchemaLoader::pg_typname_to_nextsql_type("int4", "int4"),
            Type::BuiltIn(BuiltInType::I32)
        ));
        assert!(matches!(
            SchemaLoader::pg_typname_to_nextsql_type("text", "text"),
            Type::BuiltIn(BuiltInType::String)
        ));
        assert!(matches!(
            SchemaLoader::pg_typname_to_nextsql_type("USER-DEFINED", "my_enum"),
            Type::UserDefined(s) if s == "my_enum"
        ));
        assert!(matches!(
            SchemaLoader::pg_typname_to_nextsql_type("custom_type", "custom_type"),
            Type::UserDefined(s) if s == "custom_type"
        ));
    }
}