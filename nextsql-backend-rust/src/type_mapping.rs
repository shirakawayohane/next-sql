use nextsql_core::ast::{Type, BuiltInType, UtilityType, Insertable, ChangeSet};
use nextsql_core::schema::DatabaseSchema;

/// Convert a snake_case or lowercase name to PascalCase.
/// e.g., "order_status" -> "OrderStatus", "active" -> "Active"
pub fn to_pascal_case_type(name: &str) -> String {
    name.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
            }
        })
        .collect()
}

/// Check if a UserDefined type name corresponds to a known enum in the schema.
pub fn is_enum_type(name: &str, schema: &DatabaseSchema) -> bool {
    schema.enums.contains_key(name)
}

/// Get the Rust enum type name for a PostgreSQL enum type.
pub fn enum_rust_type(name: &str) -> String {
    to_pascal_case_type(name)
}

pub fn nextsql_type_to_rust(typ: &Type) -> String {
    match typ {
        Type::BuiltIn(b) => match b {
            BuiltInType::I16 => "i16".to_string(),
            BuiltInType::I32 => "i32".to_string(),
            BuiltInType::I64 => "i64".to_string(),
            BuiltInType::F32 => "f32".to_string(),
            BuiltInType::F64 => "f64".to_string(),
            BuiltInType::String => "String".to_string(),
            BuiltInType::Bool => "bool".to_string(),
            BuiltInType::Uuid => "uuid::Uuid".to_string(),
            BuiltInType::Timestamp => "chrono::NaiveDateTime".to_string(),
            BuiltInType::Timestamptz => "chrono::DateTime<chrono::Utc>".to_string(),
            BuiltInType::Date => "chrono::NaiveDate".to_string(),
            BuiltInType::Decimal => "rust_decimal::Decimal".to_string(),
            BuiltInType::Json => "serde_json::Value".to_string(),
        },
        Type::Optional(inner) => format!("Option<{}>", nextsql_type_to_rust(inner)),
        Type::Array(inner) => format!("Vec<{}>", nextsql_type_to_rust(inner)),
        Type::UserDefined(name) => match name.as_str() {
            "numeric" => "rust_decimal::Decimal".to_string(),
            "jsonb" | "json" => "serde_json::Value".to_string(),
            "bytea" => "Vec<u8>".to_string(),
            // PostgreSQL enum types are represented as strings
            _ => "String".to_string(),
        },
        Type::Utility(u) => match u {
            UtilityType::Insertable(Insertable(inner)) => nextsql_type_to_rust(inner),
            UtilityType::ChangeSet(ChangeSet(inner)) => nextsql_type_to_rust(inner),
        },
    }
}

/// Map NextSQL type to the tokio_postgres::types::Type for parameter binding
pub fn nextsql_type_to_tosql_trait(typ: &Type) -> String {
    // For generated code, we use dyn ToSql, so this is mainly for documentation
    nextsql_type_to_rust(typ)
}

/// Map NextSQL type to the Row trait getter method name
pub fn nextsql_type_to_row_getter(typ: &Type) -> String {
    match typ {
        Type::BuiltIn(b) => match b {
            BuiltInType::I16 => "get_i16".to_string(),
            BuiltInType::I32 => "get_i32".to_string(),
            BuiltInType::I64 => "get_i64".to_string(),
            BuiltInType::F32 => "get_f32".to_string(),
            BuiltInType::F64 => "get_f64".to_string(),
            BuiltInType::String => "get_string".to_string(),
            BuiltInType::Bool => "get_bool".to_string(),
            BuiltInType::Uuid => "get_uuid".to_string(),
            BuiltInType::Timestamp => "get_timestamp".to_string(),
            BuiltInType::Timestamptz => "get_timestamptz".to_string(),
            BuiltInType::Date => "get_date".to_string(),
            BuiltInType::Decimal => "get_decimal".to_string(),
            BuiltInType::Json => "get_json".to_string(),
        },
        Type::Optional(inner) => {
            let inner_getter = nextsql_type_to_row_getter(inner);
            // get_i32 -> get_opt_i32
            inner_getter.replace("get_", "get_opt_")
        },
        Type::Array(inner) => {
            let inner_getter = nextsql_type_to_row_getter(inner);
            // get_i32 -> get_vec_i32
            inner_getter.replace("get_", "get_vec_")
        },
        Type::UserDefined(name) => match name.as_str() {
            "numeric" => "get_numeric".to_string(),
            "jsonb" | "json" => "get_json".to_string(),
            // PostgreSQL enum types are fetched as strings
            _ => "get_string".to_string(),
        },
        _ => "get_string".to_string(), // fallback
    }
}
