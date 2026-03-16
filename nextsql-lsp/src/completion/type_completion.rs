use crate::schema_cache::SchemaCache;
use super::utils::snake_to_camel_case;
use std::sync::Arc;
use tower_lsp::lsp_types::*;

pub trait TypeCompletionProvider {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>>;
    fn get_file_uri(&self) -> Option<&str>;

    async fn get_type_completions(&self) -> Vec<CompletionItem> {
        let mut completions = Vec::new();

        // Add Insertable as a type with snippet
        completions.push(CompletionItem {
            label: "Insertable".to_string(),
            kind: Some(CompletionItemKind::INTERFACE),
            detail: Some("Insertable<T> - Insert type for models".to_string()),
            documentation: Some(Documentation::String(
                "A utility type that makes optional fields with default values".to_string(),
            )),
            insert_text: Some("Insertable<$1>$0".to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        });

        // Add built-in types
        let builtin_types = vec![
            ("i16", "16-bit signed integer"),
            ("i32", "32-bit signed integer"),
            ("i64", "64-bit signed integer"),
            ("f32", "32-bit floating point"),
            ("f64", "64-bit floating point"),
            ("string", "String type"),
            ("bool", "Boolean type"),
            ("uuid", "UUID type"),
            ("timestamp", "Timestamp type"),
            ("timestamptz", "Timestamp with timezone"),
            ("date", "Date type"),
            ("numeric", "Arbitrary precision numeric type"),
            ("decimal", "Decimal type (arbitrary precision)"),
            ("json", "JSON value type"),
        ];

        for (type_name, description) in builtin_types {
            completions.push(CompletionItem {
                label: type_name.to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some(description.to_string()),
                insert_text: Some(type_name.to_string()),
                ..Default::default()
            });
        }

        // Add table names as user-defined types
        if let (Some(schema_cache), Some(file_uri)) = (self.get_schema_cache(), self.get_file_uri()) {
            let path = std::path::Path::new(file_uri.trim_start_matches("file://"));

            if let Some(schema) = schema_cache.get_schema_for_file(path).await {
                for table_name in schema.tables.keys() {
                    let model_name = snake_to_camel_case(table_name);
                    
                    completions.push(CompletionItem {
                        label: model_name.clone(),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("Model type for table {}", table_name)),
                        documentation: Some(Documentation::String(format!(
                            "Type representing the {} table",
                            table_name
                        ))),
                        insert_text: Some(model_name),
                        ..Default::default()
                    });
                }
            }
        }

        completions
    }
}