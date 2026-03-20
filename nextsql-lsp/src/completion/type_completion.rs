use crate::schema_cache::SchemaCache;
use std::sync::Arc;
use tower_lsp::lsp_types::*;

pub trait TypeCompletionProvider {
    fn get_text(&self) -> &str;
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>>;
    fn get_file_uri(&self) -> Option<&str>;

    async fn get_type_completions(&self) -> Vec<CompletionItem> {
        let mut completions = Vec::new();

        // Add utility types with snippet
        completions.push(CompletionItem {
            label: "Insertable".to_string(),
            kind: Some(CompletionItemKind::INTERFACE),
            detail: Some("Insertable<T> - Insert type for models".to_string()),
            documentation: Some(Documentation::String(
                "A utility type that makes optional fields with default values".to_string(),
            )),
            insert_text: Some("Insertable<$1>$0".to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            sort_text: Some("2_Insertable".to_string()),
            ..Default::default()
        });

        completions.push(CompletionItem {
            label: "ChangeSet".to_string(),
            kind: Some(CompletionItemKind::INTERFACE),
            detail: Some("ChangeSet<T> - Partial update type for models".to_string()),
            documentation: Some(Documentation::String(
                "A utility type for partial updates where all fields are optional".to_string(),
            )),
            insert_text: Some("ChangeSet<$1>$0".to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            sort_text: Some("2_ChangeSet".to_string()),
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
            ("decimal", "Decimal type (arbitrary precision)"),
            ("json", "JSON value type"),
        ];

        for (type_name, description) in builtin_types {
            completions.push(CompletionItem {
                label: type_name.to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some(description.to_string()),
                insert_text: Some(type_name.to_string()),
                sort_text: Some(format!("1_{}", type_name)),
                ..Default::default()
            });
        }

        // Add valtype names defined in the current file
        {
            use std::sync::LazyLock;
            static RE: LazyLock<regex::Regex> = LazyLock::new(|| {
                regex::Regex::new(r"valtype\s+(\w+)\s*=\s*(\w+)").unwrap()
            });
            for cap in RE.captures_iter(self.get_text()) {
                let valtype_name = &cap[1];
                let base_type = &cap[2];
                completions.push(CompletionItem {
                    label: valtype_name.to_string(),
                    kind: Some(CompletionItemKind::TYPE_PARAMETER),
                    detail: Some(format!("valtype {} = {}", valtype_name, base_type)),
                    documentation: Some(Documentation::String(format!(
                        "Value type alias for {}",
                        base_type
                    ))),
                    insert_text: Some(valtype_name.to_string()),
                    sort_text: Some(format!("0_{}", valtype_name)),
                    ..Default::default()
                });
            }
        }

        // Add input type names defined in the current file
        {
            use std::sync::LazyLock;
            static RE: LazyLock<regex::Regex> = LazyLock::new(|| {
                regex::Regex::new(r"input\s+(\w+)\s*\{").unwrap()
            });
            for cap in RE.captures_iter(self.get_text()) {
                let input_name = &cap[1];
                completions.push(CompletionItem {
                    label: input_name.to_string(),
                    kind: Some(CompletionItemKind::STRUCT),
                    detail: Some(format!("input {}", input_name)),
                    documentation: Some(Documentation::String(format!(
                        "Input type '{}'",
                        input_name
                    ))),
                    insert_text: Some(input_name.to_string()),
                    sort_text: Some(format!("0_{}", input_name)),
                    ..Default::default()
                });
            }
        }

        // Add valtypes from other files via schema cache
        if let Some(schema_cache) = self.get_schema_cache() {
            if let Some(file_uri) = self.get_file_uri() {
                let file_path = std::path::Path::new(file_uri);
                let other_valtypes = schema_cache.get_all_valtypes_except(file_path).await;
                // Collect current file valtype names to avoid duplicates
                let current_valtype_names: std::collections::HashSet<String> = completions.iter()
                    .filter(|c| c.kind == Some(CompletionItemKind::TYPE_PARAMETER))
                    .map(|c| c.label.clone())
                    .collect();
                for (name, base_type) in other_valtypes {
                    if !current_valtype_names.contains(&name) {
                        completions.push(CompletionItem {
                            label: name.clone(),
                            kind: Some(CompletionItemKind::TYPE_PARAMETER),
                            detail: Some(format!("valtype {} = {:?}", name, base_type)),
                            documentation: Some(Documentation::String(format!(
                                "Value type alias for {:?} (from another file)",
                                base_type
                            ))),
                            insert_text: Some(name.clone()),
                            sort_text: Some(format!("0_{}", name)),
                            ..Default::default()
                        });
                    }
                }
            }

            // Add enum types from database schema
            if let Some(file_uri) = self.get_file_uri() {
                let file_path = std::path::Path::new(file_uri);
                if let Some(schema) = schema_cache.get_schema_for_file(file_path).await {
                    for (enum_name, enum_schema) in &schema.enums {
                        let variants_preview = enum_schema.variants.iter()
                            .take(5)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ");
                        let detail = if enum_schema.variants.len() > 5 {
                            format!("{}, ...", variants_preview)
                        } else {
                            variants_preview
                        };
                        completions.push(CompletionItem {
                            label: enum_name.clone(),
                            kind: Some(CompletionItemKind::ENUM),
                            detail: Some(format!("enum {}", enum_name)),
                            documentation: Some(Documentation::String(format!(
                                "Database enum type\nVariants: {}",
                                detail
                            ))),
                            insert_text: Some(enum_name.clone()),
                            sort_text: Some(format!("0_{}", enum_name)),
                            ..Default::default()
                        });
                    }
                }
            }
        }

        completions
    }
}