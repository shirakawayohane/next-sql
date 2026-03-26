use crate::schema_cache::SchemaCache;
use nextsql_core::{parse_module, ast::{TopLevel, RelationType}};
use std::sync::Arc;
use tower_lsp::lsp_types::*;

pub trait FieldCompletionProvider {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>>;
    fn get_file_uri(&self) -> Option<&str>;
    fn get_text(&self) -> &str;

    async fn get_field_completions(&self, table_name: &str) -> Vec<CompletionItem> {
        let mut completions = Vec::new();
        eprintln!("LSP: get_field_completions for table: '{table_name}'");

        if let (Some(schema_cache), Some(file_uri)) = (self.get_schema_cache(), self.get_file_uri()) {
            let path = std::path::Path::new(file_uri.trim_start_matches("file://"));
            eprintln!("LSP: File path for schema lookup: {path:?}");

            if let Some(schema) = schema_cache.get_schema_for_file(path).await {
                eprintln!("LSP: Schema found, looking for table '{table_name}'");
                if let Some(table) = schema.tables.get(table_name) {
                    eprintln!(
                        "LSP: Table '{}' found with {} columns",
                        table_name,
                        table.columns.len()
                    );
                    for column in &table.columns {
                        let type_info = format!("{}", column.column_type);
                        let mut detail = type_info.clone();
                        if column.nullable {
                            detail.push_str(" (nullable)");
                        }
                        if column.primary_key {
                            detail.push_str(" (primary key)");
                        }

                        eprintln!("LSP: Adding column completion: {}", column.name);
                        completions.push(CompletionItem {
                            label: column.name.clone(),
                            kind: Some(CompletionItemKind::FIELD),
                            detail: Some(detail),
                            documentation: Some(Documentation::String(format!(
                                "Column '{}' of table '{}'",
                                column.name, table_name
                            ))),
                            insert_text: Some(column.name.clone()),
                            ..Default::default()
                        });
                    }
                } else {
                    eprintln!("LSP: Table '{table_name}' not found in schema");
                }
            } else {
                eprintln!("LSP: No schema found for file");
            }
        } else {
            eprintln!("LSP: No schema cache or file URI available");
        }

        // Parse the module to get relation definitions
        if let Ok(module) = parse_module(self.get_text()) {
            eprintln!("LSP: Module parsed successfully, checking for relations");
            
            // Collect relations for this table
            let mut relations_for_table = Vec::new();
            
            for toplevel in &module.toplevels {
                if let TopLevel::Relation(relation) = toplevel {
                    if relation.decl.for_table == table_name {
                        relations_for_table.push(relation);
                    }
                }
            }
            
            eprintln!("LSP: Found {} relations for table '{}'", relations_for_table.len(), table_name);
            
            // Add relation completions
            for relation in relations_for_table {
                let relation_type_str = match relation.decl.relation_type {
                    RelationType::Relation => "relation",
                    RelationType::Aggregation => "aggregation",
                };
                
                let is_optional = relation.decl.modifiers.iter()
                    .any(|m| matches!(m, nextsql_core::ast::RelationModifier::Optional));
                
                let optional_str = if is_optional { " (optional)" } else { "" };
                
                let detail = format!("{relation_type_str}{optional_str}");
                
                eprintln!("LSP: Adding relation completion: {}", relation.decl.name);
                completions.push(CompletionItem {
                    label: relation.decl.name.clone(),
                    kind: Some(CompletionItemKind::REFERENCE),
                    detail: Some(detail),
                    documentation: Some(Documentation::String(format!(
                        "{} '{}' for table '{}'",
                        relation_type_str, relation.decl.name, table_name
                    ))),
                    insert_text: Some(relation.decl.name.clone()),
                    ..Default::default()
                });
            }
        } else {
            eprintln!("LSP: Failed to parse module for relation completions");
        }

        eprintln!("LSP: Returning {} field completions", completions.len());
        completions
    }
}