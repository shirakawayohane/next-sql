use crate::schema_cache::SchemaCache;
use std::sync::Arc;
use tower_lsp::lsp_types::*;

pub trait TableCompletionProvider {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>>;
    fn get_file_uri(&self) -> Option<&str>;

    async fn get_table_completions(&self) -> Vec<CompletionItem> {
        let mut completions = Vec::new();

        if let (Some(schema_cache), Some(file_uri)) = (self.get_schema_cache(), self.get_file_uri()) {
            let path = std::path::Path::new(file_uri.trim_start_matches("file://"));

            if let Some(schema) = schema_cache.get_schema_for_file(path).await {
                for table_name in schema.tables.keys() {
                    completions.push(CompletionItem {
                        label: table_name.clone(),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("Table: {}", table_name)),
                        documentation: Some(Documentation::String(format!(
                            "Table {} from database schema",
                            table_name
                        ))),
                        insert_text: Some(table_name.clone()),
                        ..Default::default()
                    });
                }
            }
        }

        // フォールバック: スキーマが利用できない場合の一般的なテーブル名
        if completions.is_empty() {
            let common_tables = vec!["users", "posts", "comments", "orders", "products"];
            for table in common_tables {
                completions.push(CompletionItem {
                    label: table.to_string(),
                    kind: Some(CompletionItemKind::CLASS),
                    detail: Some(format!("Table: {} (suggestion)", table)),
                    documentation: Some(Documentation::String(
                        "Common table name suggestion".to_string(),
                    )),
                    insert_text: Some(table.to_string()),
                    ..Default::default()
                });
            }
        }

        completions
    }
}