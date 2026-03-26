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
                        detail: Some(format!("Table: {table_name}")),
                        documentation: Some(Documentation::String(format!(
                            "Table {table_name} from database schema"
                        ))),
                        insert_text: Some(table_name.clone()),
                        ..Default::default()
                    });
                }
            }
        }

        completions
    }
}