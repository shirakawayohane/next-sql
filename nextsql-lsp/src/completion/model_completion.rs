use crate::schema_cache::SchemaCache;
use super::utils::{snake_to_camel_case, utf16_position_to_byte_index};
use std::sync::Arc;
use tower_lsp::lsp_types::*;

pub trait ModelCompletionProvider {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>>;
    fn get_file_uri(&self) -> Option<&str>;

    async fn get_model_completions_for_insertable(&self, has_closing_bracket: bool, position: Position, current_line: &str) -> Vec<CompletionItem> {
        let mut completions = Vec::new();

        if let (Some(schema_cache), Some(file_uri)) = (self.get_schema_cache(), self.get_file_uri()) {
            let path = std::path::Path::new(file_uri.trim_start_matches("file://"));

            if let Some(schema) = schema_cache.get_schema_for_file(path).await {
                for table_name in schema.tables.keys() {
                    let model_name = snake_to_camel_case(table_name);
                    
                    // Calculate the range for text edit
                    let byte_position = utf16_position_to_byte_index(current_line, position.character as usize);
                    let before_cursor = &current_line[..byte_position.min(current_line.len())];
                    
                    // Find where "Insertable<" or "ChangeSet<" starts
                    let (insertable_start, pattern_len) = if let Some(pos) = before_cursor.rfind("ChangeSet<") {
                        (pos, "ChangeSet<".len())
                    } else {
                        (before_cursor.rfind("Insertable<").unwrap_or(byte_position), "Insertable<".len())
                    };
                    let start_of_model = insertable_start + pattern_len;
                    
                    // Calculate start position in UTF-16
                    let start_char_count = current_line[..start_of_model].chars().count();
                    let start_position = Position {
                        line: position.line,
                        character: start_char_count as u32,
                    };
                    
                    // Calculate end position
                    let end_position = if has_closing_bracket {
                        // Include the closing bracket in the replacement range
                        Position {
                            line: position.line,
                            character: position.character + 1,
                        }
                    } else {
                        position
                    };
                    
                    // Create the replacement text
                    let replacement_text = if has_closing_bracket {
                        // Include the closing bracket and use snippet to position cursor after it
                        format!("{model_name}>$0")
                    } else {
                        format!("{model_name}>")
                    };
                    
                    let mut completion_item = CompletionItem {
                        label: model_name.clone(),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("Model for table {table_name}")),
                        documentation: Some(Documentation::String(format!(
                            "Utility type for {table_name} table"
                        ))),
                        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                            range: Range {
                                start: start_position,
                                end: end_position,
                            },
                            new_text: replacement_text.clone(),
                        })),
                        ..Default::default()
                    };
                    
                    // Use snippet format if we need to position cursor
                    if has_closing_bracket {
                        completion_item.insert_text_format = Some(InsertTextFormat::SNIPPET);
                    }
                    
                    completions.push(completion_item);
                }
            }
        }

        // フォールバック: スキーマが利用できない場合の一般的なモデル名
        if completions.is_empty() {
            let common_models = vec![
                ("users", "Users"),
                ("posts", "Posts"),
                ("comments", "Comments"),
                ("orders", "Orders"),
                ("products", "Products"),
            ];
            
            // Calculate positions for fallback completions
            let byte_position = utf16_position_to_byte_index(current_line, position.character as usize);
            let before_cursor = &current_line[..byte_position.min(current_line.len())];
            
            let (insertable_start, pattern_len) = if let Some(pos) = before_cursor.rfind("ChangeSet<") {
                (pos, "ChangeSet<".len())
            } else {
                (before_cursor.rfind("Insertable<").unwrap_or(byte_position), "Insertable<".len())
            };
            let start_of_model = insertable_start + pattern_len;
            
            let start_char_count = current_line[..start_of_model].chars().count();
            let start_position = Position {
                line: position.line,
                character: start_char_count as u32,
            };
            
            let end_position = if has_closing_bracket {
                Position {
                    line: position.line,
                    character: position.character + 1,
                }
            } else {
                position
            };
            
            for (table, model) in common_models {
                let replacement_text = if has_closing_bracket {
                    format!("{model}>$0")
                } else {
                    format!("{model}>")
                };
                
                let mut completion_item = CompletionItem {
                    label: model.to_string(),
                    kind: Some(CompletionItemKind::CLASS),
                    detail: Some(format!("Model for table {table} (suggestion)")),
                    documentation: Some(Documentation::String(format!(
                        "Utility type for {table} table"
                    ))),
                    text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                        range: Range {
                            start: start_position,
                            end: end_position,
                        },
                        new_text: replacement_text,
                    })),
                    ..Default::default()
                };
                
                if has_closing_bracket {
                    completion_item.insert_text_format = Some(InsertTextFormat::SNIPPET);
                }
                
                completions.push(completion_item);
            }
        }

        completions
    }
}