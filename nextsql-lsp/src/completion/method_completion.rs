use super::context::CompletionContext;
use tower_lsp::lsp_types::*;

pub trait MethodCompletionProvider {
    fn get_text(&self) -> &str;

    fn get_method_completions(&self, context: CompletionContext) -> Vec<CompletionItem> {
        match context {
            CompletionContext::QueryMethod => {
                vec![
                    self.create_method_completion(
                        "select",
                        "select(fields)",
                        "Select specific fields",
                    ),
                    self.create_method_completion("where", "where(condition)", "Filter rows"),
                    self.create_method_completion("join", "join(table)", "Join with another table"),
                    self.create_method_completion(
                        "limit",
                        "limit(count)",
                        "Limit number of results",
                    ),
                    self.create_method_completion(
                        "offset",
                        "offset(count)",
                        "Skip number of results",
                    ),
                    self.create_method_completion("orderBy", "orderBy(field)", "Order results"),
                    self.create_method_completion("groupBy", "groupBy(field)", "Group results"),
                    self.create_method_completion("distinct", "distinct()", "Remove duplicate rows"),
                    self.create_method_completion("having", "having($1)", "Filter groups by condition"),
                    self.create_method_completion("union", "union($1)", "Combine results with UNION"),
                    self.create_method_completion("unionAll", "unionAll($1)", "Combine all results with UNION ALL"),
                ]
            }
            CompletionContext::MutationMethod => {
                // より正確なコンテキスト判定のため、直前のテキストを確認
                let is_after_insert = self.get_text().contains("insert(");
                let is_after_update = self.get_text().contains("update(");
                let is_after_delete = self.get_text().contains("delete(");

                let mut methods = vec![];

                if is_after_insert {
                    methods.push(self.create_method_completion(
                        "value",
                        "value({$1})",
                        "Set values for insert",
                    ));
                    methods.push(self.create_method_completion(
                        "values",
                        "values($1)",
                        "Set multiple values",
                    ));
                    methods.push(self.create_method_completion(
                        "onConflict",
                        "onConflict($1)",
                        "Handle unique constraint conflicts",
                    ));
                } else if is_after_update {
                    methods.push(self.create_method_completion(
                        "where",
                        "where($1)",
                        "Filter rows to update",
                    ));
                    methods.push(self.create_method_completion(
                        "set",
                        "set({$1})",
                        "Set field values for update",
                    ));
                } else if is_after_delete {
                    methods.push(self.create_method_completion(
                        "where",
                        "where($1)",
                        "Filter rows to delete",
                    ));
                }

                // 共通メソッド
                methods.push(self.create_method_completion(
                    "returning",
                    "returning($1)",
                    "Return specific fields",
                ));

                methods
            }
            CompletionContext::TableReference => {
                vec![
                    self.create_method_completion(
                        "select",
                        "select(fields)",
                        "Select specific fields",
                    ),
                    self.create_method_completion("where", "where(condition)", "Filter rows"),
                    self.create_method_completion("join", "join(table)", "Join with another table"),
                    self.create_method_completion(
                        "limit",
                        "limit(count)",
                        "Limit number of results",
                    ),
                    self.create_method_completion(
                        "offset",
                        "offset(count)",
                        "Skip number of results",
                    ),
                ]
            }
            CompletionContext::ExpressionMethod => {
                vec![
                    self.create_method_completion("isNull", "isNull()", "Check if value is NULL"),
                    self.create_method_completion("isNotNull", "isNotNull()", "Check if value is NOT NULL"),
                    self.create_method_completion("like", "like($1)", "Pattern matching (LIKE)"),
                    self.create_method_completion("ilike", "ilike($1)", "Case-insensitive LIKE"),
                    self.create_method_completion("between", "between($1, $2)", "Range check (BETWEEN)"),
                    self.create_method_completion("eqAny", "eqAny($1)", "Check if value is in array"),
                    self.create_method_completion("in", "in($1)", "Check if value is in list"),
                    self.create_method_completion("asc", "asc()", "Sort ascending"),
                    self.create_method_completion("desc", "desc()", "Sort descending"),
                ]
            }
            CompletionContext::OnConflictAction => {
                vec![
                    self.create_method_completion("doUpdate", "doUpdate({$1})", "Update on conflict"),
                    self.create_method_completion("doNothing", "doNothing()", "Do nothing on conflict"),
                ]
            }
            CompletionContext::Unknown => vec![],
            CompletionContext::TableField(_) => vec![], // フィールド補完は別メソッドで処理
            CompletionContext::TableJoinMethod(_) => {
                vec![
                    self.create_method_completion(
                        "innerJoin",
                        "innerJoin($1, $2)",
                        "Inner join with another table",
                    ),
                    self.create_method_completion(
                        "leftJoin",
                        "leftJoin($1, $2)",
                        "Left outer join with another table",
                    ),
                    self.create_method_completion(
                        "rightJoin",
                        "rightJoin($1, $2)",
                        "Right outer join with another table",
                    ),
                    self.create_method_completion(
                        "fullOuterJoin",
                        "fullOuterJoin($1, $2)",
                        "Full outer join with another table",
                    ),
                    self.create_method_completion(
                        "crossJoin",
                        "crossJoin($1)",
                        "Cross join with another table",
                    ),
                ]
            }
        }
    }

    fn create_method_completion(
        &self,
        label: &str,
        insert_text: &str,
        documentation: &str,
    ) -> CompletionItem {
        CompletionItem {
            label: label.to_string(),
            kind: Some(CompletionItemKind::METHOD),
            detail: Some(format!(".{}", insert_text)),
            documentation: Some(Documentation::String(documentation.to_string())),
            insert_text: Some(insert_text.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        }
    }
}