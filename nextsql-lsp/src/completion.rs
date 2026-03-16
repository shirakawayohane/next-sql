use crate::schema_cache::SchemaCache;
use std::sync::Arc;
use tower_lsp::lsp_types::*;

mod context;
mod field_completion;
mod method_completion;
mod model_completion;
mod table_completion;
mod type_completion;
mod utils;

use context::{
    CompletionContext, FunctionContext, analyze_context_before_dot, check_function_context,
    is_after_insertable_angle_bracket, is_after_variable_colon
};
use field_completion::FieldCompletionProvider;
use method_completion::MethodCompletionProvider;
use model_completion::ModelCompletionProvider;
use table_completion::TableCompletionProvider;
use type_completion::TypeCompletionProvider;
use utils::utf16_position_to_byte_index;

pub struct CompletionProvider<'a> {
    text: &'a str,
    schema_cache: Option<Arc<SchemaCache>>,
    file_uri: Option<String>,
}

impl<'a> CompletionProvider<'a> {
    #[allow(dead_code)]
    pub fn new(text: &'a str) -> Self {
        Self {
            text,
            schema_cache: None,
            file_uri: None,
        }
    }

    pub fn with_schema_cache(
        text: &'a str,
        schema_cache: Arc<SchemaCache>,
        file_uri: String,
    ) -> Self {
        Self {
            text,
            schema_cache: Some(schema_cache),
            file_uri: Some(file_uri),
        }
    }

    pub async fn get_completions(&self, position: Position) -> Vec<CompletionItem> {
        let mut completions = Vec::new();

        eprintln!("LSP: get_completions called at position: {:?}", position);

        // カーソル位置の前の文字を取得
        let lines: Vec<&str> = self.text.lines().collect();
        if position.line as usize >= lines.len() {
            eprintln!(
                "LSP: Position line {} is out of bounds (total lines: {})",
                position.line,
                lines.len()
            );
            return completions;
        }

        let current_line = lines[position.line as usize];

        // LSPはUTF-16コード単位で文字位置を計算するため、バイト位置に変換
        let byte_position = utf16_position_to_byte_index(current_line, position.character as usize);

        eprintln!("LSP: Current line {}: '{}'", position.line, current_line);
        eprintln!(
            "LSP: Character position (UTF-16): {}, byte position: {}",
            position.character, byte_position
        );

        if byte_position == 0 {
            return completions;
        }

        let before_cursor = &current_line[..byte_position.min(current_line.len())];
        let after_cursor = &current_line[byte_position.min(current_line.len())..];
        
        // Collect all text up to the current position to find aliases
        let text_before_position = if position.line > 0 {
            let mut full_text = String::new();
            for i in 0..position.line as usize {
                full_text.push_str(lines[i]);
                full_text.push('\n');
            }
            full_text.push_str(before_cursor);
            full_text
        } else {
            before_cursor.to_string()
        };
        
        eprintln!("LSP: Text before cursor: '{}'", before_cursor);
        eprintln!("LSP: Text after cursor: '{}'", after_cursor);

        // "Insertable<" の後でのモデル名補完
        if is_after_insertable_angle_bracket(before_cursor) {
            eprintln!("LSP: Detected Insertable< context");
            let has_closing_bracket = after_cursor.chars().next() == Some('>');
            completions.extend(self.get_model_completions_for_insertable(has_closing_bracket, position, current_line).await);
        }
        // "$variable: " の後での型補完
        else if is_after_variable_colon(before_cursor) {
            eprintln!("LSP: Detected $variable: context");
            completions.extend(self.get_type_completions().await);
        }
        // "." が直前にある場合のメソッド補完
        else if before_cursor.ends_with('.') {
            let context = analyze_context_before_dot(self.text, before_cursor);
            match &context {
                CompletionContext::TableField(table_name) => {
                    // For field completions, table_name has already been resolved from alias
                    // We trust the context analyzer to only provide valid table references
                    completions.extend(self.get_field_completions(table_name).await);
                }
                CompletionContext::TableJoinMethod(_) => {
                    completions.extend(self.get_method_completions(context));
                }
                _ => {
                    completions.extend(self.get_method_completions(context));
                }
            }
        }
        // "insert(" などの関数呼び出しでのテーブル名補完
        else if let Some(function_context) = check_function_context(before_cursor) {
            match function_context {
                FunctionContext::Insert | FunctionContext::Update | FunctionContext::Delete => {
                    completions.extend(self.get_table_completions().await);
                }
                FunctionContext::From => {
                    completions.extend(self.get_table_completions().await);
                }
                FunctionContext::JoinMethod => {
                    completions.extend(self.get_table_completions().await);
                }
                FunctionContext::JoinCondition => {
                    // Add all table names directly (not from FROM clause)
                    // since we're in a join condition context where all tables should be available
                    completions.extend(self.get_table_completions().await);
                }
            }
        }

        completions
    }
}

// Delegate to specific completion providers
impl<'a> FieldCompletionProvider for CompletionProvider<'a> {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>> {
        self.schema_cache.as_ref()
    }
    
    fn get_text(&self) -> &str {
        self.text
    }
    
    fn get_file_uri(&self) -> Option<&str> {
        self.file_uri.as_deref()
    }
}

impl<'a> MethodCompletionProvider for CompletionProvider<'a> {
    fn get_text(&self) -> &str {
        self.text
    }
}

impl<'a> TableCompletionProvider for CompletionProvider<'a> {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>> {
        self.schema_cache.as_ref()
    }
    
    fn get_file_uri(&self) -> Option<&str> {
        self.file_uri.as_deref()
    }
}

impl<'a> TypeCompletionProvider for CompletionProvider<'a> {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>> {
        self.schema_cache.as_ref()
    }
    
    fn get_file_uri(&self) -> Option<&str> {
        self.file_uri.as_deref()
    }
}

impl<'a> ModelCompletionProvider for CompletionProvider<'a> {
    fn get_schema_cache(&self) -> Option<&Arc<SchemaCache>> {
        self.schema_cache.as_ref()
    }
    
    fn get_file_uri(&self) -> Option<&str> {
        self.file_uri.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use context::{extract_from_tables, extract_table_before_dot, is_table_reference_context};

    #[test]
    fn test_query_method_completion() {
        let provider = CompletionProvider::new("query test() { from(users).");
        let context = analyze_context_before_dot(provider.text, "query test() { from(users).");
        assert_eq!(context, CompletionContext::TableReference);
    }

    #[test]
    fn test_mutation_method_completion() {
        let provider = CompletionProvider::new("mutation test() { insert(users).");
        let context = analyze_context_before_dot(provider.text, "mutation test() { insert(users).");
        assert_eq!(context, CompletionContext::MutationMethod);
    }

    #[test]
    fn test_function_context_detection() {
        // insert( の後
        assert_eq!(
            check_function_context("mutation test() { insert("),
            Some(FunctionContext::Insert)
        );

        // update( の後
        assert_eq!(
            check_function_context("mutation test() { update("),
            Some(FunctionContext::Update)
        );

        // leftJoin( の後
        assert_eq!(
            check_function_context("from(users.leftJoin("),
            Some(FunctionContext::JoinMethod)
        );

        // innerJoin( の後
        assert_eq!(
            check_function_context("from(users.innerJoin("),
            Some(FunctionContext::JoinMethod)
        );

        // leftJoin( の第2引数（join条件）
        assert_eq!(
            check_function_context("from(users.leftJoin(posts, "),
            Some(FunctionContext::JoinCondition)
        );

        // innerJoin( の第2引数（join条件）
        assert_eq!(
            check_function_context("from(users.innerJoin(posts, u.id == "),
            Some(FunctionContext::JoinCondition)
        );

        // 括弧が閉じられている場合
        assert_eq!(
            check_function_context("mutation test() { insert(users) "),
            None
        );
    }

    #[test]
    fn test_method_chaining_context() {
        let provider = CompletionProvider::new("");

        // メソッドチェーンが既に始まっている場合
        let context = analyze_context_before_dot(provider.text, "query test() { from(users).select(*).");
        assert_eq!(context, CompletionContext::QueryMethod);

        // mutation内でのメソッドチェーン
        let context =
            analyze_context_before_dot(provider.text, "mutation test() { insert(users).value({}).");
        assert_eq!(context, CompletionContext::MutationMethod);
    }

    #[test]
    fn test_specific_method_detection() {
        let provider = CompletionProvider::new("");

        // insert直後
        let context = analyze_context_before_dot(provider.text, "mutation test() { insert(users).");
        assert_eq!(context, CompletionContext::MutationMethod);

        // update直後
        let context = analyze_context_before_dot(provider.text, "mutation test() { update(users).");
        assert_eq!(context, CompletionContext::MutationMethod);

        // delete直後
        let context = analyze_context_before_dot(provider.text, "mutation test() { delete(users).");
        assert_eq!(context, CompletionContext::MutationMethod);
    }

    #[test]
    fn test_table_field_detection() {
        let text = "query test() { from(users) .where(users.";
        let _provider = CompletionProvider::new(text);

        // テーブル名の後のドット
        let context =
            analyze_context_before_dot(text, "query test() { from(users) .where(users.");
        assert_eq!(context, CompletionContext::TableField("users".to_string()));
    }

    #[test]
    fn test_table_join_method_detection() {
        // Test inside from()
        let text = "query test() { from(users.";
        let context = analyze_context_before_dot(text, text);
        assert_eq!(context, CompletionContext::TableJoinMethod("users".to_string()));
        
        // Test after alias =
        let text = "query test() { alias joined = users.";
        let context = analyze_context_before_dot(text, text);
        assert_eq!(context, CompletionContext::TableJoinMethod("users".to_string()));
        
        // Test with alias in from()
        let text = "query test() { alias u = users\n from(u.";
        let context = analyze_context_before_dot(text, text);
        assert_eq!(context, CompletionContext::TableJoinMethod("users".to_string()));
        
        // Test inside join condition - should NOT be join method context
        let full_text = "query test() { alias u = users\n from(users.leftJoin(posts, u.";
        let text_before_dot = "query test() { alias u = users\n from(users.leftJoin(posts, u.";
        let context = analyze_context_before_dot(full_text, text_before_dot);
        assert_ne!(context, CompletionContext::TableJoinMethod("users".to_string()));
        assert_eq!(context, CompletionContext::TableField("users".to_string())); // Should be field context
    }

    #[tokio::test]
    async fn test_join_method_completions() {
        let provider = CompletionProvider::new("query test() { from(users.");
        
        // Test join method completions in from()
        let position = Position {
            line: 0,
            character: 26, // After "from(users."
        };
        
        let completions = provider.get_completions(position).await;
        
        // Should have join method completions
        assert!(!completions.is_empty());
        
        // Check for leftJoin method
        let left_join = completions.iter().find(|c| c.label == "leftJoin");
        assert!(left_join.is_some());
        
        let lj = left_join.unwrap();
        assert_eq!(lj.kind, Some(CompletionItemKind::METHOD));
        assert_eq!(lj.insert_text, Some("leftJoin($1, $2)".to_string()));
        
        // Check for other join methods
        assert!(completions.iter().any(|c| c.label == "innerJoin"));
        assert!(completions.iter().any(|c| c.label == "rightJoin"));
        assert!(completions.iter().any(|c| c.label == "fullOuterJoin"));
        assert!(completions.iter().any(|c| c.label == "crossJoin"));
    }



    #[test]
    fn test_extract_table_before_dot() {
        // 単純なテーブル名
        assert_eq!(
            extract_table_before_dot("", "users"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_before_dot("", "where(users"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_before_dot("", ".select(posts"),
            Some("posts".to_string())
        );

        // 複雑なケース
        assert_eq!(
            extract_table_before_dot("", "from(users).where(orders"),
            Some("orders".to_string())
        );
        assert_eq!(extract_table_before_dot("", ""), None);
        assert_eq!(extract_table_before_dot("", "123"), None);
    }




    #[tokio::test]
    async fn test_field_completion_with_schema() {
        use crate::schema_cache::SchemaCache;
        use nextsql_core::{BuiltInType, ColumnSchema, DatabaseSchema, TableSchema, Type};

        // テスト用のプロジェクトディレクトリを作成
        let temp_dir = std::env::temp_dir().join("nextsql_test_field_completion");
        std::fs::create_dir_all(&temp_dir).unwrap();

        // next-sql.tomlファイルを作成（find_project_rootが見つけられるように）
        let config_path = temp_dir.join("next-sql.toml");
        std::fs::write(&config_path, "[database]\nurl = \"\"\n").unwrap();

        // スキーマキャッシュをモック
        let schema_cache = SchemaCache::new();

        // usersテーブルのスキーマを作成
        let mut users_table = TableSchema::new("users".to_string());
        users_table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        users_table.add_column(ColumnSchema {
            name: "email".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });
        users_table.add_column(ColumnSchema {
            name: "name".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: true,
            primary_key: false,
            has_default: false,
            default_value: None,
        });

        // データベーススキーマを作成
        let mut db_schema = DatabaseSchema::new();
        db_schema.add_table(users_table);

        // スキーマキャッシュに追加
        schema_cache
            .set_test_schema(temp_dir.clone(), db_schema)
            .await;

        // テスト用のファイルパス（temp_dir内のファイル）
        let file_path = temp_dir.join("query.nsql");
        let file_uri = format!("file://{}", file_path.display());

        // CompletionProviderを作成
        let text = "query findUserById($id: uuid) {\n  from(users)\n  .where(users.";
        let provider =
            CompletionProvider::with_schema_cache(text, Arc::new(schema_cache), file_uri);

        // カーソル位置を設定（users.の後）
        // Line 2は "  .where(users." なので、その長さは15
        let position = Position {
            line: 2,
            character: 15, // "  .where(users."の長さ
        };

        // 補完を取得
        let completions = provider.get_completions(position).await;

        // テスト用ファイルをクリーンアップ
        let _ = std::fs::remove_dir_all(&temp_dir);

        // id列が補完候補に含まれていることを確認
        assert!(!completions.is_empty(), "Completions should not be empty");

        let id_completion = completions.iter().find(|c| c.label == "id");
        assert!(
            id_completion.is_some(),
            "id column should be in completions"
        );

        let id = id_completion.unwrap();
        assert_eq!(id.kind, Some(CompletionItemKind::FIELD));
        assert!(id.detail.as_ref().unwrap().contains("uuid"));
        assert!(id.detail.as_ref().unwrap().contains("primary key"));

        // 他のフィールドも確認
        let email_completion = completions.iter().find(|c| c.label == "email");
        assert!(
            email_completion.is_some(),
            "email column should be in completions"
        );

        let name_completion = completions.iter().find(|c| c.label == "name");
        assert!(
            name_completion.is_some(),
            "name column should be in completions"
        );
        assert!(name_completion
            .unwrap()
            .detail
            .as_ref()
            .unwrap()
            .contains("nullable"));
    }

    #[test]
    fn test_insertable_angle_bracket_detection() {
        // Test Insertable< pattern detection
        assert!(is_after_insertable_angle_bracket("mutation insertUser($new: Insertable<"));
        assert!(is_after_insertable_angle_bracket("$new: Insertable<"));
        assert!(is_after_insertable_angle_bracket("Insertable<U"));
        assert!(is_after_insertable_angle_bracket("foo: Insertable< "));
        
        // Test non-matching patterns
        assert!(!is_after_insertable_angle_bracket("Insertable<User>"));
        assert!(!is_after_insertable_angle_bracket("Insertable"));
        assert!(!is_after_insertable_angle_bracket("Insert<"));
        assert!(!is_after_insertable_angle_bracket("Insertable<User>.field"));
    }

    #[tokio::test]
    async fn test_insertable_completion_with_existing_bracket() {
        // Test scenario where user types: Insertable<|>
        // and the cursor is between < and >
        let text = "mutation insertUser($new: Insertable<>) {\n  insert(users)\n  .values($new)\n}";
        let provider = CompletionProvider::with_schema_cache(text, Arc::new(crate::schema_cache::SchemaCache::new()), "file:///test.nsql".to_string());
        
        // Position is after < but before >
        let position = Position {
            line: 0,
            character: 37, // Position after "Insertable<"
        };
        
        let completions = provider.get_completions(position).await;
        
        // Should have model completions
        assert!(!completions.is_empty());
        
        // Check that Users model doesn't include >
        let users_model = completions.iter().find(|c| c.label == "Users");
        assert!(users_model.is_some());
        
        let users = users_model.unwrap();
        
        // Check that it uses snippet format with cursor positioning
        assert_eq!(users.insert_text_format, Some(InsertTextFormat::SNIPPET));
        
        // Check text edit includes the closing bracket and positions cursor after it
        if let Some(CompletionTextEdit::Edit(text_edit)) = &users.text_edit {
            assert_eq!(text_edit.new_text, "Users>$0", "Should include > and position cursor after it");
            assert_eq!(text_edit.range.end.character, 38, "Should replace up to and including the closing bracket");
        } else {
            panic!("Expected text edit");
        }
    }

    #[test]
    fn test_variable_colon_detection() {
        // Test $variable: pattern detection
        assert!(is_after_variable_colon("mutation test($name: "));
        assert!(is_after_variable_colon("$userId: "));
        assert!(is_after_variable_colon("query foo($id: u"));
        assert!(is_after_variable_colon("$my_var: Ins"));
        
        // Test non-matching patterns
        assert!(!is_after_variable_colon("$name: string,"));
        assert!(!is_after_variable_colon("$name: uuid)"));
        assert!(!is_after_variable_colon("name: "));
        assert!(!is_after_variable_colon(": string"));
    }

    #[tokio::test]
    async fn test_model_completions_for_insertable() {
        let provider = CompletionProvider::new("");
        let position = Position { line: 0, character: 37 };
        let current_line = "mutation insertUser($new: Insertable<";
        
        // Test without closing bracket
        let completions = provider.get_model_completions_for_insertable(false, position, current_line).await;
        
        // Should have fallback model names
        assert!(!completions.is_empty());
        
        // Check for Users model
        let users_model = completions.iter().find(|c| c.label == "Users");
        assert!(users_model.is_some());
        
        let users = users_model.unwrap();
        assert_eq!(users.kind, Some(CompletionItemKind::CLASS));
        
        // Check text edit
        if let Some(CompletionTextEdit::Edit(text_edit)) = &users.text_edit {
            assert_eq!(text_edit.new_text, "Users>");
        } else {
            panic!("Expected text edit");
        }
        
        // Test with closing bracket
        let current_line_with_bracket = "mutation insertUser($new: Insertable<>) {";
        let completions_with_bracket = provider.get_model_completions_for_insertable(true, position, current_line_with_bracket).await;
        
        let users_model_with_bracket = completions_with_bracket.iter().find(|c| c.label == "Users");
        assert!(users_model_with_bracket.is_some());
        
        let users_with_bracket = users_model_with_bracket.unwrap();
        assert_eq!(users_with_bracket.insert_text_format, Some(InsertTextFormat::SNIPPET));
        
        if let Some(CompletionTextEdit::Edit(text_edit)) = &users_with_bracket.text_edit {
            assert_eq!(text_edit.new_text, "Users>$0");
        } else {
            panic!("Expected text edit");
        }
    }

    #[tokio::test]
    async fn test_type_completions() {
        let provider = CompletionProvider::new("");
        let completions = provider.get_type_completions().await;
        
        // Should have Insertable and built-in types
        assert!(!completions.is_empty());
        
        // Check for Insertable type with snippet
        let insertable = completions.iter().find(|c| c.label == "Insertable");
        assert!(insertable.is_some());
        
        let ins = insertable.unwrap();
        assert_eq!(ins.kind, Some(CompletionItemKind::INTERFACE));
        assert_eq!(ins.insert_text, Some("Insertable<$1>$0".to_string()));
        assert_eq!(ins.insert_text_format, Some(InsertTextFormat::SNIPPET));
        
        // Check for built-in types
        let string_type = completions.iter().find(|c| c.label == "string");
        assert!(string_type.is_some());
        
        let uuid_type = completions.iter().find(|c| c.label == "uuid");
        assert!(uuid_type.is_some());
    }

    
    #[test]
    fn test_extract_from_tables() {
        // Simple from clause
        let text = "query test() { from(users) }";
        let tables = extract_from_tables(text);
        assert_eq!(tables, vec!["users"]);
        
        
        // From with join
        let text = "query test() { from(users.leftJoin(posts, u.id == p.user_id)) }";
        let tables = extract_from_tables(text);
        assert_eq!(tables, vec!["users"]);
    }
    
    #[test]
    fn test_is_table_reference_context() {
        // In select
        assert!(is_table_reference_context("query test() { from(users).select(", ""));
        
        // In where
        assert!(is_table_reference_context("query test() { from(users).where(", ""));
        
        // In join
        assert!(is_table_reference_context("query test() { from(users).leftJoin(posts, ", ""));
        
        // Not in context
        assert!(!is_table_reference_context("query test() { ", ""));
        
        // Closed context
        assert!(!is_table_reference_context("query test() { from(users).select(u.name) }", ""));
    }
    
    #[tokio::test]
    async fn test_table_completions_in_from_without_schema() {
        let text = "query test() {\n  from(";
        let provider = CompletionProvider::new(text);

        let position = Position {
            line: 1,
            character: 7, // After "from("
        };

        let completions = provider.get_completions(position).await;

        // Without schema, no table completions should be returned
        assert!(completions.is_empty());
    }

    #[tokio::test]
    async fn test_table_completions_in_join_method_without_schema() {
        let text = "query test() {\n  from(users.leftJoin(";
        let provider = CompletionProvider::new(text);

        let position = Position {
            line: 1,
            character: 23, // After "from(users.leftJoin("
        };

        let completions = provider.get_completions(position).await;

        // Without schema, no table completions should be returned
        assert!(completions.is_empty());
    }
    
    
    #[tokio::test]
    async fn test_field_completions_in_join_condition() {
        use crate::schema_cache::SchemaCache;
        use nextsql_core::{BuiltInType, ColumnSchema, DatabaseSchema, TableSchema, Type};

        // テスト用のプロジェクトディレクトリを作成
        let temp_dir = std::env::temp_dir().join("nextsql_test_join_field_completion");
        std::fs::create_dir_all(&temp_dir).unwrap();

        // next-sql.tomlファイルを作成
        let config_path = temp_dir.join("next-sql.toml");
        std::fs::write(&config_path, "[database]\nurl = \"\"\n").unwrap();

        // スキーマキャッシュをモック
        let schema_cache = SchemaCache::new();

        // usersテーブルのスキーマを作成
        let mut users_table = TableSchema::new("users".to_string());
        users_table.add_column(ColumnSchema {
            name: "id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: true,
            has_default: true,
            default_value: Some("gen_random_uuid()".to_string()),
        });
        
        // postsテーブルのスキーマを作成
        let mut posts_table = TableSchema::new("posts".to_string());
        posts_table.add_column(ColumnSchema {
            name: "user_id".to_string(),
            column_type: Type::BuiltIn(BuiltInType::Uuid),
            nullable: false,
            primary_key: false,
            has_default: false,
            default_value: None,
        });

        // データベーススキーマを作成
        let mut db_schema = DatabaseSchema::new();
        db_schema.add_table(users_table);
        db_schema.add_table(posts_table);

        // スキーマキャッシュに追加
        schema_cache
            .set_test_schema(temp_dir.clone(), db_schema)
            .await;

        // テスト用のファイルパス
        let file_path = temp_dir.join("query.nsql");
        let file_uri = format!("file://{}", file_path.display());

        // Arc<SchemaCache>を作成
        let schema_cache_arc = Arc::new(schema_cache);

        // CompletionProviderを作成
        let text = "query test() {\n  alias u = users\n  alias p = posts\n  from(users.leftJoin(posts, u.";
        let provider =
            CompletionProvider::with_schema_cache(text, Arc::clone(&schema_cache_arc), file_uri);

        // カーソル位置を設定（u.の後）
        let position = Position {
            line: 3,
            character: 32, // After "from(users.leftJoin(posts, u."
        };

        // 補完を取得
        let completions = provider.get_completions(position).await;

        // テスト用ファイルをクリーンアップ
        let _ = std::fs::remove_dir_all(&temp_dir);

        // フィールド補完が動作することを確認
        assert!(!completions.is_empty(), "Should have field completions for alias u in join condition");
        assert!(completions.iter().any(|c| c.label == "id" && c.kind == Some(CompletionItemKind::FIELD)));
        
        // JOINメソッドの補完が出ないことを確認
        assert!(!completions.iter().any(|c| c.label == "leftJoin" || c.label == "innerJoin"));
    }
    
    

}