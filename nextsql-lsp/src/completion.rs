use crate::schema_cache::SchemaCache;
use std::sync::Arc;
use tower_lsp::lsp_types::*;

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
        let byte_position =
            self.utf16_position_to_byte_index(current_line, position.character as usize);

        eprintln!("LSP: Current line {}: '{}'", position.line, current_line);
        eprintln!(
            "LSP: Character position (UTF-16): {}, byte position: {}",
            position.character, byte_position
        );

        if byte_position == 0 {
            return completions;
        }

        let before_cursor = &current_line[..byte_position.min(current_line.len())];
        eprintln!("LSP: Text before cursor: '{}'", before_cursor);

        // "." が直前にある場合のメソッド補完
        if before_cursor.ends_with('.') {
            let context = self.analyze_context_before_dot(before_cursor);
            match &context {
                CompletionContext::TableField(table_name) => {
                    completions.extend(self.get_field_completions(table_name).await);
                }
                _ => {
                    completions.extend(self.get_method_completions(context));
                }
            }
        }
        // "insert(" などの関数呼び出しでのテーブル名補完
        else if let Some(function_context) = self.check_function_context(before_cursor) {
            match function_context {
                FunctionContext::Insert | FunctionContext::Update | FunctionContext::Delete => {
                    completions.extend(self.get_table_completions().await);
                }
                FunctionContext::From => {
                    completions.extend(self.get_table_completions().await);
                }
            }
        }
        // "alias <name> =" の後でのテーブル名補完
        else if self.is_after_alias_equals(before_cursor) {
            eprintln!("LSP: Detected alias assignment context");
            // Check if we need to add a space after =
            let needs_space = before_cursor.trim_end().ends_with('=');
            let table_completions = self.get_table_completions_for_alias(needs_space).await;
            eprintln!("LSP: Found {} table completions for alias (needs_space: {})", table_completions.len(), needs_space);
            completions.extend(table_completions);
        }

        completions
    }

    fn extract_table_before_dot(&self, text: &str) -> Option<String> {
        eprintln!("LSP: extract_table_before_dot - text: '{}'", text);

        // ドットの直前の識別子を抽出
        let parts: Vec<&str> = text.split_whitespace().collect();
        eprintln!("LSP: Split parts: {:?}", parts);

        if let Some(last_part) = parts.last() {
            eprintln!("LSP: Last part: '{}'", last_part);

            // 識別子の文字のみを取得（句読点などを除外）
            let identifier: String = last_part
                .chars()
                .rev()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect::<String>()
                .chars()
                .rev()
                .collect();

            eprintln!("LSP: Extracted identifier: '{}'", identifier);

            // 識別子は文字で始まる必要がある（数字のみは除外）
            if !identifier.is_empty() && identifier.chars().next().unwrap().is_alphabetic() {
                // エイリアスを解決
                if let Some(table_name) = self.resolve_table_alias(&identifier) {
                    eprintln!(
                        "LSP: Resolved alias '{}' to table '{}'",
                        identifier, table_name
                    );
                    return Some(table_name);
                }
                // エイリアスでなければ、そのままテーブル名として扱う
                eprintln!("LSP: Using identifier as table name: '{}'", identifier);
                return Some(identifier);
            }
        }
        eprintln!("LSP: No table identifier found");
        None
    }

    fn resolve_table_alias(&self, alias: &str) -> Option<String> {
        eprintln!("LSP: resolve_table_alias - looking for alias: '{}'", alias);
        eprintln!("LSP: Full text to search:\n{}", self.text);

        // 新しい文法: alias エイリアス = テーブル名
        let alias_pattern = format!(r"alias\s+{}\s*=\s*(\w+)", regex::escape(alias));
        eprintln!("LSP: Regex pattern for new syntax: '{}'", alias_pattern);

        if let Ok(re) = regex::Regex::new(&alias_pattern) {
            if let Some(captures) = re.captures(self.text) {
                if let Some(table_name) = captures.get(1) {
                    eprintln!(
                        "LSP: Found table name '{}' for alias '{}' (new syntax)",
                        table_name.as_str(),
                        alias
                    );
                    return Some(table_name.as_str().to_string());
                }
            }
        }

        // 旧文法もサポート（後方互換性のため）: テーブル名<エイリアス>
        let old_alias_pattern = format!(r"(\w+)<{}>", regex::escape(alias));
        eprintln!("LSP: Regex pattern for old syntax: '{}'", old_alias_pattern);

        if let Ok(re) = regex::Regex::new(&old_alias_pattern) {
            if let Some(captures) = re.captures(self.text) {
                if let Some(table_name) = captures.get(1) {
                    eprintln!(
                        "LSP: Found table name '{}' for alias '{}' (old syntax)",
                        table_name.as_str(),
                        alias
                    );
                    return Some(table_name.as_str().to_string());
                }
            }
        }

        eprintln!("LSP: No table found for alias '{}'", alias);
        None
    }

    async fn get_field_completions(&self, table_name: &str) -> Vec<CompletionItem> {
        let mut completions = Vec::new();
        eprintln!("LSP: get_field_completions for table: '{}'", table_name);

        if let (Some(schema_cache), Some(_file_uri)) = (&self.schema_cache, &self.file_uri) {
            let path = std::path::Path::new(
                self.file_uri
                    .as_ref()
                    .unwrap()
                    .trim_start_matches("file://"),
            );
            eprintln!("LSP: File path for schema lookup: {:?}", path);

            if let Some(schema) = schema_cache.get_schema_for_file(path).await {
                eprintln!("LSP: Schema found, looking for table '{}'", table_name);
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
                    eprintln!("LSP: Table '{}' not found in schema", table_name);
                }
            } else {
                eprintln!("LSP: No schema found for file");
            }
        } else {
            eprintln!("LSP: No schema cache or file URI available");
        }

        eprintln!("LSP: Returning {} field completions", completions.len());
        completions
    }

    fn utf16_position_to_byte_index(&self, text: &str, utf16_pos: usize) -> usize {
        let mut utf16_count = 0;
        let mut byte_index = 0;

        for ch in text.chars() {
            if utf16_count >= utf16_pos {
                break;
            }

            // UTF-16でのコード単位数を計算
            let utf16_len = if ch.len_utf16() == 1 { 1 } else { 2 };
            utf16_count += utf16_len;
            byte_index += ch.len_utf8();
        }

        byte_index
    }

    fn analyze_context_before_dot(&self, text_before_dot: &str) -> CompletionContext {
        let trimmed = text_before_dot.trim_end_matches('.');
        eprintln!("LSP: analyze_context_before_dot - trimmed: '{}'", trimmed);

        // まず、テーブル名.かどうかをチェック
        if let Some(table_name) = self.extract_table_before_dot(trimmed) {
            eprintln!(
                "LSP: Detected table field context for table: {}",
                table_name
            );
            return CompletionContext::TableField(table_name);
        }

        // 直前の閉じ括弧を探して、その前の関数名を確認
        if let Some(close_paren_pos) = trimmed.rfind(')') {
            // 対応する開き括弧を探す
            let mut paren_count = 1;
            let mut open_paren_pos = None;

            for (i, ch) in trimmed[..close_paren_pos].chars().rev().enumerate() {
                match ch {
                    ')' => paren_count += 1,
                    '(' => {
                        paren_count -= 1;
                        if paren_count == 0 {
                            open_paren_pos = Some(close_paren_pos - i - 1);
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if let Some(open_pos) = open_paren_pos {
                // 括弧の前の関数名を取得
                let before_paren = &trimmed[..open_pos];
                let function_name = before_paren
                    .split_whitespace()
                    .last()
                    .unwrap_or("")
                    .split(['(', ')', ',', '{', '}', '.'])
                    .last()
                    .unwrap_or("");

                // 関数名に基づいてコンテキストを判定
                match function_name {
                    "insert" | "update" | "delete" => return CompletionContext::MutationMethod,
                    "from" => return CompletionContext::TableReference,
                    _ => {}
                }
            }
        }

        // mutation宣言内かどうかチェック
        if trimmed.contains("mutation ") {
            return CompletionContext::MutationMethod;
        }

        // query宣言内かどうかチェック
        if trimmed.contains("query ") {
            return CompletionContext::QueryMethod;
        }

        CompletionContext::Unknown
    }

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
                ]
            }
            CompletionContext::MutationMethod => {
                // より正確なコンテキスト判定のため、直前のテキストを確認
                let is_after_insert = self.text.contains("insert(");
                let is_after_update = self.text.contains("update(");
                let is_after_delete = self.text.contains("delete(");

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
            CompletionContext::Unknown => vec![],
            CompletionContext::TableField(_) => vec![], // フィールド補完は別メソッドで処理
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

    fn check_function_context(&self, before_cursor: &str) -> Option<FunctionContext> {
        // 最後の開いた括弧を探す
        if let Some(open_paren_pos) = before_cursor.rfind('(') {
            // 括弧が閉じられていないかチェック
            let after_paren = &before_cursor[open_paren_pos + 1..];
            if after_paren.contains(')') {
                return None;
            }

            // 括弧の前の単語を取得
            let before_paren = &before_cursor[..open_paren_pos];
            let function_name = before_paren
                .split_whitespace()
                .last()
                .unwrap_or("")
                .split(['(', ')', ',', '{', '}', '.'])
                .last()
                .unwrap_or("");

            match function_name {
                "insert" => Some(FunctionContext::Insert),
                "update" => Some(FunctionContext::Update),
                "delete" => Some(FunctionContext::Delete),
                "from" => Some(FunctionContext::From),
                _ => None,
            }
        } else {
            None
        }
    }

    fn is_after_alias_equals(&self, before_cursor: &str) -> bool {
        eprintln!("LSP: Checking for alias pattern in: '{}'", before_cursor);
        
        // Check if we're in the pattern "alias <name> = " or "alias <name> = <partial>"
        // First, check if the line contains "alias" and "="
        if !before_cursor.contains("alias") || !before_cursor.contains("=") {
            return false;
        }
        
        // Find the last occurrence of "=" to handle cases where user is typing after it
        if let Some(equals_pos) = before_cursor.rfind('=') {
            let before_equals = &before_cursor[..equals_pos];
            let after_equals = &before_cursor[equals_pos + 1..];
            
            // Check if we're still in the table name part (no space or complex expression after =)
            // Allow partial table names but not complete statements
            let after_trimmed = after_equals.trim();
            if after_trimmed.contains('.') || after_trimmed.contains('(') || after_trimmed.contains(')') {
                return false;
            }
            
            // Check if the pattern before = matches "alias <identifier>"
            let parts: Vec<&str> = before_equals.trim().split_whitespace().collect();
            if parts.len() >= 2 && parts[parts.len() - 2] == "alias" {
                eprintln!("LSP: Found alias pattern: alias {} = {}", parts[parts.len() - 1], after_equals.trim());
                return true;
            }
        }
        
        false
    }

    async fn get_table_completions(&self) -> Vec<CompletionItem> {
        self.get_table_completions_for_alias(false).await
    }

    async fn get_table_completions_for_alias(&self, needs_space: bool) -> Vec<CompletionItem> {
        let mut completions = Vec::new();

        if let (Some(schema_cache), Some(file_uri)) = (&self.schema_cache, &self.file_uri) {
            let path = std::path::Path::new(file_uri.trim_start_matches("file://"));

            if let Some(schema) = schema_cache.get_schema_for_file(path).await {
                for table_name in schema.tables.keys() {
                    let insert_text = if needs_space {
                        format!(" {}", table_name)
                    } else {
                        table_name.clone()
                    };

                    completions.push(CompletionItem {
                        label: table_name.clone(),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("Table: {}", table_name)),
                        documentation: Some(Documentation::String(format!(
                            "Table {} from database schema",
                            table_name
                        ))),
                        insert_text: Some(insert_text),
                        ..Default::default()
                    });
                }
            }
        }

        // フォールバック: スキーマが利用できない場合の一般的なテーブル名
        if completions.is_empty() {
            let common_tables = vec!["users", "posts", "comments", "orders", "products"];
            for table in common_tables {
                let insert_text = if needs_space {
                    format!(" {}", table)
                } else {
                    table.to_string()
                };

                completions.push(CompletionItem {
                    label: table.to_string(),
                    kind: Some(CompletionItemKind::CLASS),
                    detail: Some(format!("Table: {} (suggestion)", table)),
                    documentation: Some(Documentation::String(
                        "Common table name suggestion".to_string(),
                    )),
                    insert_text: Some(insert_text),
                    ..Default::default()
                });
            }
        }

        completions
    }
}

#[derive(Debug, PartialEq)]
enum CompletionContext {
    QueryMethod,
    MutationMethod,
    TableReference,
    TableField(String), // テーブル名を保持
    Unknown,
}

#[derive(Debug, PartialEq)]
enum FunctionContext {
    Insert,
    Update,
    Delete,
    From,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_method_completion() {
        let provider = CompletionProvider::new("query test() { from(users).");
        let context = provider.analyze_context_before_dot("query test() { from(users).");
        assert_eq!(context, CompletionContext::TableReference);
    }

    #[test]
    fn test_mutation_method_completion() {
        let provider = CompletionProvider::new("mutation test() { insert(users).");
        let context = provider.analyze_context_before_dot("mutation test() { insert(users).");
        assert_eq!(context, CompletionContext::MutationMethod);
    }

    #[test]
    fn test_function_context_detection() {
        let provider = CompletionProvider::new("");

        // insert( の後
        assert_eq!(
            provider.check_function_context("mutation test() { insert("),
            Some(FunctionContext::Insert)
        );

        // update( の後
        assert_eq!(
            provider.check_function_context("mutation test() { update("),
            Some(FunctionContext::Update)
        );

        // 括弧が閉じられている場合
        assert_eq!(
            provider.check_function_context("mutation test() { insert(users) "),
            None
        );
    }

    #[test]
    fn test_method_chaining_context() {
        let provider = CompletionProvider::new("");

        // メソッドチェーンが既に始まっている場合
        let context = provider.analyze_context_before_dot("query test() { from(users).select(*).");
        assert_eq!(context, CompletionContext::QueryMethod);

        // mutation内でのメソッドチェーン
        let context =
            provider.analyze_context_before_dot("mutation test() { insert(users).value({}).");
        assert_eq!(context, CompletionContext::MutationMethod);
    }

    #[test]
    fn test_specific_method_detection() {
        let provider = CompletionProvider::new("");

        // insert直後
        let context = provider.analyze_context_before_dot("mutation test() { insert(users).");
        assert_eq!(context, CompletionContext::MutationMethod);

        // update直後
        let context = provider.analyze_context_before_dot("mutation test() { update(users).");
        assert_eq!(context, CompletionContext::MutationMethod);

        // delete直後
        let context = provider.analyze_context_before_dot("mutation test() { delete(users).");
        assert_eq!(context, CompletionContext::MutationMethod);
    }

    #[test]
    fn test_table_field_detection() {
        let provider = CompletionProvider::new("query test() { from(users) .where(users.");

        // テーブル名の後のドット
        let context =
            provider.analyze_context_before_dot("query test() { from(users) .where(users.");
        assert_eq!(context, CompletionContext::TableField("users".to_string()));
    }

    #[test]
    fn test_extract_table_before_dot() {
        let provider = CompletionProvider::new("");

        // 単純なテーブル名
        assert_eq!(
            provider.extract_table_before_dot("users"),
            Some("users".to_string())
        );
        assert_eq!(
            provider.extract_table_before_dot("where(users"),
            Some("users".to_string())
        );
        assert_eq!(
            provider.extract_table_before_dot(".select(posts"),
            Some("posts".to_string())
        );

        // 複雑なケース
        assert_eq!(
            provider.extract_table_before_dot("from(users).where(orders"),
            Some("orders".to_string())
        );
        assert_eq!(provider.extract_table_before_dot(""), None);
        assert_eq!(provider.extract_table_before_dot("123"), None);
    }

    #[test]
    fn test_table_alias_resolution() {
        // 新しい文法: alias文を使用
        let provider = CompletionProvider::new("query test() { alias u = users\n from(users) .where(u.");

        // エイリアスの解決
        assert_eq!(provider.resolve_table_alias("u"), Some("users".to_string()));

        // エイリアスを持つテーブルフィールドの検出
        let context =
            provider.analyze_context_before_dot("query test() { alias u = users\n from(users) .where(u.");
        assert_eq!(context, CompletionContext::TableField("users".to_string()));

        // 旧文法も動作することを確認（後方互換性）
        let provider_old = CompletionProvider::new("query test() { from(users<u>) .where(u.");
        assert_eq!(provider_old.resolve_table_alias("u"), Some("users".to_string()));
    }

    #[test]
    fn test_multiple_table_aliases() {
        // 新しい文法: 複数のalias文
        let provider =
            CompletionProvider::new("query test() {\n  alias u = users\n  alias p = posts\n  from(users).innerJoin(posts, u.id == p.user_id).where(p.");

        // 複数のエイリアスがある場合
        assert_eq!(provider.resolve_table_alias("u"), Some("users".to_string()));
        assert_eq!(provider.resolve_table_alias("p"), Some("posts".to_string()));

        // postsテーブルのフィールド検出
        let context = provider
            .analyze_context_before_dot("query test() {\n  alias u = users\n  alias p = posts\n  from(users).innerJoin(posts, u.id == p.user_id).where(p.");
        assert_eq!(context, CompletionContext::TableField("posts".to_string()));
    }

    #[test]
    fn test_alias_equals_completion() {
        let provider = CompletionProvider::new("query test() { alias u = ");
        
        // Test various alias patterns
        assert!(provider.is_after_alias_equals("alias u = "));
        assert!(provider.is_after_alias_equals("  alias myTable = "));
        assert!(provider.is_after_alias_equals("alias u ="));
        assert!(provider.is_after_alias_equals("alias u = "));
        assert!(provider.is_after_alias_equals("alias u = us"));  // Partial table name
        assert!(provider.is_after_alias_equals("alias u = u"));   // Single character
        
        // Test non-matching patterns
        assert!(!provider.is_after_alias_equals("alias u"));
        assert!(!provider.is_after_alias_equals("from(users)"));
        assert!(!provider.is_after_alias_equals("alias = "));
        assert!(!provider.is_after_alias_equals("alias u = users.id")); // Complete expression
        assert!(!provider.is_after_alias_equals("alias u = from(")); // Function call
    }

    #[tokio::test]
    async fn test_alias_completion_with_space() {
        let provider = CompletionProvider::new("query test() { alias u =");
        
        // Test that space is added when cursor is right after =
        let completions = provider.get_table_completions_for_alias(true).await;
        
        // Check that all completions have a space prefix
        for completion in &completions {
            if let Some(insert_text) = &completion.insert_text {
                assert!(insert_text.starts_with(' '), "Insert text should start with space: '{}'", insert_text);
            }
        }
        
        // Test that space is NOT added when there's already content after =
        let completions_no_space = provider.get_table_completions_for_alias(false).await;
        
        for completion in &completions_no_space {
            if let Some(insert_text) = &completion.insert_text {
                assert!(!insert_text.starts_with(' '), "Insert text should NOT start with space: '{}'", insert_text);
            }
        }
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

    #[tokio::test]
    async fn test_field_completion_with_alias() {
        use crate::schema_cache::SchemaCache;
        use nextsql_core::{BuiltInType, ColumnSchema, DatabaseSchema, TableSchema, Type};

        // テスト用のプロジェクトディレクトリを作成
        let temp_dir = std::env::temp_dir().join("nextsql_test_alias_completion");
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
        users_table.add_column(ColumnSchema {
            name: "email".to_string(),
            column_type: Type::BuiltIn(BuiltInType::String),
            nullable: false,
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

        // テスト用のファイルパス
        let file_path = temp_dir.join("query.nsql");
        let file_uri = format!("file://{}", file_path.display());

        // Arc<SchemaCache>を作成
        let schema_cache_arc = Arc::new(schema_cache);

        // CompletionProviderを作成（エイリアス使用）
        let text = "query findUserById($id: uuid) {\n  from(users<u>)\n  .where(u.";
        let provider =
            CompletionProvider::with_schema_cache(text, Arc::clone(&schema_cache_arc), file_uri);

        // カーソル位置を設定（u.の後）
        // "  .where(u." の長さは11文字
        let position = Position {
            line: 2,
            character: 11, // "  .where(u."の長さ
        };

        // 補完を取得
        let completions = provider.get_completions(position).await;

        // テスト用ファイルをクリーンアップ
        let _ = std::fs::remove_dir_all(&temp_dir);

        // エイリアス経由でもid列が補完候補に含まれていることを確認
        assert!(
            !completions.is_empty(),
            "Completions should not be empty for alias"
        );

        let id_completion = completions.iter().find(|c| c.label == "id");
        assert!(
            id_completion.is_some(),
            "id column should be in completions for alias u"
        );

        let email_completion = completions.iter().find(|c| c.label == "email");
        assert!(
            email_completion.is_some(),
            "email column should be in completions for alias u"
        );
    }
}
