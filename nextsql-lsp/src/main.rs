use std::collections::HashMap;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

mod completion;
mod diagnostics;
mod schema_cache;

use completion::CompletionProvider;
use diagnostics::DiagnosticsProvider;
use schema_cache::SchemaCache;

#[derive(Debug)]
pub struct NextSqlLanguageServer {
    client: Client,
    document_map: tokio::sync::RwLock<HashMap<String, String>>,
    schema_cache: std::sync::Arc<SchemaCache>,
    workspace_root: tokio::sync::RwLock<Option<std::path::PathBuf>>,
}

impl NextSqlLanguageServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            document_map: tokio::sync::RwLock::new(HashMap::new()),
            schema_cache: std::sync::Arc::new(SchemaCache::new()),
            workspace_root: tokio::sync::RwLock::new(None),
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for NextSqlLanguageServer {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        // Extract workspace root from root_uri or root_path
        if let Some(root_uri) = params.root_uri {
            if let Ok(path) = root_uri.to_file_path() {
                let mut workspace_root = self.workspace_root.write().await;
                *workspace_root = Some(path);
            }
        }

        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(false),
                    trigger_characters: Some(vec![".".to_string(), "=".to_string(), " ".to_string()]),
                    work_done_progress_options: Default::default(),
                    all_commit_characters: None,
                    completion_item: None,
                }),
                definition_provider: Some(OneOf::Left(true)),
                ..ServerCapabilities::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "NextSQL Language Server initialized!")
            .await;

        // Load schema from database if next-sql.toml exists, then validate all files
        let workspace_root = self.workspace_root.read().await.clone();
        if let Some(root) = workspace_root {
            let schema_cache = self.schema_cache.clone();
            let client = self.client.clone();
            Self::load_schema_on_init(root.clone(), schema_cache, client).await;

            // スキーマロード後に全プロジェクトファイルを検証して診断を公開する
            self.validate_all_files_in_root(&root).await;
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.document_map.write().await.insert(
            params.text_document.uri.to_string(),
            params.text_document.text.clone(),
        );

        if params.text_document.uri.path().ends_with("next-sql.toml") {
            self.validate_toml_document(&params.text_document.uri, &params.text_document.text)
                .await;
        } else {
            self.ensure_schema_loaded(params.text_document.uri.path()).await;
            self.update_valtype_cache(params.text_document.uri.path(), &params.text_document.text).await;
            self.validate_document(&params.text_document.uri, &params.text_document.text)
                .await;
        }
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let mut document_map = self.document_map.write().await;
        if let Some(change) = params.content_changes.into_iter().next() {
            document_map.insert(params.text_document.uri.to_string(), change.text.clone());
            drop(document_map);

            if params.text_document.uri.path().ends_with("next-sql.toml") {
                let file_path = std::path::Path::new(params.text_document.uri.path());
                if let Some(project_root) = self.schema_cache.find_project_root(file_path) {
                    self.schema_cache.invalidate_cache(&project_root).await;
                }
                self.validate_toml_document(&params.text_document.uri, &change.text)
                    .await;
            } else {
                self.update_valtype_cache(params.text_document.uri.path(), &change.text).await;
                self.validate_document(&params.text_document.uri, &change.text)
                    .await;
            }
        }
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        if let Some(text) = params.text {
            if params.text_document.uri.path().ends_with("next-sql.toml") {
                // Invalidate schema cache when next-sql.toml is saved
                let file_path = std::path::Path::new(params.text_document.uri.path());
                if let Some(project_root) = self.schema_cache.find_project_root(file_path) {
                    self.schema_cache.invalidate_cache(&project_root).await;
                    // Reload schema from database in background
                    let schema_cache = self.schema_cache.clone();
                    let client = self.client.clone();
                    let root = project_root.clone();
                    tokio::spawn(async move {
                        Self::load_schema_on_init(root, schema_cache, client).await;
                    });
                }
                self.validate_toml_document(&params.text_document.uri, &text)
                    .await;
            } else {
                self.validate_all_project_files(params.text_document.uri.path()).await;
            }
        }
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri.to_string();
        let position = params.text_document_position_params.position;

        let document_map = self.document_map.read().await;
        let text = match document_map.get(&uri) {
            Some(t) => t.clone(),
            None => return Ok(None),
        };
        drop(document_map);

        // Get the word at cursor position
        let word = match Self::get_word_at_position(&text, position) {
            Some(name) => name,
            None => return Ok(None),
        };

        // Load schema
        let file_path = params.text_document_position_params.text_document.uri.path().to_string();
        let schema = match self.schema_cache.get_schema_for_file(std::path::Path::new(&file_path)).await {
            Some(s) => s,
            None => return Ok(None),
        };

        let (_, table_lines, column_lines) = Self::generate_schema_document(&schema);
        let target_uri = Url::parse("nextsql-schema:///schema.nsql").unwrap();

        // 1. Field key inside value/set/doUpdate: { field: ... }
        if let Some((table_name, field_name)) = Self::resolve_field_context(&text, position, &schema) {
            if field_name == word {
                if let Some(cols) = column_lines.get(&table_name) {
                    if let Some(&line) = cols.get(&field_name) {
                        return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                            uri: target_uri,
                            range: Range {
                                start: Position { line, character: 4 },
                                end: Position { line, character: 4 + field_name.len() as u32 },
                            },
                        })));
                    }
                }
            }
        }

        // 2. Expression field access: table.field (cursor on `field` part)
        if let Some((table_name, field_name)) = Self::resolve_dot_access(&text, position, &schema) {
            if field_name == word {
                if let Some(cols) = column_lines.get(&table_name) {
                    if let Some(&line) = cols.get(&field_name) {
                        return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                            uri: target_uri,
                            range: Range {
                                start: Position { line, character: 4 },
                                end: Position { line, character: 4 + field_name.len() as u32 },
                            },
                        })));
                    }
                }
            }
        }

        // 3. Table name resolution
        if schema.tables.contains_key(&word) {
            let line = table_lines.get(&word).copied().unwrap_or(0);
            return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                uri: target_uri,
                range: Range {
                    start: Position { line, character: 0 },
                    end: Position { line, character: 0 },
                },
            })));
        }

        // 4. ValType name resolution (e.g., OrganizationId in `$org_id: OrganizationId`)
        if let Some((vt_path, vt_line)) = self.schema_cache.get_valtype_location(&word).await {
            let vt_uri = Url::from_file_path(&vt_path).ok();
            if let Some(uri) = vt_uri {
                return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                    uri,
                    range: Range {
                        start: Position { line: vt_line, character: 0 },
                        end: Position { line: vt_line, character: 0 },
                    },
                })));
            }
        }

        Ok(None)
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        eprintln!("LSP: completion handler called");
        let uri = params.text_document_position.text_document.uri.to_string();
        let position = params.text_document_position.position;
        eprintln!("LSP: completion requested for {} at {:?}", uri, position);

        let document_map = self.document_map.read().await;
        if let Some(text) = document_map.get(&uri) {
            // Clone Arc for the async block
            let schema_cache = self.schema_cache.clone();
            let text_owned = text.clone();
            let uri_str = uri.clone();
            
            // Run completion in a separate task
            let completions = tokio::task::spawn(async move {
                let provider = CompletionProvider::with_schema_cache(&text_owned, schema_cache, uri_str);
                provider.get_completions(position).await
            })
            .await
            .unwrap_or_else(|_| Vec::new());
            
            return Ok(Some(CompletionResponse::Array(completions)));
        }

        Ok(None)
    }
}

impl NextSqlLanguageServer {
    /// カスタムリクエスト: 仮想スキーマドキュメントの内容を返す
    async fn get_schema_document(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        // params から file_path を取得してプロジェクトを特定する
        let file_path = params
            .get("filePath")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let schema = if !file_path.is_empty() {
            self.schema_cache
                .get_schema_for_file(std::path::Path::new(file_path))
                .await
        } else {
            None
        };

        match schema {
            Some(schema) => {
                let (doc, _, _) = Self::generate_schema_document(&schema);
                Ok(serde_json::json!({ "content": doc }))
            }
            None => Ok(serde_json::json!({ "content": "// No schema loaded\n" })),
        }
    }

    /// カーソル位置の単語を取得する
    fn get_word_at_position(text: &str, position: Position) -> Option<String> {
        let lines: Vec<&str> = text.lines().collect();
        let line = lines.get(position.line as usize)?;
        let char_pos = position.character as usize;

        if char_pos > line.len() {
            return None;
        }

        let bytes = line.as_bytes();
        let mut start = char_pos;
        let mut end = char_pos;

        while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
            start -= 1;
        }
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }

        if start == end {
            return None;
        }

        Some(line[start..end].to_string())
    }

    /// カーソル位置がオブジェクトリテラル内のフィールドキーかを判定し、
    /// 対応する(テーブル名, フィールド名)を返す
    ///
    /// 対象パターン:
    ///   insert(table).value({ field: ... })
    ///   insert(table).values({ field: ... })
    ///   from(table)...set({ field: ... })
    ///   .doUpdate({ field: ... })
    fn resolve_field_context(
        text: &str,
        position: Position,
        schema: &nextsql_core::DatabaseSchema,
    ) -> Option<(String, String)> {
        let lines: Vec<&str> = text.lines().collect();
        let current_line = lines.get(position.line as usize)?;
        let char_pos = position.character as usize;

        // Check if cursor is on a field key pattern: `ident:` (identifier followed by colon)
        // The word at cursor must be followed by `:` (possibly with whitespace)
        let bytes = current_line.as_bytes();
        let mut word_start = char_pos;
        let mut word_end = char_pos;

        while word_start > 0 && (bytes[word_start - 1].is_ascii_alphanumeric() || bytes[word_start - 1] == b'_') {
            word_start -= 1;
        }
        while word_end < bytes.len() && (bytes[word_end].is_ascii_alphanumeric() || bytes[word_end] == b'_') {
            word_end += 1;
        }

        if word_start == word_end {
            return None;
        }

        let field_name = &current_line[word_start..word_end];

        // After the word, expect optional whitespace then `:`
        let rest = current_line[word_end..].trim_start();
        if !rest.starts_with(':') {
            return None;
        }

        // Now find the enclosing `{` and determine which method call it belongs to
        // Walk backwards from cursor position through the text to find the opening `{`
        let cursor_offset = lines[..position.line as usize]
            .iter()
            .map(|l| l.len() + 1) // +1 for newline
            .sum::<usize>()
            + char_pos;

        let text_before = &text[..cursor_offset];

        // Find matching `{` by counting braces
        let mut brace_depth = 0;
        let mut brace_pos = None;
        for (i, ch) in text_before.char_indices().rev() {
            match ch {
                '}' => brace_depth += 1,
                '{' => {
                    if brace_depth == 0 {
                        brace_pos = Some(i);
                        break;
                    }
                    brace_depth -= 1;
                }
                _ => {}
            }
        }

        let brace_pos = brace_pos?;

        // Look at what's before the `{` to find the method call
        let before_brace = text_before[..brace_pos].trim_end();

        // Match patterns like `.value(`, `.values(`, `.set(`, `.doUpdate(`
        let is_field_context = before_brace.ends_with(".value(")
            || before_brace.ends_with(".values(")
            || before_brace.ends_with(".set(")
            || before_brace.ends_with(".doUpdate(");

        if !is_field_context {
            return None;
        }

        // Find the table name by searching for `insert(table)` or `from(table)` earlier in the same statement
        // Walk back further to find the statement start
        let statement_text = &text[..brace_pos];

        // Try insert(table) pattern
        if let Some(table) = Self::extract_table_from_pattern(statement_text, "insert(", schema) {
            return Some((table, field_name.to_string()));
        }

        // Try from(table) pattern (for update queries)
        if let Some(table) = Self::extract_table_from_pattern(statement_text, "from(", schema) {
            return Some((table, field_name.to_string()));
        }

        None
    }

    /// テキスト内から `pattern_prefix + table_name + )` を後方から検索してテーブル名を抽出する
    fn extract_table_from_pattern(
        text: &str,
        pattern_prefix: &str,
        schema: &nextsql_core::DatabaseSchema,
    ) -> Option<String> {
        // Search from the end backwards for the most recent occurrence
        let mut search_from = text.len();
        while let Some(pos) = text[..search_from].rfind(pattern_prefix) {
            let after = &text[pos + pattern_prefix.len()..];
            if let Some(close_paren) = after.find(')') {
                let candidate = after[..close_paren].trim();
                if schema.tables.contains_key(candidate) {
                    return Some(candidate.to_string());
                }
            }
            search_from = pos;
        }
        None
    }

    /// 式中の `table.field` パターンを解決する
    /// カーソルが `field` 部分にある場合、直前の `.` とその前のテーブル名を取得する
    /// `table.relation.field` のようなチェーンの場合は最初のテーブル名のカラムとして解決
    fn resolve_dot_access(
        text: &str,
        position: Position,
        schema: &nextsql_core::DatabaseSchema,
    ) -> Option<(String, String)> {
        let lines: Vec<&str> = text.lines().collect();
        let current_line = lines.get(position.line as usize)?;
        let char_pos = position.character as usize;

        if char_pos > current_line.len() {
            return None;
        }

        let bytes = current_line.as_bytes();

        // Find the word at cursor (the field name candidate)
        let mut word_start = char_pos;
        let mut word_end = char_pos;
        while word_start > 0 && (bytes[word_start - 1].is_ascii_alphanumeric() || bytes[word_start - 1] == b'_') {
            word_start -= 1;
        }
        while word_end < bytes.len() && (bytes[word_end].is_ascii_alphanumeric() || bytes[word_end] == b'_') {
            word_end += 1;
        }
        if word_start == word_end {
            return None;
        }

        let field_name = &current_line[word_start..word_end];

        // Check there's a `.` immediately before the word
        if word_start == 0 || bytes[word_start - 1] != b'.' {
            return None;
        }

        // Walk backwards from the dot to collect the chain: e.g. `users.field` or `posts.author.field`
        // We need to find the root table name (the leftmost identifier in the chain)
        let mut pos = word_start - 1; // position of the `.`
        let mut chain_parts: Vec<&str> = Vec::new();

        loop {
            // Move before the dot
            if pos == 0 {
                break;
            }

            // Read the identifier before this dot
            let ident_end = pos;
            let mut ident_start = pos;
            while ident_start > 0 && (bytes[ident_start - 1].is_ascii_alphanumeric() || bytes[ident_start - 1] == b'_') {
                ident_start -= 1;
            }
            if ident_start == ident_end {
                break;
            }

            let part = &current_line[ident_start..ident_end];
            chain_parts.push(part);

            // Check if there's another dot before this identifier
            if ident_start > 0 && bytes[ident_start - 1] == b'.' {
                pos = ident_start - 1;
            } else {
                break;
            }
        }

        if chain_parts.is_empty() {
            return None;
        }

        // chain_parts is in reverse order: for `users.name` it's ["users"], for `posts.author.name` it's ["author", "posts"]
        // The root (table) is the last element
        let root_table = chain_parts.last()?;

        // Check if root is a known table
        if schema.tables.contains_key(*root_table) {
            // For direct `table.field`, check field exists
            if chain_parts.len() == 1 {
                let table = schema.tables.get(*root_table)?;
                if table.has_column(field_name) {
                    return Some((root_table.to_string(), field_name.to_string()));
                }
            } else {
                // For chained access like `posts.author.name`, the field belongs to the root table
                // only if it's a direct column. Otherwise we can't resolve through relations here.
                // Still try direct column match on the root table as a best-effort.
                let table = schema.tables.get(*root_table)?;
                if table.has_column(field_name) {
                    return Some((root_table.to_string(), field_name.to_string()));
                }
            }
        }

        None
    }

    /// スキーマ情報からNextSQL風の仮想ドキュメントを生成する
    /// 戻り値: (ドキュメントテキスト, テーブル名→行番号, テーブル名→(カラム名→行番号))
    fn generate_schema_document(
        schema: &nextsql_core::DatabaseSchema,
    ) -> (String, HashMap<String, u32>, HashMap<String, HashMap<String, u32>>) {
        let mut doc = String::new();
        let mut table_lines: HashMap<String, u32> = HashMap::new();
        let mut column_lines: HashMap<String, HashMap<String, u32>> = HashMap::new();
        let mut current_line: u32 = 0;

        doc.push_str("// NextSQL Database Schema (auto-generated)\n");
        current_line += 1;

        // Sort tables by name for consistent ordering
        let mut table_names: Vec<&String> = schema.tables.keys().collect();
        table_names.sort();

        for (i, table_name) in table_names.iter().enumerate() {
            if i > 0 {
                doc.push('\n');
                current_line += 1;
            }

            let table = &schema.tables[*table_name];
            table_lines.insert(table_name.to_string(), current_line);

            let mut cols: HashMap<String, u32> = HashMap::new();

            doc.push_str(&format!("table {} {{\n", table_name));
            current_line += 1;

            for column in &table.columns {
                cols.insert(column.name.clone(), current_line);

                let type_str = if column.nullable {
                    format!("{}?", column.column_type)
                } else {
                    format!("{}", column.column_type)
                };

                let mut annotations = Vec::new();
                if column.primary_key {
                    annotations.push("primary_key".to_string());
                }
                if column.has_default {
                    if let Some(ref default_val) = column.default_value {
                        annotations.push(format!("default: {}", default_val));
                    } else {
                        annotations.push("default".to_string());
                    }
                }

                if annotations.is_empty() {
                    doc.push_str(&format!("    {}: {}\n", column.name, type_str));
                } else {
                    doc.push_str(&format!(
                        "    {}: {} // {}\n",
                        column.name,
                        type_str,
                        annotations.join(", ")
                    ));
                }
                current_line += 1;
            }

            doc.push_str("}\n");
            current_line += 1;

            column_lines.insert(table_name.to_string(), cols);
        }

        (doc, table_lines, column_lines)
    }

    /// .nsqlファイルのテキストからValType定義を抽出してキャッシュを更新する
    async fn update_valtype_cache(&self, file_path: &str, text: &str) {
        let valtypes = SchemaCache::extract_valtypes_with_lines(text);
        self.schema_cache
            .update_valtypes_for_file(std::path::PathBuf::from(file_path), valtypes)
            .await;
    }

    /// .nsqlファイルを開いた時に、プロジェクトルートを探してスキーマが未ロードならロードする
    async fn ensure_schema_loaded(&self, file_path: &str) {
        let path = std::path::Path::new(file_path);
        if let Some(project_root) = self.schema_cache.find_project_root(path) {
            if self.schema_cache.get_schema_for_file(path).await.is_none() {
                let schema_cache = self.schema_cache.clone();
                let client = self.client.clone();
                let root = project_root.clone();
                tokio::spawn(async move {
                    Self::load_schema_on_init(root, schema_cache, client).await;
                });
            } else {
                // スキーマは既にキャッシュ済みだが、ValTypeキャッシュが未充填なら充填
                self.schema_cache.ensure_valtype_cache_populated(&project_root).await;
            }
        }
    }

    /// プロジェクト内の全.nsqlファイルの診断を実行する
    async fn validate_all_project_files(&self, file_path: &str) {
        let path = std::path::Path::new(file_path);
        let project_root = match self.schema_cache.find_project_root(path) {
            Some(root) => root,
            None => return,
        };

        self.validate_all_files_in_root(&project_root).await;
    }

    /// 指定されたプロジェクトルート内の全.nsqlファイルの診断を実行する
    async fn validate_all_files_in_root(&self, project_root: &std::path::Path) {
        let pattern = format!("{}/**/*.nsql", project_root.display());
        let entries: Vec<_> = glob::glob(&pattern)
            .into_iter()
            .flatten()
            .flatten()
            .collect();

        for entry in entries {
            let uri_string = format!("file://{}", entry.display());
            let text = {
                let doc_map = self.document_map.read().await;
                if let Some(text) = doc_map.get(&uri_string) {
                    text.clone()
                } else {
                    match std::fs::read_to_string(&entry) {
                        Ok(t) => t,
                        Err(_) => continue,
                    }
                }
            };

            // ValTypeキャッシュも更新
            self.update_valtype_cache(entry.to_str().unwrap_or(""), &text).await;

            if let Ok(uri) = Url::parse(&uri_string) {
                self.validate_document(&uri, &text).await;
            }
        }
    }

    async fn load_schema_on_init(
        root: std::path::PathBuf,
        schema_cache: std::sync::Arc<SchemaCache>,
        client: Client,
    ) {
        let config_path = root.join("next-sql.toml");
        if !config_path.exists() {
            client
                .log_message(
                    MessageType::INFO,
                    "No next-sql.toml found in workspace root, skipping schema loading",
                )
                .await;
            return;
        }

        client
            .log_message(MessageType::INFO, "Found next-sql.toml, loading database schema...")
            .await;

        let cache = schema_cache.clone();
        match tokio::task::spawn_blocking(move || {
            // Read and parse config
            let config_str = match std::fs::read_to_string(&config_path) {
                Ok(s) => s,
                Err(e) => return Err(format!("Failed to read next-sql.toml: {}", e)),
            };

            let config: nextsql_cli::config::NextSqlConfig = match toml::from_str(&config_str) {
                Ok(c) => c,
                Err(e) => return Err(format!("Failed to parse next-sql.toml: {}", e)),
            };

            let db_url = match config.database_url {
                Some(url) => url,
                None => return Err("No database_url in next-sql.toml".to_string()),
            };

            // Connect to database and load schema
            let config = match db_url.parse::<postgres::Config>() {
                Ok(c) => c,
                Err(e) => return Err(format!("Invalid database_url: {}", e)),
            };

            let mut pg_client = match config.connect(postgres::NoTls) {
                Ok(c) => c,
                Err(e) => return Err(format!("Failed to connect to database: {}", e)),
            };

            match nextsql_core::SchemaLoader::load_from_database(&mut pg_client) {
                Ok(schema) => Ok(schema),
                Err(e) => Err(format!("Failed to load schema from database: {}", e)),
            }
        })
        .await
        {
            Ok(Ok(schema)) => {
                let table_count = schema.tables.len();
                cache.insert_schema(root.clone(), schema).await;
                // プロジェクト内の全.nsqlファイルからValType定義をキャッシュに充填
                cache.populate_valtype_cache(&root).await;
                client
                    .log_message(
                        MessageType::INFO,
                        format!(
                            "Database schema loaded successfully ({} tables)",
                            table_count
                        ),
                    )
                    .await;
            }
            Ok(Err(msg)) => {
                client
                    .log_message(
                        MessageType::WARNING,
                        format!("Schema loading skipped: {}", msg),
                    )
                    .await;
            }
            Err(e) => {
                client
                    .log_message(
                        MessageType::WARNING,
                        format!("Schema loading task failed: {}", e),
                    )
                    .await;
            }
        }
    }

    async fn validate_document(&self, uri: &Url, text: &str) {
        eprintln!("LSP: validate_document called for: {}", uri);
        
        // Clone Arc for async move
        let schema_cache = self.schema_cache.clone();
        let uri_str = uri.to_string();
        let text_owned = text.to_string();
        
        // Run validation with proper error handling
        let diagnostics = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let provider = DiagnosticsProvider::with_schema_cache(&text_owned, schema_cache, uri_str);
            provider
        })) {
            Ok(provider) => provider.get_diagnostics().await,
            Err(panic_info) => {
                let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else {
                    format!("{:?}", panic_info)
                };
                eprintln!("LSP: Panic occurred: {}", msg);
                vec![Diagnostic {
                    range: Range {
                        start: Position { line: 0, character: 0 },
                        end: Position { line: 0, character: text.len() as u32 },
                    },
                    severity: Some(DiagnosticSeverity::ERROR),
                    code: None,
                    code_description: None,
                    source: Some("nextsql-lsp".to_string()),
                    message: format!("Internal error: {}", msg),
                    related_information: None,
                    tags: None,
                    data: None,
                }]
            }
        };

        self.client
            .publish_diagnostics(uri.clone(), diagnostics, None)
            .await;
    }

    async fn validate_toml_document(&self, uri: &Url, text: &str) {
        let diagnostics = self.validate_toml_config(text);
        self.client
            .publish_diagnostics(uri.clone(), diagnostics, None)
            .await;
    }

    fn validate_toml_config(&self, text: &str) -> Vec<Diagnostic> {
        use nextsql_cli::config::NextSqlConfig;
        
        // TOMLパースを試行
        match toml::from_str::<NextSqlConfig>(text) {
            Ok(config) => {
                // 設定をJSONに変換してスキーマ検証
                match serde_json::to_value(&config) {
                    Ok(json_value) => {
                        let schema_str = include_str!("../../nextsql-cli/schemas/next-sql.schema.json");
                        match serde_json::from_str::<serde_json::Value>(schema_str) {
                            Ok(schema) => {
                                match jsonschema::JSONSchema::compile(&schema) {
                                    Ok(compiled_schema) => {
                                        if let Err(errors) = compiled_schema.validate(&json_value) {
                                            errors
                                                .map(|error| Diagnostic {
                                                    range: Range {
                                                        start: Position { line: 0, character: 0 },
                                                        end: Position { line: 0, character: text.len() as u32 },
                                                    },
                                                    severity: Some(DiagnosticSeverity::ERROR),
                                                    code: None,
                                                    code_description: None,
                                                    source: Some("nextsql-toml-validator".to_string()),
                                                    message: format!("Configuration validation error: {}", error),
                                                    related_information: None,
                                                    tags: None,
                                                    data: None,
                                                })
                                                .collect()
                                        } else {
                                            Vec::new() // 検証成功
                                        }
                                    }
                                    Err(e) => vec![self.create_error_diagnostic(text, &format!("Schema compilation error: {}", e))],
                                }
                            }
                            Err(e) => vec![self.create_error_diagnostic(text, &format!("Schema parse error: {}", e))],
                        }
                    }
                    Err(e) => vec![self.create_error_diagnostic(text, &format!("JSON conversion error: {}", e))],
                }
            }
            Err(e) => {
                // TOMLパースエラーの場合、エラー位置を特定
                self.parse_toml_error(text, &e.to_string())
            }
        }
    }

    fn parse_toml_error(&self, text: &str, error_msg: &str) -> Vec<Diagnostic> {
        let lines: Vec<&str> = text.lines().collect();
        
        // TOMLパースエラーメッセージから行と列の情報を抽出
        static RE_LINE_COL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
            regex::Regex::new(r"at line (\d+), column (\d+)").unwrap()
        });
        if let Some(captures) = RE_LINE_COL.captures(error_msg) {
            if let (Ok(line), Ok(column)) = (
                captures.get(1).unwrap().as_str().parse::<u32>(),
                captures.get(2).unwrap().as_str().parse::<u32>()
            ) {
                let line_idx = (line - 1) as usize;
                let line_length = if line_idx < lines.len() {
                    lines[line_idx].len() as u32
                } else {
                    0
                };
                
                return vec![Diagnostic {
                    range: Range {
                        start: Position { line: line - 1, character: column - 1 },
                        end: Position { line: line - 1, character: line_length },
                    },
                    severity: Some(DiagnosticSeverity::ERROR),
                    code: None,
                    code_description: None,
                    source: Some("nextsql-toml-validator".to_string()),
                    message: error_msg.to_string(),
                    related_information: None,
                    tags: None,
                    data: None,
                }];
            }
        }
        
        // エラーメッセージから無効なフィールド名を抽出
        if let Some(field_name) = self.extract_unknown_field(error_msg) {
            // テキスト内でそのフィールドを検索
            for (line_num, line) in lines.iter().enumerate() {
                if line.trim_start().starts_with(&field_name) {
                    let start_char = line.find(&field_name).unwrap_or(0) as u32;
                    let end_char = start_char + field_name.len() as u32;
                    
                    return vec![Diagnostic {
                        range: Range {
                            start: Position { line: line_num as u32, character: start_char },
                            end: Position { line: line_num as u32, character: end_char },
                        },
                        severity: Some(DiagnosticSeverity::ERROR),
                        code: None,
                        code_description: None,
                        source: Some("nextsql-toml-validator".to_string()),
                        message: error_msg.to_string(),
                        related_information: None,
                        tags: None,
                        data: None,
                    }];
                }
            }
        }
        
        // フォールバック: エラー位置が特定できない場合は最初の行
        vec![self.create_error_diagnostic(text, error_msg)]
    }

    fn extract_unknown_field(&self, error_msg: &str) -> Option<String> {
        static RE_UNKNOWN_FIELD: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
            regex::Regex::new(r"unknown field `([^`]+)`").unwrap()
        });
        if let Some(captures) = RE_UNKNOWN_FIELD.captures(error_msg) {
            return Some(captures.get(1).unwrap().as_str().to_string());
        }
        None
    }

    fn create_error_diagnostic(&self, text: &str, message: &str) -> Diagnostic {
        Diagnostic {
            range: Range {
                start: Position { line: 0, character: 0 },
                end: Position { line: 0, character: text.len() as u32 },
            },
            severity: Some(DiagnosticSeverity::ERROR),
            code: None,
            code_description: None,
            source: Some("nextsql-toml-validator".to_string()),
            message: message.to_string(),
            related_information: None,
            tags: None,
            data: None,
        }
    }
}

#[tokio::main]
async fn main() {
    // パニックハンドラーを設定してサーバーがクラッシュしないように
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("LSP Server panic: {:?}", panic_info);
    }));

    // LSPではstdoutは通信に使われるため、ログはstderrに出力する
    // ANSIカラーコードも無効化する
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::build(|client| NextSqlLanguageServer::new(client))
        .custom_method("nextsql/getSchemaDocument", NextSqlLanguageServer::get_schema_document)
        .finish();
    Server::new(stdin, stdout, socket).serve(service).await;
}
