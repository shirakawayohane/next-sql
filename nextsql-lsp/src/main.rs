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

        // Load schema from database in the background if next-sql.toml exists
        let workspace_root = self.workspace_root.read().await.clone();
        if let Some(root) = workspace_root {
            let schema_cache = self.schema_cache.clone();
            let client = self.client.clone();
            tokio::spawn(async move {
                Self::load_schema_on_init(root, schema_cache, client).await;
            });
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

        // next-sql.tomlファイルかNextSQLファイルかで処理を分ける
        if params.text_document.uri.path().ends_with("next-sql.toml") {
            self.validate_toml_document(&params.text_document.uri, &params.text_document.text)
                .await;
        } else {
            // .nsqlファイルを開いた時、スキーマが未ロードならロードする
            self.ensure_schema_loaded(params.text_document.uri.path()).await;
            self.validate_all_project_files(params.text_document.uri.path()).await;
        }
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let mut document_map = self.document_map.write().await;
        if let Some(change) = params.content_changes.into_iter().next() {
            document_map.insert(params.text_document.uri.to_string(), change.text.clone());
            drop(document_map);
            
            if params.text_document.uri.path().ends_with("next-sql.toml") {
                // Invalidate schema cache when next-sql.toml changes
                let file_path = std::path::Path::new(params.text_document.uri.path());
                if let Some(project_root) = self.schema_cache.find_project_root(file_path) {
                    self.schema_cache.invalidate_cache(&project_root).await;
                }
                self.validate_toml_document(&params.text_document.uri, &change.text)
                    .await;
            } else {
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
        let table_name = match Self::get_table_name_at_position(&text, position) {
            Some(name) => name,
            None => return Ok(None),
        };

        // Check if it's a known table in the schema
        let file_path = params.text_document_position_params.text_document.uri.path().to_string();
        let schema = match self.schema_cache.get_schema_for_file(std::path::Path::new(&file_path)).await {
            Some(s) => s,
            None => return Ok(None),
        };

        if !schema.tables.contains_key(&table_name) {
            return Ok(None);
        }

        // Generate the virtual schema document and find the table's line
        let (_, table_lines) = Self::generate_schema_document(&schema);
        let line = table_lines.get(&table_name).copied().unwrap_or(0);

        let target_uri = Url::parse("nextsql-schema:///schema.nsql").unwrap();
        let target_range = Range {
            start: Position { line, character: 0 },
            end: Position { line, character: 0 },
        };

        Ok(Some(GotoDefinitionResponse::Scalar(Location {
            uri: target_uri,
            range: target_range,
        })))
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
            // file_path がない場合、キャッシュ内の最初のスキーマを使う
            let schemas = self.schema_cache.schemas_write().await;
            schemas.values().next().cloned()
        };

        match schema {
            Some(schema) => {
                let (doc, _) = Self::generate_schema_document(&schema);
                Ok(serde_json::json!({ "content": doc }))
            }
            None => Ok(serde_json::json!({ "content": "// No schema loaded\n" })),
        }
    }

    /// カーソル位置のテーブル名を取得する
    fn get_table_name_at_position(text: &str, position: Position) -> Option<String> {
        let lines: Vec<&str> = text.lines().collect();
        let line = lines.get(position.line as usize)?;
        let char_pos = position.character as usize;

        if char_pos > line.len() {
            return None;
        }

        // Find word boundaries around cursor - table names are identifiers (alphanumeric + underscore)
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

        let word = &line[start..end];
        Some(word.to_string())
    }

    /// スキーマ情報からNextSQL風の仮想ドキュメントを生成する
    /// 戻り値: (ドキュメントテキスト, テーブル名→行番号のマップ)
    fn generate_schema_document(schema: &nextsql_core::DatabaseSchema) -> (String, HashMap<String, u32>) {
        let mut doc = String::new();
        let mut table_lines: HashMap<String, u32> = HashMap::new();
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

            doc.push_str(&format!("table {} {{\n", table_name));
            current_line += 1;

            for column in &table.columns {
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
        }

        (doc, table_lines)
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

        let pattern = format!("{}/**/*.nsql", project_root.display());
        let entries: Vec<_> = glob::glob(&pattern)
            .into_iter()
            .flatten()
            .flatten()
            .collect();

        for entry in entries {
            let uri_string = format!("file://{}", entry.display());
            // document_mapに既にあればそのテキストを使う、なければファイルから読む
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

        let project_root = root.clone();
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

            // Try to load from cached JSON file first
            let schema_cache_path = project_root.join(".nextsql").join("schema.json");
            if schema_cache_path.exists() {
                if let Ok(contents) = std::fs::read_to_string(&schema_cache_path) {
                    if let Ok(schema) = nextsql_core::SchemaLoader::load_from_json(&contents) {
                        return Ok((schema, Some(db_url)));
                    }
                }
            }

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
                Ok(schema) => {
                    // Save schema cache to disk
                    let cache_path = project_root.join(".nextsql").join("schema.json");
                    if let Some(parent) = cache_path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    if let Ok(json) = nextsql_core::SchemaLoader::save_to_json(&schema) {
                        let _ = std::fs::write(&cache_path, json);
                    }
                    Ok((schema, None))
                }
                Err(e) => Err(format!("Failed to load schema from database: {}", e)),
            }
        })
        .await
        {
            Ok(Ok((schema, _))) => {
                let table_count = schema.tables.len();
                let mut schemas = cache.schemas_write().await;
                schemas.insert(root.clone(), schema);
                drop(schemas);
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
        let diagnostics = match tokio::task::spawn_blocking(move || {
            eprintln!("LSP: Inside spawn_blocking");
            
            // Use runtime handle to run async code in blocking context
            let handle = match tokio::runtime::Handle::try_current() {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("LSP: Failed to get runtime handle: {:?}", e);
                    return Err(format!("No runtime handle: {}", e));
                }
            };
            
            // Wrap the entire block in catch_unwind
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                eprintln!("LSP: Inside catch_unwind, creating provider");
                handle.block_on(async move {
                    let provider = DiagnosticsProvider::with_schema_cache(&text_owned, schema_cache, uri_str);
                    eprintln!("LSP: Provider created, calling get_diagnostics");
                    let result = provider.get_diagnostics().await;
                    eprintln!("LSP: get_diagnostics returned");
                    result
                })
            })) {
                Ok(diags) => {
                    eprintln!("LSP: Diagnostics generated successfully");
                    Ok(diags)
                }
                Err(panic_info) => {
                    let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        format!("{:?}", panic_info)
                    };
                    eprintln!("LSP: Panic occurred: {}", msg);
                    Err(format!("Panic: {}", msg))
                }
            }
        })
        .await
        {
            Ok(Ok(diags)) => diags,
            Ok(Err(error_msg)) => {
                eprintln!("LSP: Error in validation: {}", error_msg);
                vec![Diagnostic {
                    range: Range {
                        start: Position { line: 0, character: 0 },
                        end: Position { line: 0, character: text.len() as u32 },
                    },
                    severity: Some(DiagnosticSeverity::ERROR),
                    code: None,
                    code_description: None,
                    source: Some("nextsql-lsp".to_string()),
                    message: format!("Internal error: {}", error_msg),
                    related_information: None,
                    tags: None,
                    data: None,
                }]
            }
            Err(join_error) => {
                eprintln!("LSP: Task join error: {:?}", join_error);
                vec![Diagnostic {
                    range: Range {
                        start: Position { line: 0, character: 0 },
                        end: Position { line: 0, character: text.len() as u32 },
                    },
                    severity: Some(DiagnosticSeverity::ERROR),
                    code: None,
                    code_description: None,
                    source: Some("nextsql-lsp".to_string()),
                    message: format!("Internal error: Task join failed"),
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
        if let Some(captures) = regex::Regex::new(r"at line (\d+), column (\d+)")
            .unwrap()
            .captures(error_msg) 
        {
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
        // "unknown field `field_name`" パターンからフィールド名を抽出
        if let Some(captures) = regex::Regex::new(r"unknown field `([^`]+)`")
            .unwrap()
            .captures(error_msg)
        {
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
