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
}

impl NextSqlLanguageServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            document_map: tokio::sync::RwLock::new(HashMap::new()),
            schema_cache: std::sync::Arc::new(SchemaCache::new()),
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for NextSqlLanguageServer {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
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
                ..ServerCapabilities::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "NextSQL Language Server initialized!")
            .await;
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
                }
                self.validate_toml_document(&params.text_document.uri, &text)
                    .await;
            } else {
                self.validate_document(&params.text_document.uri, &text)
                    .await;
            }
        }
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

    let (service, socket) = LspService::new(|client| NextSqlLanguageServer::new(client));
    Server::new(stdin, stdout, socket).serve(service).await;
}
