use crate::schema_cache::SchemaCache;
use nextsql_core::*;
use std::sync::Arc;
use tower_lsp::lsp_types::*;

pub struct DiagnosticsProvider<'a> {
    text: &'a str,
    schema_cache: Option<Arc<SchemaCache>>,
    file_uri: Option<String>,
}

impl<'a> DiagnosticsProvider<'a> {
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

    pub async fn get_diagnostics(&self) -> Vec<Diagnostic> {
        eprintln!("DiagnosticsProvider: get_diagnostics called");
        let mut diagnostics = Vec::new();

        // 空のテキストまたは空白のみの場合はスキップ
        if self.text.trim().is_empty() {
            eprintln!("DiagnosticsProvider: Empty text, returning empty diagnostics");
            return diagnostics;
        }

        eprintln!("DiagnosticsProvider: Attempting to parse module");
        // パースを試行 - panicをキャッチする
        let parse_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parse_module(self.text)));

        match parse_result {
            Ok(Ok(_module)) => {
                eprintln!("DiagnosticsProvider: Parse successful");
                // パースが成功した場合、型検証を行う（スキーマがある場合のみ）
                if let Some(_schema_cache) = &self.schema_cache {
                    if let Some(file_uri) = &self.file_uri {
                        eprintln!(
                            "DiagnosticsProvider: Schema cache available, file_uri: {}",
                            file_uri
                        );
                        // 型検証は一旦スキップして、基本的な診断のみ行う
                        // TODO: 型検証を有効にする場合は以下のコメントを外す
                        // diagnostics.extend(self.validate_types(&module, schema_cache, file_uri).await);
                    }
                }
            }
            Ok(Err(error)) => {
                eprintln!("DiagnosticsProvider: Parse error: {:?}", error);
                // パースエラーを診断として追加
                let diagnostic = self.create_diagnostic_from_parse_error(error);
                diagnostics.push(diagnostic);
            }
            Err(panic_info) => {
                let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else {
                    format!("{:?}", panic_info)
                };
                eprintln!("DiagnosticsProvider: Parser panicked: {}", msg);

                // パニックエラーを診断として追加
                diagnostics.push(Diagnostic {
                    range: Range {
                        start: Position {
                            line: 0,
                            character: 0,
                        },
                        end: Position {
                            line: 0,
                            character: self.text.len() as u32,
                        },
                    },
                    severity: Some(DiagnosticSeverity::ERROR),
                    code: None,
                    code_description: None,
                    source: Some("nextsql".to_string()),
                    message: format!("Internal parser error: {}", msg),
                    related_information: None,
                    tags: None,
                    data: None,
                });
            }
        }

        eprintln!(
            "DiagnosticsProvider: Returning {} diagnostics",
            diagnostics.len()
        );
        diagnostics
    }

    fn create_diagnostic_from_parse_error(&self, error: pest::error::Error<Rule>) -> Diagnostic {
        let (line, column) = match &error.line_col {
            pest::error::LineColLocation::Pos((line, col)) => (*line - 1, *col - 1),
            pest::error::LineColLocation::Span((start_line, start_col), _) => {
                (*start_line - 1, *start_col - 1)
            }
        };

        let message = match &error.variant {
            pest::error::ErrorVariant::ParsingError {
                positives,
                negatives,
            } => {
                if !positives.is_empty() {
                    format!(
                        "Expected one of: {}",
                        positives
                            .iter()
                            .map(|r| format!("{:?}", r))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else if !negatives.is_empty() {
                    format!(
                        "Unexpected: {}",
                        negatives
                            .iter()
                            .map(|r| format!("{:?}", r))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "Parsing error".to_string()
                }
            }
            pest::error::ErrorVariant::CustomError { message } => message.clone(),
        };

        Diagnostic {
            range: Range {
                start: Position {
                    line: line as u32,
                    character: column as u32,
                },
                end: Position {
                    line: line as u32,
                    character: (column + 1) as u32,
                },
            },
            severity: Some(DiagnosticSeverity::ERROR),
            code: None,
            code_description: None,
            source: Some("nextsql".to_string()),
            message,
            related_information: None,
            tags: None,
            data: None,
        }
    }

    #[allow(dead_code)]
    async fn validate_types(
        &self,
        module: &Module,
        schema_cache: &Arc<SchemaCache>,
        file_uri: &str,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Try to load schema for this file
        let path = std::path::Path::new(file_uri.trim_start_matches("file://"));
        let schema = match schema_cache.get_schema_for_file(path).await {
            Some(schema) => schema,
            None => {
                // Try to load schema
                if let Some(project_root) = schema_cache.find_project_root(path) {
                    match schema_cache.load_schema_for_project(&project_root).await {
                        Ok(schema) => schema,
                        Err(_) => return diagnostics, // Cannot validate without schema
                    }
                } else {
                    return diagnostics;
                }
            }
        };

        // Create type validator
        let mut validator = TypeValidator::new(&schema);
        let validation_errors = validator.validate_module(module);

        // Convert validation errors to diagnostics
        for error in validation_errors {
            let diagnostic = self.create_diagnostic_from_validation_error(error);
            diagnostics.push(diagnostic);
        }

        diagnostics
    }

    #[allow(dead_code)]
    fn create_diagnostic_from_validation_error(&self, error: ValidationError) -> Diagnostic {
        // For now, we don't have exact positions, so we'll highlight the whole line
        // In a real implementation, we'd track positions during parsing
        let range = if let Some(span) = error.span {
            let start_line = self.text[..span.start].matches('\n').count() as u32;
            let start_char = span.start
                - self.text[..span.start]
                    .rfind('\n')
                    .map(|i| i + 1)
                    .unwrap_or(0);
            let end_line = self.text[..span.end].matches('\n').count() as u32;
            let end_char = span.end
                - self.text[..span.end]
                    .rfind('\n')
                    .map(|i| i + 1)
                    .unwrap_or(0);

            Range {
                start: Position {
                    line: start_line,
                    character: start_char as u32,
                },
                end: Position {
                    line: end_line,
                    character: end_char as u32,
                },
            }
        } else {
            // Default to first line
            Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: self.text.lines().next().unwrap_or("").len() as u32,
                },
            }
        };

        Diagnostic {
            range,
            severity: Some(DiagnosticSeverity::ERROR),
            code: None,
            code_description: None,
            source: Some("nextsql-type-checker".to_string()),
            message: error.message,
            related_information: None,
            tags: None,
            data: None,
        }
    }
}
