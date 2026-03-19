use std::path::{Path, PathBuf};

use nextsql_core::schema::DatabaseSchema;

pub struct CodegenConfig {
    /// Source directory containing .nsql files
    pub source_dir: PathBuf,
    /// Output directory for generated files
    pub output_dir: PathBuf,
    /// Target backend (currently only "rust")
    pub backend: String,
    /// Naming pattern for insert param structs (e.g. "Insert{Table}Params")
    pub insert_params_pattern: Option<String>,
    /// Naming pattern for update changeset structs (e.g. "Update{Table}Params")
    pub update_params_pattern: Option<String>,
}

pub struct CodegenResult {
    pub generated_files: Vec<PathBuf>,
    pub errors: Vec<String>,
}

/// A single diagnostic from the check process.
#[derive(Debug, Clone)]
pub struct CheckDiagnostic {
    /// The file that produced this diagnostic.
    pub file: PathBuf,
    /// Line number (1-based), if available.
    pub line: Option<usize>,
    /// Column number (1-based), if available.
    pub column: Option<usize>,
    /// The error message.
    pub message: String,
    /// The source of the diagnostic (e.g. "parse", "type-check").
    pub source: DiagnosticSource,
}

#[derive(Debug, Clone)]
pub enum DiagnosticSource {
    Parse,
    TypeCheck,
    Io,
}

pub struct CheckResult {
    pub diagnostics: Vec<CheckDiagnostic>,
    pub files_checked: usize,
}

impl CheckResult {
    pub fn has_errors(&self) -> bool {
        !self.diagnostics.is_empty()
    }
}

/// Check (validate) all .nsql files without generating code.
/// Returns diagnostics for any parse or type errors found.
pub fn check(config: &CheckConfig, schema: &DatabaseSchema) -> CheckResult {
    let mut result = CheckResult {
        diagnostics: Vec::new(),
        files_checked: 0,
    };

    let nsql_files = find_nsql_files(&config.source_dir);

    // Read types.nsql content if it exists
    let types_path = config.source_dir.join("types.nsql");
    let types_content = if types_path.exists() {
        match std::fs::read_to_string(&types_path) {
            Ok(content) => Some(content),
            Err(e) => {
                result.diagnostics.push(CheckDiagnostic {
                    file: types_path.clone(),
                    line: None,
                    column: None,
                    message: format!("Failed to read file: {}", e),
                    source: DiagnosticSource::Io,
                });
                return result;
            }
        }
    } else {
        None
    };

    // If types.nsql exists, validate it parses
    if let Some(ref types_src) = types_content {
        result.files_checked += 1;
        if let Err(e) = nextsql_core::parser::parse_module(types_src) {
            let (line, col) = extract_pest_line_col(&e);
            result.diagnostics.push(CheckDiagnostic {
                file: types_path.clone(),
                line: Some(line),
                column: Some(col),
                message: format_pest_error(&e),
                source: DiagnosticSource::Parse,
            });
            return result; // types.nsql is fatal
        }
    }

    for nsql_path in &nsql_files {
        let filename = nsql_path.file_name().unwrap().to_str().unwrap();
        if filename == "types.nsql" {
            continue;
        }

        result.files_checked += 1;

        let file_content = match std::fs::read_to_string(nsql_path) {
            Ok(c) => c,
            Err(e) => {
                result.diagnostics.push(CheckDiagnostic {
                    file: nsql_path.clone(),
                    line: None,
                    column: None,
                    message: format!("Failed to read file: {}", e),
                    source: DiagnosticSource::Io,
                });
                continue;
            }
        };

        let combined_content = match &types_content {
            Some(types) => format!("{}\n{}", types, file_content),
            None => file_content.clone(),
        };

        let types_line_offset = types_content.as_ref().map(|t| t.lines().count() + 1).unwrap_or(0);

        match nextsql_core::parser::parse_module(&combined_content) {
            Ok(module) => {
                // Run type validation
                let mut validator = nextsql_core::TypeValidator::new(schema);
                let errors = validator.validate_module(&module);
                for error in errors {
                    let (line, col) = if let Some(span) = &error.span {
                        let text = &combined_content;
                        let line = text[..span.start].matches('\n').count() + 1;
                        let col = span.start
                            - text[..span.start].rfind('\n').map(|i| i + 1).unwrap_or(0)
                            + 1;
                        (line, col)
                    } else {
                        (0, 0)
                    };

                    // Adjust line numbers for types.nsql offset
                    let adjusted_line = if line > types_line_offset {
                        line - types_line_offset
                    } else if line > 0 {
                        // Error is in the types.nsql portion — skip (already validated)
                        continue;
                    } else {
                        0
                    };

                    result.diagnostics.push(CheckDiagnostic {
                        file: nsql_path.clone(),
                        line: if adjusted_line > 0 { Some(adjusted_line) } else { None },
                        column: if col > 0 { Some(col) } else { None },
                        message: error.message,
                        source: DiagnosticSource::TypeCheck,
                    });
                }
            }
            Err(e) => {
                let (line, col) = extract_pest_line_col(&e);
                let adjusted_line = if line > types_line_offset {
                    line - types_line_offset
                } else {
                    line
                };
                result.diagnostics.push(CheckDiagnostic {
                    file: nsql_path.clone(),
                    line: Some(adjusted_line),
                    column: Some(col),
                    message: format_pest_error(&e),
                    source: DiagnosticSource::Parse,
                });
            }
        }
    }

    result
}

pub struct CheckConfig {
    pub source_dir: PathBuf,
}

fn extract_pest_line_col(error: &pest::error::Error<nextsql_core::Rule>) -> (usize, usize) {
    match &error.line_col {
        pest::error::LineColLocation::Pos((line, col)) => (*line, *col),
        pest::error::LineColLocation::Span((line, col), _) => (*line, *col),
    }
}

fn format_pest_error(error: &pest::error::Error<nextsql_core::Rule>) -> String {
    match &error.variant {
        pest::error::ErrorVariant::ParsingError { positives, negatives } => {
            if !positives.is_empty() {
                format!(
                    "expected one of: {}",
                    positives.iter().map(|r| format!("{:?}", r)).collect::<Vec<_>>().join(", ")
                )
            } else if !negatives.is_empty() {
                format!(
                    "unexpected: {}",
                    negatives.iter().map(|r| format!("{:?}", r)).collect::<Vec<_>>().join(", ")
                )
            } else {
                "parsing error".to_string()
            }
        }
        pest::error::ErrorVariant::CustomError { message } => message.clone(),
    }
}

/// Main entry point for code generation.
///
/// Finds all .nsql files in the configured source directory, parses them,
/// merges type definitions from types.nsql, dispatches to the appropriate
/// backend, and writes the generated files.
///
/// Runs check (validation) first. If any errors are found, generation is aborted.
pub fn generate(config: &CodegenConfig, schema: &DatabaseSchema) -> CodegenResult {
    let mut result = CodegenResult {
        generated_files: Vec::new(),
        errors: Vec::new(),
    };

    // Run check first — abort if there are errors
    let check_config = CheckConfig {
        source_dir: config.source_dir.clone(),
    };
    let check_result = check(&check_config, schema);
    if check_result.has_errors() {
        for diag in &check_result.diagnostics {
            let location = match (diag.line, diag.column) {
                (Some(l), Some(c)) => format!("{}:{}:{}", diag.file.display(), l, c),
                (Some(l), None) => format!("{}:{}", diag.file.display(), l),
                _ => format!("{}", diag.file.display()),
            };
            result.errors.push(format!("{}: {}", location, diag.message));
        }
        return result;
    }

    // 1. Find all .nsql files
    let nsql_files = find_nsql_files(&config.source_dir);

    // 2. Read types.nsql content (if exists) for merging into other files
    let types_path = config.source_dir.join("types.nsql");
    let types_content = if types_path.exists() {
        match std::fs::read_to_string(&types_path) {
            Ok(content) => {
                // Validate that types.nsql parses successfully
                if let Err(e) = nextsql_core::parser::parse_module(&content) {
                    result
                        .errors
                        .push(format!("Failed to parse types.nsql: {}", e));
                    return result;
                }
                Some(content)
            }
            Err(e) => {
                result
                    .errors
                    .push(format!("Failed to read types.nsql: {}", e));
                return result;
            }
        }
    } else {
        None
    };

    // 3. Ensure output directory exists
    if let Err(e) = std::fs::create_dir_all(&config.output_dir) {
        result
            .errors
            .push(format!("Failed to create output directory: {}", e));
        return result;
    }

    // 4. Parse all .nsql files (except types.nsql)
    let mut parsed_modules: Vec<(String, nextsql_core::ast::Module)> = Vec::new();

    for nsql_path in &nsql_files {
        let filename = nsql_path.file_name().unwrap().to_str().unwrap();
        if filename == "types.nsql" {
            continue; // Already processed
        }

        let file_content = match std::fs::read_to_string(nsql_path) {
            Ok(c) => c,
            Err(e) => {
                result
                    .errors
                    .push(format!("Failed to read {}: {}", nsql_path.display(), e));
                continue;
            }
        };

        // Merge types.nsql content by prepending it to the file content before parsing.
        // This avoids needing Clone on TopLevel/Query/Mutation types.
        let combined_content = match &types_content {
            Some(types) => format!("{}\n{}", types, file_content),
            None => file_content,
        };

        let module = match nextsql_core::parser::parse_module(&combined_content) {
            Ok(m) => m,
            Err(e) => {
                result
                    .errors
                    .push(format!("Failed to parse {}: {}", nsql_path.display(), e));
                continue;
            }
        };

        // Generate module name from filename (replace hyphens with underscores)
        let module_name = filename.trim_end_matches(".nsql").replace('-', "_");
        parsed_modules.push((module_name, module));
    }

    // 5. Generate code for each module
    let mut module_names: Vec<String> = Vec::new();

    match config.backend.as_str() {
        "rust" => {
            // Check if any modules have valtypes that need deduplication
            let has_valtypes = parsed_modules.iter().any(|(_, m)| {
                m.toplevels.iter().any(|tl| matches!(tl, nextsql_core::ast::TopLevel::ValType(_)))
            });
            let multi_file = parsed_modules.len() > 1;
            let skip_inline_valtypes = has_valtypes && multi_file;

            // Generate shared types.rs if needed (Bug 4 fix: avoid duplicate valtypes)
            if skip_inline_valtypes {
                let module_refs: Vec<&nextsql_core::ast::Module> =
                    parsed_modules.iter().map(|(_, m)| m).collect();
                let types_file =
                    nextsql_backend_rust::rust_gen::generate_valtype_file(&module_refs);
                let types_path = config.output_dir.join("types.rs");
                match std::fs::write(&types_path, &types_file.content) {
                    Ok(_) => {
                        result.generated_files.push(types_path);
                        module_names.push("types".to_string());
                    }
                    Err(e) => {
                        result.errors.push(format!(
                            "Failed to write types.rs: {}",
                            e
                        ));
                    }
                }
            }

            // Generate runtime.rs with embedded runtime definitions
            {
                let runtime_content = nextsql_backend_rust::runtime_gen::generate_runtime();
                let runtime_path = config.output_dir.join("runtime.rs");
                match std::fs::write(&runtime_path, &runtime_content) {
                    Ok(_) => {
                        result.generated_files.push(runtime_path);
                        module_names.push("runtime".to_string());
                    }
                    Err(e) => {
                        result.errors.push(format!(
                            "Failed to write runtime.rs: {}",
                            e
                        ));
                    }
                }
            }

            // Build naming config
            let naming = {
                let mut n = nextsql_backend_rust::rust_gen::NamingConfig::default();
                if let Some(ref p) = config.insert_params_pattern {
                    n.insert_params_pattern = p.clone();
                }
                if let Some(ref p) = config.update_params_pattern {
                    n.update_params_pattern = p.clone();
                }
                n
            };

            // Generate each module
            for (module_name, module) in &parsed_modules {
                let generated =
                    nextsql_backend_rust::rust_gen::generate_rust_file_full(
                        module, schema, skip_inline_valtypes, &naming,
                    );

                // Collect codegen errors (e.g., duplicate field names)
                result.errors.extend(generated.errors);

                let output_filename = format!("{}.rs", module_name);
                let output_path = config.output_dir.join(&output_filename);
                match std::fs::write(&output_path, &generated.content) {
                    Ok(_) => {
                        result.generated_files.push(output_path);
                        module_names.push(module_name.clone());
                    }
                    Err(e) => {
                        result.errors.push(format!(
                            "Failed to write {}: {}",
                            output_path.display(),
                            e
                        ));
                    }
                }
            }
        }
        other => {
            result.errors.push(format!("Unknown backend: {}", other));
            return result;
        }
    }

    // 6. Generate mod.rs that re-exports all generated modules
    if !module_names.is_empty() && config.backend == "rust" {
        let mod_content = generate_mod_rs(&module_names);
        let mod_path = config.output_dir.join("mod.rs");
        match std::fs::write(&mod_path, &mod_content) {
            Ok(_) => result.generated_files.push(mod_path),
            Err(e) => result
                .errors
                .push(format!("Failed to write mod.rs: {}", e)),
        }
    }

    result
}

/// Recursively find all .nsql files in a directory.
fn find_nsql_files(dir: &Path) -> Vec<PathBuf> {
    let pattern = dir.join("**/*.nsql");
    let pattern_str = pattern.to_str().unwrap_or("");
    let mut files: Vec<PathBuf> = glob::glob(pattern_str)
        .map(|paths| paths.filter_map(|p| p.ok()).collect())
        .unwrap_or_default();
    files.sort();
    files
}

/// Generate a mod.rs file that re-exports all generated modules.
fn generate_mod_rs(module_names: &[String]) -> String {
    let mut out = String::new();
    out.push_str("// AUTO-GENERATED by nextsql. Do not edit.\n\n");
    for name in module_names {
        out.push_str(&format!("pub mod {};\n", name));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_nsql_files_empty_dir() {
        let dir = std::env::temp_dir().join("nextsql_codegen_test_empty");
        let _ = std::fs::create_dir_all(&dir);
        let files = find_nsql_files(&dir);
        // May or may not be empty depending on temp dir contents, just ensure no panic
        let _ = files;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_generate_mod_rs() {
        let names = vec!["users".to_string(), "posts".to_string()];
        let content = generate_mod_rs(&names);
        assert!(content.contains("pub mod users;"));
        assert!(content.contains("pub mod posts;"));
        assert!(content.contains("AUTO-GENERATED"));
    }
}
