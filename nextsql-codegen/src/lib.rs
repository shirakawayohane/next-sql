use std::path::{Path, PathBuf};

use nextsql_core::schema::DatabaseSchema;

/// Version of nextsql-backend-rust-runtime to reference in generated Cargo.toml.
/// Kept in sync with workspace version by release-all.yml.
const RUNTIME_CRATE_VERSION: &str = "0.3.2";

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
    /// If set, generate a project manifest (Cargo.toml for Rust) with this package name.
    /// The manifest is placed in output_dir's parent directory (e.g. output_dir=src/ → Cargo.toml at crate root).
    pub package_name: Option<String>,
    /// Glob patterns for type definition files (valtypes, relations, input types).
    /// These files are prepended to every other .nsql file before parsing.
    pub type_files: Vec<String>,
    /// Optional local path to nextsql-backend-rust-runtime crate.
    /// When set, the generated Cargo.toml uses a path dependency instead of a crates.io version.
    pub runtime_crate_path: Option<PathBuf>,
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

    // Read type definition files
    let type_file_paths = find_type_files(&config.source_dir, &config.type_files);
    let types_content = match read_type_files_content(&type_file_paths) {
        Ok(content) => content,
        Err((path, e)) => {
            result.diagnostics.push(CheckDiagnostic {
                file: path,
                line: None,
                column: None,
                message: format!("Failed to read file: {}", e),
                source: DiagnosticSource::Io,
            });
            return result;
        }
    };

    // If type files exist, validate they parse
    if let Some(ref types_src) = types_content {
        result.files_checked += 1;
        if let Err(e) = nextsql_core::parser::parse_module(types_src) {
            let (line, col) = extract_pest_line_col(&e);
            let file = type_file_paths.first().cloned().unwrap_or_else(|| config.source_dir.join("types.nsql"));
            result.diagnostics.push(CheckDiagnostic {
                file,
                line: Some(line),
                column: Some(col),
                message: format_pest_error(&e),
                source: DiagnosticSource::Parse,
            });
            return result; // type files parse error is fatal
        }
    }

    for nsql_path in &nsql_files {
        if type_file_paths.iter().any(|tp| tp == nsql_path) {
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
    /// Glob patterns for type definition files.
    pub type_files: Vec<String>,
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
/// Runs check (validation) as part of parsing. If any errors are found, generation is aborted.
pub fn generate(config: &CodegenConfig, schema: &DatabaseSchema) -> CodegenResult {
    let mut result = CodegenResult {
        generated_files: Vec::new(),
        errors: Vec::new(),
    };

    // 1. Find all .nsql files
    let nsql_files = find_nsql_files(&config.source_dir);

    // 2. Read type definition files (valtypes, relations, input types) for merging
    let type_file_paths = find_type_files(&config.source_dir, &config.type_files);
    let types_content = match read_type_files_content(&type_file_paths) {
        Ok(content) => {
            if let Some(ref c) = content {
                if let Err(e) = nextsql_core::parser::parse_module(c) {
                    result.errors.push(format!("Failed to parse type files: {}", e));
                    return result;
                }
            }
            content
        }
        Err((path, e)) => {
            result.errors.push(format!("Failed to read {}: {}", path.display(), e));
            return result;
        }
    };

    // 3. Ensure output directory exists
    let generated_dir = if config.backend == "rust" {
        config.output_dir.join("generated")
    } else {
        config.output_dir.clone()
    };
    if let Err(e) = std::fs::create_dir_all(&generated_dir) {
        result
            .errors
            .push(format!("Failed to create output directory: {}", e));
        return result;
    }

    // Collect existing .rs files so we can delete stale ones after generation
    let existing_rs_files: Vec<PathBuf> = std::fs::read_dir(&generated_dir)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("rs"))
        .collect();

    // 4. Parse all .nsql files (except types.nsql), validate, then collect
    let mut parsed_modules: Vec<(String, nextsql_core::ast::Module)> = Vec::new();
    let types_line_offset = types_content.as_ref().map(|t| t.lines().count() + 1).unwrap_or(0);

    for nsql_path in &nsql_files {
        let filename = nsql_path.file_name().unwrap().to_str().unwrap();
        if type_file_paths.iter().any(|tp| tp == nsql_path) {
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
                let (line, col) = extract_pest_line_col(&e);
                let adjusted_line = if line > types_line_offset {
                    line - types_line_offset
                } else {
                    line
                };
                result
                    .errors
                    .push(format!("{}:{}:{}: {}", nsql_path.display(), adjusted_line, col, format_pest_error(&e)));
                continue;
            }
        };

        // Run type validation
        let mut validator = nextsql_core::TypeValidator::new(schema);
        let errors = validator.validate_module(&module);
        if !errors.is_empty() {
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

                let adjusted_line = if line > types_line_offset {
                    line - types_line_offset
                } else if line > 0 {
                    continue; // Error is in the types.nsql portion — skip
                } else {
                    0
                };

                let location = match (adjusted_line, col) {
                    (l, c) if l > 0 && c > 0 => format!("{}:{}:{}", nsql_path.display(), l, c),
                    (l, _) if l > 0 => format!("{}:{}", nsql_path.display(), l),
                    _ => format!("{}", nsql_path.display()),
                };
                result.errors.push(format!("{}: {}", location, error.message));
            }
            continue;
        }

        // Generate module name from filename (replace hyphens with underscores)
        let module_name = filename.trim_end_matches(".nsql").replace('-', "_");
        parsed_modules.push((module_name, module));
    }

    if !result.errors.is_empty() {
        return result;
    }

    // 5. Generate code for each module
    let mut module_names: Vec<String> = Vec::new();

    match config.backend.as_str() {
        "rust" => {
            // Check if shared types.rs is needed (multi-file with valtypes or shared enums)
            let module_refs: Vec<&nextsql_core::ast::Module> =
                parsed_modules.iter().map(|(_, m)| m).collect();
            let has_valtypes = parsed_modules.iter().any(|(_, m)| {
                m.toplevels.iter().any(|tl| matches!(tl, nextsql_core::ast::TopLevel::ValType(_)))
            });
            let multi_file = parsed_modules.len() > 1;
            let shared_enums = if multi_file {
                nextsql_backend_rust::rust_gen::compute_shared_enums(&module_refs, schema)
            } else {
                std::collections::HashSet::new()
            };
            let needs_shared_types = has_valtypes || !shared_enums.is_empty();

            // Generate shared types.rs if needed
            if needs_shared_types {
                let types_file =
                    nextsql_backend_rust::rust_gen::generate_valtype_file(&module_refs, schema);
                let types_path = generated_dir.join("types.rs");
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
                        module, schema, needs_shared_types, &naming, &shared_enums,
                    );

                // Collect codegen errors (e.g., duplicate field names)
                result.errors.extend(generated.errors);

                let output_filename = format!("{}.rs", module_name);
                let output_path = generated_dir.join(&output_filename);
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

    // 6. Generate mod.rs and lib.rs for Rust backend.
    if !module_names.is_empty() && config.backend == "rust" {
        // Generate generated/mod.rs with module declarations
        let mod_content = generate_mod_rs(&module_names);
        let mod_path = generated_dir.join("mod.rs");
        match std::fs::write(&mod_path, &mod_content) {
            Ok(_) => result.generated_files.push(mod_path),
            Err(e) => result
                .errors
                .push(format!("Failed to write generated/mod.rs: {}", e)),
        }

        // Update lib.rs: only replace the nextsql-managed section, preserving user code
        let lib_path = config.output_dir.join("lib.rs");
        let lib_content = update_lib_rs(&lib_path, &module_names);
        match std::fs::write(&lib_path, &lib_content) {
            Ok(_) => result.generated_files.push(lib_path),
            Err(e) => result
                .errors
                .push(format!("Failed to write lib.rs: {}", e)),
        }
    }

    // 7. Delete stale .rs files from generated_dir that were not part of this generation
    for old_file in &existing_rs_files {
        if !result.generated_files.iter().any(|gf| gf == old_file) {
            let _ = std::fs::remove_file(old_file);
        }
    }

    // 8. Generate Cargo.toml for Rust backend
    if config.backend == "rust" {
        let cargo_dir = config.output_dir.parent().unwrap_or(&config.output_dir);
        let package_name = config.package_name.clone().unwrap_or_else(|| {
            // Canonicalize to resolve "." and ".." before extracting the directory name
            cargo_dir
                .canonicalize()
                .ok()
                .and_then(|p| p.file_name().map(|n| n.to_os_string()))
                .and_then(|n| n.to_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "generated".to_string())
                .replace('-', "_")
        });
        let cargo_path = cargo_dir.join("Cargo.toml");
        if !cargo_path.exists() {
            let cargo_content = generate_cargo_toml(&package_name, config.runtime_crate_path.as_deref());
            if let Err(e) = std::fs::create_dir_all(cargo_dir) {
                result
                    .errors
                    .push(format!("Failed to create crate root directory: {}", e));
            }
            match std::fs::write(&cargo_path, &cargo_content) {
                Ok(_) => result.generated_files.push(cargo_path),
                Err(e) => result
                    .errors
                    .push(format!("Failed to write Cargo.toml: {}", e)),
            }
        } else if config.runtime_crate_path.is_none() {
            // Update nextsql-backend-rust-runtime version in existing Cargo.toml
            if let Ok(content) = std::fs::read_to_string(&cargo_path) {
                let updated = update_runtime_version_in_cargo_toml(&content, RUNTIME_CRATE_VERSION);
                if updated != content {
                    if let Err(e) = std::fs::write(&cargo_path, &updated) {
                        result
                            .errors
                            .push(format!("Failed to update Cargo.toml: {}", e));
                    }
                }
            }
        }
    }

    result
}

/// Find type definition files by resolving glob patterns from config against source_dir.
/// Returns all matching files in sorted order.
fn find_type_files(source_dir: &Path, type_file_patterns: &[String]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for pattern in type_file_patterns {
        let full_pattern = source_dir.join(pattern);
        let pattern_str = full_pattern.to_str().unwrap_or("");
        if let Ok(paths) = glob::glob(pattern_str) {
            for path in paths.filter_map(|p| p.ok()).filter(|p| p.is_file()) {
                if !files.contains(&path) {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    files
}

/// Read and concatenate all type definition files into a single string.
fn read_type_files_content(type_files: &[PathBuf]) -> Result<Option<String>, (PathBuf, std::io::Error)> {
    if type_files.is_empty() {
        return Ok(None);
    }
    let mut combined = String::new();
    for path in type_files {
        if path.exists() {
            let content = std::fs::read_to_string(path).map_err(|e| (path.clone(), e))?;
            if !combined.is_empty() {
                combined.push('\n');
            }
            combined.push_str(&content);
        }
    }
    if combined.is_empty() {
        Ok(None)
    } else {
        Ok(Some(combined))
    }
}

/// Recursively find all .nsql files in a directory.
fn find_nsql_files(dir: &Path) -> Vec<PathBuf> {
    let pattern = dir.join("**/*.nsql");
    let pattern_str = pattern.to_str().unwrap_or("");
    let mut files: Vec<PathBuf> = glob::glob(pattern_str)
        .map(|paths| paths.filter_map(|p| p.ok()).filter(|p| p.is_file()).collect())
        .unwrap_or_default();
    files.sort();
    files
}

/// Generate a Cargo.toml for the generated crate.
fn generate_cargo_toml(crate_name: &str, runtime_crate_path: Option<&Path>) -> String {
    let runtime_dep = match runtime_crate_path {
        Some(path) => format!(
            "nextsql-backend-rust-runtime = {{ path = \"{}\", default-features = false }}",
            path.display()
        ),
        None => format!("nextsql-backend-rust-runtime = {{ version = \"{RUNTIME_CRATE_VERSION}\", default-features = false }}"),
    };
    format!(
        r#"# AUTO-GENERATED by nextsql. Do not edit.

[package]
name = "{crate_name}"
version = "0.0.0"
edition = "2021"

[lints.clippy]
all = "allow"

[dependencies]
{runtime_dep}
chrono = {{ version = "0.4", features = ["serde"] }}
uuid = {{ version = "1", features = ["v4", "serde"] }}
serde_json = "1"
rust_decimal = "1"
"#
    )
}

/// Update the nextsql-backend-rust-runtime version in an existing Cargo.toml.
/// Handles both `nextsql-backend-rust-runtime = "VERSION"` and
/// `nextsql-backend-rust-runtime = { version = "VERSION", ... }` formats.
fn update_runtime_version_in_cargo_toml(content: &str, new_version: &str) -> String {
    let mut result = String::with_capacity(content.len());
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("nextsql-backend-rust-runtime") {
            // Find `version = "X.Y.Z"` or just `= "X.Y.Z"` and replace the version
            // Look for the pattern: `"` <old_version> `"`  that follows `version` or `=`
            if let Some(replaced) = replace_version_in_dep_line(trimmed, new_version) {
                result.push_str(&replaced);
            } else {
                result.push_str(line);
            }
        } else {
            result.push_str(line);
        }
        result.push('\n');
    }
    result
}

/// Replace the version string in a dependency line.
/// Supports: `dep = "VERSION"` and `dep = { version = "VERSION", ... }`
fn replace_version_in_dep_line(line: &str, new_version: &str) -> Option<String> {
    // Find the quoted version string to replace.
    // For inline table, look for `version = "..."` first.
    // For simple format, look for `= "..."`.
    let search_start = if let Some(ver_pos) = line.find("version") {
        ver_pos
    } else {
        0
    };
    let rest = &line[search_start..];
    // Find opening quote
    let open_quote = rest.find('"')?;
    // Find closing quote
    let close_quote = open_quote + 1 + rest[open_quote + 1..].find('"')?;
    // Reconstruct: everything before opening quote + 1 (include the quote), new version, closing quote onward
    let abs_open = search_start + open_quote;
    let abs_close = search_start + close_quote;
    let mut new_line = String::new();
    new_line.push_str(&line[..abs_open + 1]); // up to and including opening quote
    new_line.push_str(new_version);
    new_line.push_str(&line[abs_close..]); // from closing quote onward (includes it)
    Some(new_line)
}

/// Generate a mod.rs file that declares all generated modules.
fn generate_mod_rs(module_names: &[String]) -> String {
    let mut out = String::new();
    out.push_str("// AUTO-GENERATED by nextsql. Do not edit.\n\n");
    for name in module_names {
        out.push_str(&format!("pub mod {};\n", name));
    }
    out
}

const NEXTSQL_BEGIN_MARKER: &str = "// nextsql:begin";
const NEXTSQL_END_MARKER: &str = "// nextsql:end";

/// Generate the nextsql-managed section content for lib.rs.
fn generate_lib_rs_section(module_names: &[String]) -> String {
    let mut out = String::new();
    out.push_str(NEXTSQL_BEGIN_MARKER);
    out.push('\n');
    out.push_str("mod generated;\n");
    for name in module_names {
        out.push_str(&format!("pub use generated::{};\n", name));
    }
    out.push_str(NEXTSQL_END_MARKER);
    out
}

/// Read existing lib.rs (if any) and replace only the nextsql-managed section.
/// User code outside the markers is preserved.
fn update_lib_rs(lib_path: &Path, module_names: &[String]) -> String {
    let section = generate_lib_rs_section(module_names);

    let existing = std::fs::read_to_string(lib_path).unwrap_or_default();
    if existing.is_empty() {
        return format!("{}\n", section);
    }

    if let (Some(begin), Some(end)) = (
        existing.find(NEXTSQL_BEGIN_MARKER),
        existing.find(NEXTSQL_END_MARKER),
    ) {
        let end = end + NEXTSQL_END_MARKER.len();
        let mut result = String::with_capacity(existing.len());
        result.push_str(&existing[..begin]);
        result.push_str(&section);
        result.push_str(&existing[end..]);
        result
    } else {
        // No markers found: prepend the section
        format!("{}\n{}", section, existing)
    }
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

    #[test]
    fn test_generate_lib_rs_section() {
        let names = vec!["users".to_string(), "posts".to_string()];
        let content = generate_lib_rs_section(&names);
        assert!(content.contains("mod generated;"));
        assert!(content.contains("pub use generated::users;"));
        assert!(content.contains("pub use generated::posts;"));
        assert!(content.starts_with(NEXTSQL_BEGIN_MARKER));
        assert!(content.ends_with(NEXTSQL_END_MARKER));
    }

    #[test]
    fn test_update_lib_rs_preserves_user_code() {
        let dir = std::env::temp_dir().join("nextsql_update_lib_test");
        let _ = std::fs::create_dir_all(&dir);
        let lib_path = dir.join("lib.rs");

        // Simulate existing lib.rs with user code and markers
        let existing = format!(
            "use some_crate::Thing;\n\n{}\nmod generated;\npub use generated::old_module;\n{}\n\nfn my_custom_fn() {{}}\n",
            NEXTSQL_BEGIN_MARKER, NEXTSQL_END_MARKER
        );
        std::fs::write(&lib_path, &existing).unwrap();

        let names = vec!["users".to_string()];
        let result = update_lib_rs(&lib_path, &names);

        // User code preserved
        assert!(result.contains("use some_crate::Thing;"));
        assert!(result.contains("fn my_custom_fn() {}"));
        // Old module removed, new modules present
        assert!(!result.contains("old_module"));
        assert!(result.contains("pub use generated::users;"));
    }

    #[test]
    fn test_update_runtime_version_simple_string() {
        let input = r#"[dependencies]
nextsql-backend-rust-runtime = "0.1.0"
chrono = "0.4"
"#;
        let result = update_runtime_version_in_cargo_toml(input, "0.3.2");
        assert!(result.contains(r#"nextsql-backend-rust-runtime = "0.3.2""#));
        assert!(result.contains(r#"chrono = "0.4""#));
    }

    #[test]
    fn test_update_runtime_version_inline_table() {
        let input = r#"[dependencies]
nextsql-backend-rust-runtime = { version = "0.1.0", default-features = false }
chrono = "0.4"
"#;
        let result = update_runtime_version_in_cargo_toml(input, "0.3.2");
        // Verify the exact line format is preserved with proper quoting
        assert!(result.contains(
            r#"nextsql-backend-rust-runtime = { version = "0.3.2", default-features = false }"#
        ));
        assert!(!result.contains("\"0.1.0\""));
    }

    #[test]
    fn test_update_runtime_version_no_match() {
        let input = r#"[dependencies]
chrono = "0.4"
"#;
        let result = update_runtime_version_in_cargo_toml(input, "0.3.2");
        assert_eq!(result, format!("{}\n", input.lines().collect::<Vec<_>>().join("\n")));
    }

    #[test]
    fn test_update_lib_rs_no_markers_prepends() {
        let dir = std::env::temp_dir().join("nextsql_update_lib_no_marker");
        let _ = std::fs::create_dir_all(&dir);
        let lib_path = dir.join("lib.rs");

        let existing = "fn user_code() {}\n";
        std::fs::write(&lib_path, existing).unwrap();

        let names = vec!["users".to_string()];
        let result = update_lib_rs(&lib_path, &names);

        assert!(result.contains("fn user_code() {}"));
        assert!(result.contains("pub use generated::users;"));
        // Markers should be at the top
        assert!(result.starts_with(NEXTSQL_BEGIN_MARKER));
    }
}
