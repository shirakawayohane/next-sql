use std::path::{Path, PathBuf};

use nextsql_core::schema::DatabaseSchema;

pub struct CodegenConfig {
    /// Source directory containing .nsql files
    pub source_dir: PathBuf,
    /// Output directory for generated files
    pub output_dir: PathBuf,
    /// Target backend (currently only "rust")
    pub backend: String,
}

pub struct CodegenResult {
    pub generated_files: Vec<PathBuf>,
    pub errors: Vec<String>,
}

/// Main entry point for code generation.
///
/// Finds all .nsql files in the configured source directory, parses them,
/// merges type definitions from types.nsql, dispatches to the appropriate
/// backend, and writes the generated files.
pub fn generate(config: &CodegenConfig, schema: &DatabaseSchema) -> CodegenResult {
    let mut result = CodegenResult {
        generated_files: Vec::new(),
        errors: Vec::new(),
    };

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

            // Generate each module
            for (module_name, module) in &parsed_modules {
                let generated =
                    nextsql_backend_rust::rust_gen::generate_rust_file_with_options(
                        module, schema, skip_inline_valtypes,
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
