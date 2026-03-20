use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NextSqlConfig {
    pub database_url: Option<String>,
    pub files: FilesConfig,
    pub target: TargetConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codegen: Option<CodegenConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodegenConfig {
    /// Naming pattern for insert param structs. Default: "Insert{Table}Params"
    pub insert_params: Option<String>,
    /// Naming pattern for update changeset structs. Default: "Update{Table}Params"
    pub update_params: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesConfig {
    pub includes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub target_language: String,
    pub target_directory: String,
    /// Package name for the generated project manifest (e.g. Cargo.toml for Rust,
    /// package.json for TypeScript). If set, a manifest will be generated with the
    /// necessary dependencies and lint suppression for generated code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_name: Option<String>,
}

impl Default for NextSqlConfig {
    fn default() -> Self {
        Self {
            database_url: None,
            files: FilesConfig {
                includes: vec!["**".to_string()],
            },
            target: TargetConfig {
                target_language: "rust".to_string(),
                target_directory: ".".to_string(),
                package_name: None,
            },
            codegen: None,
        }
    }
}

impl NextSqlConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: NextSqlConfig = toml::from_str(&content)?;
        Self::validate_config(&config)?;
        Ok(config)
    }

    pub fn validate_config(config: &NextSqlConfig) -> Result<(), Box<dyn std::error::Error>> {
        const SUPPORTED_LANGUAGES: &[&str] = &["rust"];
        if !SUPPORTED_LANGUAGES.contains(&config.target.target_language.as_str()) {
            return Err(format!(
                "Configuration validation failed:\nValidation error: unsupported target_language '{}', expected one of: {}",
                config.target.target_language,
                SUPPORTED_LANGUAGES.join(", ")
            ).into());
        }
        Ok(())
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        Self::validate_config(self)?;
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}
