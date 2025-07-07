use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NextSqlConfig {
    pub database_url: Option<String>,
    pub files: FilesConfig,
    pub target: TargetConfig,
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
                target_directory: "../generated".to_string(),
            },
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

    fn validate_config(config: &NextSqlConfig) -> Result<(), Box<dyn std::error::Error>> {
        // TOMLをJSONに変換してスキーマ検証
        let json_value = serde_json::to_value(config)?;
        let schema_str = include_str!("../schemas/next-sql.schema.json");
        let schema: serde_json::Value = serde_json::from_str(schema_str)?;
        
        let compiled_schema = jsonschema::JSONSchema::compile(&schema)
            .map_err(|e| format!("Schema compilation failed: {}", e))?;
            
        if let Err(errors) = compiled_schema.validate(&json_value) {
            let error_messages: Vec<String> = errors
                .map(|error| format!("Validation error: {}", error))
                .collect();
            return Err(format!("Configuration validation failed:\n{}", error_messages.join("\n")).into());
        }
        
        Ok(())
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        Self::validate_config(self)?;
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    pub fn init_project<P: AsRef<Path>>(path: P) -> Result<(), Box<dyn std::error::Error>> {
        // ディレクトリが存在しない場合は作成
        if !path.as_ref().exists() {
            std::fs::create_dir_all(&path)?;
        }
        
        let config_path = path.as_ref().join("next-sql.toml");
        
        if config_path.exists() {
            return Err("next-sql.toml already exists".into());
        }

        let config = NextSqlConfig::default();
        config.save_to_file(&config_path)?;
        
        println!("Initialized NextSQL project at {}", path.as_ref().display());
        println!("Created next-sql.toml with default configuration");
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_valid_config_validation() {
        let config = NextSqlConfig {
            database_url: Some("postgresql://localhost:5432/test".to_string()),
            files: FilesConfig {
                includes: vec!["src/**".to_string()],
            },
            target: TargetConfig {
                target_language: "rust".to_string(),
                target_directory: "../generated".to_string(),
            },
        };
        
        assert!(NextSqlConfig::validate_config(&config).is_ok());
    }

    #[test]
    fn test_invalid_target_language() {
        let config = NextSqlConfig {
            database_url: None,
            files: FilesConfig {
                includes: vec!["src/**".to_string()],
            },
            target: TargetConfig {
                target_language: "invalid_language".to_string(),
                target_directory: "../generated".to_string(),
            },
        };
        
        let result = NextSqlConfig::validate_config(&config);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        println!("Error message: {}", error_msg);
        assert!(error_msg.contains("validation") || error_msg.contains("enum"));
    }

    #[test]
    fn test_load_from_file_with_validation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.toml");
        
        // 有効な設定ファイルを作成
        std::fs::write(&file_path, r#"
[files]
includes = ["src/**"]

[target]
target_language = "rust"
target_directory = "../generated"
"#).unwrap();
        
        let result = NextSqlConfig::load_from_file(&file_path);
        if let Err(e) = &result {
            println!("Error loading valid config: {}", e);
        }
        assert!(result.is_ok());
        
        // 無効な設定ファイルを作成
        std::fs::write(&file_path, r#"
[files]
includes = ["src/**"]

[target]
target_language = "invalid"
target_directory = "../generated"
"#).unwrap();
        
        let result = NextSqlConfig::load_from_file(&file_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_save_to_file_with_validation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.toml");
        
        // 有効な設定
        let valid_config = NextSqlConfig {
            database_url: None,
            files: FilesConfig {
                includes: vec!["src/**".to_string()],
            },
            target: TargetConfig {
                target_language: "rust".to_string(),
                target_directory: "../generated".to_string(),
            },
        };
        assert!(valid_config.save_to_file(&file_path).is_ok());
        
        // 無効な設定
        let invalid_config = NextSqlConfig {
            database_url: None,
            files: FilesConfig {
                includes: vec!["src/**".to_string()],
            },
            target: TargetConfig {
                target_language: "invalid".to_string(),
                target_directory: "../generated".to_string(),
            },
        };
        assert!(invalid_config.save_to_file(&file_path).is_err());
    }
}