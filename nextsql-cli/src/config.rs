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

    fn validate_config(config: &NextSqlConfig) -> Result<(), Box<dyn std::error::Error>> {
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

    pub fn init_project<P: AsRef<Path>>(path: P) -> Result<(), Box<dyn std::error::Error>> {
        // ディレクトリが存在しない場合は作成
        if !path.as_ref().exists() {
            std::fs::create_dir_all(&path)?;
        }

        let config_path = path.as_ref().join("next-sql.toml");

        if !config_path.exists() {
            // next-sql.toml を手書きで生成（database_url を空でも含めるため）
            let config_content = r#"# NextSQL Configuration File

# Database connection URL for schema loading and type validation
database_url = ""

[files]
# Glob patterns for NextSQL files to include
includes = ["**"]

[target]
# Target language for code generation
target_language = "rust"

# Output directory for generated code
target_directory = "."
"#;
            fs::write(&config_path, config_content)?;
            println!("Created next-sql.toml with default configuration");
        }

        // .nsql/ ディレクトリを作成
        let nsql_dir = path.as_ref().join(".nsql");
        fs::create_dir_all(&nsql_dir)?;

        // .gitignore に .nsql/ を追加
        let gitignore_path = path.as_ref().join(".gitignore");
        let needs_entry = if gitignore_path.exists() {
            let content = fs::read_to_string(&gitignore_path)?;
            !content.lines().any(|line| line.trim() == ".nsql")
        } else {
            true
        };
        if needs_entry {
            use std::io::Write;
            let mut f = fs::OpenOptions::new().create(true).append(true).open(&gitignore_path)?;
            writeln!(f, ".nsql")?;
        }

        println!("Initialized NextSQL project at {}", path.as_ref().display());

        // .claude ディレクトリを探して、見つかればスキルを配置
        let absolute_path = fs::canonicalize(path.as_ref())?;
        if let Some(claude_dir) = Self::find_claude_dir(&absolute_path) {
            Self::install_skill(&claude_dir)?;
        }

        Ok(())
    }

    /// 親ディレクトリを遡って .claude ディレクトリを探す
    /// ホームディレクトリ直下の ~/.claude（グローバル設定）は除外する
    fn find_claude_dir(start: &Path) -> Option<std::path::PathBuf> {
        let home_dir = dirs::home_dir();
        let mut current = Some(start);
        while let Some(dir) = current {
            let claude_dir = dir.join(".claude");
            if claude_dir.is_dir() {
                // ~/.claude（グローバル）はスキップ
                if let Some(ref home) = home_dir {
                    if dir == home.as_path() {
                        return None;
                    }
                }
                return Some(claude_dir);
            }
            current = dir.parent();
        }
        None
    }

    fn install_skill(claude_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let skill_dir = claude_dir.join("skills/learn-next-sql");
        fs::create_dir_all(&skill_dir)?;

        let skill_content = include_str!(concat!(env!("OUT_DIR"), "/skill.md"));
        let skill_path = skill_dir.join("SKILL.md");
        fs::write(&skill_path, skill_content)?;

        println!("Installed Claude Code skill at {}", skill_path.display());
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
                target_directory: ".".to_string(),
                package_name: None,
            },
            codegen: None,
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
                target_directory: ".".to_string(),
                package_name: None,
            },
            codegen: None,
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
                target_directory: ".".to_string(),
                package_name: None,
            },
            codegen: None,
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
                target_directory: ".".to_string(),
                package_name: None,
            },
            codegen: None,
        };
        assert!(invalid_config.save_to_file(&file_path).is_err());
    }
}