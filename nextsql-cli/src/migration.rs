use std::fs;
use std::path::{Path, PathBuf};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationInfo {
    pub timestamp: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug)]
pub struct MigrationManager {
    pub migrations_dir: PathBuf,
}

impl MigrationManager {
    pub fn new(migrations_dir: impl AsRef<Path>) -> Self {
        Self {
            migrations_dir: migrations_dir.as_ref().to_path_buf(),
        }
    }

    pub fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.migrations_dir.exists() {
            fs::create_dir_all(&self.migrations_dir)?;
            println!("Created migrations directory: {}", self.migrations_dir.display());
        }
        Ok(())
    }

    pub fn generate_migration(&self, name: &str, description: Option<&str>) -> Result<MigrationInfo, Box<dyn std::error::Error>> {
        self.init()?;
        
        let now: DateTime<Local> = Local::now();
        let timestamp = now.format("%Y%m%d%H%M%S").to_string();
        let migration_name = format!("{timestamp}_{name}");
        let migration_dir = self.migrations_dir.join(&migration_name);
        
        if migration_dir.exists() {
            return Err(format!("Migration directory already exists: {}", migration_dir.display()).into());
        }

        fs::create_dir_all(&migration_dir)?;
        
        let up_sql_path = migration_dir.join("up.sql");
        let down_sql_path = migration_dir.join("down.sql");
        
        let up_sql_content = format!(
            "-- Migration: {}\n-- Created at: {}\n{}\n\n-- Write your up migration here\n",
            name,
            now.format("%Y-%m-%d %H:%M:%S"),
            description.map(|d| format!("-- Description: {d}")).unwrap_or_default()
        );
        
        let down_sql_content = format!(
            "-- Migration: {}\n-- Created at: {}\n{}\n\n-- Write your down migration here\n",
            name,
            now.format("%Y-%m-%d %H:%M:%S"),
            description.map(|d| format!("-- Description: {d}")).unwrap_or_default()
        );
        
        fs::write(&up_sql_path, up_sql_content)?;
        fs::write(&down_sql_path, down_sql_content)?;
        
        println!("Created migration: {}", migration_dir.display());
        println!("  up.sql: {}", up_sql_path.display());
        println!("  down.sql: {}", down_sql_path.display());
        
        Ok(MigrationInfo {
            timestamp: migration_name,
            name: name.to_string(),
            description: description.map(|s| s.to_string()),
        })
    }

    pub fn list_migrations(&self) -> Result<Vec<MigrationInfo>, Box<dyn std::error::Error>> {
        if !self.migrations_dir.exists() {
            return Ok(Vec::new());
        }

        let mut migrations = Vec::new();
        
        for entry in fs::read_dir(&self.migrations_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if is_valid_migration_timestamp(dir_name) {
                        let up_sql = path.join("up.sql");
                        let down_sql = path.join("down.sql");
                        
                        if up_sql.exists() && down_sql.exists() {
                            migrations.push(MigrationInfo {
                                timestamp: dir_name.to_string(),
                                name: extract_migration_name(&up_sql).unwrap_or_else(|| dir_name.to_string()),
                                description: None,
                            });
                        }
                    }
                }
            }
        }
        
        migrations.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(migrations)
    }

    pub fn get_migration_path(&self, timestamp: &str) -> PathBuf {
        self.migrations_dir.join(timestamp)
    }

    pub fn run_migration(&self, timestamp: &str, direction: MigrationDirection) -> Result<(), Box<dyn std::error::Error>> {
        let migration_path = self.get_migration_path(timestamp);
        
        if !migration_path.exists() {
            return Err(format!("Migration not found: {timestamp}").into());
        }

        let sql_file = match direction {
            MigrationDirection::Up => migration_path.join("up.sql"),
            MigrationDirection::Down => migration_path.join("down.sql"),
        };

        if !sql_file.exists() {
            return Err(format!("SQL file not found: {}", sql_file.display()).into());
        }

        let sql_content = fs::read_to_string(&sql_file)?;
        println!("Running migration {timestamp} ({direction})");
        println!("SQL content:\n{sql_content}");
        
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MigrationDirection {
    Up,
    Down,
}

impl std::fmt::Display for MigrationDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationDirection::Up => write!(f, "up"),
            MigrationDirection::Down => write!(f, "down"),
        }
    }
}

fn is_valid_migration_timestamp(migration_name: &str) -> bool {
    let parts: Vec<&str> = migration_name.split('_').collect();
    if parts.len() < 2 {
        return false;
    }
    
    let timestamp = parts[0];
    if timestamp.len() != 14 {
        return false;
    }
    
    timestamp.chars().all(char::is_numeric)
}

fn extract_migration_name(sql_file: &Path) -> Option<String> {
    if let Ok(content) = fs::read_to_string(sql_file) {
        for line in content.lines() {
            if line.starts_with("-- Migration: ") {
                return Some(line.trim_start_matches("-- Migration: ").to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_migration_manager_init() {
        let temp_dir = tempdir().unwrap();
        let migrations_dir = temp_dir.path().join("migrations");
        
        let manager = MigrationManager::new(&migrations_dir);
        manager.init().unwrap();
        
        assert!(migrations_dir.exists());
    }

    #[test]
    fn test_is_valid_migration_timestamp() {
        assert!(is_valid_migration_timestamp("20240101123045_create_users"));
        assert!(is_valid_migration_timestamp("20241231235959_add_posts"));
        assert!(!is_valid_migration_timestamp("2024011123045_invalid"));
        assert!(!is_valid_migration_timestamp("20240101_no_time"));
        assert!(!is_valid_migration_timestamp("invalid_timestamp"));
        assert!(!is_valid_migration_timestamp("create_users"));
    }

    #[test]
    fn test_generate_migration() {
        let temp_dir = tempdir().unwrap();
        let migrations_dir = temp_dir.path().join("migrations");
        
        let manager = MigrationManager::new(&migrations_dir);
        let migration = manager.generate_migration("create_users_table", Some("Create users table")).unwrap();
        
        assert!(!migration.timestamp.is_empty());
        assert_eq!(migration.name, "create_users_table");
        assert_eq!(migration.description, Some("Create users table".to_string()));
        
        let migration_path = migrations_dir.join(&migration.timestamp);
        assert!(migration_path.exists());
        assert!(migration_path.join("up.sql").exists());
        assert!(migration_path.join("down.sql").exists());
    }
}