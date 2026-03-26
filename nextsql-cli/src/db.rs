use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::fs;
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MigrationRecord {
    pub id: Uuid,
    pub migration_name: String,
    pub executed_at: DateTime<Utc>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5438,
            database: "nextsql".to_string(),
            username: "nextsql".to_string(),
            password: String::new(),
        }
    }
}

impl DatabaseConfig {
    pub fn connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

pub struct DatabaseMigrationManager {
    pool: PgPool,
}

impl DatabaseMigrationManager {
    pub async fn new(config: &DatabaseConfig) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(&config.connection_string()).await?;
        
        let manager = Self { pool };
        manager.ensure_migration_table().await?;
        
        Ok(manager)
    }

    pub async fn ensure_migration_table(&self) -> Result<(), sqlx::Error> {
        let create_table_sql = r#"
            CREATE TABLE IF NOT EXISTS nextsql_migrations (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                migration_name VARCHAR(255) NOT NULL UNIQUE,
                executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                success BOOLEAN NOT NULL DEFAULT true,
                error_message TEXT
            )
        "#;

        sqlx::query(create_table_sql).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_executed_migrations(&self) -> Result<Vec<MigrationRecord>, sqlx::Error> {
        let records = sqlx::query_as::<_, MigrationRecord>(
            "SELECT id, migration_name, executed_at, success, error_message 
             FROM nextsql_migrations 
             WHERE success = true 
             ORDER BY executed_at ASC"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    pub async fn is_migration_executed(&self, migration_name: &str) -> Result<bool, sqlx::Error> {
        let row = sqlx::query(
            "SELECT COUNT(*) as count FROM nextsql_migrations 
             WHERE migration_name = $1 AND success = true"
        )
        .bind(migration_name)
        .fetch_one(&self.pool)
        .await?;

        let count: i64 = row.get("count");
        Ok(count > 0)
    }

    pub async fn execute_migration_up(&self, migration_name: &str, migration_file: &Path) -> Result<(), Box<dyn std::error::Error>> {
        // マイグレーションがすでに実行されているかチェック
        if self.is_migration_executed(migration_name).await? {
            return Err(format!("Migration {} has already been executed", migration_name).into());
        }

        // SQLファイルを読み込み
        let sql_content = fs::read_to_string(migration_file)?;
        
        // トランザクション開始
        let mut tx = self.pool.begin().await?;
        
        match self.execute_sql_content(&sql_content, &mut tx).await {
            Ok(_) => {
                // マイグレーション記録を追加
                self.record_migration_success(migration_name, &mut tx).await?;
                
                // コミット
                tx.commit().await?;
                println!("Successfully executed migration: {}", migration_name);
                Ok(())
            }
            Err(e) => {
                // ロールバック
                tx.rollback().await?;
                
                // エラー記録を追加
                self.record_migration_error(migration_name, &e.to_string()).await?;
                
                Err(format!("Failed to execute migration {}: {}", migration_name, e).into())
            }
        }
    }

    pub async fn execute_migration_down(&self, migration_name: &str, migration_file: &Path) -> Result<(), Box<dyn std::error::Error>> {
        // マイグレーションが実行されているかチェック
        if !self.is_migration_executed(migration_name).await? {
            return Err(format!("Migration {} has not been executed", migration_name).into());
        }

        // SQLファイルを読み込み
        let sql_content = fs::read_to_string(migration_file)?;
        
        // トランザクション開始
        let mut tx = self.pool.begin().await?;
        
        match self.execute_sql_content(&sql_content, &mut tx).await {
            Ok(_) => {
                // マイグレーション記録を削除
                sqlx::query("DELETE FROM nextsql_migrations WHERE migration_name = $1")
                    .bind(migration_name)
                    .execute(&mut *tx)
                    .await?;
                
                // コミット
                tx.commit().await?;
                println!("Successfully rolled back migration: {}", migration_name);
                Ok(())
            }
            Err(e) => {
                // ロールバック
                tx.rollback().await?;
                
                Err(format!("Failed to rollback migration {}: {}", migration_name, e).into())
            }
        }
    }

    async fn execute_sql_content(&self, sql_content: &str, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(), sqlx::Error> {
        let sql_commands = split_sql_statements(sql_content);

        for sql_command in sql_commands {
            if !sql_command.is_empty() {
                sqlx::query(&sql_command).execute(&mut **tx).await?;
            }
        }

        Ok(())
    }

    async fn record_migration_success(&self, migration_name: &str, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO nextsql_migrations (migration_name, success) VALUES ($1, true)"
        )
        .bind(migration_name)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn record_migration_error(&self, migration_name: &str, error_message: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO nextsql_migrations (migration_name, success, error_message) VALUES ($1, false, $2)"
        )
        .bind(migration_name)
        .bind(error_message)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_migration_status(&self) -> Result<Vec<MigrationRecord>, sqlx::Error> {
        let records = sqlx::query_as::<_, MigrationRecord>(
            "SELECT id, migration_name, executed_at, success, error_message 
             FROM nextsql_migrations 
             ORDER BY executed_at DESC"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }
}

/// Split SQL content into individual statements, correctly handling:
/// - Single-quoted string literals (semicolons and `--` inside are preserved)
/// - Dollar-quoted strings like `$$...$$` (semicolons and `--` inside are preserved)
/// - Line comments (`--` to end of line) outside of strings
fn split_sql_statements(sql_content: &str) -> Vec<String> {
    let mut statements: Vec<String> = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = sql_content.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Check for line comment (-- outside of strings)
        if i + 1 < len && chars[i] == '-' && chars[i + 1] == '-' {
            // Skip until end of line
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            // Preserve the newline as whitespace separator
            if i < len {
                current.push('\n');
                i += 1;
            }
            continue;
        }

        // Check for dollar-quoted string ($$...$$)
        if chars[i] == '$' {
            // Scan for the dollar-quote tag: $<optional_tag>$
            let tag_start = i;
            i += 1;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            if i < len && chars[i] == '$' {
                i += 1;
                let tag: String = chars[tag_start..i].iter().collect();
                current.push_str(&tag);
                // Read until the closing dollar-quote tag
                loop {
                    if i >= len {
                        break;
                    }
                    if chars[i] == '$' {
                        let candidate: String = chars[i..].iter().take(tag.len()).collect();
                        if candidate == tag {
                            current.push_str(&tag);
                            i += tag.len();
                            break;
                        }
                    }
                    current.push(chars[i]);
                    i += 1;
                }
                continue;
            }
            // Not a valid dollar-quote; push the consumed characters
            let consumed: String = chars[tag_start..i].iter().collect();
            current.push_str(&consumed);
            continue;
        }

        // Check for single-quoted string literal
        if chars[i] == '\'' {
            current.push('\'');
            i += 1;
            while i < len {
                if chars[i] == '\'' {
                    current.push('\'');
                    i += 1;
                    // Handle escaped quote ('')
                    if i < len && chars[i] == '\'' {
                        current.push('\'');
                        i += 1;
                        continue;
                    }
                    break;
                }
                current.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // Statement separator
        if chars[i] == ';' {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                statements.push(trimmed);
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(chars[i]);
        i += 1;
    }

    // Push any remaining statement
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        statements.push(trimmed);
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_connection_string() {
        let config = DatabaseConfig::default();
        assert_eq!(
            config.connection_string(),
            "postgres://nextsql:@localhost:5438/nextsql"
        );
    }

    #[test]
    fn test_custom_database_config() {
        let config = DatabaseConfig {
            host: "example.com".to_string(),
            port: 5432,
            database: "test_db".to_string(),
            username: "test_user".to_string(),
            password: "test_pass".to_string(),
        };

        assert_eq!(
            config.connection_string(),
            "postgres://test_user:test_pass@example.com:5432/test_db"
        );
    }

    #[test]
    fn test_split_simple_statements() {
        let sql = "CREATE TABLE t (id INT); INSERT INTO t VALUES (1);";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1)"]);
    }

    #[test]
    fn test_split_semicolon_in_single_quoted_string() {
        let sql = "INSERT INTO t VALUES ('a;b'); SELECT 1;";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["INSERT INTO t VALUES ('a;b')", "SELECT 1"]);
    }

    #[test]
    fn test_split_escaped_quotes_in_string() {
        let sql = "INSERT INTO t VALUES ('it''s;here'); SELECT 1;";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["INSERT INTO t VALUES ('it''s;here')", "SELECT 1"]);
    }

    #[test]
    fn test_split_dollar_quoted_string() {
        let sql = "CREATE FUNCTION f() RETURNS void AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql; SELECT 1;";
        let result = split_sql_statements(sql);
        assert_eq!(result.len(), 2);
        assert!(result[0].contains("BEGIN NULL; END;"));
        assert_eq!(result[1], "SELECT 1");
    }

    #[test]
    fn test_split_tagged_dollar_quote() {
        let sql = "CREATE FUNCTION f() RETURNS void AS $fn$ BEGIN NULL; END; $fn$ LANGUAGE plpgsql; SELECT 1;";
        let result = split_sql_statements(sql);
        assert_eq!(result.len(), 2);
        assert!(result[0].contains("BEGIN NULL; END;"));
    }

    #[test]
    fn test_split_strips_line_comments() {
        let sql = "-- this is a comment\nSELECT 1;\n-- another comment\nSELECT 2;";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_split_preserves_double_dash_in_string() {
        let sql = "INSERT INTO t VALUES ('--not a comment'); SELECT 1;";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["INSERT INTO t VALUES ('--not a comment')", "SELECT 1"]);
    }

    #[test]
    fn test_split_no_trailing_semicolon() {
        let sql = "SELECT 1";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1"]);
    }

    #[test]
    fn test_split_empty_input() {
        let result = split_sql_statements("");
        assert!(result.is_empty());
    }
}