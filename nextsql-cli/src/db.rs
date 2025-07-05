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
            password: "password".to_string(),
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
        // コメント行と空行を除去してSQLコマンドを分割
        let cleaned_content = sql_content
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
            
        let sql_commands: Vec<String> = cleaned_content
            .split(';')
            .filter(|cmd| !cmd.trim().is_empty())
            .map(|cmd| cmd.trim().to_string())
            .collect();

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_connection_string() {
        let config = DatabaseConfig::default();
        assert_eq!(
            config.connection_string(),
            "postgres://nextsql:password@localhost:5438/nextsql"
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
}