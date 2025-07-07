mod config;
mod db;
mod migration;

use nextsql_core::*;

use clap::{Parser, Subcommand};
use config::NextSqlConfig;
use db::{DatabaseConfig, DatabaseMigrationManager};
use migration::{MigrationDirection, MigrationManager};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "nextsql")]
#[command(about = "NextSQL CLI tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new NextSQL project
    Init {
        /// Project directory path (default: current directory)
        dir: Option<PathBuf>,
    },
    /// Migration commands
    Migration {
        #[command(subcommand)]
        action: MigrationCommands,
    },
    /// Parse and validate NextSQL files
    Parse {
        /// Input file path
        file: PathBuf,
    },
    /// Validate NextSQL configuration file
    ValidateConfig {
        /// Config file path (default: next-sql.toml)
        #[arg(default_value = "next-sql.toml")]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum MigrationCommands {
    /// Initialize migrations directory
    Init {
        /// Migrations directory path (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Generate a new migration
    Generate {
        /// Migration name
        name: String,
        /// Migration description
        #[arg(short, long)]
        description: Option<String>,
        /// Migrations directory path (default: ./migrations)
        #[arg(long = "dir", default_value = "migrations")]
        migrations_dir: PathBuf,
    },
    /// List all migrations
    List {
        /// Migrations directory path (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Run migration up (file-based)
    Up {
        /// Migration timestamp (optional, runs all pending if not specified)
        timestamp: Option<String>,
        /// Migrations directory path (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Run migration down (file-based)
    Down {
        /// Migration timestamp (required)
        timestamp: String,
        /// Migrations directory path (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
    },
    /// Run migration up against database
    DbUp {
        /// Migration timestamp (optional, runs all pending if not specified)
        timestamp: Option<String>,
        /// Migrations directory path (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
        /// Database host
        #[arg(long, default_value = "localhost")]
        host: String,
        /// Database port
        #[arg(long, default_value = "5438")]
        port: u16,
        /// Database name
        #[arg(long, default_value = "nextsql")]
        database: String,
        /// Database username
        #[arg(long, default_value = "nextsql")]
        username: String,
        /// Database password
        #[arg(long, default_value = "password")]
        password: String,
    },
    /// Run migration down against database
    DbDown {
        /// Migration timestamp (required)
        timestamp: String,
        /// Migrations directory path (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        dir: PathBuf,
        /// Database host
        #[arg(long, default_value = "localhost")]
        host: String,
        /// Database port
        #[arg(long, default_value = "5438")]
        port: u16,
        /// Database name
        #[arg(long, default_value = "nextsql")]
        database: String,
        /// Database username
        #[arg(long, default_value = "nextsql")]
        username: String,
        /// Database password
        #[arg(long, default_value = "password")]
        password: String,
    },
    /// Show database migration status
    DbStatus {
        /// Database host
        #[arg(long, default_value = "localhost")]
        host: String,
        /// Database port
        #[arg(long, default_value = "5438")]
        port: u16,
        /// Database name
        #[arg(long, default_value = "nextsql")]
        database: String,
        /// Database username
        #[arg(long, default_value = "nextsql")]
        username: String,
        /// Database password
        #[arg(long, default_value = "password")]
        password: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { dir } => {
            if let Err(e) = handle_init_command(dir) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Migration { action } => {
            if let Err(e) = handle_migration_command(action).await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Parse { file } => {
            if let Err(e) = handle_parse_command(file) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::ValidateConfig { config } => {
            if let Err(e) = handle_validate_config_command(config) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn handle_init_command(dir: Option<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let target_dir = dir.unwrap_or_else(|| PathBuf::from("."));
    NextSqlConfig::init_project(target_dir)
}

fn handle_validate_config_command(config_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    println!("Validating configuration file: {}", config_path.display());
    
    match NextSqlConfig::load_from_file(&config_path) {
        Ok(_) => {
            println!("✓ Configuration is valid");
            Ok(())
        }
        Err(e) => {
            println!("✗ Configuration validation failed:");
            Err(e)
        }
    }
}

async fn handle_migration_command(
    action: MigrationCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        MigrationCommands::Init { dir } => {
            let manager = MigrationManager::new(dir);
            manager.init()?;
            println!("Initialized migrations directory");
        }
        MigrationCommands::Generate {
            name,
            description,
            migrations_dir,
        } => {
            let manager = MigrationManager::new(migrations_dir);
            let migration = manager.generate_migration(&name, description.as_deref())?;
            println!("Generated migration: {}", migration.timestamp);
        }
        MigrationCommands::List { dir } => {
            let manager = MigrationManager::new(dir);
            let migrations = manager.list_migrations()?;

            if migrations.is_empty() {
                println!("No migrations found");
            } else {
                println!("Migrations:");
                for migration in migrations {
                    println!("  {}", migration.timestamp);
                }
            }
        }
        MigrationCommands::Up { timestamp, dir } => {
            let manager = MigrationManager::new(dir);

            if let Some(ts) = timestamp {
                manager.run_migration(&ts, MigrationDirection::Up)?;
            } else {
                let migrations = manager.list_migrations()?;
                println!("Running all pending migrations...");
                for migration in migrations {
                    manager.run_migration(&migration.timestamp, MigrationDirection::Up)?;
                }
                println!("All migrations completed");
            }
        }
        MigrationCommands::Down { timestamp, dir } => {
            let manager = MigrationManager::new(dir);
            manager.run_migration(&timestamp, MigrationDirection::Down)?;
        }
        MigrationCommands::DbUp {
            timestamp,
            dir,
            host,
            port,
            database,
            username,
            password,
        } => {
            let config = DatabaseConfig {
                host,
                port,
                database,
                username,
                password,
            };
            let db_manager = DatabaseMigrationManager::new(&config).await?;
            let file_manager = MigrationManager::new(dir);

            if let Some(ts) = timestamp {
                // 特定のマイグレーションを実行
                let migration_path = file_manager.get_migration_path(&ts);
                let up_sql_path = migration_path.join("up.sql");

                if !up_sql_path.exists() {
                    return Err(
                        format!("Migration file not found: {}", up_sql_path.display()).into(),
                    );
                }

                db_manager.execute_migration_up(&ts, &up_sql_path).await?;
            } else {
                // 未実行のマイグレーションをすべて実行
                let file_migrations = file_manager.list_migrations()?;
                let executed_migrations = db_manager.get_executed_migrations().await?;
                let executed_names: std::collections::HashSet<String> = executed_migrations
                    .into_iter()
                    .map(|m| m.migration_name)
                    .collect();

                let pending_migrations: Vec<_> = file_migrations
                    .into_iter()
                    .filter(|m| !executed_names.contains(&m.timestamp))
                    .collect();

                if pending_migrations.is_empty() {
                    println!("No pending migrations to execute");
                } else {
                    println!(
                        "Executing {} pending migrations...",
                        pending_migrations.len()
                    );
                    for migration in pending_migrations {
                        let migration_path = file_manager.get_migration_path(&migration.timestamp);
                        let up_sql_path = migration_path.join("up.sql");

                        if up_sql_path.exists() {
                            db_manager
                                .execute_migration_up(&migration.timestamp, &up_sql_path)
                                .await?;
                        } else {
                            eprintln!(
                                "Warning: up.sql not found for migration {}",
                                migration.timestamp
                            );
                        }
                    }
                    println!("All pending migrations completed");
                }
            }
        }
        MigrationCommands::DbDown {
            timestamp,
            dir,
            host,
            port,
            database,
            username,
            password,
        } => {
            let config = DatabaseConfig {
                host,
                port,
                database,
                username,
                password,
            };
            let db_manager = DatabaseMigrationManager::new(&config).await?;
            let file_manager = MigrationManager::new(dir);

            let migration_path = file_manager.get_migration_path(&timestamp);
            let down_sql_path = migration_path.join("down.sql");

            if !down_sql_path.exists() {
                return Err(
                    format!("Migration file not found: {}", down_sql_path.display()).into(),
                );
            }

            db_manager
                .execute_migration_down(&timestamp, &down_sql_path)
                .await?;
        }
        MigrationCommands::DbStatus {
            host,
            port,
            database,
            username,
            password,
        } => {
            let config = DatabaseConfig {
                host,
                port,
                database,
                username,
                password,
            };
            let db_manager = DatabaseMigrationManager::new(&config).await?;

            let migration_records = db_manager.get_migration_status().await?;

            if migration_records.is_empty() {
                println!("No migrations have been executed");
            } else {
                println!("Migration Status:");
                println!(
                    "{:<30} {:<20} {:<10} {}",
                    "Migration", "Executed At", "Success", "Error"
                );
                println!("{:-<80}", "");

                for record in migration_records {
                    let error_msg = record.error_message.unwrap_or_else(|| "-".to_string());
                    println!(
                        "{:<30} {:<20} {:<10} {}",
                        record.migration_name,
                        record.executed_at.format("%Y-%m-%d %H:%M:%S"),
                        record.success,
                        error_msg
                    );
                }
            }
        }
    }

    Ok(())
}

fn handle_parse_command(file: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(&file)?;

    match parser::parse_module(&content) {
        Ok(module) => {
            println!("Parsing successful!");
            println!("Module contains {} top-level items", module.toplevels.len());

            for (i, toplevel) in module.toplevels.iter().enumerate() {
                match toplevel {
                    ast::TopLevel::Query(query) => {
                        println!("  {}: Query '{}'", i + 1, query.decl.name);
                    }
                    ast::TopLevel::Mutation(mutation) => {
                        println!("  {}: Mutation '{}'", i + 1, mutation.decl.name);
                    }
                    ast::TopLevel::With(with_statement) => {
                        println!("  {}: With '{}'", i + 1, with_statement.name);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
