mod config;
mod db;
mod migration;

use nextsql_core::*;

use clap::{error::ErrorKind, CommandFactory, Parser, Subcommand};
use config::{NextSqlConfig, NextSqlConfigExt};
use db::{DatabaseConfig, DatabaseMigrationManager};
use migration::{MigrationDirection, MigrationManager};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "nsql")]
#[command(about = "NextSQL CLI tool")]
#[command(version, disable_version_flag = true)]
struct Cli {
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    version: (),
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new NextSQL project
    Init {
        /// Project directory path (default: current directory)
        dir: Option<PathBuf>,
        /// Skip automatic Claude Code skill installation
        #[arg(long)]
        no_skill: bool,
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
    /// Generate code from NextSQL files
    Gen {
        /// Project directory containing next-sql.toml (default: current directory)
        #[arg(default_value = ".")]
        dir: PathBuf,
    },
    /// Check NextSQL files for errors (without generating code)
    Check {
        /// Project directory containing next-sql.toml (default: current directory)
        #[arg(default_value = ".")]
        dir: PathBuf,
    },
    /// Validate NextSQL configuration file
    ValidateConfig {
        /// Config file path (default: next-sql.toml)
        #[arg(default_value = "next-sql.toml")]
        config: PathBuf,
    },
    /// Watch for file changes and re-run code generation
    Watch {
        /// Project directory containing next-sql.toml (default: current directory)
        #[arg(default_value = ".")]
        dir: PathBuf,
    },
    /// Update nsql to the latest version
    Update,
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
        /// Database password (can also be set via NEXTSQL_DB_PASSWORD env var)
        #[arg(long, env = "NEXTSQL_DB_PASSWORD")]
        password: Option<String>,
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
        /// Database password (can also be set via NEXTSQL_DB_PASSWORD env var)
        #[arg(long, env = "NEXTSQL_DB_PASSWORD")]
        password: Option<String>,
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
        /// Database password (can also be set via NEXTSQL_DB_PASSWORD env var)
        #[arg(long, env = "NEXTSQL_DB_PASSWORD")]
        password: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) if e.kind() == ErrorKind::InvalidSubcommand => {
            let _ = Cli::command().print_help();
            eprintln!();
            e.exit();
        }
        Err(e) => e.exit(),
    };

    match cli.command {
        Commands::Init { dir, no_skill } => {
            if let Err(e) = handle_init_command(dir, no_skill) {
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
        Commands::Gen { dir } => {
            if let Err(e) = handle_generate_command(dir) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Check { dir } => {
            if let Err(e) = handle_check_command(dir) {
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
        Commands::Watch { dir } => {
            if let Err(e) = handle_watch_command(dir) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Update => {
            if let Err(e) = handle_update_command().await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn handle_init_command(dir: Option<PathBuf>, no_skill: bool) -> Result<(), Box<dyn std::error::Error>> {
    let target_dir = dir.unwrap_or_else(|| PathBuf::from("."));
    NextSqlConfig::init_project(target_dir, !no_skill)
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
                password: password.unwrap_or_default(),
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
                password: password.unwrap_or_default(),
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
                password: password.unwrap_or_default(),
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

fn handle_generate_command(
    dir: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load config from next-sql.toml (required)
    let nsql_config_path = dir.join("next-sql.toml");
    let nsql_config = config::NextSqlConfig::load_from_file(&nsql_config_path)?;

    let db_schema = resolve_schema(&dir)?;

    let target_directory = &nsql_config.target.target_directory;
    let output_dir = dir.join(target_directory).join("src");

    let config = nextsql_codegen::CodegenConfig {
        source_dir: dir,
        output_dir,
        backend: nsql_config.target.target_language.clone(),
        insert_params_pattern: nsql_config.codegen.as_ref()
            .and_then(|c| c.insert_params.clone()),
        update_params_pattern: nsql_config.codegen.as_ref()
            .and_then(|c| c.update_params.clone()),
        package_name: nsql_config.target.package_name.clone(),
        type_files: nsql_config.files.type_files.clone(),
        runtime_crate_path: None,
    };

    let result = nextsql_codegen::generate(&config, &db_schema);

    for file in &result.generated_files {
        println!("Generated: {}", file.display());
    }

    if !result.errors.is_empty() {
        for err in &result.errors {
            eprintln!("Error: {}", err);
        }
        return Err(format!("{} error(s) during code generation", result.errors.len()).into());
    }

    println!(
        "Successfully generated {} files",
        result.generated_files.len()
    );
    Ok(())
}

fn handle_watch_command(dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
    use std::sync::mpsc;
    use std::time::Duration;

    // Validate config exists
    let nsql_config_path = dir.join("next-sql.toml");
    let nsql_config = config::NextSqlConfig::load_from_file(&nsql_config_path)?;
    let output_dir = dir.join(&nsql_config.target.target_directory);
    let output_dir_canonical = std::fs::canonicalize(&output_dir).unwrap_or(output_dir);

    let dir_display = std::fs::canonicalize(&dir)
        .unwrap_or_else(|_| dir.clone())
        .display()
        .to_string();

    eprintln!("[watch] Running initial generation...");
    if let Err(e) = handle_generate_command(dir.clone()) {
        eprintln!("[watch] {}", e);
    }

    eprintln!("[watch] Watching for changes in {}...", dir_display);

    let (tx, rx) = mpsc::channel();
    let mut debouncer = new_debouncer(Duration::from_millis(300), tx)?;

    debouncer
        .watcher()
        .watch(&dir, notify::RecursiveMode::Recursive)?;

    loop {
        match rx.recv() {
            Ok(Ok(events)) => {
                let has_relevant_change = events.iter().any(|event| {
                    if event.kind != DebouncedEventKind::Any {
                        return false;
                    }
                    let path = &event.path;

                    // Skip changes in output directory
                    if let Ok(canonical) = std::fs::canonicalize(path) {
                        if canonical.starts_with(&output_dir_canonical) {
                            return false;
                        }
                    }

                    let ext = path.extension().and_then(|e| e.to_str());
                    let name = path.file_name().and_then(|n| n.to_str());
                    ext == Some("nsql")
                        || name == Some("next-sql.toml")
                        || name == Some("schema.json")
                });

                if has_relevant_change {
                    eprintln!("\n[watch] Change detected, regenerating...");
                    if let Err(e) = handle_generate_command(dir.clone()) {
                        eprintln!("[watch] {}", e);
                    }
                }
            }
            Ok(Err(e)) => {
                eprintln!("[watch] Watcher error: {}", e);
            }
            Err(_) => {
                eprintln!("[watch] Watcher disconnected");
                break;
            }
        }
    }

    Ok(())
}

/// Resolve the schema for a given source directory.
/// Priority: database connection from next-sql.toml > schema.json file > empty schema
/// Schema cache with fingerprint for change detection.
#[derive(serde::Serialize, serde::Deserialize)]
struct SchemaCache {
    fingerprint: String,
    schema: nextsql_core::schema::DatabaseSchema,
}

fn resolve_schema(
    source: &Path,
) -> Result<nextsql_core::schema::DatabaseSchema, Box<dyn std::error::Error>> {
    use nextsql_core::schema::DatabaseSchema;
    use nextsql_core::SchemaLoader;

    // Find project root by walking up from source dir looking for next-sql.toml
    let source_abs = std::fs::canonicalize(source).unwrap_or_else(|_| source.to_path_buf());
    let project_root = find_project_root(&source_abs);

    let cache_path = project_root.as_ref().map(|root| root.join(".nsql/schema_cache.json"));

    if let Some(ref root) = project_root {
        let config_path = root.join("next-sql.toml");
        if config_path.exists() {
            if let Ok(config) = NextSqlConfig::load_from_file(&config_path) {
                if let Some(ref db_url) = config.database_url {
                    if !db_url.is_empty() {
                        let cp = cache_path.clone();
                        match tokio::task::block_in_place(|| {
                            let mut client = connect_database(db_url)?;
                            resolve_schema_with_cache(&mut client, &cp)
                        }) {
                            Ok(schema) => {
                                return Ok(schema);
                            }
                            Err(e) => {
                                eprintln!("Warning: could not load schema from database: {}", e);
                            }
                        }
                    }
                }
            }
        }

        // Fallback: try loading from schema.json in the project root
        let schema_json_path = root.join("schema.json");
        if schema_json_path.exists() {
            match std::fs::read_to_string(&schema_json_path) {
                Ok(json_str) => match SchemaLoader::load_from_json(&json_str) {
                    Ok(schema) => {
                        return Ok(schema);
                    }
                    Err(e) => {
                        eprintln!("Warning: could not parse schema.json: {}", e);
                    }
                },
                Err(e) => {
                    eprintln!("Warning: could not read schema.json: {}", e);
                }
            }
        }
    }

    // Fallback: try loading from .nsql/schema_cache.json
    if let Some(ref cp) = cache_path {
        if let Some(cache) = load_cache(cp) {
            return Ok(cache.schema);
        }
    }

    // Fallback: empty schema
    Ok(DatabaseSchema::new())
}

/// Compare DB fingerprint against cache. If unchanged, use cache; otherwise full-load and update.
/// Must be called within tokio::task::block_in_place because postgres crate uses block_on internally.
fn resolve_schema_with_cache(
    client: &mut postgres::Client,
    cache_path: &Option<PathBuf>,
) -> Result<nextsql_core::schema::DatabaseSchema, Box<dyn std::error::Error>> {
    use nextsql_core::SchemaLoader;

    let fingerprint = SchemaLoader::fetch_schema_fingerprint(client)?;

    // Check if cache matches
    if let Some(ref cp) = cache_path {
        if let Some(cache) = load_cache(cp) {
            if cache.fingerprint == fingerprint {
                return Ok(cache.schema);
            }
        }
    }

    // Cache miss or fingerprint mismatch — full load
    let schema = SchemaLoader::load_from_database(client)?;

    // Save cache
    if let Some(ref cp) = cache_path {
        let cache = SchemaCache {
            fingerprint,
            schema: schema.clone(),
        };
        if let Ok(json) = serde_json::to_string_pretty(&cache) {
            let _ = std::fs::create_dir_all(cp.parent().unwrap());
            let _ = std::fs::write(cp, json);
        }
    }

    Ok(schema)
}

fn load_cache(path: &Path) -> Option<SchemaCache> {
    let json_str = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&json_str).ok()
}

fn find_project_root(start: &Path) -> Option<PathBuf> {
    let mut current = start;
    loop {
        if current.join("next-sql.toml").exists() {
            return Some(current.to_path_buf());
        }
        // If start is a file, also check its parent
        current = current.parent()?;
    }
}

/// Connect to the database and return the client.
fn connect_database(db_url: &str) -> Result<postgres::Client, Box<dyn std::error::Error>> {
    let mut config = db_url.parse::<postgres::Config>()?;
    config.connect_timeout(std::time::Duration::from_secs(3));
    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(false)
        .build()?;
    let tls = postgres_native_tls::MakeTlsConnector::new(connector);
    let client = config.connect(tls)?;
    Ok(client)
}

fn handle_check_command(
    dir: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    use nextsql_codegen::{CheckConfig, DiagnosticSource};

    let db_schema = resolve_schema(&dir)?;

    let nsql_config_path = dir.join("next-sql.toml");
    let nsql_config = config::NextSqlConfig::load_from_file(&nsql_config_path)?;

    let config = CheckConfig {
        source_dir: dir,
        type_files: nsql_config.files.type_files.clone(),
    };

    let use_color = std::io::IsTerminal::is_terminal(&std::io::stderr());

    let red = if use_color { "\x1b[1;31m" } else { "" };
    let cyan = if use_color { "\x1b[1;36m" } else { "" };
    let green = if use_color { "\x1b[1;32m" } else { "" };
    let yellow = if use_color { "\x1b[1;33m" } else { "" };
    let bold = if use_color { "\x1b[1m" } else { "" };
    let reset = if use_color { "\x1b[0m" } else { "" };

    eprintln!("{green}    Checking{reset} nextsql files...");

    let result = nextsql_codegen::check(&config, &db_schema);

    if result.diagnostics.is_empty() {
        eprintln!(
            "{green}    Finished{reset} checking {} file(s). No errors found.",
            result.files_checked
        );
        Ok(())
    } else {
        for diag in &result.diagnostics {
            let (label, label_color) = match diag.source {
                DiagnosticSource::Parse => ("parse error", red),
                DiagnosticSource::TypeCheck => ("type error", red),
                DiagnosticSource::Io => ("io error", yellow),
            };

            let location = match (diag.line, diag.column) {
                (Some(l), Some(c)) => format!("{}:{}:{}", diag.file.display(), l, c),
                (Some(l), None) => format!("{}:{}", diag.file.display(), l),
                _ => format!("{}", diag.file.display()),
            };

            eprintln!(
                "{label_color}error[{label}]{reset}{bold}: {}{reset}",
                diag.message
            );
            eprintln!("  {cyan}-->{reset} {}", location);
            eprintln!();
        }

        let error_count = result.diagnostics.len();
        Err(format!(
            "{red}error{reset}: could not compile due to {bold}{}{reset} error{}",
            error_count,
            if error_count == 1 { "" } else { "s" }
        )
        .into())
    }
}

async fn handle_update_command() -> Result<(), Box<dyn std::error::Error>> {
    const REPO: &str = "shirakawayohane/next-sql";
    const GITHUB_API: &str = "https://api.github.com/repos";

    let current_version = env!("CARGO_PKG_VERSION");
    println!("Current version: v{}", current_version);
    println!("Checking for updates...");

    let target_triple = get_target_triple()?;

    let client = reqwest::Client::builder()
        .user_agent("nsql-updater")
        .build()?;

    let release: serde_json::Value = client
        .get(format!("{}/{}/releases/latest", GITHUB_API, REPO))
        .send()
        .await?
        .error_for_status()
        .map_err(|e| format!("Failed to fetch latest release: {}", e))?
        .json()
        .await?;

    let tag_name = release["tag_name"]
        .as_str()
        .ok_or("Missing tag_name in release")?;

    let latest_version = tag_name.trim_start_matches('v');
    if latest_version == current_version {
        println!("Already up to date (v{}).", current_version);
        return Ok(());
    }

    println!("New version available: {} (current: v{})", tag_name, current_version);

    let assets = release["assets"]
        .as_array()
        .ok_or("Missing assets in release")?;

    // Find the asset matching the current platform
    // cargo-dist names assets like: nextsql-cli-aarch64-apple-darwin.tar.xz
    let asset = assets
        .iter()
        .find(|a| {
            let name = a["name"].as_str().unwrap_or("");
            name.starts_with("nextsql-cli-")
                && name.contains(&target_triple)
                && !name.ends_with(".sha256")
        })
        .ok_or_else(|| format!("No release asset found for target: {}", target_triple))?;

    let asset_name = asset["name"].as_str().unwrap_or("unknown");
    let download_url = asset["browser_download_url"]
        .as_str()
        .ok_or("Missing download URL")?;

    // Find the corresponding .sha256 checksum file
    let sha256_asset = assets.iter().find(|a| {
        let name = a["name"].as_str().unwrap_or("");
        name == format!("{}.sha256", asset_name)
    });

    println!("Downloading {}...", asset_name);

    let response = client
        .get(download_url)
        .send()
        .await?
        .error_for_status()
        .map_err(|e| format!("Failed to download: {}", e))?;

    let bytes = response.bytes().await?;

    // Verify SHA256 checksum if .sha256 file is available
    if let Some(sha256_asset) = sha256_asset {
        let sha256_url = sha256_asset["browser_download_url"]
            .as_str()
            .ok_or("Missing download URL for .sha256 file")?;

        let sha256_response = client
            .get(sha256_url)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| format!("Failed to download .sha256 file: {}", e))?;

        let sha256_content = sha256_response.text().await?;

        // cargo-dist format: "<hash>  <filename>"
        let expected_hash = sha256_content
            .split_whitespace()
            .next()
            .ok_or("Invalid .sha256 file format: empty content")?;

        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        let computed_hash = format!("{:x}", hasher.finalize());

        if computed_hash != expected_hash {
            return Err(format!(
                "SHA256 checksum mismatch!
  Expected: {}
  Computed: {}",
                expected_hash, computed_hash
            )
            .into());
        }

        println!("SHA256 checksum verified.");
    } else {
        println!("Warning: No .sha256 checksum file found for this release. Skipping verification.");
    }

    let current_exe = std::env::current_exe()?;
    let tmp_dir = tempfile::tempdir()?;

    // Write archive to temp directory
    let archive_path = tmp_dir.path().join(asset_name);
    std::fs::write(&archive_path, &bytes)?;

    // Extract
    println!("Extracting...");
    if asset_name.ends_with(".tar.xz") || asset_name.ends_with(".tar.gz") {
        let status = std::process::Command::new("tar")
            .args(["xf", &archive_path.to_string_lossy(), "-C", &tmp_dir.path().to_string_lossy()])
            .status()?;
        if !status.success() {
            return Err("Failed to extract archive".into());
        }
    } else if asset_name.ends_with(".zip") {
        let archive_str = archive_path.to_string_lossy().into_owned();
        let dest_str = tmp_dir.path().to_string_lossy().into_owned();
        let status = std::process::Command::new("unzip")
            .args([&archive_str, "-d", &dest_str])
            .status()?;
        if !status.success() {
            return Err("Failed to extract archive".into());
        }
    } else {
        return Err(format!("Unsupported archive format: {}", asset_name).into());
    }

    // Find the nsql binary in extracted files
    let new_binary = find_binary_in_dir(tmp_dir.path(), "nsql")?;

    // Replace the current binary atomically
    // First, make the new binary executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&new_binary, std::fs::Permissions::from_mode(0o755))?;
    }

    // Use rename for atomic replacement. If cross-device, fall back to copy.
    let backup_path = current_exe.with_extension("old");
    // Move current binary to backup
    std::fs::rename(&current_exe, &backup_path)
        .or_else(|_| std::fs::copy(&current_exe, &backup_path).map(|_| ()))?;

    match std::fs::rename(&new_binary, &current_exe) {
        Ok(()) => {}
        Err(_) => {
            // Cross-device: copy instead
            if let Err(e) = std::fs::copy(&new_binary, &current_exe) {
                // Restore backup
                let _ = std::fs::rename(&backup_path, &current_exe);
                return Err(format!("Failed to install new binary: {}", e).into());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&current_exe, std::fs::Permissions::from_mode(0o755))?;
            }
        }
    }

    // Remove backup
    let _ = std::fs::remove_file(&backup_path);

    println!("Successfully updated to {}!", tag_name);
    Ok(())
}

fn get_target_triple() -> Result<String, Box<dyn std::error::Error>> {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;

    let triple = match (arch, os) {
        ("aarch64", "macos") => "aarch64-apple-darwin",
        ("x86_64", "macos") => "x86_64-apple-darwin",
        ("x86_64", "linux") => "x86_64-unknown-linux-gnu",
        ("x86_64", "windows") => "x86_64-pc-windows-msvc",
        _ => return Err(format!("Unsupported platform: {}-{}", arch, os).into()),
    };

    Ok(triple.to_string())
}

fn find_binary_in_dir(dir: &Path, name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    for entry in walkdir(dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy() == name
            || entry.file_name().to_string_lossy() == format!("{}.exe", name)
        {
            return Ok(entry.path().to_path_buf());
        }
    }
    Err(format!("Binary '{}' not found in extracted archive", name).into())
}

fn walkdir(
    dir: &Path,
) -> Result<
    impl Iterator<Item = Result<std::fs::DirEntry, std::io::Error>>,
    Box<dyn std::error::Error>,
> {
    let mut entries = Vec::new();
    collect_entries(dir, &mut entries)?;
    Ok(entries.into_iter().map(Ok))
}

fn collect_entries(
    dir: &Path,
    entries: &mut Vec<std::fs::DirEntry>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_entries(&entry.path(), entries)?;
        } else {
            entries.push(entry);
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
                    ast::TopLevel::Relation(relation) => {
                        println!("  {}: Relation '{}' for {}", i + 1, relation.decl.name, relation.decl.for_table);
                    }
                    ast::TopLevel::ValType(vt) => {
                        println!("  {}: ValType '{}'", i + 1, vt.name);
                    }
                    ast::TopLevel::Input(input) => {
                        println!("  {}: Input '{}' ({} fields)", i + 1, input.name, input.fields.len());
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
