use nextsql_core::{DatabaseSchema, SchemaLoader};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::path::{Path, PathBuf};
use std::collections::HashMap;

#[derive(Debug)]
pub struct SchemaCache {
    schemas: Arc<RwLock<HashMap<PathBuf, DatabaseSchema>>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_schema_for_file(&self, file_path: &Path) -> Option<DatabaseSchema> {
        eprintln!("SchemaCache: get_schema_for_file called for: {:?}", file_path);
        
        // Find the project root by looking for next-sql.toml
        let project_root = match self.find_project_root(file_path) {
            Some(root) => {
                eprintln!("SchemaCache: Found project root: {:?}", root);
                root
            }
            None => {
                eprintln!("SchemaCache: No project root found for: {:?}", file_path);
                return None;
            }
        };
        
        // Check if schema is already cached
        {
            let schemas = self.schemas.read().await;
            if let Some(schema) = schemas.get(&project_root) {
                eprintln!("SchemaCache: Schema found in cache");
                return Some(schema.clone());
            }
        }
        
        eprintln!("SchemaCache: Schema not in cache, attempting to load");
        
        // Try to load schema
        match self.load_schema_for_project(&project_root).await {
            Ok(schema) => {
                eprintln!("SchemaCache: Schema loaded successfully");
                // Cache the schema
                let mut schemas = self.schemas.write().await;
                schemas.insert(project_root, schema.clone());
                Some(schema)
            }
            Err(e) => {
                eprintln!("SchemaCache: Failed to load schema: {}", e);
                None
            }
        }
    }

    pub async fn load_schema_for_project(&self, project_root: &Path) -> Result<DatabaseSchema, String> {
        // Check if we already have the schema cached
        {
            let schemas = self.schemas.read().await;
            if let Some(schema) = schemas.get(project_root) {
                return Ok(schema.clone());
            }
        }

        // Try to load from cached JSON file first
        let schema_cache_path = project_root.join(".nextsql").join("schema.json");
        if schema_cache_path.exists() {
            eprintln!("SchemaCache: Trying to load from cache: {:?}", schema_cache_path);
            if let Ok(contents) = std::fs::read_to_string(&schema_cache_path) {
                if let Ok(schema) = SchemaLoader::load_from_json(&contents) {
                    let mut schemas = self.schemas.write().await;
                    schemas.insert(project_root.to_path_buf(), schema.clone());
                    return Ok(schema);
                }
            }
        }
        
        // Try to load from development schema file
        let dev_schema_path = project_root.join("schemas").join("users.json");
        if dev_schema_path.exists() {
            eprintln!("SchemaCache: Trying to load from development schema: {:?}", dev_schema_path);
            if let Ok(contents) = std::fs::read_to_string(&dev_schema_path) {
                if let Ok(schema) = SchemaLoader::load_from_json(&contents) {
                    eprintln!("SchemaCache: Successfully loaded development schema");
                    let mut schemas = self.schemas.write().await;
                    schemas.insert(project_root.to_path_buf(), schema.clone());
                    return Ok(schema);
                } else {
                    eprintln!("SchemaCache: Failed to parse development schema");
                }
            }
        }

        // If no cached schema, try to connect to database and load
        let config_path = project_root.join("next-sql.toml");
        if !config_path.exists() {
            return Err("next-sql.toml not found".to_string());
        }

        let config_str = std::fs::read_to_string(&config_path)
            .map_err(|e| format!("Failed to read config: {}", e))?;
        
        let config: nextsql_cli::config::NextSqlConfig = toml::from_str(&config_str)
            .map_err(|e| format!("Failed to parse config: {}", e))?;

        // Parse database URL if available
        if let Some(db_url) = config.database_url {
            match self.load_schema_from_database(&db_url).await {
                Ok(schema) => {
                    // Cache the schema
                    let mut schemas = self.schemas.write().await;
                    schemas.insert(project_root.to_path_buf(), schema.clone());
                    
                    // Save to disk for next time
                    if let Err(e) = self.save_schema_cache(&schema_cache_path, &schema).await {
                        eprintln!("Failed to save schema cache: {}", e);
                    }
                    
                    Ok(schema)
                }
                Err(e) => Err(format!("Failed to load schema from database: {}", e)),
            }
        } else {
            Err("No database_url found in configuration".to_string())
        }
    }

    async fn load_schema_from_database(&self, db_url: &str) -> Result<DatabaseSchema, Box<dyn std::error::Error + Send + Sync>> {
        // Parse the connection string
        let config = db_url.parse::<postgres::Config>()?;
        
        // Connect to database
        let mut client = config.connect(postgres::NoTls)?;
        
        // Load schema
        let schema = SchemaLoader::load_from_database(&mut client)?;
        
        Ok(schema)
    }

    async fn save_schema_cache(&self, path: &Path, schema: &DatabaseSchema) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        let json = SchemaLoader::save_to_json(schema)?;
        std::fs::write(path, json)?;
        
        Ok(())
    }

    pub fn find_project_root(&self, file_path: &Path) -> Option<PathBuf> {
        eprintln!("SchemaCache: find_project_root called for: {:?}", file_path);
        
        let mut current = match file_path.parent() {
            Some(parent) => {
                eprintln!("SchemaCache: Starting search from parent: {:?}", parent);
                parent
            }
            None => {
                eprintln!("SchemaCache: No parent directory for: {:?}", file_path);
                return None;
            }
        };
        
        loop {
            let config_path = current.join("next-sql.toml");
            eprintln!("SchemaCache: Checking for config at: {:?}", config_path);
            if config_path.exists() {
                eprintln!("SchemaCache: Found config at: {:?}", current);
                return Some(current.to_path_buf());
            }
            
            current = match current.parent() {
                Some(parent) => parent,
                None => {
                    eprintln!("SchemaCache: Reached root without finding next-sql.toml");
                    return None;
                }
            };
        }
    }

    pub async fn invalidate_cache(&self, project_root: &Path) {
        let mut schemas = self.schemas.write().await;
        schemas.remove(project_root);
    }

    #[cfg(test)]
    pub async fn set_test_schema(&self, project_root: PathBuf, schema: DatabaseSchema) {
        let mut schemas = self.schemas.write().await;
        schemas.insert(project_root, schema);
    }
}