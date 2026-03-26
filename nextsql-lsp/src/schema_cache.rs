use nextsql_core::{DatabaseSchema, SchemaLoader, ast::BuiltInType};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::path::{Path, PathBuf};
use std::collections::HashMap;

/// ファイルごとのValType定義キャッシュ
/// key: ファイルパス, value: Vec<(name, base_type)>
type ValTypeCache = HashMap<PathBuf, Vec<(String, BuiltInType)>>;

/// ValType名 → 定義位置 (ファイルパス, 行番号)
type ValTypeLocationCache = HashMap<String, (PathBuf, u32)>;

#[derive(Debug)]
pub struct SchemaCache {
    schemas: RwLock<HashMap<PathBuf, Arc<DatabaseSchema>>>,
    /// ファイルパス → そのファイル内のValType定義
    valtype_cache: RwLock<ValTypeCache>,
    /// ValType名 → 定義位置
    valtype_locations: RwLock<ValTypeLocationCache>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            valtype_cache: RwLock::new(HashMap::new()),
            valtype_locations: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_schema_for_file(&self, file_path: &Path) -> Option<Arc<DatabaseSchema>> {
        // Find the project root by looking for next-sql.toml
        let project_root = self.find_project_root(file_path)?;

        // Check if schema is already cached
        {
            let schemas = self.schemas.read().await;
            if let Some(schema) = schemas.get(&project_root) {
                return Some(Arc::clone(schema));
            }
        }

        // Try to load schema
        match self.load_schema_for_project(&project_root).await {
            Ok(schema) => Some(schema),
            Err(e) => {
                eprintln!("SchemaCache: Failed to load schema: {}", e);
                None
            }
        }
    }

    pub async fn load_schema_for_project(&self, project_root: &Path) -> Result<Arc<DatabaseSchema>, String> {
        // Check if we already have the schema cached
        {
            let schemas = self.schemas.read().await;
            if let Some(schema) = schemas.get(project_root) {
                return Ok(Arc::clone(schema));
            }
        }

        // Load from database
        let config_path = project_root.join("next-sql.toml");
        if !config_path.exists() {
            return Err("next-sql.toml not found".to_string());
        }

        let config_str = std::fs::read_to_string(&config_path)
            .map_err(|e| format!("Failed to read config: {}", e))?;

        let config: nextsql_core::config::NextSqlConfig = toml::from_str(&config_str)
            .map_err(|e| format!("Failed to parse config: {}", e))?;

        if let Some(db_url) = config.database_url {
            match self.load_schema_from_database(&db_url).await {
                Ok(schema) => {
                    let schema = Arc::new(schema);
                    let mut schemas = self.schemas.write().await;
                    schemas.insert(project_root.to_path_buf(), Arc::clone(&schema));
                    Ok(schema)
                }
                Err(e) => Err(format!("Failed to load schema from database: {}", e)),
            }
        } else {
            Err("No database_url found in configuration".to_string())
        }
    }

    async fn load_schema_from_database(&self, db_url: &str) -> Result<DatabaseSchema, Box<dyn std::error::Error + Send + Sync>> {
        let db_url = db_url.to_string();
        tokio::task::spawn_blocking(move || {
            let mut config = db_url.parse::<postgres::Config>()?;
            config.connect_timeout(std::time::Duration::from_secs(3));
            let connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(false)
                .build()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
            let tls = postgres_native_tls::MakeTlsConnector::new(connector);
            let mut client = config.connect(tls)?;
            let schema = SchemaLoader::load_from_database(&mut client)?;
            Ok(schema)
        })
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
    }

    pub fn find_project_root(&self, file_path: &Path) -> Option<PathBuf> {
        let mut current = file_path.parent()?;

        loop {
            let config_path = current.join("next-sql.toml");
            if config_path.exists() {
                return Some(current.to_path_buf());
            }

            current = match current.parent() {
                Some(parent) => parent,
                None => return None,
            };
        }
    }

    /// Insert schema directly (used during initial load).
    pub async fn insert_schema(&self, project_root: PathBuf, schema: DatabaseSchema) {
        let mut schemas = self.schemas.write().await;
        schemas.insert(project_root, Arc::new(schema));
    }

    /// 特定ファイルのValType定義を更新する
    /// valtypes: Vec<(name, base_type, line_number)>
    pub async fn update_valtypes_for_file(&self, file_path: PathBuf, valtypes: Vec<(String, BuiltInType, u32)>) {
        let mut locations = self.valtype_locations.write().await;
        // 既存のこのファイルに由来する位置情報を削除
        locations.retain(|_, (path, _)| path != &file_path);

        let mut cache = self.valtype_cache.write().await;
        if valtypes.is_empty() {
            cache.remove(&file_path);
        } else {
            let types_only: Vec<(String, BuiltInType)> = valtypes.iter()
                .map(|(name, bt, _)| (name.clone(), bt.clone()))
                .collect();
            for (name, _, line) in &valtypes {
                locations.insert(name.clone(), (file_path.clone(), *line));
            }
            cache.insert(file_path, types_only);
        }
    }

    /// ValType名から定義位置を取得する
    pub async fn get_valtype_location(&self, name: &str) -> Option<(PathBuf, u32)> {
        let locations = self.valtype_locations.read().await;
        locations.get(name).cloned()
    }

    /// 指定ファイル以外の全ValType定義を取得する
    pub async fn get_all_valtypes_except(&self, exclude_file: &Path) -> Vec<(String, BuiltInType)> {
        let cache = self.valtype_cache.read().await;
        cache.iter()
            .filter(|(path, _)| path.as_path() != exclude_file)
            .flat_map(|(_, vts)| vts.clone())
            .collect()
    }

    /// プロジェクト内の全.nsqlファイルからValType定義をスキャンしてキャッシュに充填する
    pub async fn populate_valtype_cache(&self, project_root: &Path) {
        let pattern = format!("{}/**/*.nsql", project_root.display());
        let entries: Vec<_> = glob::glob(&pattern)
            .into_iter()
            .flatten()
            .flatten()
            .collect();

        for entry in entries {
            if let Ok(content) = std::fs::read_to_string(&entry) {
                let valtypes = Self::extract_valtypes_with_lines(&content);
                if !valtypes.is_empty() {
                    self.update_valtypes_for_file(entry, valtypes).await;
                }
            }
        }
    }

    /// テキストからValType定義を行番号付きで抽出する
    pub fn extract_valtypes_with_lines(text: &str) -> Vec<(String, BuiltInType, u32)> {
        let mut valtypes = Vec::new();
        if let Ok(Ok(module)) = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| nextsql_core::parse_module(text))
        ) {
            for toplevel in &module.toplevels {
                if let nextsql_core::ast::TopLevel::ValType(vt) = toplevel {
                    // ValType定義の行番号を特定（コメント行を除外して "valtype Name" を検索）
                    let line = text.lines().enumerate()
                        .find(|(_, line)| {
                            let trimmed = line.trim();
                            !trimmed.starts_with("//")
                                && trimmed.starts_with("valtype")
                                && line.contains(&vt.name)
                        })
                        .map(|(i, _)| i as u32)
                        .unwrap_or(0);
                    valtypes.push((vt.name.clone(), vt.base_type.clone(), line));
                }
            }
        }
        valtypes
    }

    /// ValTypeキャッシュが空なら、プロジェクト内の全ファイルをスキャンして充填する
    pub async fn ensure_valtype_cache_populated(&self, project_root: &Path) {
        let cache = self.valtype_cache.read().await;
        // プロジェクトルート配下のファイルが1つでもキャッシュにあればスキップ
        let has_entries = cache.keys().any(|p| p.starts_with(project_root));
        drop(cache);
        if !has_entries {
            self.populate_valtype_cache(project_root).await;
        }
    }

    pub async fn invalidate_cache(&self, project_root: &Path) {
        let mut schemas = self.schemas.write().await;
        schemas.remove(project_root);
    }

    #[cfg(test)]
    pub async fn set_test_schema(&self, project_root: PathBuf, schema: DatabaseSchema) {
        let mut schemas = self.schemas.write().await;
        schemas.insert(project_root, Arc::new(schema));
    }
}
