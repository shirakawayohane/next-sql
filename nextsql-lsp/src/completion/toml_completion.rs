use tower_lsp::lsp_types::*;

/// next-sql.toml 用の補完プロバイダ
pub struct TomlCompletionProvider<'a> {
    text: &'a str,
}

impl<'a> TomlCompletionProvider<'a> {
    pub fn new(text: &'a str) -> Self {
        Self { text }
    }

    pub fn get_completions(&self, position: Position) -> Vec<CompletionItem> {
        // split('\n') を使って末尾の改行後の空行も含める
        let lines: Vec<&str> = self.text.split('\n').collect();
        if position.line as usize >= lines.len() {
            return Vec::new();
        }

        let current_line = lines[position.line as usize];
        let before_cursor = &current_line[..current_line.len().min(position.character as usize)];

        // 現在のセクションを特定
        let current_section = self.find_current_section(&lines, position.line as usize);

        // 値の補完（= の後）
        if let Some(key) = self.extract_key_before_equals(before_cursor) {
            return self.get_value_completions(&current_section, &key);
        }

        // キーの補完
        let existing_keys = self.collect_existing_keys(&lines, position.line as usize, &current_section);
        self.get_key_completions(&current_section, &existing_keys, before_cursor)
    }

    /// カーソル位置が属するTOMLセクションを特定する
    fn find_current_section(&self, lines: &[&str], current_line: usize) -> String {
        for i in (0..=current_line).rev() {
            let trimmed = lines[i].trim();
            if trimmed.starts_with('[') && !trimmed.starts_with("[[") {
                if let Some(end) = trimmed.find(']') {
                    return trimmed[1..end].to_string();
                }
            }
        }
        String::new() // トップレベル
    }

    /// `key = ` のパターンからキー名を抽出する
    fn extract_key_before_equals(&self, before_cursor: &str) -> Option<String> {
        let trimmed = before_cursor.trim();
        if let Some(eq_pos) = trimmed.find('=') {
            let key = trimmed[..eq_pos].trim();
            if !key.is_empty() && !key.starts_with('[') {
                return Some(key.to_string());
            }
        }
        None
    }

    /// 現在のセクション内に既に存在するキーを収集する
    fn collect_existing_keys(
        &self,
        lines: &[&str],
        current_line: usize,
        current_section: &str,
    ) -> Vec<String> {
        let mut keys = Vec::new();
        let mut in_section = false;

        for (i, line) in lines.iter().enumerate() {
            let trimmed = line.trim();

            if trimmed.starts_with('[') && !trimmed.starts_with("[[") {
                if let Some(end) = trimmed.find(']') {
                    let section = &trimmed[1..end];
                    in_section = section == current_section;
                    continue;
                }
            }

            // トップレベルの場合は、セクションヘッダーが出るまでがスコープ
            if current_section.is_empty() {
                if trimmed.starts_with('[') {
                    break;
                }
                in_section = true;
            }

            if in_section && i != current_line {
                if let Some(eq_pos) = trimmed.find('=') {
                    let key = trimmed[..eq_pos].trim().to_string();
                    if !key.is_empty() {
                        keys.push(key);
                    }
                }
            }
        }
        keys
    }

    /// セクションに応じたキー補完を提供する
    fn get_key_completions(
        &self,
        section: &str,
        existing_keys: &[String],
        before_cursor: &str,
    ) -> Vec<CompletionItem> {
        let trimmed = before_cursor.trim();

        match section {
            "" => {
                // トップレベル：セクションヘッダーとトップレベルキー
                let mut items = Vec::new();

                if !existing_keys.contains(&"database_url".to_string())
                    && !trimmed.starts_with('[')
                {
                    items.push(self.create_key_completion(
                        "database_url",
                        "Database connection URL",
                        Some("database_url = \"$1\""),
                    ));
                }

                // セクションヘッダー補完
                let sections_in_doc = self.find_existing_sections();
                if !sections_in_doc.contains(&"files".to_string()) {
                    items.push(self.create_section_completion(
                        "[files]",
                        "File configuration",
                        Some("[files]\nincludes = [\"$1\"]"),
                    ));
                }
                if !sections_in_doc.contains(&"target".to_string()) {
                    items.push(self.create_section_completion(
                        "[target]",
                        "Target configuration for code generation",
                        Some("[target]\ntarget_language = \"rust\"\ntarget_directory = \"$1\""),
                    ));
                }
                if !sections_in_doc.contains(&"codegen".to_string()) {
                    items.push(self.create_section_completion(
                        "[codegen]",
                        "Code generation style configuration",
                        Some("[codegen]\ninsert_params = \"Insert{Table}Params\"\nupdate_params = \"Update{Table}Params\""),
                    ));
                }

                items
            }
            "files" => {
                let mut items = Vec::new();
                if !existing_keys.contains(&"includes".to_string()) {
                    items.push(self.create_key_completion(
                        "includes",
                        "Files to include (glob patterns)",
                        Some("includes = [\"$1\"]"),
                    ));
                }
                items
            }
            "target" => {
                let mut items = Vec::new();
                if !existing_keys.contains(&"target_language".to_string()) {
                    items.push(self.create_key_completion(
                        "target_language",
                        "Target language for code generation",
                        Some("target_language = \"rust\""),
                    ));
                }
                if !existing_keys.contains(&"target_directory".to_string()) {
                    items.push(self.create_key_completion(
                        "target_directory",
                        "Output directory for generated code",
                        Some("target_directory = \"$1\""),
                    ));
                }
                if !existing_keys.contains(&"package_name".to_string()) {
                    items.push(self.create_key_completion(
                        "package_name",
                        "Package name for generated project manifest",
                        Some("package_name = \"$1\""),
                    ));
                }
                items
            }
            "codegen" => {
                let mut items = Vec::new();
                if !existing_keys.contains(&"insert_params".to_string()) {
                    items.push(self.create_key_completion(
                        "insert_params",
                        "Naming pattern for insert param structs (use {Table} as placeholder)",
                        Some("insert_params = \"Insert{Table}Params\""),
                    ));
                }
                if !existing_keys.contains(&"update_params".to_string()) {
                    items.push(self.create_key_completion(
                        "update_params",
                        "Naming pattern for update changeset structs (use {Table} as placeholder)",
                        Some("update_params = \"Update{Table}Params\""),
                    ));
                }
                items
            }
            _ => Vec::new(),
        }
    }

    /// キーに対する値の補完を提供する
    fn get_value_completions(&self, section: &str, key: &str) -> Vec<CompletionItem> {
        match (section, key) {
            ("target", "target_language") => {
                vec![self.create_value_completion("rust", "Rust language target")]
            }
            ("", "database_url") => {
                vec![
                    self.create_value_completion(
                        "postgresql://username:password@localhost:5432/database",
                        "PostgreSQL connection URL",
                    ),
                ]
            }
            _ => Vec::new(),
        }
    }

    /// ドキュメント内の既存セクション名を収集する
    fn find_existing_sections(&self) -> Vec<String> {
        let mut sections = Vec::new();
        for line in self.text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && !trimmed.starts_with("[[") {
                if let Some(end) = trimmed.find(']') {
                    sections.push(trimmed[1..end].to_string());
                }
            }
        }
        sections
    }

    fn create_key_completion(
        &self,
        label: &str,
        detail: &str,
        snippet: Option<&str>,
    ) -> CompletionItem {
        CompletionItem {
            label: label.to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some(detail.to_string()),
            insert_text: snippet.map(|s| s.to_string()),
            insert_text_format: snippet.map(|_| InsertTextFormat::SNIPPET),
            ..Default::default()
        }
    }

    fn create_section_completion(
        &self,
        label: &str,
        detail: &str,
        snippet: Option<&str>,
    ) -> CompletionItem {
        CompletionItem {
            label: label.to_string(),
            kind: Some(CompletionItemKind::MODULE),
            detail: Some(detail.to_string()),
            insert_text: snippet.map(|s| s.to_string()),
            insert_text_format: snippet.map(|_| InsertTextFormat::SNIPPET),
            ..Default::default()
        }
    }

    fn create_value_completion(&self, label: &str, detail: &str) -> CompletionItem {
        CompletionItem {
            label: label.to_string(),
            kind: Some(CompletionItemKind::VALUE),
            detail: Some(detail.to_string()),
            insert_text: Some(format!("\"{label}\"")),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_level_completions() {
        let text = "";
        let provider = TomlCompletionProvider::new(text);
        let completions = provider.get_completions(Position { line: 0, character: 0 });

        assert!(completions.iter().any(|c| c.label == "database_url"));
        assert!(completions.iter().any(|c| c.label == "[files]"));
        assert!(completions.iter().any(|c| c.label == "[target]"));
        assert!(completions.iter().any(|c| c.label == "[codegen]"));
    }

    #[test]
    fn test_files_section_completions() {
        let text = "[files]\n";
        let provider = TomlCompletionProvider::new(text);
        let completions = provider.get_completions(Position { line: 1, character: 0 });

        assert!(completions.iter().any(|c| c.label == "includes"));
    }

    #[test]
    fn test_target_section_completions() {
        let text = "[target]\n";
        let provider = TomlCompletionProvider::new(text);
        let completions = provider.get_completions(Position { line: 1, character: 0 });

        assert!(completions.iter().any(|c| c.label == "target_language"));
        assert!(completions.iter().any(|c| c.label == "target_directory"));
        assert!(completions.iter().any(|c| c.label == "package_name"));
    }

    #[test]
    fn test_target_language_value_completion() {
        let text = "[target]\ntarget_language = ";
        let provider = TomlCompletionProvider::new(text);
        let completions = provider.get_completions(Position { line: 1, character: 20 });

        assert!(completions.iter().any(|c| c.label == "rust"));
    }

    #[test]
    fn test_existing_keys_excluded() {
        let text = "[target]\ntarget_language = \"rust\"\n";
        let provider = TomlCompletionProvider::new(text);
        let completions = provider.get_completions(Position { line: 2, character: 0 });

        assert!(!completions.iter().any(|c| c.label == "target_language"));
        assert!(completions.iter().any(|c| c.label == "target_directory"));
    }

    #[test]
    fn test_existing_sections_excluded() {
        let text = "\n[files]\nincludes = [\"**\"]\n[target]\ntarget_language = \"rust\"\ntarget_directory = \"../generated\"";
        let provider = TomlCompletionProvider::new(text);
        let completions = provider.get_completions(Position { line: 0, character: 0 });

        assert!(!completions.iter().any(|c| c.label == "[files]"));
        assert!(!completions.iter().any(|c| c.label == "[target]"));
        assert!(completions.iter().any(|c| c.label == "[codegen]"));
    }

    #[test]
    fn test_codegen_section_completions() {
        let text = "[codegen]\n";
        let provider = TomlCompletionProvider::new(text);
        let completions = provider.get_completions(Position { line: 1, character: 0 });

        assert!(completions.iter().any(|c| c.label == "insert_params"));
        assert!(completions.iter().any(|c| c.label == "update_params"));
    }
}
