#[derive(Debug, PartialEq)]
pub enum CompletionContext {
    QueryMethod,
    MutationMethod,
    TableReference,
    TableField(String), // テーブル名を保持
    TableJoinMethod(String), // テーブル名の後のjoinメソッド補完
    ExpressionMethod,  // Expression-level methods (isNull, like, between, etc.)
    OnConflictAction,  // After .onConflict(...), offer doUpdate/doNothing
    InputField(String), // Input type name - for $variable. where variable is an input type
    Unknown,
}

#[derive(Debug, PartialEq)]
pub enum FunctionContext {
    Insert,
    Update,
    Delete,
    From,
    JoinMethod,
    JoinCondition, // Second parameter of join methods
}

pub fn analyze_context_before_dot(full_text: &str, text_before_dot: &str) -> CompletionContext {
    let trimmed = text_before_dot.trim_end_matches('.');
    eprintln!("LSP: analyze_context_before_dot - trimmed: '{}'", trimmed);

    // Check if we're after a $variable. pattern where the variable is an input type
    if let Some(input_type_name) = check_input_field_context(full_text, trimmed) {
        eprintln!("LSP: Detected input field context for type: {}", input_type_name);
        return CompletionContext::InputField(input_type_name);
    }

    // Check if we're in a context where table join methods should be offered
    if is_in_join_method_context(trimmed) {
        if let Some(table_name) = extract_table_before_dot(full_text, trimmed) {
            eprintln!(
                "LSP: Detected table join method context for table: {}",
                table_name
            );
            return CompletionContext::TableJoinMethod(table_name);
        }
    }

    // Check if we're in an expression context (inside .where(), .having(), .orderBy(), etc.)
    // where the identifier before the dot is a column reference like "users.name"
    if is_in_expression_method_context(full_text, trimmed) {
        eprintln!("LSP: Detected expression method context");
        return CompletionContext::ExpressionMethod;
    }

    // まず、テーブル名.かどうかをチェック
    if let Some(table_name) = extract_table_before_dot(full_text, trimmed) {
        eprintln!(
            "LSP: Detected table field context for table: {}",
            table_name
        );
        return CompletionContext::TableField(table_name);
    }

    // 直前の閉じ括弧を探して、その前の関数名を確認
    if let Some(close_paren_pos) = trimmed.rfind(')') {
        // 対応する開き括弧を探す
        let mut paren_count = 1;
        let mut open_paren_pos = None;

        for (i, ch) in trimmed[..close_paren_pos].chars().rev().enumerate() {
            match ch {
                ')' => paren_count += 1,
                '(' => {
                    paren_count -= 1;
                    if paren_count == 0 {
                        open_paren_pos = Some(close_paren_pos - i - 1);
                        break;
                    }
                }
                _ => {}
            }
        }

        if let Some(open_pos) = open_paren_pos {
            // 括弧の前の関数名を取得
            let before_paren = &trimmed[..open_pos];
            let function_name = before_paren
                .split_whitespace()
                .last()
                .unwrap_or("")
                .split(['(', ')', ',', '{', '}', '.'])
                .last()
                .unwrap_or("");

            // 関数名に基づいてコンテキストを判定
            match function_name {
                "onConflict" => return CompletionContext::OnConflictAction,
                "insert" | "update" | "delete" => return CompletionContext::MutationMethod,
                "from" => return CompletionContext::TableReference,
                _ => {}
            }
        }
    }

    // mutation宣言内かどうかチェック
    if trimmed.contains("mutation ") {
        return CompletionContext::MutationMethod;
    }

    // query宣言内かどうかチェック
    if trimmed.contains("query ") {
        return CompletionContext::QueryMethod;
    }

    CompletionContext::Unknown
}

pub fn check_function_context(before_cursor: &str) -> Option<FunctionContext> {
    // 最後の開いた括弧を探す
    if let Some(open_paren_pos) = before_cursor.rfind('(') {
        // 括弧が閉じられていないかチェック
        let after_paren = &before_cursor[open_paren_pos + 1..];
        if after_paren.contains(')') {
            return None;
        }

        // 括弧の前の単語を取得
        let before_paren = &before_cursor[..open_paren_pos];
        let function_name = before_paren
            .split_whitespace()
            .last()
            .unwrap_or("")
            .split(['(', ')', ',', '{', '}', '.'])
            .last()
            .unwrap_or("");

        // Check if we're in a join method
        if matches!(function_name, "innerJoin" | "leftJoin" | "rightJoin" | "fullOuterJoin" | "crossJoin") {
            // Check if we're after a comma (second parameter)
            if after_paren.contains(',') {
                // Find the last comma position
                if let Some(comma_pos) = after_paren.rfind(',') {
                    let after_comma = &after_paren[comma_pos + 1..];
                    // Check if there's no closing paren after the comma
                    if !after_comma.contains(')') {
                        return Some(FunctionContext::JoinCondition);
                    }
                }
            } else {
                // First parameter
                return Some(FunctionContext::JoinMethod);
            }
        }

        match function_name {
            "insert" => Some(FunctionContext::Insert),
            "update" => Some(FunctionContext::Update),
            "delete" => Some(FunctionContext::Delete),
            "from" => Some(FunctionContext::From),
            _ => None,
        }
    } else {
        None
    }
}

pub fn extract_table_before_dot(full_text: &str, text: &str) -> Option<String> {
    eprintln!("LSP: extract_table_before_dot - text: '{}'", text);

    // ドットの直前の識別子を抽出
    let parts: Vec<&str> = text.split_whitespace().collect();
    eprintln!("LSP: Split parts: {:?}", parts);

    if let Some(last_part) = parts.last() {
        eprintln!("LSP: Last part: '{}'", last_part);

        // 識別子の文字のみを取得（句読点などを除外）
        let identifier: String = last_part
            .chars()
            .rev()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .chars()
            .rev()
            .collect();

        eprintln!("LSP: Extracted identifier: '{}'", identifier);

        // 識別子は文字で始まる必要がある（数字のみは除外）
        if !identifier.is_empty() && identifier.chars().next().unwrap().is_alphabetic() {
            // Check if the identifier is an alias and resolve it
            let resolved = resolve_alias(full_text, &identifier).unwrap_or(identifier);
            eprintln!("LSP: Using identifier as table name: '{}'", resolved);
            return Some(resolved);
        }
    }
    eprintln!("LSP: No table identifier found");
    None
}


/// Resolve an alias name to its target table name by scanning "alias X = Y" patterns in the text.
fn resolve_alias(full_text: &str, identifier: &str) -> Option<String> {
    use std::sync::LazyLock;
    static RE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"alias\s+(\w+)\s*=\s*(\w+)").unwrap()
    });
    for cap in RE.captures_iter(full_text) {
        if &cap[1] == identifier {
            eprintln!("LSP: Resolved alias '{}' to '{}'", identifier, &cap[2]);
            return Some(cap[2].to_string());
        }
    }
    None
}

fn is_in_join_method_context(text: &str) -> bool {
    eprintln!("LSP: is_in_join_method_context - checking: '{}'", text);
    
    // First, check if we're inside a join method's parameters (after comma)
    // If so, we should NOT offer join method completions
    let join_methods = ["innerJoin", "leftJoin", "rightJoin", "fullOuterJoin", "crossJoin"];
    for method in &join_methods {
        if let Some(join_pos) = text.rfind(method) {
            let after_join = &text[join_pos + method.len()..];
            if let Some(paren_pos) = after_join.find('(') {
                let after_paren = &after_join[paren_pos + 1..];
                // Check if we're after a comma (second parameter)
                if after_paren.contains(',') && !after_paren.contains(')') {
                    eprintln!("LSP: Inside join method parameters, NOT a join method context");
                    return false;
                }
            }
        }
    }
    
    // Check if we're inside from() or after alias =
    // Examples:
    // - from(users
    // - alias joined = users
    
    // Check for from( context
    if let Some(from_pos) = text.rfind("from(") {
        let after_from = &text[from_pos + 5..];
        // Count parentheses to ensure we're still inside from()
        let mut paren_count = 0;
        let mut inside_from = true;
        
        for ch in after_from.chars() {
            match ch {
                '(' => paren_count += 1,
                ')' => {
                    if paren_count == 0 {
                        // This closes the from(
                        inside_from = false;
                        break;
                    }
                    paren_count -= 1;
                }
                _ => {}
            }
        }
        
        if inside_from {
            eprintln!("LSP: Found from( context");
            return true;
        }
    }
    
    // Check for alias = context
    if text.contains("alias") && text.contains("=") {
        if let Some(equals_pos) = text.rfind('=') {
            let after_equals = &text[equals_pos + 1..].trim();
            // Check if we have a simple identifier after = (no dots, parentheses, etc.)
            if after_equals.chars().all(|c| c.is_alphanumeric() || c == '_') && !after_equals.is_empty() {
                eprintln!("LSP: Found alias = context");
                return true;
            }
        }
    }
    
    false
}

pub fn is_after_insertable_angle_bracket(before_cursor: &str) -> bool {
    eprintln!("LSP: Checking for Insertable</ChangeSet< pattern in: '{}'", before_cursor);

    // Check if the text ends with "Insertable<" or "ChangeSet<" with optional partial text
    let patterns = ["Insertable<", "ChangeSet<"];
    for pattern in &patterns {
        if let Some(pos) = before_cursor.rfind(pattern) {
            let after_bracket = &before_cursor[pos + pattern.len()..];
            // Allow partial model names but not complete statements
            if !after_bracket.contains('>') && !after_bracket.contains('.') {
                return true;
            }
        }
    }
    false
}

pub fn is_after_variable_colon(before_cursor: &str) -> bool {
    eprintln!("LSP: Checking for $variable: pattern in: '{}'", before_cursor);
    
    // Check if we're after "$identifier: " pattern
    if let Some(colon_pos) = before_cursor.rfind(':') {
        let before_colon = &before_cursor[..colon_pos];
        let after_colon = &before_cursor[colon_pos + 1..];
        
        // Check if we're still in the type part (no complex expressions)
        if after_colon.contains('(') || after_colon.contains(')') || after_colon.contains(',') {
            return false;
        }
        
        // Check if before colon has a variable pattern ($identifier)
        if let Some(dollar_pos) = before_colon.rfind('$') {
            let var_name = &before_colon[dollar_pos + 1..];
            // Variable name should be alphanumeric/underscore only
            if var_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return true;
            }
        }
    }
    
    false
}


/// Check if text ends with `$variable` and that variable's declared type is an input type.
/// Returns the input type name if so.
fn check_input_field_context(full_text: &str, text: &str) -> Option<String> {
    use std::sync::LazyLock;

    // Extract the $variable at the end of text
    static VAR_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"\$(\w+)$").unwrap()
    });
    let var_name = VAR_RE.captures(text).map(|c| c[1].to_string())?;
    eprintln!("LSP: check_input_field_context - var_name: '{}'", var_name);

    // Find the variable's type declaration in query/mutation arguments: $varName: TypeName
    static ARG_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"\$(\w+)\s*:\s*(\w+)").unwrap()
    });
    let mut var_type = None;
    for cap in ARG_RE.captures_iter(full_text) {
        if &cap[1] == var_name {
            var_type = Some(cap[2].to_string());
            break;
        }
    }
    let var_type = var_type?;
    eprintln!("LSP: check_input_field_context - var_type: '{}'", var_type);

    // Check if that type is an input type declared in the file
    static INPUT_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"input\s+(\w+)\s*\{").unwrap()
    });
    for cap in INPUT_RE.captures_iter(full_text) {
        if &cap[1] == var_type {
            return Some(var_type);
        }
    }

    None
}

/// Check if we're inside an expression context (where, having, orderBy)
/// and the dot is after a column reference (e.g., "users.name.")
/// `full_text` is used to find the enclosing expression context (.where( etc.)
/// `trimmed` is the current line before the dot, used to check the table.column pattern
fn is_in_expression_method_context(full_text: &str, trimmed: &str) -> bool {
    eprintln!("LSP: is_in_expression_method_context - checking full_text for expression context");

    let expression_contexts = [
        ".where(", ".having(", ".orderBy(",
    ];

    for context in &expression_contexts {
        if let Some(pos) = full_text.rfind(context) {
            let after_context = &full_text[pos + context.len()..];

            // Count parentheses to see if we're still inside
            let mut paren_count = 1;
            let mut still_inside = true;
            for ch in after_context.chars() {
                match ch {
                    '(' => paren_count += 1,
                    ')' => {
                        paren_count -= 1;
                        if paren_count == 0 {
                            still_inside = false;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if still_inside {
                // We're inside an expression context.
                // Check if the current line ends with "table.column" pattern
                if let Some(dot_pos) = trimmed.rfind('.') {
                    let after_dot = &trimmed[dot_pos + 1..];
                    let before_dot_part = &trimmed[..dot_pos];

                    // Get the identifier before the dot
                    let ident_before = before_dot_part
                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                        .last()
                        .unwrap_or("");

                    // Both parts should be valid identifiers
                    if !ident_before.is_empty()
                        && ident_before.chars().next().unwrap().is_alphabetic()
                        && !after_dot.is_empty()
                        && after_dot.chars().next().unwrap().is_alphabetic()
                        && after_dot.chars().all(|c| c.is_alphanumeric() || c == '_')
                    {
                        eprintln!("LSP: Found expression method context: {}.{}", ident_before, after_dot);
                        return true;
                    }
                }
            }
        }
    }

    false
}