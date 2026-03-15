/// Convert camelCase to snake_case.
pub(super) fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Convert camelCase to PascalCase (just capitalise the first letter).
pub(super) fn to_pascal_case(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => {
            let mut out = c.to_uppercase().to_string();
            out.extend(chars);
            out
        }
    }
}

// ── Singularize / model-struct helpers ───────────────────────────────────

/// Simple singularization: strip trailing 's'.
pub(super) fn singularize(name: &str) -> String {
    if name.ends_with('s') && name.len() > 2 {
        name[..name.len() - 1].to_string()
    } else {
        name.to_string()
    }
}

/// Convert a table name like "users" or "application_headers" to PascalCase model name.
/// e.g. "users" -> "User", "application_headers" -> "ApplicationHeader"
pub(super) fn table_name_to_model_name(table_name: &str) -> String {
    let singular = singularize(table_name);
    // Convert snake_case to PascalCase
    singular
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => {
                    let mut out = c.to_uppercase().to_string();
                    out.extend(chars);
                    out
                }
            }
        })
        .collect()
}
