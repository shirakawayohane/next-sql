pub fn utf16_position_to_byte_index(text: &str, utf16_pos: usize) -> usize {
    let mut utf16_count = 0;
    let mut byte_index = 0;

    for ch in text.chars() {
        if utf16_count >= utf16_pos {
            break;
        }

        // UTF-16でのコード単位数を計算
        let utf16_len = if ch.len_utf16() == 1 { 1 } else { 2 };
        utf16_count += utf16_len;
        byte_index += ch.len_utf8();
    }

    byte_index
}

/// Remove single-line comment lines (`// ...`) from text, preserving line count
/// by replacing each comment line with an empty line.
pub fn strip_comments(text: &str) -> String {
    text.lines()
        .map(|line| {
            if line.trim_start().starts_with("//") {
                ""
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn snake_to_camel_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}