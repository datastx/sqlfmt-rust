use memchr::memchr2;

/// Skip a string literal starting at position `i` (which must point to `'` or `"`),
/// copying all characters (including delimiters) into `result`.
/// Handles backslash escapes and SQL-standard doubled-quote escapes (e.g. `''`).
/// Returns the position after the closing quote.
pub(crate) fn skip_string_literal_into(bytes: &[u8], i: usize, result: &mut String) -> usize {
    let quote = bytes[i];
    result.push(quote as char);
    let mut j = i + 1;
    while j < bytes.len() {
        // Use memchr2 to jump to the next quote or backslash
        if let Some(offset) = memchr2(quote, b'\\', &bytes[j..]) {
            let end = j + offset;
            // Copy everything between j and end as a chunk
            // SAFETY: SQL source is valid UTF-8 and we only slice at ASCII boundaries
            result.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[j..end]) });
            if bytes[end] == b'\\' && end + 1 < bytes.len() {
                result.push(bytes[end] as char);
                result.push(bytes[end + 1] as char);
                j = end + 2;
                continue;
            }
            // Check for doubled-quote escape (e.g. '' or "")
            if bytes[end] == quote && end + 1 < bytes.len() && bytes[end + 1] == quote {
                result.push(bytes[end] as char);
                result.push(bytes[end + 1] as char);
                j = end + 2;
                continue;
            }
            // Found the closing quote
            result.push(bytes[end] as char);
            return end + 1;
        } else {
            // No quote or backslash found — copy rest and return
            result.push_str(unsafe { std::str::from_utf8_unchecked(&bytes[j..]) });
            return bytes.len();
        }
    }
    j
}

/// Skip a string literal starting at position `i` (which must point to `'` or `"`).
/// Handles backslash escapes and SQL-standard doubled-quote escapes (e.g. `''`).
/// Does not copy any output. Returns the position after the closing quote.
pub(crate) fn skip_string_literal(bytes: &[u8], i: usize) -> usize {
    let quote = bytes[i];
    let mut j = i + 1;
    while j < bytes.len() {
        if let Some(offset) = memchr2(quote, b'\\', &bytes[j..]) {
            let end = j + offset;
            if bytes[end] == b'\\' && end + 1 < bytes.len() {
                j = end + 2;
                continue;
            }
            // Check for doubled-quote escape (e.g. '' or "")
            if bytes[end] == quote && end + 1 < bytes.len() && bytes[end + 1] == quote {
                j = end + 2;
                continue;
            }
            // Found the closing quote
            return end + 1;
        } else {
            return bytes.len();
        }
    }
    j
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_single_quoted_string() {
        let bytes = b"'hello' rest";
        let pos = skip_string_literal(bytes, 0);
        assert_eq!(pos, 7, "Should return position after closing quote");
    }

    #[test]
    fn test_skip_double_quoted_string() {
        let bytes = b"\"world\" rest";
        let pos = skip_string_literal(bytes, 0);
        assert_eq!(pos, 7, "Should return position after closing double quote");
    }

    #[test]
    fn test_skip_escaped_quote() {
        let bytes = b"'it\\'s' rest";
        let pos = skip_string_literal(bytes, 0);
        assert_eq!(
            pos, 7,
            "Should handle backslash escape and return correct end"
        );
    }

    #[test]
    fn test_skip_string_literal_into_copies() {
        let bytes = b"'hello' rest";
        let mut result = String::new();
        let pos = skip_string_literal_into(bytes, 0, &mut result);
        assert_eq!(pos, 7);
        assert_eq!(result, "'hello'", "Should copy delimiters and content");
    }

    #[test]
    fn test_skip_doubled_quote_escape() {
        // 'Men''s Basketball' — the '' is an escaped single quote
        let bytes = b"'Men''s Basketball' rest";
        let pos = skip_string_literal(bytes, 0);
        assert_eq!(
            pos, 19,
            "Should treat '' as escaped quote, not end of string"
        );
    }

    #[test]
    fn test_skip_doubled_quote_escape_into() {
        let bytes = b"'Men''s Basketball' rest";
        let mut result = String::new();
        let pos = skip_string_literal_into(bytes, 0, &mut result);
        assert_eq!(pos, 19);
        assert_eq!(
            result, "'Men''s Basketball'",
            "Should copy full string including escaped quotes"
        );
    }

    #[test]
    fn test_skip_multiple_doubled_quotes() {
        // 'select ''hello'' end' — two escaped quotes
        let bytes = b"'select ''hello'' end' rest";
        let pos = skip_string_literal(bytes, 0);
        assert_eq!(pos, 22, "Should handle multiple doubled-quote escapes");
    }

    #[test]
    fn test_skip_trailing_doubled_quote() {
        // 'hello world.''' — escaped quote at end of string
        let bytes = b"'hello world.''' rest";
        let pos = skip_string_literal(bytes, 0);
        assert_eq!(
            pos, 16,
            "Should handle doubled-quote at end of string content"
        );
    }

    #[test]
    fn test_skip_empty_string() {
        // '' — empty string, not an escaped quote
        let bytes = b"'' rest";
        let pos = skip_string_literal(bytes, 0);
        assert_eq!(pos, 2, "Empty string '' should be treated as empty string");
    }
}
