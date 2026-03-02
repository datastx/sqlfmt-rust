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
