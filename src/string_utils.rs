/// Skip a string literal starting at position `i` (which must point to `'` or `"`),
/// copying all characters (including delimiters) into `result`.
/// Returns the position after the closing quote.
pub(crate) fn skip_string_literal_into(bytes: &[u8], i: usize, result: &mut String) -> usize {
    let quote = bytes[i];
    result.push(quote as char);
    let mut j = i + 1;
    while j < bytes.len() && bytes[j] != quote {
        if bytes[j] == b'\\' && j + 1 < bytes.len() {
            result.push(bytes[j] as char);
            result.push(bytes[j + 1] as char);
            j += 2;
            continue;
        }
        result.push(bytes[j] as char);
        j += 1;
    }
    if j < bytes.len() {
        result.push(bytes[j] as char);
        j += 1;
    }
    j
}

/// Skip a string literal starting at position `i` (which must point to `'` or `"`).
/// Does not copy any output. Returns the position after the closing quote.
pub(crate) fn skip_string_literal(bytes: &[u8], i: usize) -> usize {
    let quote = bytes[i];
    let mut j = i + 1;
    while j < bytes.len() && bytes[j] != quote {
        if bytes[j] == b'\\' && j + 1 < bytes.len() {
            j += 1;
        }
        j += 1;
    }
    if j < bytes.len() {
        j += 1;
    }
    j
}
