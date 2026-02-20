use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::error::SqlfmtError;
use crate::formatter::QueryFormatter;
use crate::mode::Mode;
use crate::report::{FileResult, Report};

/// Format a SQL string according to the given mode.
/// This is the core API function.
pub fn format_string(source: &str, mode: &Mode) -> Result<String, SqlfmtError> {
    let dialect = mode.dialect().map_err(SqlfmtError::Config)?;

    // Step 1: Lex (parse tokens)
    let mut analyzer = dialect.initialize_analyzer(mode.line_length);
    let mut query = analyzer.parse_query(source)?;
    let mut arena = std::mem::take(&mut analyzer.arena);

    // Step 2: Format (5-stage pipeline)
    let formatter = QueryFormatter::new(mode.line_length, mode.no_jinjafmt);
    formatter.format(&mut query, &mut arena);

    // Step 3: Render
    let result = query.render(&arena);

    // Step 4: Safety check (optional)
    if mode.should_safety_check() {
        safety_check(source, &result, mode)?;
    }

    Ok(result)
}

/// Run the formatter on a collection of files.
pub fn run(files: &[PathBuf], mode: &Mode) -> Report {
    let matching_paths = get_matching_paths(files, mode);
    let mut report = Report::new();

    if mode.single_process || matching_paths.len() <= 1 {
        for path in &matching_paths {
            let result = format_file(path, mode);
            report.add(result);
        }
    } else {
        // Parallel processing with rayon
        use rayon::prelude::*;

        let num_threads = if mode.threads > 0 {
            mode.threads
        } else {
            0 // rayon default: all available cores
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .expect("failed to build rayon thread pool");

        let results: Vec<FileResult> = pool.install(|| {
            matching_paths
                .par_iter()
                .map(|path| format_file(path, mode))
                .collect()
        });
        for result in results {
            report.add(result);
        }
    }

    report
}

/// Format a single file.
fn format_file(path: &Path, mode: &Mode) -> FileResult {
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            return FileResult {
                path: path.to_path_buf(),
                status: crate::report::FileStatus::Error,
                error: Some(format!("Read error: {}", e)),
            };
        }
    };

    let formatted = match format_string(&source, mode) {
        Ok(f) => f,
        Err(e) => {
            return FileResult {
                path: path.to_path_buf(),
                status: crate::report::FileStatus::Error,
                error: Some(format!("{}", e)),
            };
        }
    };

    if source == formatted {
        return FileResult {
            path: path.to_path_buf(),
            status: crate::report::FileStatus::Unchanged,
            error: None,
        };
    }

    if mode.check || mode.diff {
        if mode.diff {
            print_diff(path, &source, &formatted);
        }
        return FileResult {
            path: path.to_path_buf(),
            status: crate::report::FileStatus::Changed,
            error: None,
        };
    }

    // Write formatted output
    match std::fs::write(path, &formatted) {
        Ok(_) => FileResult {
            path: path.to_path_buf(),
            status: crate::report::FileStatus::Changed,
            error: None,
        },
        Err(e) => FileResult {
            path: path.to_path_buf(),
            status: crate::report::FileStatus::Error,
            error: Some(format!("Write error: {}", e)),
        },
    }
}

/// Get all SQL file paths that match the given inputs.
pub fn get_matching_paths(paths: &[PathBuf], mode: &Mode) -> Vec<PathBuf> {
    let extensions = mode.sql_extensions();
    let mut result = HashSet::new();

    // Pre-compile glob patterns once instead of per-file
    let exclude_patterns: Vec<glob::Pattern> = mode
        .exclude
        .iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect();

    for path in paths {
        if path.is_file() {
            if is_sql_file(path, extensions) {
                result.insert(path.clone());
            }
        } else if path.is_dir() {
            collect_sql_files(path, extensions, &exclude_patterns, &mut result);
        }
    }

    let mut sorted: Vec<PathBuf> = result.into_iter().collect();
    sorted.sort();
    sorted
}

/// Check if a file has a SQL extension.
fn is_sql_file(path: &Path, extensions: &[&str]) -> bool {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    extensions.iter().any(|ext| name.ends_with(ext))
}

/// Recursively collect SQL files from a directory.
fn collect_sql_files(
    dir: &Path,
    extensions: &[&str],
    exclude_patterns: &[glob::Pattern],
    result: &mut HashSet<PathBuf>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        // Skip hidden directories and excluded patterns
        if name.starts_with('.') {
            continue;
        }
        if exclude_patterns.iter().any(|p| p.matches(&name)) {
            continue;
        }

        if path.is_dir() {
            collect_sql_files(&path, extensions, exclude_patterns, result);
        } else if is_sql_file(&path, extensions) {
            result.insert(path);
        }
    }
}

/// Perform safety equivalence check: re-lex the formatted output
/// and verify tokens match the original.
/// Mirrors Python's safety check which compares Token objects
/// (type and raw token text, not values which may have been lowercased).
fn safety_check(original: &str, formatted: &str, mode: &Mode) -> Result<(), SqlfmtError> {
    use crate::token::TokenType;

    let dialect = mode.dialect().map_err(SqlfmtError::Config)?;
    let mut analyzer1 = dialect.initialize_analyzer(mode.line_length);
    let mut analyzer2 = dialect.initialize_analyzer(mode.line_length);

    let query1 = analyzer1.parse_query(original)?;
    let query2 = analyzer2.parse_query(formatted)?;

    // Collect tokens, skipping whitespace-only tokens (Newline)
    let tokens1: Vec<_> = query1
        .tokens(&analyzer1.arena)
        .into_iter()
        .filter(|n| n.token.token_type != TokenType::Newline)
        .collect();

    let tokens2: Vec<_> = query2
        .tokens(&analyzer2.arena)
        .into_iter()
        .filter(|n| n.token.token_type != TokenType::Newline)
        .collect();

    if tokens1.len() != tokens2.len() {
        return Err(SqlfmtError::Equivalence(format!(
            "Token count mismatch: original has {} tokens, formatted has {}",
            tokens1.len(),
            tokens2.len()
        )));
    }

    for (i, (n1, n2)) in tokens1.iter().zip(tokens2.iter()).enumerate() {
        // Compare token type
        if n1.token.token_type != n2.token.token_type {
            return Err(SqlfmtError::Equivalence(format!(
                "Token type mismatch at position {}: original {:?} '{}', formatted {:?} '{}'",
                i, n1.token.token_type, n1.token.token, n2.token.token_type, n2.token.token
            )));
        }
        // Compare token text (case-insensitive for keywords)
        let t1 = n1.token.token.to_lowercase();
        let t2 = n2.token.token.to_lowercase();
        // Normalize whitespace for multi-word tokens and Jinja delimiters
        let t1_norm = normalize_token_text(&t1, n1.token.token_type);
        let t2_norm = normalize_token_text(&t2, n2.token.token_type);
        if t1_norm != t2_norm {
            return Err(SqlfmtError::Equivalence(format!(
                "Token text mismatch at position {}: original '{}', formatted '{}'",
                i, n1.token.token, n2.token.token
            )));
        }
    }

    Ok(())
}

/// Normalize token text for equivalence comparison.
/// For Jinja tokens, strip delimiters and normalize all internal whitespace
/// so that `{{foo}}`, `{{ foo }}`, and multi-line Jinja tokens compare
/// as equivalent when their content is semantically the same.
fn normalize_token_text(text: &str, token_type: crate::token::TokenType) -> String {
    use crate::token::TokenType;
    match token_type {
        TokenType::JinjaExpression => {
            // Strip {{ / }} (and optional - for whitespace control)
            let inner = text
                .trim_start_matches("{{-")
                .trim_start_matches("{{")
                .trim_end_matches("-}}")
                .trim_end_matches("}}");
            // Normalize all internal whitespace (including newlines) and quotes
            let normalized: String = inner.split_whitespace().collect::<Vec<_>>().join(" ");
            let normalized = normalized.replace('\'', "\"");
            // Normalize operator spacing (+ | ~) so a+b and a + b compare equal
            let normalized = normalize_jinja_operators(&normalized);
            // Normalize structural chars so multiline formatting compares equal
            let normalized = normalize_jinja_structure(&normalized);
            format!("{{{{ {} }}}}", normalized)
        }
        TokenType::JinjaStatement
        | TokenType::JinjaBlockStart
        | TokenType::JinjaBlockEnd
        | TokenType::JinjaBlockKeyword => {
            // Strip {% / %} (and optional -)
            let inner = text
                .trim_start_matches("{%-")
                .trim_start_matches("{%")
                .trim_end_matches("-%}")
                .trim_end_matches("%}");
            // Normalize all internal whitespace (including newlines) and quotes
            let normalized: String = inner.split_whitespace().collect::<Vec<_>>().join(" ");
            let normalized = normalized.replace('\'', "\"");
            // Normalize operator spacing (+ | ~) so a|b and a | b compare equal
            let normalized = normalize_jinja_operators(&normalized);
            // Normalize structural chars so multiline formatting compares equal
            let normalized = normalize_jinja_structure(&normalized);
            format!("{{% {} %}}", normalized)
        }
        _ => {
            // General whitespace normalization for multi-word tokens
            text.split_whitespace().collect::<Vec<_>>().join(" ")
        }
    }
}

/// Normalize structural characters in Jinja content for equivalence.
/// Removes spaces after `(` and `[`, spaces before `)` and `]`,
/// trailing commas before `)` and `]`, and normalizes comma spacing.
/// Respects string literals.
fn normalize_jinja_structure(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut result = String::with_capacity(text.len());
    let mut i = 0;

    while i < bytes.len() {
        // Skip strings
        if bytes[i] == b'"' || bytes[i] == b'\'' {
            let quote = bytes[i];
            result.push(quote as char);
            i += 1;
            while i < bytes.len() && bytes[i] != quote {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    result.push(bytes[i] as char);
                    result.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                result.push(bytes[i] as char);
                i += 1;
            }
            if i < bytes.len() {
                result.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }

        // Before ( in function calls, remove spaces: "func (" -> "func("
        // This normalizes both `mock_ref ("x")` and `mock_ref("x")` to the same form.
        if bytes[i] == b'(' {
            let trimmed_len = result.trim_end().len();
            if trimmed_len > 0 {
                let last_byte = result.as_bytes()[trimmed_len - 1];
                if last_byte.is_ascii_alphanumeric()
                    || last_byte == b'_'
                    || last_byte == b'.'
                {
                    result.truncate(trimmed_len);
                }
            }
            result.push('(');
            i += 1;
            while i < bytes.len() && bytes[i] == b' ' {
                i += 1;
            }
            continue;
        }

        // After [, skip spaces
        if bytes[i] == b'[' {
            result.push(bytes[i] as char);
            i += 1;
            while i < bytes.len() && bytes[i] == b' ' {
                i += 1;
            }
            continue;
        }

        // Before ) or ], remove trailing spaces and trailing comma from result
        if bytes[i] == b')' || bytes[i] == b']' {
            // Remove trailing whitespace
            let trimmed = result.trim_end().len();
            result.truncate(trimmed);
            // Remove trailing comma
            if result.ends_with(',') {
                result.pop();
            }
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        // After comma, normalize to exactly no space (we strip all optional spaces)
        if bytes[i] == b',' {
            result.push(',');
            i += 1;
            // Skip spaces after comma
            while i < bytes.len() && bytes[i] == b' ' {
                i += 1;
            }
            continue;
        }

        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

/// Normalize operator spacing inside Jinja content for equivalence comparison.
/// Ensures `a+b`, `a +b`, `a+ b`, and `a + b` all compare equal.
fn normalize_jinja_operators(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut result = String::with_capacity(text.len() + 16);
    let mut i = 0;

    while i < bytes.len() {
        // Skip strings
        if bytes[i] == b'"' {
            result.push('"');
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    result.push(bytes[i] as char);
                    result.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                result.push(bytes[i] as char);
                i += 1;
            }
            if i < bytes.len() {
                result.push('"');
                i += 1;
            }
            continue;
        }

        // Normalize spacing around +, |, ~, =
        let is_eq = bytes[i] == b'='
            && (i + 1 >= bytes.len() || bytes[i + 1] != b'=')
            && (i == 0 || bytes[i - 1] != b'!' && bytes[i - 1] != b'>' && bytes[i - 1] != b'<');
        if bytes[i] == b'+' || bytes[i] == b'~' || is_eq
            || (bytes[i] == b'|' && (i + 1 >= bytes.len() || bytes[i + 1] != b'|'))
        {
            // Remove trailing spaces before operator
            while result.ends_with(' ') && !result.ends_with("  ") {
                // Keep at most the space that was there
                break;
            }
            let trimmed = result.trim_end();
            let trimmed_len = trimmed.len();
            result.truncate(trimmed_len);
            result.push(' ');
            result.push(bytes[i] as char);
            result.push(' ');
            i += 1;
            // Skip whitespace after operator
            while i < bytes.len() && bytes[i] == b' ' {
                i += 1;
            }
            continue;
        }

        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

/// Print a diff between original and formatted content.
fn print_diff(path: &Path, original: &str, formatted: &str) {
    use similar::{ChangeTag, TextDiff};

    eprintln!("--- {}", path.display());
    eprintln!("+++ {}", path.display());

    let diff = TextDiff::from_lines(original, formatted);
    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        eprint!("{}{}", sign, change);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_simple_select() {
        let mode = Mode::default();
        let result = format_string("SELECT 1\n", &mode).unwrap();
        assert!(result.contains("select"));
        assert!(result.contains("1"));
    }

    #[test]
    fn test_format_preserves_semantics() {
        let mode = Mode::default();
        let source = "SELECT a, b FROM t WHERE x = 1\n";
        let result = format_string(source, &mode).unwrap();
        assert!(result.contains("a"));
        assert!(result.contains("b"));
        assert!(result.contains("t"));
    }

    #[test]
    fn test_format_empty_string() {
        let mode = Mode::default();
        let result = format_string("\n", &mode);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_sql_file() {
        let extensions = &["sql", "sql.jinja", "ddl"];
        assert!(is_sql_file(Path::new("test.sql"), extensions));
        assert!(is_sql_file(Path::new("test.sql.jinja"), extensions));
        assert!(!is_sql_file(Path::new("test.py"), extensions));
        assert!(!is_sql_file(Path::new("test.txt"), extensions));
    }

    #[test]
    fn test_format_with_duckdb_dialect() {
        let mut mode = Mode::default();
        mode.dialect_name = "duckdb".to_string();
        let result = format_string("SELECT 1\n", &mode).unwrap();
        assert!(result.contains("select"));
    }
}
