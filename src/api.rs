use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::error::SqlfmtError;
use crate::formatter::QueryFormatter;
use crate::mode::Mode;
use crate::report::{FileResult, Report};
use crate::string_utils::skip_string_literal_into;

/// Lightweight snapshot of a token for safety-check comparison.
/// Avoids re-lexing the original source by capturing token_type + text
/// from the first parse pass.
#[derive(Debug, Clone)]
struct TokenSnapshot {
    token_type: crate::token::TokenType,
    text: String,
}

/// Format a SQL string according to the given mode.
/// This is the core API function.
pub fn format_string(source: &str, mode: &Mode) -> Result<String, SqlfmtError> {
    let dialect = mode.dialect()?;

    let mut analyzer = dialect.initialize_analyzer(mode.line_length);
    let mut query = analyzer.parse_query(source)?;
    let mut arena = std::mem::take(&mut analyzer.arena);

    // Capture token snapshot before formatting (for safety check reuse).
    // This avoids re-lexing the original source in safety_check.
    let original_tokens = if mode.should_safety_check() {
        Some(
            query
                .tokens(&arena)
                .into_iter()
                .filter(|n| n.token.token_type != crate::token::TokenType::Newline)
                .map(|n| TokenSnapshot {
                    token_type: n.token.token_type,
                    text: n.token.text.clone(),
                })
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };

    let formatter = QueryFormatter::new(mode.line_length, mode.no_jinjafmt);
    formatter.format(&mut query, &mut arena);

    let result = query.render(&arena);

    if let Some(ref orig_tokens) = original_tokens {
        safety_check(orig_tokens, &result, mode)?;
    }

    Ok(result)
}

/// Run the formatter on a collection of files.
pub async fn run(files: &[PathBuf], mode: &Mode) -> Report {
    let matching_paths = get_matching_paths(files, mode);
    let mut report = Report::new();

    if mode.single_process || matching_paths.len() <= 1 {
        for path in &matching_paths {
            let result = format_file(path, mode);
            report.add(result);
        }
    } else {
        // Limit concurrency to the configured thread count (or num_cpus).
        let concurrency = if mode.threads > 0 {
            mode.threads
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        };
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));

        let mut handles = Vec::with_capacity(matching_paths.len());
        for path in matching_paths {
            let mode = mode.clone();
            let sem = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore closed");
                format_file_async(&path, &mode).await
            }));
        }
        for handle in handles {
            match handle.await {
                Ok(result) => report.add(result),
                Err(e) => report.add(FileResult {
                    path: PathBuf::from("<unknown>"),
                    status: crate::report::FileStatus::Error,
                    error: Some(format!("Task join error: {}", e)),
                }),
            }
        }
    }

    report
}

/// Format a single file asynchronously.
/// Uses async I/O for reading/writing and spawn_blocking for CPU-bound formatting.
async fn format_file_async(path: &Path, mode: &Mode) -> FileResult {
    let source = match tokio::fs::read_to_string(path).await {
        Ok(s) => s,
        Err(e) => {
            return FileResult {
                path: path.to_path_buf(),
                status: crate::report::FileStatus::Error,
                error: Some(format!("Read error: {}", e)),
            };
        }
    };

    let mode_clone = mode.clone();
    let (source, formatted) = match tokio::task::spawn_blocking(move || {
        let result = format_string(&source, &mode_clone);
        (source, result)
    })
    .await
    {
        Ok((source, Ok(f))) => (source, f),
        Ok((_, Err(e))) => {
            return FileResult {
                path: path.to_path_buf(),
                status: crate::report::FileStatus::Error,
                error: Some(format!("{}", e)),
            };
        }
        Err(e) => {
            return FileResult {
                path: path.to_path_buf(),
                status: crate::report::FileStatus::Error,
                error: Some(format!("Blocking task error: {}", e)),
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

    match tokio::fs::write(path, &formatted).await {
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
/// Accepts pre-captured token snapshots from the first parse pass,
/// avoiding a redundant re-lex of the original source (~33% savings).
fn safety_check(
    original_tokens: &[TokenSnapshot],
    formatted: &str,
    mode: &Mode,
) -> Result<(), SqlfmtError> {
    use crate::token::TokenType;

    let dialect = mode.dialect()?;
    let mut analyzer2 = dialect.initialize_analyzer(mode.line_length);
    let query2 = analyzer2.parse_query(formatted)?;

    let tokens2: Vec<_> = query2
        .tokens(&analyzer2.arena)
        .into_iter()
        .filter(|n| n.token.token_type != TokenType::Newline)
        .collect();

    if original_tokens.len() != tokens2.len() {
        return Err(SqlfmtError::Equivalence(format!(
            "Token count mismatch: original has {} tokens, formatted has {}",
            original_tokens.len(),
            tokens2.len()
        )));
    }

    for (i, (s1, n2)) in original_tokens.iter().zip(tokens2.iter()).enumerate() {
        if s1.token_type != n2.token.token_type {
            return Err(SqlfmtError::Equivalence(format!(
                "Token type mismatch at position {}: original {:?} '{}', formatted {:?} '{}'",
                i, s1.token_type, s1.text, n2.token.token_type, n2.token.text
            )));
        }
        // Fast path: if token text is identical, skip normalization entirely
        if s1.text == n2.token.text {
            continue;
        }
        // Fast path: if case-insensitively equal and single-word non-Jinja, skip
        if !s1.token_type.is_jinja()
            && !s1.text.contains(char::is_whitespace)
            && !n2.token.text.contains(char::is_whitespace)
            && s1.text.eq_ignore_ascii_case(&n2.token.text)
        {
            continue;
        }
        // Slow path: full normalization needed
        let t1 = s1.text.to_lowercase();
        let t2 = n2.token.text.to_lowercase();
        let t1_norm = normalize_token_text(&t1, s1.token_type);
        let t2_norm = normalize_token_text(&t2, n2.token.token_type);
        if t1_norm != t2_norm {
            return Err(SqlfmtError::Equivalence(format!(
                "Token text mismatch at position {}: original '{}', formatted '{}'",
                i, s1.text, n2.token.text
            )));
        }
    }

    Ok(())
}

/// Normalize token text for equivalence comparison.
/// For Jinja tokens, strip delimiters and normalize all internal whitespace
/// so that `{{foo}}`, `{{ foo }}`, and multi-line Jinja tokens compare
/// as equivalent when their content is semantically the same.
/// Join whitespace-separated words with single spaces, without intermediate Vec.
fn join_whitespace(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    for (i, word) in text.split_whitespace().enumerate() {
        if i > 0 {
            result.push(' ');
        }
        result.push_str(word);
    }
    result
}

fn normalize_token_text(text: &str, token_type: crate::token::TokenType) -> String {
    use crate::token::TokenType;
    match token_type {
        TokenType::JinjaExpression => {
            let inner = text
                .trim_start_matches("{{-")
                .trim_start_matches("{{")
                .trim_end_matches("-}}")
                .trim_end_matches("}}");
            let normalized = join_whitespace(inner);
            let normalized = normalized.replace('\'', "\"");
            let normalized = normalize_jinja_operators(&normalized);
            let normalized = normalize_jinja_structure(&normalized);
            format!("{{{{ {} }}}}", normalized)
        }
        TokenType::JinjaStatement
        | TokenType::JinjaBlockStart
        | TokenType::JinjaBlockEnd
        | TokenType::JinjaBlockKeyword => {
            let inner = text
                .trim_start_matches("{%-")
                .trim_start_matches("{%")
                .trim_end_matches("-%}")
                .trim_end_matches("%}");
            let normalized = join_whitespace(inner);
            let normalized = normalized.replace('\'', "\"");
            let normalized = normalize_jinja_operators(&normalized);
            let normalized = normalize_jinja_structure(&normalized);
            format!("{{% {} %}}", normalized)
        }
        _ => join_whitespace(text),
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
            i = skip_string_literal_into(bytes, i, &mut result);
            continue;
        }

        // Before ( in function calls, remove spaces: "func (" -> "func("
        // This normalizes both `mock_ref ("x")` and `mock_ref("x")` to the same form.
        if bytes[i] == b'(' {
            let trimmed_len = result.trim_end().len();
            if trimmed_len > 0 {
                let last_byte = result.as_bytes()[trimmed_len - 1];
                if last_byte.is_ascii_alphanumeric() || last_byte == b'_' || last_byte == b'.' {
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
        if bytes[i] == b'"' || bytes[i] == b'\'' {
            i = skip_string_literal_into(bytes, i, &mut result);
            continue;
        }

        // Normalize spacing around +, |, ~, =
        let is_eq = bytes[i] == b'='
            && (i + 1 >= bytes.len() || bytes[i + 1] != b'=')
            && (i == 0 || bytes[i - 1] != b'!' && bytes[i - 1] != b'>' && bytes[i - 1] != b'<');
        if bytes[i] == b'+'
            || bytes[i] == b'~'
            || is_eq
            || (bytes[i] == b'|' && (i + 1 >= bytes.len() || bytes[i + 1] != b'|'))
        {
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

    #[test]
    fn test_format_bracket_error() {
        let mode = Mode::default();
        let result = format_string("SELECT )\n", &mode);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SqlfmtError::Bracket(_)),
            "Expected Bracket error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_format_unterminated_comment_error() {
        let mode = Mode::default();
        let result = format_string("/* unclosed\n", &mode);
        assert!(result.is_err(), "Unterminated comment should error");
    }

    #[test]
    fn test_safety_check_valid() {
        let mode = Mode::default();
        let source = "select\n    1\n";
        // Format should succeed and pass safety check
        let result = format_string(source, &mode);
        assert!(
            result.is_ok(),
            "Well-formatted SQL should pass safety check"
        );
    }

    #[test]
    fn test_is_sql_file_jinja() {
        let extensions = &["sql", "sql.jinja", "ddl"];
        assert!(is_sql_file(Path::new("model.sql.jinja"), extensions));
    }

    #[test]
    fn test_is_sql_file_non_sql() {
        let extensions = &["sql", "sql.jinja", "ddl"];
        assert!(!is_sql_file(Path::new("script.py"), extensions));
        assert!(!is_sql_file(Path::new("readme.txt"), extensions));
        assert!(!is_sql_file(Path::new("data.csv"), extensions));
    }

    #[test]
    fn test_get_matching_paths_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let sql_file = dir.path().join("test.sql");
        std::fs::write(&sql_file, "SELECT 1\n").unwrap();

        let mode = Mode::default();
        let paths = get_matching_paths(&[sql_file.clone()], &mode);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], sql_file);
    }

    #[test]
    fn test_get_matching_paths_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.sql"), "SELECT 1\n").unwrap();
        std::fs::write(dir.path().join("b.sql"), "SELECT 2\n").unwrap();
        std::fs::write(dir.path().join("c.py"), "print(1)").unwrap();

        let mode = Mode::default();
        let paths = get_matching_paths(&[dir.path().to_path_buf()], &mode);
        assert_eq!(paths.len(), 2, "Should find only .sql files");
        assert!(paths.iter().all(|p| p.extension().unwrap() == "sql"));
    }

    #[test]
    fn test_get_matching_paths_excludes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.sql"), "SELECT 1\n").unwrap();
        std::fs::write(dir.path().join("b.sql"), "SELECT 2\n").unwrap();

        let mut mode = Mode::default();
        mode.exclude = vec!["b.sql".to_string()];
        let paths = get_matching_paths(&[dir.path().to_path_buf()], &mode);
        assert_eq!(paths.len(), 1);
    }

    #[tokio::test]
    async fn test_run_empty_files() {
        let mode = Mode::default();
        let report = run(&[], &mode).await;
        assert_eq!(report.total(), 0);
        assert!(!report.has_errors());
        assert!(!report.has_changes());
    }

    #[test]
    fn test_normalize_jinja_operators() {
        // Test that operator spacing is normalized
        let a = normalize_jinja_operators("a+b");
        let b = normalize_jinja_operators("a + b");
        assert_eq!(a, b, "Operator spacing should be normalized identically");
    }
}
