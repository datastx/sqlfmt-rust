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
    text: compact_str::CompactString,
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
    // Normalize trailing newlines to exactly one, ensuring idempotency.
    // Preserve truly empty output (empty files) as-is.
    let trimmed = result.trim_end_matches('\n');
    let result = if trimmed.is_empty() {
        result
    } else {
        format!("{}\n", trimmed)
    };

    if let Some(ref orig_tokens) = original_tokens {
        safety_check(orig_tokens, &result, mode)?;
    }

    Ok(result)
}

/// Run the formatter on a collection of files.
///
/// Uses a three-phase pipeline to avoid I/O contention at high thread counts:
/// 1. **Read phase**: Read all files into memory (sequential — avoids disk contention)
/// 2. **Format phase**: Format all files in parallel (CPU-only, no I/O)
/// 3. **Write phase**: Write changed files back to disk (sequential)
pub fn run(files: &[PathBuf], mode: &Mode) -> Report {
    let matching_paths = get_matching_paths(files, mode);
    let mut report = Report::new();

    if mode.single_process || matching_paths.len() <= 1 {
        for path in &matching_paths {
            let result = format_file(path, mode);
            report.add(result);
        }
    } else {
        use rayon::prelude::*;

        let concurrency = if mode.threads > 0 {
            mode.threads
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()
            .expect("failed to build rayon thread pool");

        // Phase 1: Read all files into memory (sequential I/O).
        let sources: Vec<(PathBuf, Result<String, String>)> = matching_paths
            .into_iter()
            .map(|path| {
                let content = std::fs::read_to_string(&path)
                    .map_err(|e| format!("Read error: {}", e));
                (path, content)
            })
            .collect();

        // Phase 2: Format in parallel (CPU-only, no I/O).
        let formatted: Vec<(PathBuf, FormatOutcome)> = pool.install(|| {
            sources
                .into_par_iter()
                .map(|(path, content)| {
                    let outcome = match content {
                        Err(err) => FormatOutcome::Error(err),
                        Ok(source) => match format_string(&source, mode) {
                            Err(e) => FormatOutcome::Error(format!("{}", e)),
                            Ok(fmt) if fmt == source => FormatOutcome::Unchanged,
                            Ok(fmt) => FormatOutcome::Changed {
                                source,
                                formatted: fmt,
                            },
                        },
                    };
                    (path, outcome)
                })
                .collect()
        });

        // Phase 3: Write changed files and build report (sequential I/O).
        for (path, outcome) in formatted {
            let result = match outcome {
                FormatOutcome::Error(err) => FileResult {
                    path,
                    status: crate::report::FileStatus::Error,
                    error: Some(err),
                },
                FormatOutcome::Unchanged => FileResult {
                    path,
                    status: crate::report::FileStatus::Unchanged,
                    error: None,
                },
                FormatOutcome::Changed { source, formatted } => {
                    if mode.check || mode.diff {
                        if mode.diff {
                            print_diff(&path, &source, &formatted);
                        }
                        FileResult {
                            path,
                            status: crate::report::FileStatus::Changed,
                            error: None,
                        }
                    } else {
                        match std::fs::write(&path, &formatted) {
                            Ok(_) => FileResult {
                                path,
                                status: crate::report::FileStatus::Changed,
                                error: None,
                            },
                            Err(e) => FileResult {
                                path,
                                status: crate::report::FileStatus::Error,
                                error: Some(format!("Write error: {}", e)),
                            },
                        }
                    }
                }
            };
            report.add(result);
        }
    }

    report
}

/// Result of formatting a single file's content (used in the pipeline).
enum FormatOutcome {
    /// File content is already correctly formatted.
    Unchanged,
    /// File content was reformatted. Carries original + formatted for diff output.
    Changed { source: String, formatted: String },
    /// An error occurred during read or format.
    Error(String),
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
            let normalized = normalize_jinja_quotes(&normalized);
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
            let normalized = normalize_jinja_quotes(&normalized);
            let normalized = normalize_jinja_operators(&normalized);
            let normalized = normalize_jinja_structure(&normalized);
            format!("{{% {} %}}", normalized)
        }
        _ => join_whitespace(text),
    }
}

/// Normalize single-quoted string delimiters to double quotes for equivalence,
/// without modifying single quotes that appear inside double-quoted strings.
/// E.g. `'hello'` → `"hello"`, but `"'csnp', 'dual'"` stays unchanged.
fn normalize_jinja_quotes(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut result = String::with_capacity(text.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'"' {
            // Already a double-quoted string — copy it verbatim
            result.push('"');
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    result.push(bytes[i] as char);
                    result.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    // Check for doubled-quote escape ""
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        result.push('"');
                        result.push('"');
                        i += 2;
                        continue;
                    }
                    result.push('"');
                    i += 1;
                    break;
                }
                result.push(bytes[i] as char);
                i += 1;
            }
        } else if bytes[i] == b'\'' {
            // Single-quoted string — check if it contains unescaped double quotes.
            // If so, keep single quotes to avoid producing ambiguous output that
            // confuses subsequent normalization passes.
            let mut has_double_quote = false;
            let mut j = i + 1;
            while j < bytes.len() {
                if bytes[j] == b'\\' && j + 1 < bytes.len() {
                    j += 2;
                    continue;
                }
                if bytes[j] == b'\'' {
                    if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                        j += 2;
                        continue;
                    }
                    break;
                }
                if bytes[j] == b'"' {
                    has_double_quote = true;
                }
                j += 1;
            }
            let out_quote = if has_double_quote { '\'' } else { '"' };
            result.push(out_quote);
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    result.push(bytes[i] as char);
                    result.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    // Check for doubled-quote escape ''
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        result.push('\'');
                        result.push('\'');
                        i += 2;
                        continue;
                    }
                    result.push(out_quote);
                    i += 1;
                    break;
                }
                result.push(bytes[i] as char);
                i += 1;
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
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

        // Around comma, normalize spacing (strip spaces before and after)
        if bytes[i] == b',' {
            // Remove trailing whitespace before comma
            let trimmed = result.trim_end().len();
            result.truncate(trimmed);
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
#[path = "api_test.rs"]
mod tests;
