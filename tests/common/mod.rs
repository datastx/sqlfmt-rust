//! Shared test helpers for integration and golden tests.
//!
//! Only uses the public API (`sqlfmt::format_string`, `sqlfmt::Mode`).

use sqlfmt::{format_string, Mode};
use std::fs;

const SENTINEL: &str = ")))))__SQLFMT_OUTPUT__(((((";

/// Default formatting mode (polyglot dialect, line_length=88).
pub fn default_mode() -> Mode {
    Mode::default()
}

/// DuckDB dialect mode.
pub fn duckdb_mode() -> Mode {
    Mode {
        dialect_name: "duckdb".to_string(),
        ..Mode::default()
    }
}

/// Read a golden test data file and return (source, expected) tuple.
///
/// Mirrors the Python `read_test_data()` logic exactly:
/// - If the file contains the sentinel, lines above = source, lines below = expected
/// - If no sentinel, the file is preformatted: expected = source
/// - Source is trimmed + "\n"; expected preserves exact whitespace
pub fn read_test_data(path: &str) -> (String, String) {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read test file {}: {}", path, e));

    let lines: Vec<&str> = content.lines().collect();

    let mut source_lines: Vec<&str> = Vec::new();
    let mut formatted_lines: Vec<&str> = Vec::new();
    let mut found_sentinel = false;

    for line in &lines {
        if line.trim() == SENTINEL {
            found_sentinel = true;
            continue;
        }
        if found_sentinel {
            formatted_lines.push(line);
        } else {
            source_lines.push(line);
        }
    }

    if !found_sentinel {
        formatted_lines = source_lines.clone();
    }

    // Source: join with newlines, trim, add trailing newline
    let source = {
        let joined = source_lines.join("\n");
        let trimmed = joined.trim();
        if trimmed.is_empty() {
            String::new()
        } else {
            format!("{}\n", trimmed)
        }
    };

    // Expected: join with newlines preserving exact content
    let expected = if formatted_lines.is_empty() {
        String::new()
    } else {
        let mut result = formatted_lines.join("\n");
        if content.ends_with('\n') {
            result.push('\n');
        }
        result
    };

    (source, expected)
}

/// Run a golden test: format source, compare to expected, check idempotency.
pub fn run_golden_test(path: &str, mode: &Mode) {
    let (source, expected) = read_test_data(path);
    let actual = format_string(&source, mode).unwrap_or_else(|e| {
        panic!("format_string failed for {}: {}", path, e);
    });
    assert_eq!(
        expected, actual,
        "\n\nFormatting mismatch for {}\n\n--- expected ---\n{}\n--- actual ---\n{}\n",
        path, expected, actual
    );
    // Idempotency check
    let second = format_string(&actual, mode).unwrap_or_else(|e| {
        panic!("Idempotency format failed for {}: {}", path, e);
    });
    assert_eq!(
        expected, second,
        "\n\nIdempotency failed for {}\n\n--- expected pass ---\n{}\n--- second pass ---\n{}\n",
        path, expected, second
    );
}

/// Run a golden error test: formatting should produce an error.
pub fn run_golden_error_test(path: &str) {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read error test file {}: {}", path, e));
    let source = format!("{}\n", content.trim());
    let result = format_string(&source, &default_mode());
    assert!(
        result.is_err(),
        "Expected error for {} but got Ok:\n{}",
        path,
        result.unwrap()
    );
}
