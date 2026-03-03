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

#[test]
fn test_run_empty_files() {
    let mode = Mode::default();
    let report = run(&[], &mode);
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
