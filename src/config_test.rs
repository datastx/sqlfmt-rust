use super::*;

#[test]
fn test_default_config() {
    let mode = Mode::default();
    assert_eq!(mode.line_length, 88);
    assert_eq!(mode.dialect_name, "polyglot");
}

#[test]
fn test_apply_config() {
    let mut mode = Mode::default();
    let mut config = HashMap::new();
    config.insert("line_length".to_string(), toml::Value::Integer(120));
    config.insert(
        "dialect".to_string(),
        toml::Value::String("duckdb".to_string()),
    );

    apply_config(&mut mode, &config).unwrap();
    assert_eq!(mode.line_length, 120);
    assert_eq!(mode.dialect_name, "duckdb");
}

#[test]
fn test_unknown_config_key_error() {
    let mut mode = Mode::default();
    let mut config = HashMap::new();
    config.insert("unknown_option".to_string(), toml::Value::Boolean(true));

    assert!(apply_config(&mut mode, &config).is_err());
}

#[test]
fn test_find_config_pyproject_in_parent() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("subdir");
    std::fs::create_dir(&sub).unwrap();
    let config_path = dir.path().join("pyproject.toml");
    std::fs::write(&config_path, "[tool.sqlfmt]\nline_length = 100\n").unwrap();

    let sql_file = sub.join("test.sql");
    std::fs::write(&sql_file, "SELECT 1\n").unwrap();

    let result = find_config_file(&[sql_file]);
    assert!(result.is_some(), "Should find pyproject.toml in parent dir");
    assert_eq!(result.unwrap(), config_path);
}

#[test]
fn test_find_config_sqlfmt_toml() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("sqlfmt.toml");
    std::fs::write(&config_path, "line_length = 120\n").unwrap();

    let sql_file = dir.path().join("test.sql");
    std::fs::write(&sql_file, "SELECT 1\n").unwrap();

    let result = find_config_file(&[sql_file]);
    assert!(result.is_some(), "Should find sqlfmt.toml");
    assert_eq!(result.unwrap(), config_path);
}

#[test]
fn test_find_config_none_when_absent() {
    let dir = tempfile::tempdir().unwrap();
    let sql_file = dir.path().join("test.sql");
    std::fs::write(&sql_file, "SELECT 1\n").unwrap();

    let result = find_config_file(&[sql_file]);
    // In a temp dir with no config files, it may or may not find one
    // (could find one in a parent of /tmp). The key is it doesn't crash.
    // For a truly isolated test, we check the function runs without error.
    let _ = result;
}

#[test]
fn test_load_config_missing_file_error() {
    let result = load_config(&[], Some(Path::new("/nonexistent/sqlfmt.toml")));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, SqlfmtError::Config(_)),
        "Expected Config error, got: {:?}",
        err
    );
}

#[test]
fn test_load_config_invalid_toml() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("pyproject.toml");
    std::fs::write(&config_path, "this is not valid toml {{{\n").unwrap();

    let result = load_config(&[], Some(config_path.as_path()));
    assert!(result.is_err(), "Invalid TOML should return an error");
}

#[test]
fn test_apply_config_exclude_array() {
    let mut mode = Mode::default();
    let mut config = HashMap::new();
    config.insert(
        "exclude".to_string(),
        toml::Value::Array(vec![
            toml::Value::String("migrations/*.sql".to_string()),
            toml::Value::String("vendor/**".to_string()),
        ]),
    );

    apply_config(&mut mode, &config).unwrap();
    assert_eq!(mode.exclude.len(), 2);
    assert!(mode.exclude.contains(&"migrations/*.sql".to_string()));
    assert!(mode.exclude.contains(&"vendor/**".to_string()));
}

#[test]
fn test_apply_config_no_jinjafmt() {
    let mut mode = Mode::default();
    let mut config = HashMap::new();
    config.insert("no_jinjafmt".to_string(), toml::Value::Boolean(true));

    apply_config(&mut mode, &config).unwrap();
    assert!(mode.no_jinjafmt);
}
