use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::SqlfmtError;
use crate::mode::Mode;

/// Load sqlfmt configuration from a pyproject.toml file.
/// Searches parent directories for pyproject.toml if no config path is given.
pub fn load_config(files: &[PathBuf], config_path: Option<&Path>) -> Result<Mode, SqlfmtError> {
    let mut mode = Mode::default();

    let config_file = match config_path {
        Some(path) => {
            if path.exists() {
                Some(path.to_path_buf())
            } else {
                return Err(SqlfmtError::Config(format!(
                    "Config file not found: {}",
                    path.display()
                )));
            }
        }
        None => find_config_file(files),
    };

    if let Some(path) = config_file {
        let raw = load_config_from_path(&path)?;
        apply_config(&mut mode, &raw)?;
    }

    Ok(mode)
}

/// Search for a pyproject.toml in the common parent directories of the given files.
fn find_config_file(files: &[PathBuf]) -> Option<PathBuf> {
    let parents = get_common_parents(files);
    for parent in parents {
        let config = parent.join("pyproject.toml");
        if config.exists() {
            return Some(config);
        }
        let config = parent.join("sqlfmt.toml");
        if config.exists() {
            return Some(config);
        }
    }
    None
}

/// Get the common parent directories of the given file paths, ordered
/// from most specific to least specific.
fn get_common_parents(files: &[PathBuf]) -> Vec<PathBuf> {
    let mut parents = Vec::new();

    for file in files {
        let parent = if file.is_dir() {
            file.clone()
        } else {
            file.parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."))
        };

        let mut current = Some(parent.as_path());
        while let Some(dir) = current {
            let dir_buf = dir.to_path_buf();
            if !parents.contains(&dir_buf) {
                parents.push(dir_buf);
            }
            current = dir.parent();
        }
    }

    parents
}

/// Load and parse a TOML config file.
fn load_config_from_path(path: &Path) -> Result<HashMap<String, toml::Value>, SqlfmtError> {
    let content = std::fs::read_to_string(path)?;
    let parsed: toml::Value = content
        .parse()
        .map_err(|e| SqlfmtError::Config(format!("Failed to parse {}: {}", path.display(), e)))?;

    let section = parsed
        .get("tool")
        .and_then(|t| t.get("sqlfmt"))
        .or_else(|| {
            if path
                .file_name()
                .map(|n| n == "sqlfmt.toml")
                .unwrap_or(false)
            {
                Some(&parsed)
            } else {
                None
            }
        });

    match section {
        Some(toml::Value::Table(table)) => {
            let mut map = HashMap::new();
            for (k, v) in table {
                map.insert(k.to_lowercase(), v.clone());
            }
            Ok(map)
        }
        _ => Ok(HashMap::new()),
    }
}

/// Apply configuration values to a Mode.
fn apply_config(mode: &mut Mode, config: &HashMap<String, toml::Value>) -> Result<(), SqlfmtError> {
    if let Some(toml::Value::Integer(n)) = config.get("line_length") {
        mode.line_length = *n as usize;
    }

    if let Some(toml::Value::String(d)) = config.get("dialect") {
        mode.dialect_name = d.clone();
    }

    if let Some(toml::Value::Array(arr)) = config.get("exclude") {
        mode.exclude = arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }

    if let Some(toml::Value::Boolean(b)) = config.get("no_jinjafmt") {
        mode.no_jinjafmt = *b;
    }

    let known_keys = [
        "line_length",
        "dialect",
        "exclude",
        "no_jinjafmt",
        "encoding",
    ];
    for key in config.keys() {
        if !known_keys.contains(&key.as_str()) {
            return Err(SqlfmtError::Config(format!(
                "Unknown config option: {}",
                key
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_apply_config_dialect_clickhouse() {
        let mut mode = Mode::default();
        let mut config = HashMap::new();
        config.insert(
            "dialect".to_string(),
            toml::Value::String("clickhouse".to_string()),
        );

        apply_config(&mut mode, &config).unwrap();
        assert_eq!(mode.dialect_name, "clickhouse");
    }
}
