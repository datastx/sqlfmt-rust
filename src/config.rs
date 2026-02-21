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
        // Also check for sqlfmt.toml
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

        // Walk up to root
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

    // Look for [tool.sqlfmt] section
    let section = parsed
        .get("tool")
        .and_then(|t| t.get("sqlfmt"))
        .or_else(|| {
            // Also try top-level keys for sqlfmt.toml
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

    // Validate no unknown keys
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
}
