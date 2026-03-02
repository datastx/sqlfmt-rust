use serde::Deserialize;

use crate::dialect::{self, Dialect};
use crate::error::SqlfmtError;

/// Mode holds all formatting configuration for sqlfmt.
#[derive(Debug, Clone, Deserialize)]
pub struct Mode {
    #[serde(default = "default_line_length")]
    pub line_length: usize,

    #[serde(default = "default_dialect")]
    pub dialect_name: String,

    #[serde(default)]
    pub check: bool,

    #[serde(default)]
    pub diff: bool,

    /// Skip safety equivalence check for faster operation.
    #[serde(default)]
    pub fast: bool,

    /// Disable Jinja formatting.
    #[serde(default)]
    pub no_jinjafmt: bool,

    /// Glob patterns to exclude.
    #[serde(default)]
    pub exclude: Vec<String>,

    #[serde(default = "default_encoding")]
    pub encoding: String,

    #[serde(default)]
    pub verbose: bool,

    #[serde(default)]
    pub quiet: bool,

    #[serde(default)]
    pub no_progressbar: bool,

    #[serde(default)]
    pub no_color: bool,

    #[serde(default)]
    pub force_color: bool,

    /// Number of threads for parallel processing (0 = all cores).
    #[serde(default)]
    pub threads: usize,

    #[serde(default)]
    pub single_process: bool,

    #[serde(default)]
    pub reset_cache: bool,
}

fn default_line_length() -> usize {
    88
}
fn default_dialect() -> String {
    "polyglot".to_string()
}
fn default_encoding() -> String {
    "utf-8".to_string()
}

impl Mode {
    /// Create the dialect for the configured dialect_name.
    pub fn dialect(&self) -> Result<Box<dyn Dialect>, SqlfmtError> {
        dialect::dialect_from_name(&self.dialect_name)
    }

    /// Whether color output is enabled.
    #[cfg(test)]
    pub fn color(&self) -> bool {
        if self.force_color {
            return true;
        }
        if self.no_color {
            return false;
        }
        if std::env::var("NO_COLOR").is_ok() {
            return false;
        }
        true
    }

    /// Whether safety check should be performed.
    pub fn should_safety_check(&self) -> bool {
        !self.fast && !self.check && !self.diff
    }

    /// SQL file extensions to process.
    pub fn sql_extensions(&self) -> &[&str] {
        &["sql", "sql.jinja", "sql.jinja2", "ddl", "dml"]
    }
}

impl Default for Mode {
    fn default() -> Self {
        Self {
            line_length: 88,
            dialect_name: "polyglot".to_string(),
            check: false,
            diff: false,
            fast: false,
            no_jinjafmt: false,
            exclude: Vec::new(),
            encoding: "utf-8".to_string(),
            verbose: false,
            quiet: false,
            no_progressbar: false,
            no_color: false,
            force_color: false,
            threads: 0,
            single_process: false,
            reset_cache: false,
        }
    }
}

#[cfg(test)]
#[path = "mode_test.rs"]
mod tests;
