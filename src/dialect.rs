use crate::analyzer::Analyzer;
use crate::error::SqlfmtError;
use crate::node_manager::NodeManager;

/// A SQL dialect defines configuration for a specific SQL variant.
pub trait Dialect: Send + Sync {
    /// Whether identifiers are case-sensitive.
    fn case_sensitive_names(&self) -> bool {
        false
    }

    /// Create an analyzer configured for this dialect.
    fn initialize_analyzer(&self, line_length: usize) -> Analyzer {
        let nm = NodeManager::new(self.case_sensitive_names());
        Analyzer::new(nm, line_length)
    }
}

/// The default dialect. Covers common usage across Snowflake, DuckDB,
/// PostgreSQL, MySQL, BigQuery, and SparkSQL.
pub struct Polyglot;

impl Dialect for Polyglot {}

/// ClickHouse dialect: same rules as Polyglot.
/// Note: ClickHouse identifiers are technically case-sensitive at the engine level,
/// but Python sqlfmt lowercases them like all other dialects.
pub struct ClickHouse;

impl Dialect for ClickHouse {}

/// DuckDB dialect: same as Polyglot for now; can be extended with DuckDB-specific
/// keywords and syntax.
pub struct DuckDb;

impl Dialect for DuckDb {}

/// Create a dialect from a string name.
pub fn dialect_from_name(name: &str) -> Result<Box<dyn Dialect>, SqlfmtError> {
    match name.to_ascii_lowercase().as_str() {
        "polyglot" => Ok(Box::new(Polyglot)),
        "clickhouse" => Ok(Box::new(ClickHouse)),
        "duckdb" => Ok(Box::new(DuckDb)),
        _ => Err(SqlfmtError::Config(format!("Unknown dialect: {}", name))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polyglot() {
        let dialect = Polyglot;
        assert!(!dialect.case_sensitive_names());
    }

    #[test]
    fn test_clickhouse_dialect() {
        let dialect = ClickHouse;
        assert!(!dialect.case_sensitive_names());
    }

    #[test]
    fn test_duckdb_dialect() {
        let dialect = DuckDb;
        assert!(!dialect.case_sensitive_names());
    }

    #[test]
    fn test_dialect_from_name() {
        assert!(dialect_from_name("polyglot").is_ok());
        assert!(dialect_from_name("clickhouse").is_ok());
        assert!(dialect_from_name("duckdb").is_ok());
        assert!(dialect_from_name("unknown").is_err());
    }

    #[test]
    fn test_initialize_analyzer() {
        let dialect = Polyglot;
        let analyzer = dialect.initialize_analyzer(88);
        assert_eq!(analyzer.line_length, 88);
    }
}
