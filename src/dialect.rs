use crate::analyzer::Analyzer;
use crate::node_manager::NodeManager;
use crate::rule::Rule;
use crate::rules;

/// A SQL dialect defines the set of lexing rules and configuration
/// for a specific SQL variant.
pub trait Dialect: Send + Sync {
    /// Get the lexing rules for this dialect.
    fn get_rules(&self) -> Vec<Rule>;

    /// Whether identifiers are case-sensitive.
    fn case_sensitive_names(&self) -> bool {
        false
    }

    /// Create an analyzer configured for this dialect.
    fn initialize_analyzer(&self, line_length: usize) -> Analyzer {
        let nm = NodeManager::new(self.case_sensitive_names());
        Analyzer::new(self.get_rules(), nm, line_length)
    }
}

/// The default dialect. Covers common usage across Snowflake, DuckDB,
/// PostgreSQL, MySQL, BigQuery, and SparkSQL.
pub struct Polyglot;

impl Dialect for Polyglot {
    fn get_rules(&self) -> Vec<Rule> {
        rules::main_rules()
    }
}

/// ClickHouse dialect: same rules as Polyglot but with case-sensitive names.
pub struct ClickHouse;

impl Dialect for ClickHouse {
    fn get_rules(&self) -> Vec<Rule> {
        rules::main_rules()
    }

    fn case_sensitive_names(&self) -> bool {
        true
    }
}

/// DuckDB dialect: same as Polyglot for now; can be extended with DuckDB-specific
/// keywords and syntax.
pub struct DuckDb;

impl Dialect for DuckDb {
    fn get_rules(&self) -> Vec<Rule> {
        rules::main_rules()
    }
}

/// Create a dialect from a string name.
pub fn dialect_from_name(name: &str) -> Result<Box<dyn Dialect>, String> {
    match name.to_ascii_lowercase().as_str() {
        "polyglot" => Ok(Box::new(Polyglot)),
        "clickhouse" => Ok(Box::new(ClickHouse)),
        "duckdb" => Ok(Box::new(DuckDb)),
        _ => Err(format!("Unknown dialect: {}", name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polyglot_rules() {
        let dialect = Polyglot;
        let rules = dialect.get_rules();
        assert!(!rules.is_empty());
        assert!(!dialect.case_sensitive_names());
    }

    #[test]
    fn test_clickhouse_case_sensitive() {
        let dialect = ClickHouse;
        assert!(dialect.case_sensitive_names());
    }

    #[test]
    fn test_duckdb_dialect() {
        let dialect = DuckDb;
        let rules = dialect.get_rules();
        assert!(!rules.is_empty());
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
