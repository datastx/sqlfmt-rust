use super::*;

#[test]
fn test_polyglot() {
    let dialect = Polyglot;
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
    assert!(dialect_from_name("duckdb").is_ok());
    assert!(dialect_from_name("unknown").is_err());
}

#[test]
fn test_initialize_analyzer() {
    let dialect = Polyglot;
    let analyzer = dialect.initialize_analyzer(88);
    assert_eq!(analyzer.line_length, 88);
}
