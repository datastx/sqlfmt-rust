use super::*;

#[test]
fn test_default_mode() {
    let mode = Mode::default();
    assert_eq!(mode.line_length, 88);
    assert_eq!(mode.dialect_name, "polyglot");
    assert!(!mode.check);
    assert!(!mode.diff);
    assert!(!mode.fast);
}

#[test]
fn test_dialect_creation() {
    let mode = Mode::default();
    assert!(mode.dialect().is_ok());

    let mut duckdb_mode = Mode::default();
    duckdb_mode.dialect_name = "duckdb".to_string();
    assert!(duckdb_mode.dialect().is_ok());
}

#[test]
fn test_color_logic() {
    let mut mode = Mode::default();
    assert!(mode.color());

    mode.no_color = true;
    assert!(!mode.color());

    mode.force_color = true;
    assert!(mode.color()); // force_color overrides no_color
}

#[test]
fn test_safety_check() {
    let mut mode = Mode::default();
    assert!(mode.should_safety_check());

    mode.fast = true;
    assert!(!mode.should_safety_check());
}
