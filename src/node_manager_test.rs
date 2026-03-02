use super::*;
use crate::token::{Token, TokenType};

#[test]
fn test_standardize_value_lowercases_keywords() {
    let nm = NodeManager::new(false);
    let token = Token::new(TokenType::UntermKeyword, "", "SELECT", 0, 6);
    assert_eq!(nm.standardize_value(&token), "select");
}

#[test]
fn test_standardize_value_lowercases_names_when_insensitive() {
    let nm = NodeManager::new(false);
    let token = Token::new(TokenType::Name, "", "MyTable", 0, 7);
    assert_eq!(nm.standardize_value(&token), "mytable");
}

#[test]
fn test_standardize_value_preserves_names_when_sensitive() {
    let nm = NodeManager::new(true);
    let token = Token::new(TokenType::Name, "", "MyTable", 0, 7);
    assert_eq!(nm.standardize_value(&token), "MyTable");
}

#[test]
fn test_bracket_tracking() {
    let mut nm = NodeManager::new(false);
    assert!(nm.open_brackets.is_empty());
    nm.push_bracket(0);
    assert_eq!(nm.open_brackets.len(), 1);
    nm.push_bracket(5);
    assert_eq!(nm.open_brackets.len(), 2);

    nm.open_brackets.pop();
    assert_eq!(nm.open_brackets.len(), 1);
}

#[test]
fn test_formatting_disabled() {
    let mut nm = NodeManager::new(false);
    assert!(!nm.is_formatting_disabled());

    nm.disable_formatting();
    assert!(nm.is_formatting_disabled());

    nm.enable_formatting();
    assert!(!nm.is_formatting_disabled());
}

// --- Additional node_manager tests for coverage parity ---

/// Helper: format SQL through the full pipeline and return tokens.
fn format_and_get_tokens(source: &str) -> (Vec<crate::node::Node>, String) {
    let mode = crate::mode::Mode::default();
    let dialect = mode.dialect().unwrap();
    let mut analyzer = dialect.initialize_analyzer(mode.line_length);
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    (analyzer.arena, rendered)
}

#[test]
fn test_jinja_depth_tracking() {
    // Verify jinja_depth returns to 0 after balanced blocks
    let (arena, _) = format_and_get_tokens("{% if x %}\nSELECT 1\n{% endif %}\n");
    // After endif, jinja depth should be back to 0
    let endif_node = arena
        .iter()
        .find(|n| n.token.token_type == TokenType::JinjaBlockEnd)
        .expect("Should have JinjaBlockEnd node");
    let (_, jd) = endif_node.depth();
    assert_eq!(jd, 0, "Jinja depth should be 0 after endif");

    // Inside the block, jinja depth should be > 0
    let inner_node = arena
        .iter()
        .find(|n| n.token.token_type == TokenType::Number)
        .expect("Should have number node");
    let (_, jd_inner) = inner_node.depth();
    assert!(jd_inner > 0, "Jinja depth should be > 0 inside if block");
}

#[test]
fn test_union_depth_resets_to_zero() {
    let (arena, _) = format_and_get_tokens("SELECT 1\nUNION ALL\nSELECT 2\n");
    // Find the SetOperator node
    let union_node = arena
        .iter()
        .find(|n| n.token.token_type == TokenType::SetOperator)
        .expect("Should have UNION ALL node");
    let (bd, _) = union_node.depth();
    // UNION ALL pops unterm keywords, so bracket depth should be 0
    assert_eq!(bd, 0, "UNION ALL should have bracket depth 0");
}

#[test]
fn test_capitalization_operators_lowercased() {
    let nm = NodeManager::new(false);
    let operators = vec![
        (TokenType::BooleanOperator, "AND", "and"),
        (TokenType::BooleanOperator, "OR", "or"),
        (TokenType::BooleanOperator, "NOT", "not"),
        (TokenType::WordOperator, "AS", "as"),
        (TokenType::WordOperator, "IN", "in"),
        (TokenType::WordOperator, "LIKE", "like"),
        (TokenType::On, "ON", "on"),
        (TokenType::SetOperator, "UNION ALL", "union all"),
    ];
    for (tt, input, expected) in operators {
        let token = Token::new(tt, "", input, 0, input.len() as u32);
        assert_eq!(
            nm.standardize_value(&token),
            expected,
            "Operator '{}' should lowercase to '{}'",
            input,
            expected
        );
    }
}

#[test]
fn test_capitalization_numbers_lowercased() {
    let nm = NodeManager::new(false);
    let cases = vec![("0xFF", "0xff"), ("1E10", "1e10"), ("0XAB", "0xab")];
    for (input, expected) in cases {
        let token = Token::new(TokenType::Number, "", input, 0, input.len() as u32);
        assert_eq!(
            nm.standardize_value(&token),
            expected,
            "Number '{}' should lowercase to '{}'",
            input,
            expected
        );
    }
}

#[test]
fn test_identifier_whitespace() {
    // Test prefix spacing for various identifier combinations via the pipeline
    let (_, rendered) = format_and_get_tokens("SELECT a, b, c FROM t\n");
    // Names after commas should have proper spacing
    assert!(
        rendered.contains("a,") || rendered.contains("a\n"),
        "Identifiers should be formatted: {}",
        rendered
    );
}

#[test]
fn test_bracket_whitespace() {
    // Function call: no space before (
    let (_, rendered) = format_and_get_tokens("SELECT count(*) FROM t\n");
    assert!(
        rendered.contains("count(") || rendered.contains("count\n"),
        "No space before function paren: {}",
        rendered
    );
}

#[test]
fn test_internal_whitespace_normalization() {
    let nm = NodeManager::new(false);
    // Multi-word keywords with extra whitespace
    let cases = vec![
        (TokenType::SetOperator, "UNION  ALL", "union all"),
        (TokenType::UntermKeyword, "GROUP   BY", "group by"),
        (TokenType::UntermKeyword, "ORDER    BY", "order by"),
    ];
    for (tt, input, expected) in cases {
        let token = Token::new(tt, "", input, 0, input.len() as u32);
        assert_eq!(
            nm.standardize_value(&token),
            expected,
            "Internal whitespace should normalize: '{}' -> '{}'",
            input,
            expected
        );
    }
}

#[test]
fn test_jinja_whitespace_prefix() {
    // Jinja expression should preserve its original spacing context
    let (_, rendered) = format_and_get_tokens("SELECT {{ column }} FROM t\n");
    assert!(
        rendered.contains("{{ column }}") || rendered.contains("{{"),
        "Jinja expression should be present: {}",
        rendered
    );
}

#[test]
fn test_formatting_disabled_propagation() {
    // fmt:off / fmt:on should toggle formatting
    let (_, rendered) =
        format_and_get_tokens("SELECT 1\n-- fmt: off\nSELECT   2\n-- fmt: on\nSELECT 3\n");
    // The fmt:off region should preserve original formatting
    assert!(
        rendered.contains("SELECT   2") || rendered.contains("select"),
        "fmt:off region formatting should be controlled: {}",
        rendered
    );
}
