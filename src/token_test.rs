use super::*;

#[test]
fn test_jinja_classification() {
    assert!(TokenType::JinjaStatement.is_jinja());
    assert!(TokenType::JinjaExpression.is_jinja());
    assert!(TokenType::JinjaBlockStart.is_jinja());
    assert!(TokenType::JinjaBlockEnd.is_jinja());
    assert!(TokenType::JinjaBlockKeyword.is_jinja());
    assert!(!TokenType::Name.is_jinja());
}

#[test]
fn test_divides_queries() {
    assert!(TokenType::Semicolon.divides_queries());
    assert!(TokenType::SetOperator.divides_queries());
    assert!(!TokenType::Name.divides_queries());
}

#[test]
fn test_bracket_classification() {
    assert!(TokenType::BracketOpen.is_opening_bracket());
    assert!(TokenType::StatementStart.is_opening_bracket());
    assert!(!TokenType::BracketClose.is_opening_bracket());

    assert!(TokenType::BracketClose.is_closing_bracket());
    assert!(TokenType::StatementEnd.is_closing_bracket());
    assert!(!TokenType::BracketOpen.is_closing_bracket());
}

#[test]
fn test_always_lowercased() {
    assert!(TokenType::UntermKeyword.is_always_lowercased());
    assert!(TokenType::BooleanOperator.is_always_lowercased());
    assert!(!TokenType::Name.is_always_lowercased());
    assert!(!TokenType::Operator.is_always_lowercased());
}

#[test]
fn test_token_creation() {
    let tok = Token::new(TokenType::Name, " ", "foo", 5u32, 8u32);
    assert_eq!(tok.token_type, TokenType::Name);
    assert_eq!(tok.prefix, " ");
    assert_eq!(tok.text, "foo");
    assert_eq!(tok.spos, 5u32);
    assert_eq!(tok.epos, 8u32);
}
