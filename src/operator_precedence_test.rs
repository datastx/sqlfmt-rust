use super::*;
use crate::node::Node;
use crate::token::Token;

fn make_node(tt: TokenType, value: &str) -> Node {
    Node::new(
        Token::new(tt, "", value, 0, value.len() as u32),
        None,
        compact_str::CompactString::new(""),
        compact_str::CompactString::from(value),
        0,
        0,
    )
}

#[test]
fn test_double_colon_precedence() {
    let node = make_node(TokenType::DoubleColon, "::");
    let arena = vec![];
    assert_eq!(
        OperatorPrecedence::from_node(&node, &arena),
        OperatorPrecedence::DoubleColon
    );
}

#[test]
fn test_boolean_operators() {
    let and_node = make_node(TokenType::BooleanOperator, "and");
    let or_node = make_node(TokenType::BooleanOperator, "or");
    let not_node = make_node(TokenType::BooleanOperator, "not");
    let arena = vec![];

    assert_eq!(
        OperatorPrecedence::from_node(&and_node, &arena),
        OperatorPrecedence::BoolAnd
    );
    assert_eq!(
        OperatorPrecedence::from_node(&or_node, &arena),
        OperatorPrecedence::BoolOr
    );
    assert_eq!(
        OperatorPrecedence::from_node(&not_node, &arena),
        OperatorPrecedence::BoolNot
    );
}

#[test]
fn test_word_operators() {
    let as_node = make_node(TokenType::WordOperator, "as");
    let in_node = make_node(TokenType::WordOperator, "in");
    let over_node = make_node(TokenType::WordOperator, "over");
    let arena = vec![];

    assert_eq!(
        OperatorPrecedence::from_node(&as_node, &arena),
        OperatorPrecedence::As
    );
    assert_eq!(
        OperatorPrecedence::from_node(&in_node, &arena),
        OperatorPrecedence::Membership
    );
    assert_eq!(
        OperatorPrecedence::from_node(&over_node, &arena),
        OperatorPrecedence::OtherTight
    );
}

#[test]
fn test_symbol_operators() {
    let plus = make_node(TokenType::Operator, "+");
    let mul = make_node(TokenType::Operator, "*");
    let eq = make_node(TokenType::Operator, "=");
    let exp = make_node(TokenType::Operator, "**");
    let arena = vec![];

    assert_eq!(
        OperatorPrecedence::from_node(&plus, &arena),
        OperatorPrecedence::Addition
    );
    assert_eq!(
        OperatorPrecedence::from_node(&mul, &arena),
        OperatorPrecedence::Multiplication
    );
    assert_eq!(
        OperatorPrecedence::from_node(&eq, &arena),
        OperatorPrecedence::Comparators
    );
    assert_eq!(
        OperatorPrecedence::from_node(&exp, &arena),
        OperatorPrecedence::Exponent
    );
}

#[test]
fn test_tier_ordering() {
    let tiers = OperatorPrecedence::tiers();
    assert_eq!(tiers.len(), 7);
    // Tiers should be in ascending order
    for window in tiers.windows(2) {
        assert!(window[0] < window[1]);
    }
}

#[test]
fn test_between_and_precedence() {
    // "between" is a Membership-level operator
    let between_node = make_node(TokenType::WordOperator, "between");
    let arena = vec![];
    assert_eq!(
        OperatorPrecedence::from_node(&between_node, &arena),
        OperatorPrecedence::Membership
    );

    // "not between" also Membership
    let not_between = make_node(TokenType::WordOperator, "not between");
    assert_eq!(
        OperatorPrecedence::from_node(&not_between, &arena),
        OperatorPrecedence::Membership
    );
}

#[test]
fn test_presence_operators() {
    let arena = vec![];

    let is_node = make_node(TokenType::WordOperator, "is");
    assert_eq!(
        OperatorPrecedence::from_node(&is_node, &arena),
        OperatorPrecedence::Presence
    );

    let is_not_node = make_node(TokenType::WordOperator, "is not");
    assert_eq!(
        OperatorPrecedence::from_node(&is_not_node, &arena),
        OperatorPrecedence::Presence
    );

    let exists_node = make_node(TokenType::WordOperator, "exists");
    assert_eq!(
        OperatorPrecedence::from_node(&exists_node, &arena),
        OperatorPrecedence::Presence
    );
}

#[test]
fn test_membership_operators() {
    let arena = vec![];

    for op in &[
        "in",
        "not in",
        "like",
        "not like",
        "ilike",
        "not ilike",
        "similar to",
    ] {
        let node = make_node(TokenType::WordOperator, op);
        assert_eq!(
            OperatorPrecedence::from_node(&node, &arena),
            OperatorPrecedence::Membership,
            "Expected Membership for '{}'",
            op
        );
    }
}

#[test]
fn test_pg_comparison_operators() {
    let arena = vec![];

    for op in &["@>", "<@", "@@", "<->", "&&", "?|", "?&", "-|-"] {
        let node = make_node(TokenType::Operator, op);
        assert_eq!(
            OperatorPrecedence::from_node(&node, &arena),
            OperatorPrecedence::Comparators,
            "Expected Comparators for '{}'",
            op
        );
    }
}

#[test]
fn test_on_precedence() {
    let arena = vec![];
    let on_node = make_node(TokenType::On, "on");
    assert_eq!(
        OperatorPrecedence::from_node(&on_node, &arena),
        OperatorPrecedence::On
    );
}

#[test]
fn test_as_precedence() {
    let arena = vec![];
    let as_node = make_node(TokenType::WordOperator, "as");
    assert_eq!(
        OperatorPrecedence::from_node(&as_node, &arena),
        OperatorPrecedence::As
    );
}
