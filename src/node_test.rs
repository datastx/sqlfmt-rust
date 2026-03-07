use super::*;
use crate::token::Token;

fn make_node(token_type: TokenType, value: &str, prev: Option<NodeIndex>) -> Node {
    Node::new(
        Token::new(token_type, "", value, 0, value.len() as u32),
        prev,
        CompactString::new(""),
        CompactString::from(value),
        0,
        0,
    )
}

#[test]
fn test_depth_empty() {
    let node = make_node(TokenType::Name, "foo", None);
    assert_eq!(node.depth(), (0, 0));
}

#[test]
fn test_depth_with_brackets() {
    let mut node = make_node(TokenType::Name, "foo", None);
    node.bracket_depth = 2;
    node.jinja_depth = 1;
    assert_eq!(node.depth(), (2, 1));
}

#[test]
fn test_is_multiplication_star() {
    // Star after a name => multiplication
    let mut arena = Vec::new();
    arena.push(make_node(TokenType::Name, "a", None));
    let mut star = make_node(TokenType::Star, "*", Some(0));
    star.token = Token::new(TokenType::Star, "", "*", 2, 3);
    assert!(star.compute_is_multiplication_star(&arena));

    // Star after SELECT => not multiplication
    let mut arena2 = Vec::new();
    arena2.push(make_node(TokenType::UntermKeyword, "select", None));
    let star2 = make_node(TokenType::Star, "*", Some(0));
    assert!(!star2.compute_is_multiplication_star(&arena2));
}

#[test]
fn test_get_previous_sql_token_skips_newline() {
    let mut arena = Vec::new();
    arena.push(make_node(TokenType::Name, "a", None));
    arena.push(make_node(TokenType::Newline, "\n", Some(0)));
    let node = make_node(TokenType::Name, "b", Some(1));
    let prev = node.get_previous_sql_token(&arena);
    assert!(prev.is_some());
    assert_eq!(prev.unwrap().text, "a");
}

#[test]
fn test_is_the_between_operator() {
    // A WordOperator "between" should be identified correctly
    let node = make_node(TokenType::WordOperator, "between", None);
    assert_eq!(node.token.token_type, TokenType::WordOperator);
    assert!(node.value.eq_ignore_ascii_case("between"));

    // Case-insensitive check
    let node2 = make_node(TokenType::WordOperator, "BETWEEN", None);
    assert!(node2.value.eq_ignore_ascii_case("between"));

    // "like" is NOT between
    let node3 = make_node(TokenType::WordOperator, "like", None);
    assert!(!node3.value.eq_ignore_ascii_case("between"));
}

#[test]
fn test_is_square_bracket_operator() {
    // Square bracket after a Name => bracket operator (array indexing)
    let mut arena = Vec::new();
    arena.push(make_node(TokenType::Name, "arr", None));
    let mut bracket = make_node(TokenType::BracketOpen, "[", Some(0));
    bracket.value = CompactString::from("[");
    assert!(bracket.compute_is_bracket_operator(&arena));

    // Square bracket after QuotedName => bracket operator
    let mut arena2 = Vec::new();
    arena2.push(make_node(TokenType::QuotedName, "\"my_col\"", None));
    let mut bracket2 = make_node(TokenType::BracketOpen, "[", Some(0));
    bracket2.value = CompactString::from("[");
    assert!(bracket2.compute_is_bracket_operator(&arena2));

    // Square bracket after BracketClose => bracket operator
    let mut arena3 = Vec::new();
    arena3.push(make_node(TokenType::BracketClose, "]", None));
    let mut bracket3 = make_node(TokenType::BracketOpen, "[", Some(0));
    bracket3.value = CompactString::from("[");
    assert!(bracket3.compute_is_bracket_operator(&arena3));

    // Square bracket with no previous node => NOT bracket operator
    let arena4: Vec<Node> = Vec::new();
    let mut bracket4 = make_node(TokenType::BracketOpen, "[", None);
    bracket4.value = CompactString::from("[");
    assert!(!bracket4.compute_is_bracket_operator(&arena4));
}

#[test]
fn test_is_the_and_after_the_between_operator() {
    // Build: BETWEEN x AND y
    let mut arena = Vec::new();
    // index 0: between
    arena.push(make_node(TokenType::WordOperator, "between", None));
    // index 1: x (name)
    arena.push(make_node(TokenType::Name, "x", Some(0)));
    // index 2: and (boolean operator)
    arena.push(make_node(TokenType::BooleanOperator, "and", Some(1)));

    // The AND at index 2 should be recognized as "the AND after BETWEEN"
    assert!(arena[2].is_the_and_after_between(&arena));

    // A standalone AND without BETWEEN should NOT be
    let mut arena2 = Vec::new();
    arena2.push(make_node(TokenType::Name, "a", None));
    arena2.push(make_node(TokenType::BooleanOperator, "and", Some(0)));
    assert!(!arena2[1].is_the_and_after_between(&arena2));
}

#[test]
fn test_is_operator_context() {
    // A WordOperator is always an operator
    let arena: Vec<Node> = Vec::new();
    let node = make_node(TokenType::WordOperator, "in", None);
    assert!(
        node.token.token_type.is_always_operator()
            || node.compute_is_multiplication_star(&arena)
            || node.compute_is_bracket_operator(&arena)
    );

    // An Operator token is always an operator
    let node2 = make_node(TokenType::Operator, "+", None);
    assert!(
        node2.token.token_type.is_always_operator()
            || node2.compute_is_multiplication_star(&arena)
            || node2.compute_is_bracket_operator(&arena)
    );

    // A Name is NOT an operator
    let node3 = make_node(TokenType::Name, "foo", None);
    assert!(
        !node3.token.token_type.is_always_operator()
            && !node3.compute_is_multiplication_star(&arena)
            && !node3.compute_is_bracket_operator(&arena)
    );
}

#[test]
fn test_node_classification_methods() {
    assert!(make_node(TokenType::UntermKeyword, "select", None).is_unterm_keyword());
    assert!(make_node(TokenType::Comma, ",", None).is_comma());
    assert!(make_node(TokenType::BracketOpen, "(", None).is_opening_bracket());
    assert!(make_node(TokenType::BracketClose, ")", None).is_closing_bracket());
    assert!(make_node(TokenType::Newline, "\n", None).is_newline());
    assert!(make_node(TokenType::BooleanOperator, "and", None).is_boolean_operator());
}

#[test]
fn test_divides_queries() {
    assert!(make_node(TokenType::Semicolon, ";", None).divides_queries());
    assert!(make_node(TokenType::SetOperator, "union all", None).divides_queries());
    assert!(!make_node(TokenType::Name, "foo", None).divides_queries());
}

#[test]
fn test_is_multiline_jinja() {
    let mut node = make_node(TokenType::JinjaExpression, "{{ foo }}", None);
    assert!(!node.is_multiline_jinja());

    node.value = CompactString::from("{{ foo\n  bar }}");
    assert!(node.is_multiline_jinja());
}

#[test]
fn test_is_opening_closing_jinja_block() {
    assert!(make_node(TokenType::JinjaBlockStart, "{% if x %}", None).is_opening_jinja_block());
    assert!(make_node(TokenType::JinjaBlockKeyword, "{% elif y %}", None).is_opening_jinja_block());
    assert!(make_node(TokenType::JinjaBlockEnd, "{% endif %}", None).is_closing_jinja_block());
    assert!(!make_node(TokenType::JinjaExpression, "{{ x }}", None).is_opening_jinja_block());
    assert!(!make_node(TokenType::JinjaExpression, "{{ x }}", None).is_closing_jinja_block());
}
