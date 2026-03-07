use super::*;
use crate::token::{Token, TokenType};

fn make_node_in_arena(
    arena: &mut Vec<Node>,
    token_type: TokenType,
    value: &str,
    prefix: &str,
) -> NodeIndex {
    let idx = arena.len();
    let prev = if idx > 0 { Some(idx - 1) } else { None };
    let mut node = Node::new(
        Token::new(token_type, "", value, 0, value.len() as u32),
        prev,
        compact_str::CompactString::from(prefix),
        compact_str::CompactString::from(value),
        0,
        0,
    );
    node.is_bracket_operator = node.compute_is_bracket_operator(arena);
    node.is_multiplication_star = node.compute_is_multiplication_star(arena);
    node.is_operator = node.token.token_type.is_always_operator()
        || node.is_multiplication_star
        || node.is_bracket_operator;
    arena.push(node);
    idx
}

#[test]
fn test_blank_line() {
    let mut arena = Vec::new();
    let idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(idx);
    assert!(line.is_blank_line(&arena));
}

#[test]
fn test_line_depth() {
    let mut arena = Vec::new();
    let idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
    arena[idx].bracket_depth = 1;
    let mut line = Line::new(None);
    line.append_node(idx);
    assert_eq!(line.depth(&arena), (1, 0));
    assert_eq!(line.indent_size(&arena), 4);
}

#[test]
fn test_render_simple() {
    let mut arena = Vec::new();
    make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let select_idx = make_node_in_arena(&mut arena, TokenType::UntermKeyword, "select", "");
    let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", " ");

    let mut line = Line::new(Some(0));
    line.nodes.push(select_idx);
    line.nodes.push(name_idx);

    let rendered = line.render(&arena);
    assert_eq!(rendered, "select a\n");
}

#[test]
fn test_starts_with_comma() {
    let mut arena = Vec::new();
    let idx = make_node_in_arena(&mut arena, TokenType::Comma, ",", "");
    let mut line = Line::new(None);
    line.append_node(idx);
    assert!(line.starts_with_comma(&arena));
}

#[test]
fn test_is_standalone_operator() {
    let mut arena = Vec::new();
    let op_idx = make_node_in_arena(&mut arena, TokenType::Operator, "+", "");
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(op_idx);
    line.append_node(nl_idx);
    assert!(line.is_standalone_operator(&arena));
}

#[test]
fn test_is_standalone_comma() {
    let mut arena = Vec::new();
    let comma_idx = make_node_in_arena(&mut arena, TokenType::Comma, ",", "");
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(comma_idx);
    line.append_node(nl_idx);
    assert!(line.is_standalone_comma(&arena));
}

#[test]
fn test_line_is_too_long() {
    let mut arena = Vec::new();
    let long_value = "a".repeat(100);
    let idx = make_node_in_arena(&mut arena, TokenType::Name, &long_value, "");
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(idx);
    line.append_node(nl_idx);
    assert!(line.len(&arena) > 88);
}

#[test]
fn test_starts_with_operator() {
    let mut arena = Vec::new();
    let op_idx = make_node_in_arena(&mut arena, TokenType::Operator, "+", "");
    let mut line = Line::new(None);
    line.append_node(op_idx);
    assert!(line.starts_with_operator(&arena));
}

#[test]
fn test_closes_bracket_from_previous_line() {
    let mut arena = Vec::new();
    let idx = make_node_in_arena(&mut arena, TokenType::BracketClose, ")", "");
    let mut line = Line::new(None);
    line.append_node(idx);
    assert!(line.closes_bracket_from_previous_line(&arena));
}

#[test]
fn test_ends_with_comma() {
    let mut arena = Vec::new();
    let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
    let comma_idx = make_node_in_arena(&mut arena, TokenType::Comma, ",", "");
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(name_idx);
    line.append_node(comma_idx);
    line.append_node(nl_idx);
    assert!(line.ends_with_comma(&arena));
}

#[test]
fn test_first_content_node_idx() {
    let mut arena = Vec::new();
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
    let mut line = Line::new(None);
    line.append_node(nl_idx);
    line.append_node(name_idx);
    assert_eq!(line.first_content_node_idx(&arena), Some(name_idx));
}

#[test]
fn test_render_blank_line() {
    let mut arena = Vec::new();
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(nl_idx);
    assert_eq!(line.render(&arena), "\n");
}

#[test]
fn test_indentation_with_depth() {
    let mut arena = Vec::new();
    let idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
    // depth 1
    arena[idx].bracket_depth = 1;
    let mut line = Line::new(None);
    line.append_node(idx);
    // 4 spaces per depth level
    assert_eq!(line.indentation(&arena), "    ");
}

#[test]
fn test_has_formatting_disabled() {
    let mut line = Line::new(None);
    assert!(!line.has_formatting_disabled());
    line.formatting_disabled = true;
    assert!(line.has_formatting_disabled());
}

#[test]
fn test_render_with_comments_inline() {
    let mut arena = Vec::new();
    let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(name_idx);
    line.append_node(nl_idx);

    // Add an inline comment
    let comment = Comment::new(
        Token::new(TokenType::Comment, "", "-- inline comment", 0, 17),
        false,
        None,
    );
    line.append_comment(comment);

    let rendered = line.render_with_comments(&arena, 88, None);
    assert!(rendered.contains("a"));
    assert!(rendered.contains("inline comment"));
}

#[test]
fn test_render_with_comments_standalone() {
    let mut arena = Vec::new();
    let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
    let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
    let mut line = Line::new(None);
    line.append_node(name_idx);
    line.append_node(nl_idx);

    // Add a standalone comment
    let comment = Comment::new(
        Token::new(TokenType::Comment, "", "-- standalone", 0, 13),
        true,
        None,
    );
    line.append_comment(comment);

    let rendered = line.render_with_comments(&arena, 88, None);
    assert!(rendered.contains("standalone"));
    assert!(rendered.contains("a"));
}
