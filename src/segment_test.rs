use super::*;
use crate::node::Node;
use crate::token::{Token, TokenType};

fn make_node(arena: &mut Vec<Node>, tt: TokenType, val: &str) -> usize {
    let idx = arena.len();
    let prev = if idx > 0 { Some(idx - 1) } else { None };
    let mut node = Node::new(
        Token::new(tt, "", val, 0, val.len() as u32),
        prev,
        compact_str::CompactString::new(""),
        compact_str::CompactString::from(val),
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

fn make_line(arena: &mut Vec<Node>, tt: TokenType, val: &str) -> Line {
    let idx = make_node(arena, tt, val);
    let nl = make_node(arena, TokenType::Newline, "\n");
    let mut line = Line::new(None);
    line.nodes.push(idx);
    line.nodes.push(nl);
    line
}

#[test]
fn test_segment_head_tail() {
    let mut arena = Vec::new();
    let line1 = make_line(&mut arena, TokenType::Name, "a");
    let line2 = make_line(&mut arena, TokenType::Name, "b");

    let seg = Segment::new(vec![line1, line2]);
    let (head_idx, _) = seg.head(&arena).unwrap();
    let (tail_from_bottom, _) = seg.tail(&arena).unwrap();
    assert_eq!(head_idx, 0);
    assert_eq!(tail_from_bottom, 0);
}

#[test]
fn test_build_segments() {
    let mut arena = Vec::new();
    let line1 = make_line(&mut arena, TokenType::Name, "a");
    let line2 = make_line(&mut arena, TokenType::Name, "b");

    let segments = build_segments(vec![line1, line2], &arena);
    assert!(!segments.is_empty());
}

#[test]
fn test_build_segments_empty() {
    let arena: Vec<Node> = Vec::new();
    let segments = build_segments(vec![], &arena);
    assert!(segments.is_empty());
}

#[test]
fn test_segment_head_raises_on_empty() {
    let arena: Vec<Node> = Vec::new();
    let seg = Segment::new(vec![]);
    assert!(seg.head(&arena).is_err());
}

#[test]
fn test_segment_tail_raises_on_empty() {
    let arena: Vec<Node> = Vec::new();
    let seg = Segment::new(vec![]);
    assert!(seg.tail(&arena).is_err());
}

#[test]
fn test_segment_head_skips_blank() {
    let mut arena = Vec::new();
    // First line: blank
    let nl_idx1 = make_node(&mut arena, TokenType::Newline, "\n");
    let mut blank_line = Line::new(None);
    blank_line.nodes.push(nl_idx1);

    // Second line: content
    let content_line = make_line(&mut arena, TokenType::Name, "a");

    let seg = Segment::new(vec![blank_line, content_line]);
    let (head_idx, _) = seg.head(&arena).unwrap();
    // Skipped the blank line
    assert_eq!(head_idx, 1);
}

#[test]
fn test_segment_is_empty() {
    let seg = Segment::new(vec![]);
    assert!(seg.is_empty());
    assert_eq!(seg.len(), 0);
}

#[test]
fn test_split_after() {
    let mut arena = Vec::new();
    let line1 = make_line(&mut arena, TokenType::Name, "a");
    let line2 = make_line(&mut arena, TokenType::Name, "b");
    let line3 = make_line(&mut arena, TokenType::Name, "c");

    let seg = Segment::new(vec![line1, line2, line3]);
    let result = seg.split_after(0, &arena);
    assert!(!result.is_empty());
    // Should have remaining lines after index 0
    let total_remaining: usize = result.iter().map(|s| s.len()).sum();
    assert_eq!(total_remaining, 2);
}
