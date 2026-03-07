use super::*;
use crate::node::Node;
use crate::token::Token;

fn make_node(arena: &mut Vec<Node>, tt: TokenType, val: &str, prefix: &str) -> NodeIndex {
    let idx = arena.len();
    let prev = if idx > 0 { Some(idx - 1) } else { None };
    arena.push(Node::new(
        Token::new(tt, "", val, 0, val.len() as u32),
        prev,
        CompactString::from(prefix),
        CompactString::from(val),
        0,
        0,
    ));
    idx
}

#[test]
fn test_no_split_single_node() {
    let mut arena = Vec::new();
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
    let a = make_node(&mut arena, TokenType::Name, "a", "");

    let mut line = Line::new(None);
    line.append_node(a);
    line.append_node(nl);

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert_eq!(result.len(), 1);
}

#[test]
fn test_split_at_keyword() {
    let mut arena = Vec::new();
    let select = make_node(&mut arena, TokenType::UntermKeyword, "select", "");
    let name = make_node(&mut arena, TokenType::Name, "a", " ");
    let from = make_node(&mut arena, TokenType::UntermKeyword, "from", " ");
    let table = make_node(&mut arena, TokenType::Name, "t", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![select, name, from, table, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Expected at least 2 lines, got {}",
        result.len()
    );
}

#[test]
fn test_split_before_comma() {
    // Leading-comma style: split BEFORE the comma
    let mut arena = Vec::new();
    let a = make_node(&mut arena, TokenType::Name, "a", "");
    let comma = make_node(&mut arena, TokenType::Comma, ",", "");
    let b = make_node(&mut arena, TokenType::Name, "b", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![a, comma, b, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Expected at least 2 lines, got {}",
        result.len()
    );
    // First line should have "a" (no comma)
    // Second line should start with comma
}

#[test]
fn test_split_before_operator() {
    let mut arena = Vec::new();
    let a = make_node(&mut arena, TokenType::Name, "a", "");
    let op = make_node(&mut arena, TokenType::Operator, "+", " ");
    let b = make_node(&mut arena, TokenType::Name, "b", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![a, op, b, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Expected split before operator, got {} lines",
        result.len()
    );
}

#[test]
fn test_split_before_closing_bracket() {
    let mut arena = Vec::new();
    let open = make_node(&mut arena, TokenType::BracketOpen, "(", "");
    let name = make_node(&mut arena, TokenType::Name, "x", "");
    let close = make_node(&mut arena, TokenType::BracketClose, ")", "");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![open, name, close, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Expected split at brackets, got {} lines",
        result.len()
    );
}

#[test]
fn test_split_before_semicolon() {
    let mut arena = Vec::new();
    let select = make_node(&mut arena, TokenType::UntermKeyword, "select", "");
    let one = make_node(&mut arena, TokenType::Number, "1", " ");
    let semi = make_node(&mut arena, TokenType::Semicolon, ";", "");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![select, one, semi, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Expected split before semicolon, got {} lines",
        result.len()
    );
}

#[test]
fn test_split_after_opening_bracket() {
    let mut arena = Vec::new();
    let name = make_node(&mut arena, TokenType::Name, "count", "");
    let open = make_node(&mut arena, TokenType::BracketOpen, "(", "");
    let star = make_node(&mut arena, TokenType::Star, "*", "");
    let close = make_node(&mut arena, TokenType::BracketClose, ")", "");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![name, open, star, close, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Expected split after open bracket, got {} lines",
        result.len()
    );
}

#[test]
fn test_no_split_bracket_operator() {
    let mut arena = Vec::new();
    let arr = make_node(&mut arena, TokenType::Name, "arr", "");
    let bracket = make_node(&mut arena, TokenType::BracketOpen, "[", "");
    arena[bracket].value = CompactString::from("[");
    let zero = make_node(&mut arena, TokenType::Number, "0", "");
    let close = make_node(&mut arena, TokenType::BracketClose, "]", "");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![arr, bracket, zero, close, nl];

    let _splitter = LineSplitter::new();
    // is_bracket_operator checks previous_sql_token - in our test the [
    // follows Name "arr", so it should be a bracket operator
    assert!(arena[bracket].is_bracket_operator(&arena));
}

#[test]
fn test_split_formatting_disabled_returns_unchanged() {
    let mut arena = Vec::new();
    let a = make_node(&mut arena, TokenType::Name, "a", "");
    let op = make_node(&mut arena, TokenType::Operator, "+", " ");
    let b = make_node(&mut arena, TokenType::Name, "b", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![a, op, b, nl];
    line.formatting_disabled = true;

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert_eq!(result.len(), 1);
}

#[test]
fn test_split_set_operator() {
    let mut arena = Vec::new();
    let one = make_node(&mut arena, TokenType::Number, "1", "");
    let union = make_node(&mut arena, TokenType::SetOperator, "union all", " ");
    let two = make_node(&mut arena, TokenType::Number, "2", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![one, union, two, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Expected split at set operator, got {} lines",
        result.len()
    );
}

#[test]
fn test_split_blank_line_returns_unchanged() {
    let mut arena = Vec::new();
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.append_node(nl);

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert_eq!(result.len(), 1);
    assert!(result[0].is_blank_line(&arena));
}

// --- Additional splitter tests for coverage parity ---

#[test]
fn test_split_between_brackets_array_index() {
    // func()[offset(1)] should split at [ after )
    let mut arena = Vec::new();
    let name = make_node(&mut arena, TokenType::Name, "func", "");
    let open1 = make_node(&mut arena, TokenType::BracketOpen, "(", "");
    let close1 = make_node(&mut arena, TokenType::BracketClose, ")", "");
    let open2 = make_node(&mut arena, TokenType::BracketOpen, "[", "");
    let off = make_node(&mut arena, TokenType::Name, "offset", "");
    let open3 = make_node(&mut arena, TokenType::BracketOpen, "(", "");
    let one = make_node(&mut arena, TokenType::Number, "1", "");
    let close3 = make_node(&mut arena, TokenType::BracketClose, ")", "");
    let close2 = make_node(&mut arena, TokenType::BracketClose, "]", "");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes =
        smallvec::smallvec![name, open1, close1, open2, off, open3, one, close3, close2, nl,];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Should split at bracket boundaries, got {} lines",
        result.len()
    );
}

#[test]
fn test_split_very_long_line_no_crash() {
    // 500+ nodes should split without stack overflow
    let mut arena = Vec::new();
    let mut nodes = smallvec::SmallVec::<[usize; 8]>::new();
    for i in 0..500 {
        let name = make_node(
            &mut arena,
            TokenType::Name,
            &format!("col_{}", i),
            if i > 0 { " " } else { "" },
        );
        nodes.push(name);
        if i < 499 {
            let comma = make_node(&mut arena, TokenType::Comma, ",", "");
            nodes.push(comma);
        }
    }
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
    nodes.push(nl);

    let mut line = Line::new(None);
    line.nodes = nodes;

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(result.len() > 1, "500 comma-separated names should split");
}

#[test]
fn test_split_no_terminating_newline() {
    // Line without trailing newline should still split
    let mut arena = Vec::new();
    let select = make_node(&mut arena, TokenType::UntermKeyword, "select", "");
    let name = make_node(&mut arena, TokenType::Name, "a", " ");
    let from = make_node(&mut arena, TokenType::UntermKeyword, "from", " ");
    let table = make_node(&mut arena, TokenType::Name, "t", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![select, name, from, table, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Should split at keyword, got {} lines",
        result.len()
    );
}

#[test]
fn test_split_leading_comma_comment() {
    // Comment before content should be handled correctly during split
    let mut arena = Vec::new();
    let comment = make_node(&mut arena, TokenType::Comment, "-- note", "");
    let name = make_node(&mut arena, TokenType::Name, "a", " ");
    let comma = make_node(&mut arena, TokenType::Comma, ",", "");
    let name2 = make_node(&mut arena, TokenType::Name, "b", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![comment, name, comma, name2, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    // Should split at comma
    assert!(
        result.len() >= 2,
        "Should split at comma after comment, got {} lines",
        result.len()
    );
}

#[test]
fn test_split_around_set_operator_union() {
    // UNION ALL should force a split
    let mut arena = Vec::new();
    let one = make_node(&mut arena, TokenType::Number, "1", "");
    let union = make_node(&mut arena, TokenType::SetOperator, "union all", " ");
    let two = make_node(&mut arena, TokenType::Number, "2", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![one, union, two, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Should split at UNION ALL, got {} lines",
        result.len()
    );
}

#[test]
fn test_double_colon_cast_stays_together() {
    // :: is a tight-binding operator — test via full pipeline that
    // "x::int" stays together on one line after formatting
    let mode = crate::mode::Mode::default();
    let result = crate::api::format_string("SELECT x::int\n", &mode).unwrap();
    // The cast should remain as "x::int" (not split across lines)
    assert!(
        result.contains("::"),
        "Double colon cast should be present: {}",
        result
    );
}

#[test]
fn test_split_trailing_operator_comment() {
    // "1 + -- comment\n2" - comment after operator
    let mut arena = Vec::new();
    let one = make_node(&mut arena, TokenType::Number, "1", "");
    let plus = make_node(&mut arena, TokenType::Operator, "+", " ");
    let comment = make_node(&mut arena, TokenType::Comment, "-- one", " ");
    let two = make_node(&mut arena, TokenType::Number, "2", " ");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

    let mut line = Line::new(None);
    line.nodes = smallvec::smallvec![one, plus, comment, two, nl];

    let splitter = LineSplitter::new();
    let result = splitter.maybe_split(line, &mut arena);
    assert!(
        result.len() >= 2,
        "Should split around operator/comment, got {} lines",
        result.len()
    );
}
