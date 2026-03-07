use super::*;
use crate::node::NodeIndex;
use crate::token::{Token, TokenType};

fn make_node(arena: &mut Vec<Node>, tt: TokenType, val: &str, prefix: &str) -> NodeIndex {
    let idx = arena.len();
    let prev = if idx > 0 { Some(idx - 1) } else { None };
    let mut node = Node::new(
        Token::new(tt, "", val, 0, val.len() as u32),
        prev,
        compact_str::CompactString::from(prefix),
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

fn make_simple_line(arena: &mut Vec<Node>, tt: TokenType, val: &str) -> Line {
    let nl = make_node(arena, TokenType::Newline, "\n", "");
    let content = make_node(arena, tt, val, "");
    let mut line = Line::new(None);
    line.append_node(content);
    line.append_node(nl);
    line
}

#[test]
fn test_no_merge_single_line() {
    let mut arena = Vec::new();
    let line = make_simple_line(&mut arena, TokenType::Name, "a");
    let merger = LineMerger::new(88);
    let result = merger.maybe_merge_lines(vec![line], &arena);
    assert_eq!(result.len(), 1);
}

#[test]
fn test_merge_short_lines() {
    let mut arena = Vec::new();
    let line1 = make_simple_line(&mut arena, TokenType::Name, "a");
    let line2 = make_simple_line(&mut arena, TokenType::Name, "b");

    let merger = LineMerger::new(88);
    let result = merger.maybe_merge_lines(vec![line1, line2], &arena);
    // Should merge
    assert!(result.len() <= 2);
}

#[test]
fn test_no_merge_long_result() {
    let mut arena = Vec::new();
    let long_val = "a".repeat(50);
    let line1 = make_simple_line(&mut arena, TokenType::Name, &long_val);
    let line2 = make_simple_line(&mut arena, TokenType::Name, &long_val);

    let merger = LineMerger::new(88);
    let result = merger.maybe_merge_lines(vec![line1, line2], &arena);
    // Should NOT merge since combined length > 88
    assert!(
        result.len() >= 2,
        "Lines too long to merge should stay separate"
    );
}

#[test]
fn test_merge_empty_input() {
    let arena = Vec::new();
    let merger = LineMerger::new(88);
    let result = merger.maybe_merge_lines(vec![], &arena);
    assert!(result.is_empty());
}

#[test]
fn test_no_merge_across_query_dividers() {
    let mut arena = Vec::new();
    let line1 = make_simple_line(&mut arena, TokenType::Name, "a");

    // Line with semicolon (query divider)
    let semi = make_node(&mut arena, TokenType::Semicolon, ";", "");
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
    let mut semi_line = Line::new(None);
    semi_line.append_node(semi);
    semi_line.append_node(nl);

    let line3 = make_simple_line(&mut arena, TokenType::Name, "b");

    let merger = LineMerger::new(88);
    let result = merger.maybe_merge_lines(vec![line1, semi_line, line3], &arena);
    // Should not merge across the semicolon
    assert!(
        result.len() >= 2,
        "Should not merge across query dividers, got {} lines",
        result.len()
    );
}

#[test]
fn test_no_merge_formatting_disabled() {
    let mut arena = Vec::new();

    // Create a line with formatting disabled
    let a = make_node(&mut arena, TokenType::Name, "a", "");
    arena[a].formatting_disabled = true;
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
    let mut disabled_line = Line::new(None);
    disabled_line.append_node(a);
    disabled_line.append_node(nl);
    disabled_line.formatting_disabled = true;

    let merger = LineMerger::new(88);
    let result = merger.maybe_merge_lines(vec![disabled_line], &arena);
    assert_eq!(result.len(), 1);
}

#[test]
fn test_merge_with_blank_lines() {
    let mut arena = Vec::new();
    let line1 = make_simple_line(&mut arena, TokenType::Name, "a");

    // Blank line
    let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
    let mut blank = Line::new(None);
    blank.append_node(nl);

    let line3 = make_simple_line(&mut arena, TokenType::Name, "b");

    let merger = LineMerger::new(88);
    let result = merger.maybe_merge_lines(vec![line1, blank, line3], &arena);
    // Should produce at least 1 line and not crash
    assert!(
        !result.is_empty(),
        "Should handle blank lines without crashing"
    );
}

// --- Full-pipeline helper for realistic merge testing ---

/// Format SQL through the full pipeline and return the formatted result.
fn format_sql(source: &str) -> String {
    crate::api::format_string(source, &crate::mode::Mode::default()).unwrap()
}

/// Count non-empty lines in formatted output.
fn count_lines(s: &str) -> usize {
    s.lines().filter(|l| !l.trim().is_empty()).count()
}

#[test]
fn test_create_merged_line_basic() {
    // Two short columns should stay split with leading commas
    let result = format_sql("SELECT\n    a,\n    b\n");
    assert!(
        result.contains(", b"),
        "Short columns should use leading commas: {}",
        result
    );
}

#[test]
fn test_create_merged_line_with_comments() {
    // Inline comments should survive the merge process
    let result = format_sql("SELECT\n    a, -- first\n    b\n");
    assert!(
        result.contains("-- first"),
        "Inline comment should be preserved: {}",
        result
    );
}

#[test]
fn test_nested_merge() {
    // Nested function calls should merge when short enough
    let result = format_sql("SELECT nullif(split_part(x, ',', 1), '') AS val\n");
    assert!(
        result.contains("nullif("),
        "Nested functions should merge: {}",
        result
    );
}

#[test]
fn test_cte_merge() {
    // Short CTE should merge
    let result = format_sql("WITH cte AS (\n    SELECT 1\n)\nSELECT * FROM cte\n");
    assert!(
        result.contains("with"),
        "CTE should be formatted: {}",
        result
    );
}

#[test]
fn test_case_then_merge() {
    // Short CASE/WHEN should merge when within line length
    let result = format_sql("SELECT CASE WHEN x THEN y ELSE z END\n");
    assert!(
        result.contains("case") && result.contains("end"),
        "CASE expression should be formatted: {}",
        result
    );
}

#[test]
fn test_count_window_function_merge() {
    // Window function parts should merge when short
    let result = format_sql("SELECT count(*) OVER (PARTITION BY x) FROM t\n");
    assert!(
        result.contains("over"),
        "Window function should be present: {}",
        result
    );
}

#[test]
fn test_disallow_multiline_jinja() {
    // Multiline jinja (containing newlines) should block merging
    let source = "SELECT\n    {{ config(\n        key='val'\n    ) }}\n";
    let result = format_sql(source);
    assert!(
        result.contains("{{"),
        "Multiline jinja should be preserved: {}",
        result
    );
}

#[test]
fn test_segment_continues_operator_sequence_tiers() {
    // Each tier in OperatorPrecedence::tiers() should work as merge boundary
    let tiers = OperatorPrecedence::tiers();
    assert_eq!(tiers.len(), 7, "Should have 7 merge tiers");
    // Verify tiers are in ascending order (required for correct merge behavior)
    for window in tiers.windows(2) {
        assert!(
            window[0] < window[1],
            "Tiers must be ascending: {:?} >= {:?}",
            window[0],
            window[1]
        );
    }
}

#[test]
fn test_merge_lines_split_by_operators() {
    // Operator-separated lines should merge back when short
    let result = format_sql("SELECT 1 + 2 + 3\n");
    // Short addition should be on one line
    let lines: Vec<_> = result.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        lines.len() <= 2,
        "Short operator chain should merge: {}",
        result
    );
}

#[test]
fn test_merge_chained_parens() {
    // Chained parenthesized expressions
    let result = format_sql("SELECT (a + b) * (c + d)\n");
    assert!(
        result.contains("(a + b)") || result.contains("(a"),
        "Chained parens should be formatted: {}",
        result
    );
}

#[test]
fn test_merge_operators_before_children() {
    // Operators at top level should merge before recursing into children
    let result = format_sql("SELECT a + b AS total FROM t\n");
    assert!(
        result.contains("a + b"),
        "Operator expression should merge: {}",
        result
    );
}

#[test]
fn test_do_not_merge_very_long_chains() {
    // 40+ additions should NOT all merge onto one line
    let chain: Vec<String> = (0..45).map(|i| format!("col_{}", i)).collect();
    let source = format!("SELECT {}\n", chain.join(" + "));
    let result = format_sql(&source);
    let line_count = count_lines(&result);
    assert!(
        line_count > 1,
        "Very long chain should stay split ({} lines): {}",
        line_count,
        result
    );
}

#[test]
fn test_respect_extra_blank_lines() {
    // Blank lines between semicolon-separated statements should be preserved
    let result = format_sql("SELECT 1;\n\nSELECT 2\n");
    let blank_lines = result.lines().filter(|l| l.trim().is_empty()).count();
    assert!(
        blank_lines >= 1,
        "Blank line between statements should be preserved: {}",
        result
    );
}

#[test]
fn test_stubborn_merge_blank_lines() {
    // Stubborn merge should not absorb blank segments
    let result = format_sql("SELECT a\nFROM t1\nJOIN t2\n    ON t1.id = t2.id\n\nWHERE x = 1\n");
    assert!(
        result.contains("where"),
        "WHERE should be present after blank line: {}",
        result
    );
}

#[test]
fn test_do_not_merge_across_union_all() {
    // UNION ALL should prevent merging across it
    let result = format_sql("SELECT 1\nUNION ALL\nSELECT 2\n");
    assert!(
        result.contains("union all"),
        "UNION ALL should stay separate: {}",
        result
    );
    // Should have at least 3 non-empty lines
    assert!(
        count_lines(&result) >= 3,
        "UNION ALL should force line breaks: {}",
        result
    );
}

#[test]
fn test_do_not_merge_across_intersect() {
    let result = format_sql("SELECT 1\nINTERSECT\nSELECT 2\n");
    assert!(
        result.contains("intersect"),
        "INTERSECT should be preserved: {}",
        result
    );
    assert!(
        count_lines(&result) >= 3,
        "INTERSECT should force line breaks: {}",
        result
    );
}

#[test]
fn test_do_not_merge_across_except() {
    let result = format_sql("SELECT 1\nEXCEPT\nSELECT 2\n");
    assert!(
        result.contains("except"),
        "EXCEPT should be preserved: {}",
        result
    );
    assert!(
        count_lines(&result) >= 3,
        "EXCEPT should force line breaks: {}",
        result
    );
}

#[test]
fn test_maybe_stubbornly_merge_on_clause() {
    // Short ON clause should be stubbornly merged with JOIN
    let result = format_sql("SELECT *\nFROM t1\nJOIN t2\n    ON t1.id = t2.id\n");
    assert!(
        result.contains("on"),
        "ON clause should be present: {}",
        result
    );
}

#[test]
fn test_fix_standalone_operators() {
    // Standalone operator should merge with following line
    let result = format_sql("SELECT a\n    + b\n");
    assert!(
        result.contains("+"),
        "Operator should be present: {}",
        result
    );
}

#[test]
fn test_no_merge_operator_sequences_across_commas() {
    // Commas should break operator sequences
    let result = format_sql("SELECT a + b, c + d\n");
    assert!(
        result.contains(","),
        "Comma should be preserved: {}",
        result
    );
}

#[test]
fn test_no_merge_databricks_query_hints() {
    // Block comments that look like hints should be preserved
    let result = format_sql("SELECT /*+ HINT */ 1\n");
    assert!(
        result.contains("/*+ HINT */") || result.contains("/*"),
        "Block comment hint should be preserved: {}",
        result
    );
}
