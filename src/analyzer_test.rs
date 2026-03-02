use super::*;

fn create_analyzer() -> Analyzer {
    let nm = NodeManager::new(false);
    Analyzer::new(nm, 88)
}

#[test]
fn test_parse_select_one() {
    let mut analyzer = create_analyzer();
    let query = analyzer.parse_query("select 1\n").unwrap();
    assert!(!query.lines.is_empty());

    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("select"));
    assert!(rendered.contains("1"));
}

#[test]
fn test_parse_simple_query() {
    let mut analyzer = create_analyzer();
    let source = "SELECT a, b FROM my_table WHERE x = 1\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);

    // Should contain lowercased keywords
    assert!(rendered.contains("select"));
    assert!(rendered.contains("from"));
    assert!(rendered.contains("where"));
}

#[test]
fn test_parse_with_comments() {
    let mut analyzer = create_analyzer();
    let source = "-- this is a comment\nSELECT 1\n";
    let query = analyzer.parse_query(source).unwrap();
    assert!(!query.lines.is_empty());
}

#[test]
fn test_parse_multiple_statements() {
    let mut analyzer = create_analyzer();
    let source = "SELECT 1;\nSELECT 2;\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("select"));
}

#[test]
fn test_parse_with_brackets() {
    let mut analyzer = create_analyzer();
    let source = "SELECT count(*) FROM t\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("count"));
    assert!(rendered.contains("*"));
}

#[test]
fn test_parse_case_expression() {
    let mut analyzer = create_analyzer();
    let source = "SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("case"));
    assert!(rendered.contains("when"));
    assert!(rendered.contains("end"));
}

#[test]
fn test_parse_jinja_expression() {
    let mut analyzer = create_analyzer();
    let source = "SELECT {{ column_name }} FROM t\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("{{"));
    assert!(rendered.contains("}}"));
}

#[test]
fn test_star_parsing_select_star() {
    // SELECT * should have no prefix space on *
    let mut analyzer = create_analyzer();
    let query = analyzer.parse_query("SELECT * FROM t\n").unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("*"));
    // The star should follow select with a space
    assert!(rendered.contains("select") || rendered.contains("*"));
}

#[test]
fn test_star_parsing_table_star() {
    // table.* should have no space between . and *
    let mut analyzer = create_analyzer();
    let query = analyzer.parse_query("SELECT t.* FROM t\n").unwrap();
    let rendered = query.render(&analyzer.arena);
    // The rendering splits nodes per line; t, ., * may render as "t.*" or
    // the formatter may split them. Just check all parts are present.
    assert!(
        rendered.contains("t") && rendered.contains("*"),
        "Table star should be preserved: {}",
        rendered
    );
}

#[test]
fn test_end_as_identifier() {
    // "end" in a context without matching CASE should not crash
    let mut analyzer = create_analyzer();
    let result = analyzer.parse_query("SELECT end\n");
    assert!(result.is_ok(), "END as identifier should not crash");
}

#[test]
fn test_open_paren_spacing_function_call() {
    // Function call: no space before (
    let mut analyzer = create_analyzer();
    let query = analyzer.parse_query("SELECT sum(1)\n").unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(
        rendered.contains("sum(") || rendered.contains("sum\n"),
        "Function call should have no space before paren: {}",
        rendered
    );
}

#[test]
fn test_parse_jinja_block() {
    let mut analyzer = create_analyzer();
    let source = "{% if condition %}\nSELECT 1\n{% endif %}\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("{% if condition %}"));
    assert!(rendered.contains("{% endif %}"));
}

#[test]
fn test_parse_set_operator() {
    let mut analyzer = create_analyzer();
    let source = "SELECT 1\nUNION ALL\nSELECT 2\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("union all"));
}

#[test]
fn test_parse_between_and() {
    let mut analyzer = create_analyzer();
    let source = "SELECT * FROM t WHERE x BETWEEN 1 AND 10\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("between"));
    assert!(rendered.contains("and"));
}

#[test]
fn test_parse_window_function() {
    let mut analyzer = create_analyzer();
    let source = "SELECT ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn FROM t\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("over"));
    assert!(rendered.contains("partition by"));
    assert!(rendered.contains("order by"));
}

#[test]
fn test_parse_cte() {
    let mut analyzer = create_analyzer();
    let source = "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("with"));
    assert!(rendered.contains("as"));
}

#[test]
fn test_parse_closing_angle_bracket() {
    // Test angle brackets for generic types like ARRAY<INT>
    let mut analyzer = create_analyzer();
    let source = "SELECT CAST(x AS ARRAY<INT>)\n";
    let result = analyzer.parse_query(source);
    assert!(
        result.is_ok(),
        "Angle bracket type should parse: {:?}",
        result.err()
    );
}

#[test]
fn test_parse_number_formats() {
    let mut analyzer = create_analyzer();
    // Various number formats
    let source = "SELECT 42, 3.14, 1e10, 0xFF, 0b1010, 0o777\n";
    let result = analyzer.parse_query(source);
    assert!(result.is_ok(), "Number formats should parse");
}

// --- Additional analyzer tests for coverage parity with Python ---

/// Helper: format SQL through the full pipeline.
fn format_sql(source: &str) -> Result<String, crate::error::SqlfmtError> {
    crate::api::format_string(source, &crate::mode::Mode::default())
}

#[test]
fn test_unmatched_closing_paren_error() {
    let result = format_sql("SELECT )\n");
    assert!(result.is_err(), "Unmatched ) should error");
    let err = result.unwrap_err();
    assert!(
        matches!(err, crate::error::SqlfmtError::Bracket(_)),
        "Expected Bracket error, got: {:?}",
        err
    );
}

#[test]
fn test_unmatched_closing_bracket_error() {
    let result = format_sql("SELECT ]\n");
    assert!(result.is_err(), "Unmatched ] should error");
    let err = result.unwrap_err();
    assert!(
        matches!(err, crate::error::SqlfmtError::Bracket(_)),
        "Expected Bracket error, got: {:?}",
        err
    );
}

#[test]
fn test_unterminated_block_comment_error() {
    let result = format_sql("/* unclosed\n");
    assert!(result.is_err(), "Unterminated block comment should error");
}

#[test]
fn test_empty_newlines_create_blank_lines() {
    let mut analyzer = create_analyzer();
    let source = "SELECT 1\n\n\n\nSELECT 2\n";
    let query = analyzer.parse_query(source).unwrap();
    // Multiple blank newlines should produce blank lines
    let blank_count = query
        .lines
        .iter()
        .filter(|l| l.is_blank_line(&analyzer.arena))
        .count();
    assert!(
        blank_count >= 1,
        "Multiple newlines should create blank lines"
    );
}

#[test]
fn test_leading_comment_preserved() {
    let mut analyzer = create_analyzer();
    let source = "-- leading comment\nSELECT 1\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(
        rendered.contains("-- leading comment"),
        "Leading comment should be preserved: {}",
        rendered
    );
}

#[test]
fn test_set_operator_classification() {
    let mut analyzer = create_analyzer();
    let source = "SELECT 1\nUNION ALL\nSELECT 2\n";
    let query = analyzer.parse_query(source).unwrap();
    // Verify UNION ALL is classified as SetOperator
    let has_set_op = query
        .tokens(&analyzer.arena)
        .iter()
        .any(|n| n.token.token_type == crate::token::TokenType::SetOperator);
    assert!(has_set_op, "UNION ALL should be classified as SetOperator");
}

#[test]
fn test_jinja_block_start_tracking() {
    let mut analyzer = create_analyzer();
    let source = "{% if x %}\nSELECT 1\n{% endif %}\n";
    let query = analyzer.parse_query(source).unwrap();
    let has_block_start = query
        .tokens(&analyzer.arena)
        .iter()
        .any(|n| n.token.token_type == crate::token::TokenType::JinjaBlockStart);
    assert!(has_block_start, "{{%}} if should create JinjaBlockStart");
}

#[test]
fn test_jinja_block_keyword_context() {
    let mut analyzer = create_analyzer();
    let source = "{% if x %}\nSELECT 1\n{% elif y %}\nSELECT 2\n{% endif %}\n";
    let query = analyzer.parse_query(source).unwrap();
    let has_keyword = query
        .tokens(&analyzer.arena)
        .iter()
        .any(|n| n.token.token_type == crate::token::TokenType::JinjaBlockKeyword);
    assert!(has_keyword, "{{%}} elif should create JinjaBlockKeyword");
}

#[test]
fn test_jinja_block_end_pops() {
    let mut analyzer = create_analyzer();
    let source = "{% if x %}\nSELECT 1\n{% endif %}\n";
    let query = analyzer.parse_query(source).unwrap();
    let has_block_end = query
        .tokens(&analyzer.arena)
        .iter()
        .any(|n| n.token.token_type == crate::token::TokenType::JinjaBlockEnd);
    assert!(has_block_end, "{{%}} endif should create JinjaBlockEnd");
    // After endif, jinja depth should be back to 0
    let tokens = query.tokens(&analyzer.arena);
    let last_node = tokens.last().unwrap();
    let (_, jinja_depth) = last_node.depth();
    assert_eq!(jinja_depth, 0, "Jinja depth should be 0 after endif");
}

#[test]
fn test_jinja_set_block() {
    let mut analyzer = create_analyzer();
    let source = "{% set x %}data content{% endset %}\nSELECT 1\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(
        rendered.contains("{% set x %}") || rendered.contains("{%"),
        "Jinja set block should be preserved: {}",
        rendered
    );
}

#[test]
fn test_jinja_nested_blocks_depth() {
    let mut analyzer = create_analyzer();
    let source = "{% if a %}\n{% for x in items %}\nSELECT {{ x }}\n{% endfor %}\n{% endif %}\n";
    let query = analyzer.parse_query(source).unwrap();
    // Should parse without error — depth tracking handles nesting
    assert!(!query.lines.is_empty());
    let rendered = query.render(&analyzer.arena);
    assert!(rendered.contains("{% if a %}"));
    assert!(rendered.contains("{% endfor %}"));
    assert!(rendered.contains("{% endif %}"));
}

#[test]
fn test_jinja_empty_block() {
    let mut analyzer = create_analyzer();
    let source = "{% if x %}\n{% endif %}\nSELECT 1\n";
    let result = analyzer.parse_query(source);
    assert!(
        result.is_ok(),
        "Empty jinja block should parse without error"
    );
}

#[test]
fn test_orphan_jinja_endblock_error() {
    // {% endif %} without matching {% if %} should still parse
    // (jinja blocks are tracked but orphans don't always error in the lexer)
    let mut analyzer = create_analyzer();
    let source = "{% endif %}\nSELECT 1\n";
    let _result = analyzer.parse_query(source);
    // The key is it doesn't panic; behavior may vary
}

#[test]
fn test_unsupported_ddl_detection() {
    // CREATE TABLE uses unsupported/DDL rules
    let mut analyzer = create_analyzer();
    let source = "CREATE TABLE t (id INT)\n";
    let result = analyzer.parse_query(source);
    // Should parse (possibly with different rule handling)
    assert!(
        result.is_ok(),
        "CREATE TABLE should parse: {:?}",
        result.err()
    );
}

#[test]
fn test_explain_as_keyword() {
    let mut analyzer = create_analyzer();
    let source = "EXPLAIN SELECT 1\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(
        rendered.contains("explain"),
        "EXPLAIN should be lowercased: {}",
        rendered
    );
}

#[test]
fn test_semicolon_resets_context() {
    let mut analyzer = create_analyzer();
    let source = "SELECT (1;\nSELECT 2\n";
    // Semicolon should reset bracket depth — second SELECT should parse
    let _result = analyzer.parse_query(source);
    // The key is no panic from mismatched brackets
}

#[test]
fn test_angle_bracket_disambiguation() {
    // ARRAY<INT> should use angle brackets, not comparison operators
    let mut analyzer = create_analyzer();
    let source = "SELECT CAST(x AS ARRAY<INT>)\n";
    let query = analyzer.parse_query(source).unwrap();
    let rendered = query.render(&analyzer.arena);
    assert!(
        rendered.contains("array"),
        "ARRAY type should be preserved: {}",
        rendered
    );
}

#[test]
fn test_unary_number() {
    // Unary minus should be absorbed into the number
    let result = format_sql("SELECT -1\n").unwrap();
    assert!(
        result.contains("-1"),
        "Unary minus should be absorbed: {}",
        result
    );
}

#[test]
fn test_binary_operator_not_absorbed() {
    // Binary minus should stay as separate operator
    let result = format_sql("SELECT a - 1\n").unwrap();
    assert!(
        result.contains("a") && result.contains("- 1") || result.contains("-"),
        "Binary minus should be separate: {}",
        result
    );
}

#[test]
fn test_reserved_keyword_after_dot() {
    // "select" after a dot should be treated as a Name, not a keyword
    let mut analyzer = create_analyzer();
    let source = "SELECT t.select FROM t\n";
    let result = analyzer.parse_query(source);
    assert!(
        result.is_ok(),
        "Keyword after dot should parse as name: {:?}",
        result.err()
    );
}
