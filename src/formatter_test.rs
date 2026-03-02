use super::*;
use crate::analyzer::Analyzer;
use crate::node_manager::NodeManager;

fn format_sql(source: &str) -> (Query, Vec<Node>) {
    let nm = NodeManager::new(false);
    let mut analyzer = Analyzer::new(nm, 88);
    let mut query = analyzer.parse_query(source).unwrap();
    let mut arena = std::mem::take(&mut analyzer.arena);

    let formatter = QueryFormatter::new(88, false);
    formatter.format(&mut query, &mut arena);

    (query, arena)
}

#[test]
fn test_format_simple_select() {
    let (query, arena) = format_sql("SELECT 1\n");
    let rendered = query.render(&arena);
    assert!(rendered.contains("select"));
    assert!(rendered.contains("1"));
}

#[test]
fn test_format_preserves_content() {
    let (query, arena) = format_sql("SELECT a, b, c FROM my_table\n");
    let rendered = query.render(&arena);
    assert!(rendered.contains("select"));
    assert!(rendered.contains("a"));
    assert!(rendered.contains("b"));
    assert!(rendered.contains("c"));
    assert!(rendered.contains("from"));
    assert!(rendered.contains("my_table"));
}

#[test]
fn test_format_removes_extra_blank_lines() {
    let (query, arena) = format_sql("SELECT 1\n\n\n\n\nSELECT 2\n");
    let rendered = query.render(&arena);
    // Should not have more than 2 consecutive blank lines at root
    assert!(!rendered.contains("\n\n\n\n"));
}

#[test]
fn test_format_splits_long_line() {
    let (query, arena) = format_sql(
        "SELECT a_very_long_field_name, another_very_long_field_name, yet_another_long_field_name, and_one_more_field FROM my_table\n",
    );
    let rendered = query.render(&arena);
    // Should be split into multiple lines since it's too long
    let line_count = rendered.lines().count();
    assert!(
        line_count > 1,
        "Long line should be split: got {} lines from: {}",
        line_count,
        rendered
    );
}

#[test]
fn test_format_merges_short_lines() {
    // When lines are short enough, the merger should combine them
    let (query, arena) = format_sql("SELECT 1\n");
    let rendered = query.render(&arena);
    // "select 1" fits on one line at 88 chars
    assert!(rendered.trim().lines().count() <= 2);
}

#[test]
fn test_format_case_expression() {
    let (query, arena) = format_sql(
        "SELECT CASE WHEN x = 1 THEN 'a' WHEN x = 2 THEN 'b' ELSE 'c' END AS result FROM t\n",
    );
    let rendered = query.render(&arena);
    assert!(rendered.contains("case"));
    assert!(rendered.contains("when"));
    assert!(rendered.contains("then"));
    assert!(rendered.contains("else"));
    assert!(rendered.contains("end"));
}

#[test]
fn test_format_join_query() {
    let (query, arena) =
        format_sql("SELECT a.id, b.name FROM table_a a LEFT JOIN table_b b ON a.id = b.a_id\n");
    let rendered = query.render(&arena);
    assert!(rendered.contains("left join"));
    assert!(rendered.contains("on"));
}

#[test]
fn test_format_with_comments() {
    let (query, arena) = format_sql("-- comment\nSELECT 1\n");
    let rendered = query.render(&arena);
    assert!(rendered.contains("select") || rendered.contains("1"));
}

#[test]
fn test_format_idempotent() {
    let source = "SELECT a, b, c FROM my_table WHERE x = 1 ORDER BY a\n";
    let (query1, arena1) = format_sql(source);
    let rendered1 = query1.render(&arena1);

    let (query2, arena2) = format_sql(&rendered1);
    let rendered2 = query2.render(&arena2);

    assert_eq!(rendered1, rendered2, "Formatting should be idempotent");
}

#[test]
fn test_format_removes_trailing_blank_lines() {
    let (query, arena) = format_sql("SELECT 1\n\n\n");
    let rendered = query.render(&arena);
    // Should not end with multiple blank lines
    assert!(
        !rendered.ends_with("\n\n\n"),
        "Should remove trailing blanks: {:?}",
        rendered
    );
}

#[test]
fn test_format_cte_query() {
    let (query, arena) =
        format_sql("WITH cte AS (SELECT 1 AS id, 'hello' AS name) SELECT * FROM cte\n");
    let rendered = query.render(&arena);
    assert!(rendered.contains("with"));
    assert!(rendered.contains("as"));
    assert!(rendered.contains("from"));
}

#[test]
fn test_format_subquery() {
    let (query, arena) =
        format_sql("SELECT * FROM (SELECT id FROM users WHERE active = true) sub\n");
    let rendered = query.render(&arena);
    assert!(rendered.contains("select"));
    assert!(rendered.contains("from"));
}

#[test]
fn test_format_jinja_block() {
    let (query, arena) = format_sql("{% if flag %}\nSELECT 1\n{% endif %}\n");
    let rendered = query.render(&arena);
    assert!(rendered.contains("{% if flag %}"));
    assert!(rendered.contains("{% endif %}"));
}
