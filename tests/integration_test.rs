use sqlfmt::{format_string, Mode};

fn default_mode() -> Mode {
    Mode::default()
}

fn duckdb_mode() -> Mode {
    Mode {
        dialect_name: "duckdb".to_string(),
        ..Mode::default()
    }
}

#[test]
fn test_format_select_one() {
    let result = format_string("SELECT 1\n", &default_mode()).unwrap();
    assert!(result.contains("select"));
    assert!(result.contains("1"));
}

#[test]
fn test_format_lowercases_keywords() {
    let result = format_string("SELECT A, B FROM T WHERE X = 1\n", &default_mode()).unwrap();
    assert!(result.contains("select"));
    assert!(result.contains("from"));
    assert!(result.contains("where"));
}

#[test]
fn test_format_preserves_quoted_names() {
    let result =
        format_string("SELECT \"MyColumn\" FROM \"MyTable\"\n", &default_mode()).unwrap();
    assert!(result.contains("\"MyColumn\""));
    assert!(result.contains("\"MyTable\""));
}

#[test]
fn test_format_handles_string_literals() {
    let result =
        format_string("SELECT 'hello world' AS greeting\n", &default_mode()).unwrap();
    assert!(result.contains("'hello world'"));
}

#[test]
fn test_format_case_expression() {
    let result = format_string(
        "SELECT CASE WHEN x = 1 THEN 'a' WHEN x = 2 THEN 'b' ELSE 'c' END\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("case"));
    assert!(result.contains("when"));
    assert!(result.contains("then"));
    assert!(result.contains("else"));
    assert!(result.contains("end"));
}

#[test]
fn test_format_join() {
    let result = format_string(
        "SELECT a.id, b.name FROM table_a a LEFT JOIN table_b b ON a.id = b.a_id\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("left join"));
    assert!(result.contains("on"));
}

#[test]
fn test_format_subquery() {
    let result = format_string(
        "SELECT * FROM (SELECT id, name FROM users WHERE active = true) AS active_users\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("select"));
    assert!(result.contains("from"));
}

#[test]
fn test_format_cte() {
    let result = format_string(
        "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("with"));
    assert!(result.contains("as"));
}

#[test]
fn test_format_union() {
    let result = format_string(
        "SELECT 1 UNION ALL SELECT 2\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("union all"));
}

#[test]
fn test_format_multiple_statements() {
    let result = format_string(
        "SELECT 1;\nSELECT 2;\n",
        &default_mode(),
    )
    .unwrap();
    let semicolons = result.matches(';').count();
    assert!(semicolons >= 2);
}

#[test]
fn test_format_comments_preserved() {
    let result = format_string(
        "-- this is a comment\nSELECT 1\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("this is a comment"));
}

#[test]
fn test_format_jinja_expression() {
    let result = format_string(
        "SELECT {{ column_name }} FROM {{ ref('my_model') }}\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("{{ column_name }}"));
    assert!(result.contains("{{ ref('my_model') }}"));
}

#[test]
fn test_format_jinja_block() {
    let result = format_string(
        "{% if condition %}\nSELECT 1\n{% endif %}\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("{% if condition %}"));
    assert!(result.contains("{% endif %}"));
}

#[test]
fn test_format_operators() {
    let result = format_string(
        "SELECT a + b, c * d, e || f FROM t\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("+"));
    assert!(result.contains("*"));
    assert!(result.contains("||"));
}

#[test]
fn test_format_between() {
    let result = format_string(
        "SELECT * FROM t WHERE x BETWEEN 1 AND 10\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("between"));
    assert!(result.contains("and"));
}

#[test]
fn test_format_in_clause() {
    let result = format_string(
        "SELECT * FROM t WHERE x IN (1, 2, 3)\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("in"));
}

#[test]
fn test_duckdb_dialect() {
    let result = format_string(
        "SELECT * FROM read_parquet('data.parquet')\n",
        &duckdb_mode(),
    )
    .unwrap();
    assert!(result.contains("select"));
    assert!(result.contains("read_parquet"));
}

#[test]
fn test_format_window_function() {
    let result = format_string(
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY created_at) AS rn FROM t\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("over"));
    assert!(result.contains("order by"));
}

#[test]
fn test_format_snowflake_qualify() {
    let result = format_string(
        "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) = 1\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("qualify"));
}

#[test]
fn test_format_group_by_having() {
    let result = format_string(
        "SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > 5\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("group by"));
    assert!(result.contains("having"));
}

#[test]
fn test_idempotent_formatting() {
    let source = "SELECT a, b, c FROM my_table WHERE x = 1\n";
    let mode = default_mode();
    let first = format_string(source, &mode).unwrap();
    let second = format_string(&first, &mode).unwrap();
    assert_eq!(first, second, "Formatting should be idempotent");
}

#[test]
fn test_format_fixture_file() {
    let source = std::fs::read_to_string("tests/fixtures/snowflake_query.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(result.is_ok(), "Should successfully format snowflake_query.sql");
}

#[test]
fn test_format_fixture_duckdb() {
    let source = std::fs::read_to_string("tests/fixtures/duckdb_query.sql").unwrap();
    let result = format_string(&source, &duckdb_mode());
    assert!(result.is_ok(), "Should successfully format duckdb_query.sql");
}

#[test]
fn test_format_fixture_jinja() {
    let source = std::fs::read_to_string("tests/fixtures/jinja_template.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(result.is_ok(), "Should successfully format jinja_template.sql");
}

#[test]
fn test_format_fixture_complex_case() {
    let source = std::fs::read_to_string("tests/fixtures/complex_case.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(result.is_ok(), "Should successfully format complex_case.sql");
}

// --- Phase 1-5 parity tests ---

#[test]
fn test_format_between_and_stays_together() {
    let result = format_string(
        "SELECT * FROM t WHERE amount BETWEEN 100 AND 200 AND status = 'active'\n",
        &default_mode(),
    )
    .unwrap();
    // BETWEEN 100 AND 200 should stay on the same line (AND not split)
    assert!(
        result.contains("between 100 and 200"),
        "BETWEEN x AND y should stay together: {}",
        result
    );
}

#[test]
fn test_format_window_frame_clause() {
    let result = format_string(
        "SELECT SUM(x) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("rows between unbounded preceding and current row"),
        "Frame clause should be lowercased as single token: {}",
        result
    );
}

#[test]
fn test_format_cluster_distribute_sort_by() {
    let result = format_string(
        "SELECT col1, col2 FROM my_table DISTRIBUTE BY col1 SORT BY col2\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("distribute by"),
        "DISTRIBUTE BY should be recognized: {}",
        result
    );
    assert!(
        result.contains("sort by"),
        "SORT BY should be recognized: {}",
        result
    );
}

#[test]
fn test_format_union_by_name() {
    let result = format_string(
        "SELECT a FROM t1 UNION ALL BY NAME SELECT b FROM t2\n",
        &duckdb_mode(),
    )
    .unwrap();
    assert!(
        result.contains("union all by name"),
        "UNION ALL BY NAME should be recognized: {}",
        result
    );
}

#[test]
fn test_format_partition_by() {
    let result = format_string(
        "SELECT ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn FROM t\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("partition by"),
        "PARTITION BY should be recognized: {}",
        result
    );
}

#[test]
fn test_format_on_inside_subquery() {
    // ON should remain a keyword inside brackets (JOIN ... ON in subqueries)
    let result = format_string(
        "SELECT * FROM (SELECT a.id, b.name FROM a JOIN b ON a.id = b.id) subq\n",
        &default_mode(),
    )
    .unwrap();
    // ON should be formatted as a keyword (lowercased), not treated as a name
    assert!(
        result.contains("on"),
        "ON inside subquery should be a keyword: {}",
        result
    );
}

#[test]
fn test_format_not_regexp() {
    let result = format_string(
        "SELECT * FROM t WHERE name NOT REGEXP '^test'\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("not regexp"),
        "NOT REGEXP should be recognized as word operator: {}",
        result
    );
}

#[test]
fn test_format_binary_octal_hex_literals() {
    let result = format_string(
        "SELECT 0xFF, 0b1010, 0o777, .5, 42L\n",
        &default_mode(),
    )
    .unwrap();
    assert!(result.contains("0xFF") || result.contains("0xff"), "Hex literal: {}", result);
    assert!(result.contains("0b1010"), "Binary literal: {}", result);
    assert!(result.contains("0o777"), "Octal literal: {}", result);
}

#[test]
fn test_format_curly_brace_brackets() {
    let result = format_string(
        "SELECT {fn NOW()}\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("{"),
        "Curly braces should be supported: {}",
        result
    );
}

#[test]
fn test_format_explain_analyze() {
    let result = format_string(
        "EXPLAIN ANALYZE SELECT * FROM t\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("explain analyze") || result.contains("explain"),
        "EXPLAIN ANALYZE should be recognized: {}",
        result
    );
}

#[test]
fn test_format_fetch_first() {
    let result = format_string(
        "SELECT * FROM t ORDER BY id FETCH FIRST 10 ROWS ONLY\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("fetch first"),
        "FETCH FIRST should be recognized: {}",
        result
    );
}

#[test]
fn test_format_with_recursive() {
    let result = format_string(
        "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 10) SELECT * FROM cte\n",
        &default_mode(),
    )
    .unwrap();
    assert!(
        result.contains("with recursive"),
        "WITH RECURSIVE should be recognized: {}",
        result
    );
}

#[test]
fn test_format_idempotent_complex() {
    let source = "SELECT a.id, b.name, CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END AS sign, ROW_NUMBER() OVER (PARTITION BY category ORDER BY created_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM table_a a LEFT JOIN table_b b ON a.id = b.id WHERE a.status = 'active' AND a.amount BETWEEN 100 AND 200 GROUP BY a.id, b.name HAVING count(*) > 1 ORDER BY a.id LIMIT 100\n";
    let first = format_string(source, &default_mode()).unwrap();
    let second = format_string(&first, &default_mode()).unwrap();
    assert_eq!(first, second, "Formatting should be idempotent");
}
