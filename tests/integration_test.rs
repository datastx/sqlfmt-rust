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
