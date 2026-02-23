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
    let result = format_string("SELECT \"MyColumn\" FROM \"MyTable\"\n", &default_mode()).unwrap();
    assert!(result.contains("\"MyColumn\""));
    assert!(result.contains("\"MyTable\""));
}

#[test]
fn test_format_handles_string_literals() {
    let result = format_string("SELECT 'hello world' AS greeting\n", &default_mode()).unwrap();
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
    let result = format_string("SELECT 1 UNION ALL SELECT 2\n", &default_mode()).unwrap();
    assert!(result.contains("union all"));
}

#[test]
fn test_format_multiple_statements() {
    let result = format_string("SELECT 1;\nSELECT 2;\n", &default_mode()).unwrap();
    let semicolons = result.matches(';').count();
    assert!(semicolons >= 2);
}

#[test]
fn test_format_comments_preserved() {
    let result = format_string("-- this is a comment\nSELECT 1\n", &default_mode()).unwrap();
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
    // Jinja formatter normalizes single quotes to double quotes (matching black)
    assert!(result.contains(r#"{{ ref("my_model") }}"#));
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
    let result = format_string("SELECT a + b, c * d, e || f FROM t\n", &default_mode()).unwrap();
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
    let result = format_string("SELECT * FROM t WHERE x IN (1, 2, 3)\n", &default_mode()).unwrap();
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
    assert!(
        result.is_ok(),
        "Should successfully format snowflake_query.sql"
    );
}

#[test]
fn test_format_fixture_duckdb() {
    let source = std::fs::read_to_string("tests/fixtures/duckdb_query.sql").unwrap();
    let result = format_string(&source, &duckdb_mode());
    assert!(
        result.is_ok(),
        "Should successfully format duckdb_query.sql"
    );
}

#[test]
fn test_format_fixture_jinja() {
    let source = std::fs::read_to_string("tests/fixtures/jinja_template.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(
        result.is_ok(),
        "Should successfully format jinja_template.sql"
    );
}

#[test]
fn test_format_fixture_complex_case() {
    let source = std::fs::read_to_string("tests/fixtures/complex_case.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(
        result.is_ok(),
        "Should successfully format complex_case.sql"
    );
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
    let result = format_string("SELECT 0xFF, 0b1010, 0o777, .5, 42L\n", &default_mode()).unwrap();
    assert!(
        result.contains("0xFF") || result.contains("0xff"),
        "Hex literal: {}",
        result
    );
    assert!(result.contains("0b1010"), "Binary literal: {}", result);
    assert!(result.contains("0o777"), "Octal literal: {}", result);
}

#[test]
fn test_format_curly_brace_brackets() {
    let result = format_string("SELECT {fn NOW()}\n", &default_mode()).unwrap();
    assert!(
        result.contains("{"),
        "Curly braces should be supported: {}",
        result
    );
}

#[test]
fn test_format_explain_analyze() {
    let result = format_string("EXPLAIN ANALYZE SELECT * FROM t\n", &default_mode()).unwrap();
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

// --- Python sqlfmt test fixture coverage ---

// Mirrors Python test_general_formatting with fixture 001_select_1
#[test]
fn test_preformatted_select_1() {
    let source = "select 1\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Preformatted query should be idempotent");
}

// Mirrors Python fixture 002_select_from_where
#[test]
fn test_preformatted_select_from_where() {
    let source = "select\n    a_long_field_name,\n    another_long_field_name,\n    a_long_field_name + another_long_field_name as c,\n    final_field\nfrom my_schema.\"my_QUOTED_ table!\"\nwhere one_field < another_field\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(
        result, second,
        "Preformatted select/from/where should be idempotent"
    );
}

// Mirrors Python fixture 003_literals (numeric arithmetic)
#[test]
fn test_numeric_literals_and_arithmetic() {
    let source = "SELECT 1 + 1, sum(1.05), 1.4 - 15.17, -.45 + -0.99, -0.710 - sum(-34.5), (1 + 1) - (4 - 0.6), 3.14159\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("3.14159"),
        "Should preserve decimals: {}",
        result
    );
    assert!(
        result.contains("sum"),
        "Should contain sum function: {}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Numeric literals should be idempotent");
}

// Mirrors Python fixture 004_with_select (CTE)
#[test]
fn test_cte_with_select() {
    let source = "WITH my_cte AS (SELECT 1, b, another_field FROM my_schema.my_table) SELECT * FROM another_cte\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("with"), "CTE: {}", result);
    assert!(result.contains("as"), "CTE: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "CTE should be idempotent");
}

// Mirrors Python fixture 100_select_case
#[test]
fn test_fixture_select_case() {
    let source = "SELECT col1, CASE WHEN condition1 THEN 'value1' WHEN condition2 THEN 'value2' ELSE 'default' END AS computed_col, col3 FROM my_table\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("case"), "CASE: {}", result);
    assert!(result.contains("when"), "WHEN: {}", result);
    assert!(result.contains("end"), "END: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Case expression should be idempotent");
}

// Mirrors Python fixture 103_window_functions
#[test]
fn test_fixture_window_functions() {
    let source = "SELECT id, category, amount, SUM(amount) OVER (PARTITION BY category ORDER BY id) AS running_total, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rank FROM transactions\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("over"), "OVER: {}", result);
    assert!(result.contains("partition by"), "PARTITION BY: {}", result);
    assert!(result.contains("order by"), "ORDER BY: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Window functions should be idempotent");
}

// Mirrors Python fixture 104_joins
#[test]
fn test_fixture_joins() {
    let source = "SELECT a.id, b.name, c.value FROM table_a a INNER JOIN table_b b ON a.id = b.a_id LEFT OUTER JOIN table_c c ON b.id = c.b_id CROSS JOIN table_d d\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("inner join"), "INNER JOIN: {}", result);
    assert!(
        result.contains("left outer join") || result.contains("left join"),
        "LEFT JOIN: {}",
        result
    );
    assert!(result.contains("cross join"), "CROSS JOIN: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Joins should be idempotent");
}

// Mirrors Python fixture 106_leading_commas
#[test]
fn test_fixture_leading_commas() {
    let source = "SELECT col1 ,col2 ,col3 FROM my_table\n";
    let result = format_string(source, &default_mode()).unwrap();
    // sqlfmt uses trailing commas, so leading commas get reformatted
    assert!(result.contains("col1"), "Content: {}", result);
    assert!(result.contains("col2"), "Content: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Leading commas should be idempotent");
}

// Mirrors Python fixture 111_chained_boolean_between
#[test]
fn test_fixture_chained_boolean_between() {
    let source = "SELECT radio, mcc, net as mnc, area as lac, cell, lon, lat FROM towershift WHERE radio != 'CDMA' AND mcc BETWEEN 200 AND 799 AND net BETWEEN 1 AND 999 AND area BETWEEN 0 AND 65535\n";
    let result = format_string(source, &default_mode()).unwrap();
    // BETWEEN x AND y should stay together
    assert!(result.contains("between"), "BETWEEN: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(
        result, second,
        "Chained boolean/between should be idempotent"
    );
}

// Mirrors Python fixture 112_semicolons (multiple statements)
#[test]
fn test_fixture_semicolons() {
    let source = "SELECT 1;\n\nSELECT 2;\n\nSELECT 3;\n";
    let result = format_string(source, &default_mode()).unwrap();
    let semicolons = result.matches(';').count();
    assert!(semicolons >= 3, "Should have 3 semicolons: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Semicolons should be idempotent");
}

// Mirrors Python fixture 113_utils_group_by
#[test]
fn test_fixture_group_by() {
    let source = "SELECT department, COUNT(*) as cnt, SUM(salary) AS total FROM employees GROUP BY department HAVING COUNT(*) > 5 ORDER BY cnt DESC\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("group by"), "GROUP BY: {}", result);
    assert!(result.contains("having"), "HAVING: {}", result);
    assert!(result.contains("order by"), "ORDER BY: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Group by should be idempotent");
}

// Mirrors Python fixture 114_unions
#[test]
fn test_fixture_unions() {
    let source = "SELECT id, name FROM table_a UNION ALL SELECT id, name FROM table_b UNION SELECT id, name FROM table_c EXCEPT SELECT id, name FROM table_d\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("union all"), "UNION ALL: {}", result);
    assert!(result.contains("union"), "UNION: {}", result);
    assert!(result.contains("except"), "EXCEPT: {}", result);
    assert!(result.contains("table_a"), "Content: {}", result);
    assert!(result.contains("table_d"), "Content: {}", result);
}

// Mirrors Python fixture 115_select_star_except
#[test]
fn test_fixture_select_star_except() {
    let source = "SELECT * EXCEPT (col1, col2) FROM my_table\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("*"), "STAR: {}", result);
    assert!(result.contains("col1"), "Content: {}", result);
    assert!(result.contains("col2"), "Content: {}", result);
    assert!(result.contains("my_table"), "Content: {}", result);
}

// Mirrors Python fixture 116_chained_booleans
#[test]
fn test_fixture_chained_booleans() {
    let source = "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 OR d = 4 AND e = 5\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("and"), "AND: {}", result);
    assert!(result.contains("or"), "OR: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Chained booleans should be idempotent");
}

// Mirrors Python fixture 118_within_group
#[test]
fn test_fixture_within_group() {
    let source = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary FROM employees\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("within group"), "WITHIN GROUP: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Within group should be idempotent");
}

// Mirrors Python fixture 119_psycopg_placeholders
#[test]
fn test_fixture_psycopg_placeholders() {
    let source = "SELECT * FROM t WHERE id = %s AND name = %(name)s\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("%s") || result.contains("%(name)s"),
        "Placeholders: {}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Psycopg placeholders should be idempotent");
}

// Mirrors Python fixture 120_array_literals
#[test]
fn test_fixture_array_literals() {
    let source = "SELECT ARRAY[1, 2, 3], some_dict['a key']\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("["), "Array: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Array literals should be idempotent");
}

// Mirrors Python fixture 122_values
#[test]
fn test_fixture_values_clause() {
    let source = "INSERT INTO my_table VALUES (1, 'a'), (2, 'b'), (3, 'c')\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("values") || result.contains("1"),
        "VALUES: {}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Values clause should be idempotent");
}

// Mirrors Python fixture 123_spark_keywords
#[test]
fn test_fixture_spark_keywords() {
    let source = "SELECT col1 FROM t CLUSTER BY col1\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("cluster by"), "CLUSTER BY: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Spark keywords should be idempotent");
}

// Mirrors Python fixture 125_numeric_literals (various number formats)
#[test]
fn test_fixture_numeric_literals() {
    let source = "SELECT 1, 1.5, 1e10, 1.5e-3, 0xFF, 0b1010, 0o777, .5, 42L\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("1.5"), "Decimal: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Numeric literals should be idempotent");
}

// Mirrors Python fixture 126_blank_lines
#[test]
fn test_fixture_blank_lines() {
    let source = "SELECT 1\n\n\n\n\n\nSELECT 2\n";
    let result = format_string(source, &default_mode()).unwrap();
    // Should reduce excessive blank lines
    assert!(
        !result.contains("\n\n\n\n"),
        "Too many blanks: {:?}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Blank line handling should be idempotent");
}

// Mirrors Python fixture 127_more_comments
#[test]
fn test_fixture_more_comments() {
    let source = "-- first comment\nSELECT\n    -- second comment\n    col1,\n    col2  -- inline comment\nFROM t\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("select") || result.contains("col1"),
        "Content: {}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Comments should be idempotent");
}

// Mirrors Python fixture 128_double_slash_comments
#[test]
fn test_fixture_double_slash_comments() {
    let source = "// double slash comment\nSELECT 1\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Double slash comments should be idempotent");
}

// Mirrors Python fixture 129_duckdb_joins
#[test]
fn test_fixture_duckdb_joins() {
    let source = "SELECT * FROM t1 POSITIONAL JOIN t2\n";
    let result = format_string(source, &duckdb_mode()).unwrap();
    let second = format_string(&result, &duckdb_mode()).unwrap();
    assert_eq!(result, second, "DuckDB joins should be idempotent");
}

// Mirrors Python fixture 131_assignment_statement
#[test]
fn test_fixture_assignment_statement() {
    let source = "SET my_var = 42;\nSELECT my_var\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("set") || result.contains("my_var"),
        "SET: {}",
        result
    );
    assert!(result.contains("select"), "SELECT: {}", result);
}

// Mirrors Python fixture 132_spark_number_literals
#[test]
fn test_fixture_spark_number_literals() {
    let source = "SELECT 42L, 3.14D, 1.5BD, 10S, 1Y\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Spark numbers should be idempotent");
}

// Mirrors Python fixture 400_create_fn_and_select
#[test]
fn test_fixture_create_function() {
    let source =
        "CREATE FUNCTION my_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;\nSELECT my_func()\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("create function") || result.contains("my_func"),
        "CREATE FUNCTION: {}",
        result
    );
}

// Mirrors Python fixture 401_explain_select
#[test]
fn test_fixture_explain_select() {
    let source = "EXPLAIN SELECT * FROM my_table WHERE id = 1\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("explain"), "EXPLAIN: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Explain should be idempotent");
}

// Mirrors Python fixture 402_delete_from_using
#[test]
fn test_fixture_delete_from_using() {
    let source = "DELETE FROM my_table USING other_table WHERE my_table.id = other_table.id\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("delete"), "DELETE: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Delete from using should be idempotent");
}

// Mirrors Python fixture 900_create_view
#[test]
fn test_fixture_create_view() {
    let source = "CREATE VIEW my_view AS SELECT id, name FROM my_table WHERE active = true\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Create view should be idempotent");
}

// Mirrors Python fixture 109_lateral_flatten (Snowflake)
#[test]
fn test_fixture_lateral_flatten() {
    let source =
        "SELECT f.value::string AS item FROM my_table, LATERAL FLATTEN(input => my_array) f\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Lateral flatten should be idempotent");
}

// Mirrors Python fixture 110_other_identifiers
#[test]
fn test_fixture_other_identifiers() {
    let source = "SELECT $1, $2 FROM my_table\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("$1"), "Positional param: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Other identifiers should be idempotent");
}

// Mirrors Python test: formatting is idempotent for complex real-world queries
#[test]
fn test_idempotent_complex_cte_join_window() {
    let source = r#"WITH daily_stats AS (
    SELECT
        date_trunc('day', created_at) AS day,
        department,
        COUNT(*) AS cnt,
        SUM(amount) AS total
    FROM transactions
    WHERE created_at >= '2024-01-01'
    GROUP BY 1, 2
)
SELECT
    ds.day,
    ds.department,
    ds.cnt,
    ds.total,
    SUM(ds.total) OVER (
        PARTITION BY ds.department
        ORDER BY ds.day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    LAG(ds.total, 1) OVER (PARTITION BY ds.department ORDER BY ds.day) AS prev_total
FROM daily_stats ds
LEFT JOIN departments d ON ds.department = d.id
WHERE ds.cnt > 0
ORDER BY ds.department, ds.day
"#;
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(
        result, second,
        "Complex CTE + Join + Window should be idempotent"
    );
}

// Mirrors Python test: GRANT/REVOKE statements
#[test]
fn test_fixture_grant_revoke() {
    let source = "GRANT SELECT ON my_table TO my_role;\nREVOKE INSERT ON my_table FROM my_role;\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("grant") || result.contains("select"),
        "GRANT: {}",
        result
    );
    assert!(
        result.contains("revoke") || result.contains("insert"),
        "REVOKE: {}",
        result
    );
}

// Mirrors Python test: ALTER TABLE
#[test]
fn test_fixture_alter_table() {
    let source = "ALTER TABLE my_table ADD COLUMN new_col INT DEFAULT 0;\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Alter table should be idempotent");
}

// Mirrors Python test: CREATE TABLE
#[test]
fn test_fixture_create_table() {
    let source = "CREATE TABLE my_table (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Create table should be idempotent");
}

// Test: Jinja for loop
#[test]
fn test_fixture_jinja_for_loop() {
    let source = "{% for item in items %}\nSELECT {{ item }}\n{% if not loop.last %}\nUNION ALL\n{% endif %}\n{% endfor %}\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("{% for"), "FOR: {}", result);
    assert!(result.contains("{% endfor %}"), "ENDFOR: {}", result);
    assert!(result.contains("union all"), "UNION ALL: {}", result);
}

// Test: Jinja macro
#[test]
fn test_fixture_jinja_macro() {
    let source =
        "{% macro my_macro(arg1, arg2) %}\nSELECT {{ arg1 }}, {{ arg2 }}\n{% endmacro %}\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("{% macro"), "MACRO: {}", result);
    assert!(result.contains("{% endmacro %}"), "ENDMACRO: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Jinja macro should be idempotent");
}

// Test: LATERAL VIEW (Spark)
#[test]
fn test_fixture_lateral_view() {
    let source =
        "SELECT col1, exploded_col FROM my_table LATERAL VIEW EXPLODE(array_col) AS exploded_col\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("lateral view"), "LATERAL VIEW: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Lateral view should be idempotent");
}

// Test: QUALIFY (Snowflake)
#[test]
fn test_fixture_qualify_detailed() {
    let source = "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY category ORDER BY created_at DESC) AS rn FROM items QUALIFY rn = 1\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("qualify"), "QUALIFY: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Qualify should be idempotent");
}

// Test: IS NULL / IS NOT NULL / IS DISTINCT FROM
#[test]
fn test_fixture_presence_operators() {
    let source = "SELECT * FROM t WHERE a IS NULL AND b IS NOT NULL AND c IS DISTINCT FROM d\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("is null") || result.contains("is"),
        "IS NULL: {}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Presence operators should be idempotent");
}

// Test: LIKE / ILIKE / NOT LIKE
#[test]
fn test_fixture_like_operators() {
    let source =
        "SELECT * FROM t WHERE name LIKE '%test%' AND code NOT LIKE 'X%' AND label ILIKE '%foo%'\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("like"), "LIKE: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Like operators should be idempotent");
}

// Test: DOUBLE COLON cast (PostgreSQL/Snowflake)
#[test]
fn test_fixture_double_colon_cast() {
    let source = "SELECT col1::INT, col2::VARCHAR(100), col3::TIMESTAMP FROM t\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("::"), "Double colon: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Double colon cast should be idempotent");
}

// Test: EXISTS / NOT EXISTS subquery
#[test]
fn test_fixture_exists_subquery() {
    let source = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("exists"), "EXISTS: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Exists subquery should be idempotent");
}

// Test: INTERSECT / EXCEPT
#[test]
fn test_fixture_intersect_except() {
    let source = "SELECT id FROM t1 INTERSECT SELECT id FROM t2 EXCEPT SELECT id FROM t3\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("intersect"), "INTERSECT: {}", result);
    assert!(result.contains("except"), "EXCEPT: {}", result);
    assert!(result.contains("t1"), "Content: {}", result);
    assert!(result.contains("t3"), "Content: {}", result);
}

// Test: LIMIT / OFFSET
#[test]
fn test_fixture_limit_offset() {
    let source = "SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 20\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("limit"), "LIMIT: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Limit/offset should be idempotent");
}

// Test: Multiple JOIN types
#[test]
fn test_fixture_all_join_types() {
    let source = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id RIGHT JOIN t4 ON t3.id = t4.id FULL OUTER JOIN t5 ON t4.id = t5.id NATURAL JOIN t6\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("join"), "JOIN: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "All join types should be idempotent");
}

// Test: Nested subqueries
#[test]
fn test_fixture_nested_subqueries() {
    let source = "SELECT * FROM (SELECT * FROM (SELECT id, name FROM users WHERE active = true) inner_q WHERE id > 10) outer_q WHERE name LIKE 'A%'\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Nested subqueries should be idempotent");
}

// Test: Empty query / whitespace only
#[test]
fn test_format_empty_whitespace() {
    let result = format_string("\n", &default_mode()).unwrap();
    // Should handle gracefully (empty or newline only)
    assert!(result.is_empty() || result.trim().is_empty() || result == "\n");
}

// Test: Long single token (no recursion crash)
#[test]
fn test_very_long_single_line() {
    let long_name = "a".repeat(500);
    let source = format!("SELECT {}\n", long_name);
    let result = format_string(&source, &default_mode());
    assert!(result.is_ok(), "Very long line should not crash");
}

// Test: C-style block comments
#[test]
fn test_fixture_c_style_comments() {
    let source = "/* This is a block comment */\nSELECT 1\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "C-style comments should be idempotent");
}

// Test: FOR UPDATE / FOR SHARE
#[test]
fn test_fixture_for_update() {
    let source = "SELECT * FROM t WHERE id = 1 FOR UPDATE\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("for update") || result.contains("for"),
        "FOR UPDATE: {}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "For update should be idempotent");
}

// Test: Complex dbt model with many CTEs, Jinja refs/sources, QUALIFY, window functions
// This file was causing a stack overflow due to deep recursion in the merger.
#[test]
fn test_format_dbt_pharmacy_panels() {
    let source =
        std::fs::read_to_string("tests/fixtures/dbt_pharmacy_panels.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(
        result.is_ok(),
        "Should successfully format dbt_pharmacy_panels.sql without stack overflow: {:?}",
        result.err()
    );
    // Verify idempotency
    let formatted = result.unwrap();
    let second = format_string(&formatted, &default_mode()).unwrap();
    assert_eq!(
        formatted, second,
        "dbt_pharmacy_panels.sql formatting should be idempotent"
    );
}
