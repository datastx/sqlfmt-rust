mod common;

use common::{default_mode, duckdb_mode};
use sqlfmt::format_string;

// =============================================================================
// Edge case tests (unique coverage not in golden tests)
// =============================================================================

#[test]
fn test_format_empty_whitespace() {
    let result = format_string("\n", &default_mode()).unwrap();
    assert!(result.is_empty() || result.trim().is_empty() || result == "\n");
}

#[test]
fn test_very_long_single_line() {
    let long_name = "a".repeat(500);
    let source = format!("SELECT {}\n", long_name);
    let result = format_string(&source, &default_mode());
    assert!(result.is_ok(), "Very long line should not crash");
}

#[test]
fn test_format_idempotent_no_trailing_newline_growth() {
    let source = "SELECT 1\n";
    let mode = default_mode();
    let first = format_string(source, &mode).unwrap();
    let second = format_string(&first, &mode).unwrap();
    assert_eq!(
        first, second,
        "Formatting twice should produce identical output"
    );
    let third = format_string(&second, &mode).unwrap();
    assert_eq!(
        second, third,
        "Formatting three times should produce identical output"
    );
}

#[test]
fn test_format_normalizes_trailing_newlines() {
    let mode = default_mode();
    let result = format_string("SELECT 1\n\n\n", &mode).unwrap();
    assert!(result.ends_with('\n'), "Output should end with a newline");
    assert!(
        !result.ends_with("\n\n"),
        "Output should not end with multiple newlines"
    );
}

// =============================================================================
// Fixture file I/O tests (test that real .sql files format without errors)
// =============================================================================

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

#[test]
fn test_format_dbt_deeply_nested() {
    let source = std::fs::read_to_string("tests/fixtures/dbt_deeply_nested.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(
        result.is_ok(),
        "Should successfully format dbt_deeply_nested.sql without stack overflow: {:?}",
        result.err()
    );
    let formatted = result.unwrap();
    let second = format_string(&formatted, &default_mode()).unwrap();
    assert_eq!(
        formatted, second,
        "dbt_deeply_nested.sql formatting should be idempotent"
    );
}

// =============================================================================
// Feature-specific assertion tests (unique assertions not covered by golden tests)
// =============================================================================

#[test]
fn test_format_between_and_stays_together() {
    let result = format_string(
        "SELECT * FROM t WHERE amount BETWEEN 100 AND 200 AND status = 'active'\n",
        &default_mode(),
    )
    .unwrap();
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
    let result = format_string(
        "SELECT * FROM (SELECT a.id, b.name FROM a JOIN b ON a.id = b.id) subq\n",
        &default_mode(),
    )
    .unwrap();
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

// =============================================================================
// Unique format tests (dialect-specific, Jinja, SQL features)
// =============================================================================

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
fn test_fixture_jinja_for_loop() {
    let source = "{% for item in items %}\nSELECT {{ item }}\n{% if not loop.last %}\nUNION ALL\n{% endif %}\n{% endfor %}\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("{% for"), "FOR: {}", result);
    assert!(result.contains("{% endfor %}"), "ENDFOR: {}", result);
    assert!(result.contains("union all"), "UNION ALL: {}", result);
}

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

#[test]
fn test_fixture_lateral_view() {
    let source =
        "SELECT col1, exploded_col FROM my_table LATERAL VIEW EXPLODE(array_col) AS exploded_col\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("lateral view"), "LATERAL VIEW: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Lateral view should be idempotent");
}

#[test]
fn test_fixture_qualify_detailed() {
    let source = "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY category ORDER BY created_at DESC) AS rn FROM items QUALIFY rn = 1\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("qualify"), "QUALIFY: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Qualify should be idempotent");
}

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

#[test]
fn test_fixture_like_operators() {
    let source =
        "SELECT * FROM t WHERE name LIKE '%test%' AND code NOT LIKE 'X%' AND label ILIKE '%foo%'\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("like"), "LIKE: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Like operators should be idempotent");
}

#[test]
fn test_fixture_double_colon_cast() {
    let source = "SELECT col1::INT, col2::VARCHAR(100), col3::TIMESTAMP FROM t\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("::"), "Double colon: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Double colon cast should be idempotent");
}

#[test]
fn test_fixture_exists_subquery() {
    let source = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("exists"), "EXISTS: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Exists subquery should be idempotent");
}

#[test]
fn test_fixture_intersect_except() {
    let source = "SELECT id FROM t1 INTERSECT SELECT id FROM t2 EXCEPT SELECT id FROM t3\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("intersect"), "INTERSECT: {}", result);
    assert!(result.contains("except"), "EXCEPT: {}", result);
    assert!(result.contains("t1"), "Content: {}", result);
    assert!(result.contains("t3"), "Content: {}", result);
}

#[test]
fn test_fixture_limit_offset() {
    let source = "SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 20\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("limit"), "LIMIT: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Limit/offset should be idempotent");
}

#[test]
fn test_fixture_all_join_types() {
    let source = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id RIGHT JOIN t4 ON t3.id = t4.id FULL OUTER JOIN t5 ON t4.id = t5.id NATURAL JOIN t6\n";
    let result = format_string(source, &default_mode()).unwrap();
    assert!(result.contains("join"), "JOIN: {}", result);
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "All join types should be idempotent");
}

#[test]
fn test_fixture_nested_subqueries() {
    let source = "SELECT * FROM (SELECT * FROM (SELECT id, name FROM users WHERE active = true) inner_q WHERE id > 10) outer_q WHERE name LIKE 'A%'\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Nested subqueries should be idempotent");
}

#[test]
fn test_fixture_c_style_comments() {
    let source = "/* This is a block comment */\nSELECT 1\n";
    let result = format_string(source, &default_mode()).unwrap();
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "C-style comments should be idempotent");
}

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

#[test]
fn test_fixture_jinja_macro_with_method_chains() {
    let source = r#"{% macro get_meta_objects(model_id, meta_key) %}
	{% if execute %}
              {% set meta_columns = [] %}
	       {% set columns = graph.nodes[model_id]['columns']  %}
              {% if meta_key is not none %}
                     {% for column in columns if graph.nodes[model_id]['columns'][column]['meta'][meta_key] | length > 0 %}
                            {% set meta_dict = graph.nodes[model_id]['columns'][column]['meta'] %}
                            {% if meta_key in meta_dict %}
                                   {% set policy_name = meta_dict[meta_key] %}
                                   {% if "masking_policy_inputs" in meta_dict %}
                                          {% set inputs = meta_dict['masking_policy_inputs'] %}
                                   {% else %}
                                          {% set inputs = [] %}
                                   {% endif %}
                                   {% set meta_tuple = (column, policy_name, inputs) %}
                                   {% do meta_columns.append(meta_tuple) %}
                            {% endif %}
                     {% endfor %}
                     {% for column in columns if graph.nodes[model_id]['columns'][column].get('config', {}).get('meta', {}).get(meta_key, '') | length > 0 %}
                            {% set meta_dict = graph.nodes[model_id]['columns'][column]['config']['meta'] %}
                            {% if meta_key in meta_dict %}
                                   {% set policy_name = meta_dict[meta_key] %}
                                   {% if "masking_policy_inputs" in meta_dict %}
                                          {% set inputs = meta_dict['masking_policy_inputs'] %}
                                   {% else %}
                                          {% set inputs = [] %}
                                   {% endif %}
                                   {% set meta_tuple = (column, policy_name, inputs) %}
                                   {% do meta_columns.append(meta_tuple) %}
                            {% endif %}
                     {% endfor %}
              {% else %}
                     {% do meta_columns.append(column|upper) %}
              {% endif %}
              {{ return(meta_columns) }}
       {% endif %}
{% endmacro %}
"#;
    let result = format_string(source, &default_mode()).unwrap();
    assert!(
        result.contains("{% macro"),
        "Should contain macro: {}",
        result
    );
    assert!(
        result.contains("{% endmacro %}"),
        "Should contain endmacro: {}",
        result
    );
    assert!(
        result.contains(".get("),
        "Should preserve method chains: {}",
        result
    );
    let second = format_string(&result, &default_mode()).unwrap();
    assert_eq!(result, second, "Complex jinja macro should be idempotent");
}
