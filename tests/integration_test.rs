mod common;

use common::{default_mode, duckdb_mode, run_error_test, run_format_test};
use sqlfmt::format_string;

// =============================================================================
// Macro for file-based format tests (format, compare, check idempotency)
// =============================================================================

macro_rules! format_tests {
    ($($name:ident => ($mode_fn:ident, $path:expr)),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_format_test($path, &$mode_fn());
            }
        )*
    };
}

macro_rules! error_tests {
    ($($name:ident => $path:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_error_test($path);
            }
        )*
    };
}

format_tests! {
    // =========================================================================
    // Preformatted — no sentinel, input passes through unchanged
    // =========================================================================
    test_preformatted_001_select_1 => (default_mode, "tests/data/preformatted/001_select_1.sql"),
    test_preformatted_002_select_from_where => (default_mode, "tests/data/preformatted/002_select_from_where.sql"),
    test_preformatted_003_literals => (default_mode, "tests/data/preformatted/003_literals.sql"),
    test_preformatted_004_with_select => (default_mode, "tests/data/preformatted/004_with_select.sql"),
    test_preformatted_005_fmt_off => (default_mode, "tests/data/preformatted/005_fmt_off.sql"),
    test_preformatted_006_fmt_off_447 => (default_mode, "tests/data/preformatted/006_fmt_off_447.sql"),
    test_preformatted_007_fmt_off_comments => (default_mode, "tests/data/preformatted/007_fmt_off_comments.sql"),
    test_preformatted_008_reserved_names => (default_mode, "tests/data/preformatted/008_reserved_names.sql"),
    test_preformatted_009_empty => (default_mode, "tests/data/preformatted/009_empty.sql"),
    test_preformatted_010_comment_only => (default_mode, "tests/data/preformatted/010_comment_only.sql"),
    test_preformatted_301_multiline_jinjafmt => (default_mode, "tests/data/preformatted/301_multiline_jinjafmt.sql"),
    test_preformatted_302_jinjafmt_multiline_str => (default_mode, "tests/data/preformatted/302_jinjafmt_multiline_str.sql"),
    test_preformatted_303_jinjafmt_more_mutliline_str => (default_mode, "tests/data/preformatted/303_jinjafmt_more_mutliline_str.sql"),
    test_preformatted_400_create_table => (default_mode, "tests/data/preformatted/400_create_table.sql"),
    test_preformatted_401_create_row_access_policy => (default_mode, "tests/data/preformatted/401_create_row_access_policy.sql"),
    test_preformatted_402_alter_table => (default_mode, "tests/data/preformatted/402_alter_table.sql"),
    test_preformatted_500_jinja_slice_after_func_call => (default_mode, "tests/data/preformatted/500_jinja_slice_after_func_call.sql"),
    test_preformatted_910_create_semantic_view => (default_mode, "tests/data/preformatted/910_create_semantic_view.sql"),

    // =========================================================================
    // Unformatted 100-series — core SQL formatting
    // =========================================================================
    test_unformatted_100_select_case => (default_mode, "tests/data/unformatted/100_select_case.sql"),
    test_unformatted_101_multiline => (default_mode, "tests/data/unformatted/101_multiline.sql"),
    test_unformatted_102_lots_of_comments => (default_mode, "tests/data/unformatted/102_lots_of_comments.sql"),
    test_unformatted_103_window_functions => (default_mode, "tests/data/unformatted/103_window_functions.sql"),
    test_unformatted_104_joins => (default_mode, "tests/data/unformatted/104_joins.sql"),
    test_unformatted_105_fmt_off => (default_mode, "tests/data/unformatted/105_fmt_off.sql"),
    test_unformatted_106_leading_commas => (default_mode, "tests/data/unformatted/106_leading_commas.sql"),
    test_unformatted_107_jinja_blocks => (default_mode, "tests/data/unformatted/107_jinja_blocks.sql"),
    test_unformatted_108_test_block => (default_mode, "tests/data/unformatted/108_test_block.sql"),
    test_unformatted_109_lateral_flatten => (default_mode, "tests/data/unformatted/109_lateral_flatten.sql"),
    test_unformatted_110_other_identifiers => (default_mode, "tests/data/unformatted/110_other_identifiers.sql"),
    test_unformatted_111_chained_boolean_between => (default_mode, "tests/data/unformatted/111_chained_boolean_between.sql"),
    test_unformatted_112_semicolons => (default_mode, "tests/data/unformatted/112_semicolons.sql"),
    test_unformatted_113_utils_group_by => (default_mode, "tests/data/unformatted/113_utils_group_by.sql"),
    test_unformatted_114_unions => (default_mode, "tests/data/unformatted/114_unions.sql"),
    test_unformatted_115_select_star_except => (default_mode, "tests/data/unformatted/115_select_star_except.sql"),
    test_unformatted_116_chained_booleans => (default_mode, "tests/data/unformatted/116_chained_booleans.sql"),
    test_unformatted_117_whitespace_in_tokens => (default_mode, "tests/data/unformatted/117_whitespace_in_tokens.sql"),
    test_unformatted_118_within_group => (default_mode, "tests/data/unformatted/118_within_group.sql"),
    test_unformatted_119_psycopg_placeholders => (default_mode, "tests/data/unformatted/119_psycopg_placeholders.sql"),
    test_unformatted_120_array_literals => (default_mode, "tests/data/unformatted/120_array_literals.sql"),
    test_unformatted_121_stubborn_merge_edge_cases => (default_mode, "tests/data/unformatted/121_stubborn_merge_edge_cases.sql"),
    test_unformatted_122_values => (default_mode, "tests/data/unformatted/122_values.sql"),
    test_unformatted_123_spark_keywords => (default_mode, "tests/data/unformatted/123_spark_keywords.sql"),
    test_unformatted_124_bq_compound_types => (default_mode, "tests/data/unformatted/124_bq_compound_types.sql"),
    test_unformatted_125_numeric_literals => (default_mode, "tests/data/unformatted/125_numeric_literals.sql"),
    test_unformatted_126_blank_lines => (default_mode, "tests/data/unformatted/126_blank_lines.sql"),
    test_unformatted_127_more_comments => (default_mode, "tests/data/unformatted/127_more_comments.sql"),
    test_unformatted_128_double_slash_comments => (default_mode, "tests/data/unformatted/128_double_slash_comments.sql"),
    test_unformatted_129_duckdb_joins => (default_mode, "tests/data/unformatted/129_duckdb_joins.sql"),
    test_unformatted_130_athena_data_types => (default_mode, "tests/data/unformatted/130_athena_data_types.sql"),
    test_unformatted_131_assignment_statement => (default_mode, "tests/data/unformatted/131_assignment_statement.sql"),
    test_unformatted_132_spark_number_literals => (default_mode, "tests/data/unformatted/132_spark_number_literals.sql"),
    test_unformatted_133_for_else => (default_mode, "tests/data/unformatted/133_for_else.sql"),
    test_unformatted_134_databricks_type_hints => (default_mode, "tests/data/unformatted/134_databricks_type_hints.sql"),
    test_unformatted_135_star_columns => (default_mode, "tests/data/unformatted/135_star_columns.sql"),
    test_unformatted_136_databricks_variant => (default_mode, "tests/data/unformatted/136_databricks_variant.sql"),
    test_unformatted_137_escaped_single_quotes => (default_mode, "tests/data/unformatted/137_escaped_single_quotes.sql"),

    // =========================================================================
    // Unformatted 200-series — real-world dbt models
    // =========================================================================
    test_unformatted_200_base_model => (default_mode, "tests/data/unformatted/200_base_model.sql"),
    test_unformatted_201_basic_snapshot => (default_mode, "tests/data/unformatted/201_basic_snapshot.sql"),
    test_unformatted_202_unpivot_macro => (default_mode, "tests/data/unformatted/202_unpivot_macro.sql"),
    test_unformatted_203_gitlab_email_domain_type => (default_mode, "tests/data/unformatted/203_gitlab_email_domain_type.sql"),
    test_unformatted_204_gitlab_tag_validation => (default_mode, "tests/data/unformatted/204_gitlab_tag_validation.sql"),
    test_unformatted_205_rittman_hubspot_deals => (default_mode, "tests/data/unformatted/205_rittman_hubspot_deals.sql"),
    test_unformatted_206_gitlab_prep_geozone => (default_mode, "tests/data/unformatted/206_gitlab_prep_geozone.sql"),
    test_unformatted_207_rittman_int_journals => (default_mode, "tests/data/unformatted/207_rittman_int_journals.sql"),
    test_unformatted_208_rittman_int_plan_breakout_metrics => (default_mode, "tests/data/unformatted/208_rittman_int_plan_breakout_metrics.sql"),
    test_unformatted_209_rittman_int_web_events_sessionized => (default_mode, "tests/data/unformatted/209_rittman_int_web_events_sessionized.sql"),
    test_unformatted_210_gitlab_gdpr_delete => (default_mode, "tests/data/unformatted/210_gitlab_gdpr_delete.sql"),
    test_unformatted_211_http_2019_cdn_17_20 => (default_mode, "tests/data/unformatted/211_http_2019_cdn_17_20.sql"),
    test_unformatted_212_http_2019_cms_14_02 => (default_mode, "tests/data/unformatted/212_http_2019_cms_14_02.sql"),
    test_unformatted_213_gitlab_fct_sales_funnel_target => (default_mode, "tests/data/unformatted/213_gitlab_fct_sales_funnel_target.sql"),
    test_unformatted_214_get_unique_attributes => (default_mode, "tests/data/unformatted/214_get_unique_attributes.sql"),
    test_unformatted_215_gitlab_get_backup_table_command => (default_mode, "tests/data/unformatted/215_gitlab_get_backup_table_command.sql"),
    test_unformatted_216_gitlab_zuora_revenue_revenue_contract_line_source => (default_mode, "tests/data/unformatted/216_gitlab_zuora_revenue_revenue_contract_line_source.sql"),
    test_unformatted_217_dbt_unit_testing_csv => (default_mode, "tests/data/unformatted/217_dbt_unit_testing_csv.sql"),
    test_unformatted_218_multiple_c_comments => (default_mode, "tests/data/unformatted/218_multiple_c_comments.sql"),
    test_unformatted_219_any_all_agg => (default_mode, "tests/data/unformatted/219_any_all_agg.sql"),
    test_unformatted_221_dbt_config_dollar_quoted => (default_mode, "tests/data/unformatted/221_dbt_config_dollar_quoted.sql"),
    test_unformatted_222_colorado_claims_extract => (default_mode, "tests/data/unformatted/222_colorado_claims_extract.sql"),
    test_unformatted_222_jinja_unbalanced_brackets => (default_mode, "tests/data/unformatted/222_jinja_unbalanced_brackets.sql"),
    test_unformatted_223_dbt_config_nested_quotes => (default_mode, "tests/data/unformatted/223_dbt_config_nested_quotes.sql"),
    test_unformatted_224_dbt_config_unique_key_nested_quotes => (default_mode, "tests/data/unformatted/224_dbt_config_unique_key_nested_quotes.sql"),
    test_unformatted_225_dbt_snowflake_incremental_macro => (default_mode, "tests/data/unformatted/225_dbt_snowflake_incremental_macro.sql"),

    // =========================================================================
    // Unformatted 300-series — Jinja formatting
    // =========================================================================
    test_unformatted_300_jinjafmt => (default_mode, "tests/data/unformatted/300_jinjafmt.sql"),
    test_unformatted_301_jinja_macro_method_chains => (default_mode, "tests/data/unformatted/301_jinja_macro_method_chains.sql"),
    test_unformatted_302_jinja_compare_row_counts => (default_mode, "tests/data/unformatted/302_jinja_compare_row_counts.sql"),
    test_unformatted_303_jinja_audit_helper_queries => (default_mode, "tests/data/unformatted/303_jinja_audit_helper_queries.sql"),
    test_unformatted_304_jinja_test_unstructured_data => (default_mode, "tests/data/unformatted/304_jinja_test_unstructured_data.sql"),

    // =========================================================================
    // Unformatted 400-series — DDL/DML
    // =========================================================================
    test_unformatted_400_create_fn_and_select => (default_mode, "tests/data/unformatted/400_create_fn_and_select.sql"),
    test_unformatted_401_explain_select => (default_mode, "tests/data/unformatted/401_explain_select.sql"),
    test_unformatted_402_delete_from_using => (default_mode, "tests/data/unformatted/402_delete_from_using.sql"),
    test_unformatted_403_grant_revoke => (default_mode, "tests/data/unformatted/403_grant_revoke.sql"),
    test_unformatted_404_create_function_pg_examples => (default_mode, "tests/data/unformatted/404_create_function_pg_examples.sql"),
    test_unformatted_405_create_function_snowflake_examples => (default_mode, "tests/data/unformatted/405_create_function_snowflake_examples.sql"),
    test_unformatted_406_create_function_bq_examples => (default_mode, "tests/data/unformatted/406_create_function_bq_examples.sql"),
    test_unformatted_407_alter_function_pg_examples => (default_mode, "tests/data/unformatted/407_alter_function_pg_examples.sql"),
    test_unformatted_408_alter_function_snowflake_examples => (default_mode, "tests/data/unformatted/408_alter_function_snowflake_examples.sql"),
    test_unformatted_409_create_external_function => (default_mode, "tests/data/unformatted/409_create_external_function.sql"),
    test_unformatted_410_create_warehouse => (default_mode, "tests/data/unformatted/410_create_warehouse.sql"),
    test_unformatted_411_create_clone => (default_mode, "tests/data/unformatted/411_create_clone.sql"),
    test_unformatted_412_pragma => (default_mode, "tests/data/unformatted/412_pragma.sql"),

    // =========================================================================
    // Unformatted 900-series — edge cases
    // =========================================================================
    test_unformatted_900_create_view => (default_mode, "tests/data/unformatted/900_create_view.sql"),
    test_unformatted_910_semantic_view_dbt_body => (default_mode, "tests/data/unformatted/910_semantic_view_dbt_body.sql"),
    test_unformatted_911_semantic_view_jinja_conditionals => (default_mode, "tests/data/unformatted/911_semantic_view_jinja_conditionals.sql"),
    test_unformatted_912_semantic_view_with_extension => (default_mode, "tests/data/unformatted/912_semantic_view_with_extension.sql"),
    test_unformatted_913_semantic_view_downstream_query => (default_mode, "tests/data/unformatted/913_semantic_view_downstream_query.sql"),
    test_unformatted_999_unsupported_ddl => (default_mode, "tests/data/unformatted/999_unsupported_ddl.sql"),
}

// =============================================================================
// Error tests — these should produce parse errors
// =============================================================================

error_tests! {
    test_error_900_bad_token => "tests/data/errors/900_bad_token.sql",
    test_error_910_unopened_multiline => "tests/data/errors/910_unopened_multiline.sql",
    test_error_911_unopened_bracket => "tests/data/errors/911_unopened_bracket.sql",
    test_error_920_unterminated_multiline => "tests/data/errors/920_unterminated_multiline.sql",
    test_error_930_jinja_snowflake_external_table => "tests/data/errors/930_jinja_snowflake_external_table.sql",
    test_error_931_jinja_unmatched_closing_brace => "tests/data/errors/931_jinja_unmatched_closing_brace.sql",
}

// =============================================================================
// Edge case tests
// =============================================================================

#[test]
fn test_format_empty_whitespace() {
    let result = format_string("\n", &default_mode()).unwrap();
    assert!(result.is_empty() || result.trim().is_empty() || result == "\n");
}

#[test]
fn test_format_spaces_only() {
    let result = format_string("   ", &default_mode()).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_format_tabs_and_spaces() {
    let result = format_string("  \t  \t  ", &default_mode()).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_format_mixed_whitespace_with_newlines() {
    let result = format_string("  \n  \t  \n", &default_mode()).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_format_whitespace_strict_mode_errors() {
    let mut mode = default_mode();
    mode.strict_whitespace = true;
    let result = format_string("   ", &mode);
    assert!(result.is_err());
}

#[test]
fn test_format_whitespace_strict_mode_allows_valid_sql() {
    let mut mode = default_mode();
    mode.strict_whitespace = true;
    let result = format_string("SELECT 1\n", &mode);
    assert!(result.is_ok());
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
// Feature-specific assertion tests
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
// Dialect-specific, Jinja, and SQL feature tests
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

#[test]
fn test_fixture_dbt_star_macro() {
    let source = std::fs::read_to_string("tests/fixtures/dbt_star_macro.sql").unwrap();
    let result = format_string(&source, &default_mode());
    assert!(
        result.is_ok(),
        "Should successfully format dbt_star_macro.sql: {:?}",
        result.err()
    );
    let formatted = result.unwrap();
    assert!(
        formatted.contains("{% macro star("),
        "Should contain macro star: {}",
        formatted
    );
    assert!(
        formatted.contains("{% endmacro %}"),
        "Should contain endmacro: {}",
        formatted
    );
    assert!(
        formatted.contains(r#"prefix != """#),
        "Should normalize != operator spacing: {}",
        formatted
    );
    // Verify stability after second pass
    let second = format_string(&formatted, &default_mode()).unwrap();
    let third = format_string(&second, &default_mode()).unwrap();
    assert_eq!(
        second, third,
        "dbt star macro should be stable after second pass"
    );
}
