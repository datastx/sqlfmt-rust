use sqlfmt::{format_string, Mode};
use std::fs;

const SENTINEL: &str = ")))))__SQLFMT_OUTPUT__(((((";

/// Read a golden test data file and return (source, expected) tuple.
///
/// Mirrors the Python `read_test_data()` logic exactly:
/// - If the file contains the sentinel, lines above = source, lines below = expected
/// - If no sentinel, the file is preformatted: expected = source
/// - Source is trimmed + "\n"; expected preserves exact whitespace
fn read_test_data(path: &str) -> (String, String) {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read test file {}: {}", path, e));

    let lines: Vec<&str> = content.lines().collect();

    let mut source_lines: Vec<&str> = Vec::new();
    let mut formatted_lines: Vec<&str> = Vec::new();
    let mut found_sentinel = false;

    for line in &lines {
        if line.trim() == SENTINEL {
            found_sentinel = true;
            continue;
        }
        if found_sentinel {
            formatted_lines.push(line);
        } else {
            source_lines.push(line);
        }
    }

    if !found_sentinel {
        formatted_lines = source_lines.clone();
    }

    // Source: join with newlines, trim, add trailing newline
    // (matching Python: "".join(source_query).strip() + "\n")
    let source = {
        let joined = source_lines.join("\n");
        let trimmed = joined.trim();
        if trimmed.is_empty() {
            String::new()
        } else {
            format!("{}\n", trimmed)
        }
    };

    // Expected: join with newlines preserving exact content
    // (matching Python: "".join(formatted_query) — but Python readlines() keeps \n)
    let expected = if formatted_lines.is_empty() {
        String::new()
    } else {
        // Reconstruct with newlines (readlines in Python preserves \n on each line)
        let mut result = formatted_lines.join("\n");
        // If the original content ended with a newline after the last formatted line,
        // preserve it
        if content.ends_with('\n') {
            result.push('\n');
        }
        result
    };

    (source, expected)
}

fn default_mode() -> Mode {
    Mode::default()
}

fn clickhouse_mode() -> Mode {
    Mode {
        dialect_name: "clickhouse".to_string(),
        ..Mode::default()
    }
}

fn run_golden_test(path: &str, mode: &Mode) {
    let (source, expected) = read_test_data(path);
    let actual = format_string(&source, mode).unwrap_or_else(|e| {
        panic!("format_string failed for {}: {}", path, e);
    });
    assert_eq!(
        expected, actual,
        "\n\nFormatting mismatch for {}\n\n--- expected ---\n{}\n--- actual ---\n{}\n",
        path, expected, actual
    );
    // Idempotency check
    let second = format_string(&actual, mode).unwrap_or_else(|e| {
        panic!("Idempotency format failed for {}: {}", path, e);
    });
    assert_eq!(
        expected, second,
        "\n\nIdempotency failed for {}\n\n--- expected ---\n{}\n--- second pass ---\n{}\n",
        path, expected, second
    );
}

fn run_golden_error_test(path: &str) {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read error test file {}: {}", path, e));
    let source = format!("{}\n", content.trim());
    let result = format_string(&source, &default_mode());
    assert!(
        result.is_err(),
        "Expected error for {} but got Ok:\n{}",
        path,
        result.unwrap()
    );
}

macro_rules! golden_tests {
    (mode: $mode_fn:ident, $($name:ident => $path:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_golden_test($path, &$mode_fn());
            }
        )*
    };
}

macro_rules! golden_error_tests {
    ($($name:ident => $path:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                run_golden_error_test($path);
            }
        )*
    };
}

// =============================================================================
// Preformatted golden tests (16 files)
// These files have no sentinel — input should pass through unchanged.
// =============================================================================

golden_tests! {
    mode: default_mode,
    golden_preformatted_001_select_1 => "tests/data/preformatted/001_select_1.sql",
    golden_preformatted_002_select_from_where => "tests/data/preformatted/002_select_from_where.sql",
    golden_preformatted_003_literals => "tests/data/preformatted/003_literals.sql",
    golden_preformatted_004_with_select => "tests/data/preformatted/004_with_select.sql",
    golden_preformatted_005_fmt_off => "tests/data/preformatted/005_fmt_off.sql",
    golden_preformatted_006_fmt_off_447 => "tests/data/preformatted/006_fmt_off_447.sql",
    golden_preformatted_007_fmt_off_comments => "tests/data/preformatted/007_fmt_off_comments.sql",
    golden_preformatted_008_reserved_names => "tests/data/preformatted/008_reserved_names.sql",
    golden_preformatted_009_empty => "tests/data/preformatted/009_empty.sql",
    golden_preformatted_010_comment_only => "tests/data/preformatted/010_comment_only.sql",
    golden_preformatted_301_multiline_jinjafmt => "tests/data/preformatted/301_multiline_jinjafmt.sql",
    golden_preformatted_302_jinjafmt_multiline_str => "tests/data/preformatted/302_jinjafmt_multiline_str.sql",
    golden_preformatted_303_jinjafmt_more_mutliline_str => "tests/data/preformatted/303_jinjafmt_more_mutliline_str.sql",
    golden_preformatted_400_create_table => "tests/data/preformatted/400_create_table.sql",
    golden_preformatted_401_create_row_access_policy => "tests/data/preformatted/401_create_row_access_policy.sql",
    golden_preformatted_402_alter_table => "tests/data/preformatted/402_alter_table.sql",
}

// =============================================================================
// Unformatted golden tests — 100-series (core SQL formatting)
// =============================================================================

golden_tests! {
    mode: default_mode,
    golden_unformatted_100_select_case => "tests/data/unformatted/100_select_case.sql",
    golden_unformatted_101_multiline => "tests/data/unformatted/101_multiline.sql",
    golden_unformatted_102_lots_of_comments => "tests/data/unformatted/102_lots_of_comments.sql",
    golden_unformatted_103_window_functions => "tests/data/unformatted/103_window_functions.sql",
    golden_unformatted_104_joins => "tests/data/unformatted/104_joins.sql",
    golden_unformatted_105_fmt_off => "tests/data/unformatted/105_fmt_off.sql",
    golden_unformatted_106_leading_commas => "tests/data/unformatted/106_leading_commas.sql",
    golden_unformatted_107_jinja_blocks => "tests/data/unformatted/107_jinja_blocks.sql",
    golden_unformatted_108_test_block => "tests/data/unformatted/108_test_block.sql",
    golden_unformatted_109_lateral_flatten => "tests/data/unformatted/109_lateral_flatten.sql",
    golden_unformatted_110_other_identifiers => "tests/data/unformatted/110_other_identifiers.sql",
    golden_unformatted_111_chained_boolean_between => "tests/data/unformatted/111_chained_boolean_between.sql",
    golden_unformatted_112_semicolons => "tests/data/unformatted/112_semicolons.sql",
    golden_unformatted_113_utils_group_by => "tests/data/unformatted/113_utils_group_by.sql",
    golden_unformatted_114_unions => "tests/data/unformatted/114_unions.sql",
    golden_unformatted_115_select_star_except => "tests/data/unformatted/115_select_star_except.sql",
    golden_unformatted_116_chained_booleans => "tests/data/unformatted/116_chained_booleans.sql",
    golden_unformatted_117_whitespace_in_tokens => "tests/data/unformatted/117_whitespace_in_tokens.sql",
    golden_unformatted_118_within_group => "tests/data/unformatted/118_within_group.sql",
    golden_unformatted_119_psycopg_placeholders => "tests/data/unformatted/119_psycopg_placeholders.sql",
    golden_unformatted_120_array_literals => "tests/data/unformatted/120_array_literals.sql",
    golden_unformatted_121_stubborn_merge_edge_cases => "tests/data/unformatted/121_stubborn_merge_edge_cases.sql",
    golden_unformatted_122_values => "tests/data/unformatted/122_values.sql",
    golden_unformatted_123_spark_keywords => "tests/data/unformatted/123_spark_keywords.sql",
    golden_unformatted_124_bq_compound_types => "tests/data/unformatted/124_bq_compound_types.sql",
    golden_unformatted_125_numeric_literals => "tests/data/unformatted/125_numeric_literals.sql",
    golden_unformatted_126_blank_lines => "tests/data/unformatted/126_blank_lines.sql",
    golden_unformatted_127_more_comments => "tests/data/unformatted/127_more_comments.sql",
    golden_unformatted_128_double_slash_comments => "tests/data/unformatted/128_double_slash_comments.sql",
    golden_unformatted_129_duckdb_joins => "tests/data/unformatted/129_duckdb_joins.sql",
    golden_unformatted_130_athena_data_types => "tests/data/unformatted/130_athena_data_types.sql",
    golden_unformatted_131_assignment_statement => "tests/data/unformatted/131_assignment_statement.sql",
    golden_unformatted_132_spark_number_literals => "tests/data/unformatted/132_spark_number_literals.sql",
    golden_unformatted_133_for_else => "tests/data/unformatted/133_for_else.sql",
    golden_unformatted_134_databricks_type_hints => "tests/data/unformatted/134_databricks_type_hints.sql",
    golden_unformatted_135_star_columns => "tests/data/unformatted/135_star_columns.sql",
    golden_unformatted_136_databricks_variant => "tests/data/unformatted/136_databricks_variant.sql",
    golden_unformatted_137_escaped_single_quotes => "tests/data/unformatted/137_escaped_single_quotes.sql",
}

// =============================================================================
// Unformatted golden tests — 200-series (real-world dbt models)
// =============================================================================

golden_tests! {
    mode: default_mode,
    golden_unformatted_200_base_model => "tests/data/unformatted/200_base_model.sql",
    golden_unformatted_201_basic_snapshot => "tests/data/unformatted/201_basic_snapshot.sql",
    golden_unformatted_202_unpivot_macro => "tests/data/unformatted/202_unpivot_macro.sql",
    golden_unformatted_203_gitlab_email_domain_type => "tests/data/unformatted/203_gitlab_email_domain_type.sql",
    golden_unformatted_204_gitlab_tag_validation => "tests/data/unformatted/204_gitlab_tag_validation.sql",
    golden_unformatted_205_rittman_hubspot_deals => "tests/data/unformatted/205_rittman_hubspot_deals.sql",
    golden_unformatted_206_gitlab_prep_geozone => "tests/data/unformatted/206_gitlab_prep_geozone.sql",
    golden_unformatted_207_rittman_int_journals => "tests/data/unformatted/207_rittman_int_journals.sql",
    golden_unformatted_208_rittman_int_plan_breakout_metrics => "tests/data/unformatted/208_rittman_int_plan_breakout_metrics.sql",
    golden_unformatted_209_rittman_int_web_events_sessionized => "tests/data/unformatted/209_rittman_int_web_events_sessionized.sql",
    golden_unformatted_210_gitlab_gdpr_delete => "tests/data/unformatted/210_gitlab_gdpr_delete.sql",
    golden_unformatted_211_http_2019_cdn_17_20 => "tests/data/unformatted/211_http_2019_cdn_17_20.sql",
    golden_unformatted_212_http_2019_cms_14_02 => "tests/data/unformatted/212_http_2019_cms_14_02.sql",
    golden_unformatted_213_gitlab_fct_sales_funnel_target => "tests/data/unformatted/213_gitlab_fct_sales_funnel_target.sql",
    golden_unformatted_214_get_unique_attributes => "tests/data/unformatted/214_get_unique_attributes.sql",
    golden_unformatted_215_gitlab_get_backup_table_command => "tests/data/unformatted/215_gitlab_get_backup_table_command.sql",
    golden_unformatted_216_gitlab_zuora_revenue_revenue_contract_line_source => "tests/data/unformatted/216_gitlab_zuora_revenue_revenue_contract_line_source.sql",
    golden_unformatted_217_dbt_unit_testing_csv => "tests/data/unformatted/217_dbt_unit_testing_csv.sql",
    golden_unformatted_218_multiple_c_comments => "tests/data/unformatted/218_multiple_c_comments.sql",
    golden_unformatted_219_any_all_agg => "tests/data/unformatted/219_any_all_agg.sql",
    golden_unformatted_221_dbt_config_dollar_quoted => "tests/data/unformatted/221_dbt_config_dollar_quoted.sql",
    golden_unformatted_222_colorado_claims_extract => "tests/data/unformatted/222_colorado_claims_extract.sql",
    golden_unformatted_222_jinja_unbalanced_brackets => "tests/data/unformatted/222_jinja_unbalanced_brackets.sql",
}

// ClickHouse dialect tests
golden_tests! {
    mode: clickhouse_mode,
    golden_unformatted_220_clickhouse_joins => "tests/data/unformatted/220_clickhouse_joins.sql",
}

// =============================================================================
// Unformatted golden tests — 300-series (Jinja formatting)
// =============================================================================

golden_tests! {
    mode: default_mode,
    golden_unformatted_300_jinjafmt => "tests/data/unformatted/300_jinjafmt.sql",
}

// =============================================================================
// Unformatted golden tests — 400-series (DDL/DML)
// =============================================================================

golden_tests! {
    mode: default_mode,
    golden_unformatted_400_create_fn_and_select => "tests/data/unformatted/400_create_fn_and_select.sql",
    golden_unformatted_401_explain_select => "tests/data/unformatted/401_explain_select.sql",
    golden_unformatted_402_delete_from_using => "tests/data/unformatted/402_delete_from_using.sql",
    golden_unformatted_403_grant_revoke => "tests/data/unformatted/403_grant_revoke.sql",
    golden_unformatted_404_create_function_pg_examples => "tests/data/unformatted/404_create_function_pg_examples.sql",
    golden_unformatted_405_create_function_snowflake_examples => "tests/data/unformatted/405_create_function_snowflake_examples.sql",
    golden_unformatted_406_create_function_bq_examples => "tests/data/unformatted/406_create_function_bq_examples.sql",
    golden_unformatted_407_alter_function_pg_examples => "tests/data/unformatted/407_alter_function_pg_examples.sql",
    golden_unformatted_408_alter_function_snowflake_examples => "tests/data/unformatted/408_alter_function_snowflake_examples.sql",
    golden_unformatted_409_create_external_function => "tests/data/unformatted/409_create_external_function.sql",
    golden_unformatted_410_create_warehouse => "tests/data/unformatted/410_create_warehouse.sql",
    golden_unformatted_411_create_clone => "tests/data/unformatted/411_create_clone.sql",
    golden_unformatted_412_pragma => "tests/data/unformatted/412_pragma.sql",
}

// =============================================================================
// Unformatted golden tests — 900-series (edge cases)
// =============================================================================

golden_tests! {
    mode: default_mode,
    golden_unformatted_900_create_view => "tests/data/unformatted/900_create_view.sql",
    golden_unformatted_999_unsupported_ddl => "tests/data/unformatted/999_unsupported_ddl.sql",
}

// =============================================================================
// Error golden tests (4 files) — these should produce parse errors
// =============================================================================

golden_error_tests! {
    golden_error_900_bad_token => "tests/data/errors/900_bad_token.sql",
    golden_error_910_unopened_multiline => "tests/data/errors/910_unopened_multiline.sql",
    golden_error_911_unopened_bracket => "tests/data/errors/911_unopened_bracket.sql",
    golden_error_920_unterminated_multiline => "tests/data/errors/920_unterminated_multiline.sql",
}
