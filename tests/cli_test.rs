//! CLI integration tests for the sqlfmt binary.
//! Mirrors Python sqlfmt's `test_end_to_end.py`.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

/// Helper: get a Command for the sqlfmt binary.
fn sqlfmt() -> Command {
    Command::cargo_bin("sqlfmt").expect("binary should exist")
}

/// Helper: create a temp directory with SQL files copied from fixtures.
fn setup_temp_dir(files: &[(&str, &str)]) -> TempDir {
    let dir = TempDir::new().expect("create temp dir");
    for (name, content) in files {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, content).unwrap();
    }
    dir
}

// ─── Preformatted files (should be left unchanged) ───

#[test]
fn test_preformatted_file_unchanged() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("unchanged"));
}

#[test]
fn test_preformatted_check_mode_passes() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt().arg("--check").arg(dir.path()).assert().success();
}

#[test]
fn test_preformatted_diff_mode_no_output() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt().arg("--diff").arg(dir.path()).assert().success();
}

#[test]
fn test_preformatted_verbose_mode() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg("--verbose")
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("file(s) processed"));
}

// ─── Unformatted files (should be reformatted) ───

#[test]
fn test_unformatted_file_reformatted() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt()
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("reformatted").or(predicate::str::contains("changed")));

    // Verify file was actually modified
    let content = fs::read_to_string(dir.path().join("query.sql")).unwrap();
    assert_eq!(content.trim(), "select 1");
}

#[test]
fn test_unformatted_check_mode_fails() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt().arg("--check").arg(dir.path()).assert().code(1);

    // File should NOT be modified in check mode
    let content = fs::read_to_string(dir.path().join("query.sql")).unwrap();
    assert_eq!(content, "SELECT    1\n");
}

#[test]
fn test_unformatted_check_mode_verbose() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt()
        .arg("--check")
        .arg("--verbose")
        .arg(dir.path())
        .assert()
        .code(1);
}

#[test]
fn test_unformatted_diff_mode_shows_diff() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    // --diff alone shows the diff but exits 0 (only --check triggers exit 1)
    sqlfmt()
        .arg("--diff")
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("---").or(predicate::str::contains("select")));
}

#[test]
fn test_unformatted_diff_check_combined() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt()
        .arg("--check")
        .arg("--diff")
        .arg(dir.path())
        .assert()
        .code(1);
}

// ─── Stdin mode ───

#[test]
fn test_stdin_formats_sql() {
    sqlfmt()
        .arg("-")
        .write_stdin("SELECT    1\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("select 1"));
}

#[test]
fn test_stdin_preserves_preformatted() {
    sqlfmt()
        .arg("-")
        .write_stdin("select 1\n")
        .assert()
        .success()
        .stdout("select 1\n");
}

#[test]
fn test_stdin_empty_input() {
    sqlfmt().arg("-").write_stdin("\n").assert().success();
}

#[test]
fn test_stdin_normalizes_trailing_newline() {
    sqlfmt()
        .arg("-")
        .write_stdin("select 1\n\n")
        .assert()
        .success()
        .stdout(predicate::str::ends_with("\n"));
}

#[test]
fn test_stdin_lowercases_keywords() {
    sqlfmt()
        .arg("-")
        .write_stdin("SELECT A, B FROM T WHERE X = 1\n")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("select")
                .and(predicate::str::contains("from"))
                .and(predicate::str::contains("where")),
        );
}

// ─── Error handling ───

#[test]
fn test_error_file_exits_with_code_2() {
    let dir = setup_temp_dir(&[("bad.sql", "select $\n")]);
    sqlfmt()
        .arg(dir.path())
        .assert()
        .code(2)
        .stderr(predicate::str::contains("error"));
}

#[test]
fn test_error_with_check_mode() {
    let dir = setup_temp_dir(&[("bad.sql", "select $\n")]);
    sqlfmt().arg("--check").arg(dir.path()).assert().code(2);
}

// ─── Multiple files ───

#[test]
fn test_multiple_files_mixed_status() {
    let dir = setup_temp_dir(&[
        ("formatted.sql", "select 1\n"),
        ("unformatted.sql", "SELECT    2\n"),
    ]);
    sqlfmt()
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("file(s) processed"));

    // Formatted file unchanged
    assert_eq!(
        fs::read_to_string(dir.path().join("formatted.sql")).unwrap(),
        "select 1\n"
    );
    // Unformatted file was reformatted
    let content = fs::read_to_string(dir.path().join("unformatted.sql")).unwrap();
    assert!(content.contains("select"));
    assert!(!content.contains("SELECT"));
}

#[test]
fn test_check_mode_with_mixed_files() {
    let dir = setup_temp_dir(&[
        ("formatted.sql", "select 1\n"),
        ("unformatted.sql", "SELECT    2\n"),
    ]);
    sqlfmt().arg("--check").arg(dir.path()).assert().code(1);
}

// ─── File discovery ───

#[test]
fn test_discovers_sql_files_recursively() {
    let dir = setup_temp_dir(&[
        ("top.sql", "select 1\n"),
        ("subdir/nested.sql", "select 2\n"),
    ]);
    sqlfmt()
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("2 file(s) processed"));
}

#[test]
fn test_ignores_non_sql_files() {
    let dir = setup_temp_dir(&[
        ("query.sql", "select 1\n"),
        ("readme.md", "# Not SQL\n"),
        ("data.csv", "a,b,c\n"),
    ]);
    sqlfmt()
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("1 file(s) processed"));
}

#[test]
fn test_single_file_argument() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    let file_path = dir.path().join("query.sql");
    sqlfmt().arg(&file_path).assert().success();

    let content = fs::read_to_string(&file_path).unwrap();
    assert_eq!(content.trim(), "select 1");
}

#[test]
fn test_empty_directory() {
    let dir = TempDir::new().expect("create temp dir");
    sqlfmt()
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("0 file(s) processed"));
}

// ─── Quiet mode ───

#[test]
fn test_quiet_mode_suppresses_output() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg("--quiet")
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::is_empty().or(predicate::str::contains("error").not()));
}

#[test]
fn test_quiet_mode_with_check() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt()
        .arg("--quiet")
        .arg("--check")
        .arg(dir.path())
        .assert()
        .code(1);
}

// ─── Line length ───

#[test]
fn test_custom_line_length() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg("-l")
        .arg("120")
        .arg(dir.path())
        .assert()
        .success();
}

#[test]
fn test_line_length_long_flag() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg("--line-length")
        .arg("40")
        .arg(dir.path())
        .assert()
        .success();
}

// ─── Dialect selection ───

#[test]
fn test_dialect_polyglot() {
    sqlfmt()
        .arg("-")
        .arg("--dialect")
        .arg("polyglot")
        .write_stdin("SELECT 1\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("select"));
}

#[test]
fn test_dialect_duckdb() {
    sqlfmt()
        .arg("-")
        .arg("--dialect")
        .arg("duckdb")
        .write_stdin("SELECT 1\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("select"));
}

#[test]
fn test_dialect_clickhouse() {
    sqlfmt()
        .arg("-")
        .arg("--dialect")
        .arg("clickhouse")
        .write_stdin("SELECT 1\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("select"));
}

#[test]
fn test_invalid_dialect() {
    sqlfmt()
        .arg("-")
        .arg("--dialect")
        .arg("invalid_dialect")
        .write_stdin("SELECT 1\n")
        .assert()
        .code(2);
}

// ─── Other CLI flags ───

#[test]
fn test_no_jinjafmt_flag() {
    sqlfmt()
        .arg("-")
        .arg("--no-jinjafmt")
        .write_stdin("select {{ my_var }}\n")
        .assert()
        .success();
}

#[test]
fn test_fast_flag_skips_safety_check() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt().arg("--fast").arg(dir.path()).assert().success();
}

#[test]
fn test_no_color_flag() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg("--no-color")
        .arg(dir.path())
        .assert()
        .success();
}

#[test]
fn test_single_process_flag() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg("--single-process")
        .arg(dir.path())
        .assert()
        .success();
}

#[test]
fn test_version_flag() {
    sqlfmt()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("sqlfmt"));
}

#[test]
fn test_help_flag() {
    sqlfmt()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("SQL formatter"));
}

// ─── Thread count ───

#[test]
fn test_threads_flag() {
    let dir = setup_temp_dir(&[("a.sql", "SELECT    1\n"), ("b.sql", "SELECT    2\n")]);
    sqlfmt()
        .arg("--threads")
        .arg("2")
        .arg(dir.path())
        .assert()
        .success()
        .stderr(predicate::str::contains("2 file(s) processed"));
}

#[test]
fn test_threads_short_flag() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt()
        .arg("-t")
        .arg("1")
        .arg(dir.path())
        .assert()
        .success();
}

#[test]
fn test_threads_zero_uses_all_cores() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt()
        .arg("--threads")
        .arg("0")
        .arg(dir.path())
        .assert()
        .success();
}

// ─── Idempotency (format twice produces same result) ───

#[test]
fn test_idempotent_formatting_via_cli() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT a, b, c FROM my_table WHERE x = 1\n")]);
    let file_path = dir.path().join("query.sql");

    // First format
    sqlfmt().arg(&file_path).assert().success();
    let first_pass = fs::read_to_string(&file_path).unwrap();

    // Second format (should be unchanged)
    sqlfmt().arg("--check").arg(&file_path).assert().success(); // exit 0 means no changes needed

    let second_pass = fs::read_to_string(&file_path).unwrap();
    assert_eq!(first_pass, second_pass, "Formatting should be idempotent");
}

// ─── Complex formatting scenarios via stdin ───

#[test]
fn test_stdin_formats_cte() {
    let input = "WITH cte AS (SELECT 1 AS id, 'hello' AS name) SELECT * FROM cte\n";
    sqlfmt()
        .arg("-")
        .write_stdin(input)
        .assert()
        .success()
        .stdout(predicate::str::contains("with").and(predicate::str::contains("select")));
}

#[test]
fn test_stdin_formats_join() {
    let input = "SELECT a.id, b.name FROM table_a a INNER JOIN table_b b ON a.id = b.a_id\n";
    sqlfmt()
        .arg("-")
        .write_stdin(input)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("select")
                .and(predicate::str::contains("inner join"))
                .and(predicate::str::contains("on")),
        );
}

#[test]
fn test_stdin_formats_case_expression() {
    let input = "SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END AS sign\n";
    sqlfmt()
        .arg("-")
        .write_stdin(input)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("case")
                .and(predicate::str::contains("when"))
                .and(predicate::str::contains("then"))
                .and(predicate::str::contains("end")),
        );
}

#[test]
fn test_stdin_formats_window_function() {
    let input =
        "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees\n";
    sqlfmt()
        .arg("-")
        .write_stdin(input)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("row_number")
                .and(predicate::str::contains("over"))
                .and(predicate::str::contains("partition by")),
        );
}

#[test]
fn test_stdin_formats_comment_only() {
    sqlfmt()
        .arg("-")
        .write_stdin("-- just a comment\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("-- just a comment"));
}

#[test]
fn test_stdin_formats_multiple_statements() {
    sqlfmt()
        .arg("-")
        .write_stdin("SELECT 1; SELECT 2\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("select").and(predicate::str::contains(";")));
}

// ─── Exclude patterns ───

#[test]
fn test_exclude_pattern() {
    let dir = setup_temp_dir(&[
        ("include.sql", "SELECT    1\n"),
        ("exclude_me.sql", "SELECT    2\n"),
    ]);
    sqlfmt()
        .arg("--exclude")
        .arg("exclude_*")
        .arg(dir.path())
        .assert()
        .success();

    // Included file should be reformatted
    let included = fs::read_to_string(dir.path().join("include.sql")).unwrap();
    assert!(included.contains("select"));

    // Excluded file should remain unchanged
    let excluded = fs::read_to_string(dir.path().join("exclude_me.sql")).unwrap();
    assert_eq!(excluded, "SELECT    2\n");
}

// ─── Exit code summary ───
// exit 0: success (all files formatted or unchanged)
// exit 1: --check found files that would be reformatted
// exit 2: errors occurred (bad SQL, bad dialect, etc.)

#[test]
fn test_exit_code_0_on_success() {
    let dir = setup_temp_dir(&[("query.sql", "select 1\n")]);
    sqlfmt().arg(dir.path()).assert().code(0);
}

#[test]
fn test_exit_code_1_on_check_failure() {
    let dir = setup_temp_dir(&[("query.sql", "SELECT    1\n")]);
    sqlfmt().arg("--check").arg(dir.path()).assert().code(1);
}

#[test]
fn test_exit_code_2_on_error() {
    let dir = setup_temp_dir(&[("bad.sql", "select $\n")]);
    sqlfmt().arg(dir.path()).assert().code(2);
}
