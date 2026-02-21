use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sqlfmt::{format_string, Mode};

fn load_test_file(name: &str) -> String {
    let path = format!("tests/data/unformatted/{}", name);
    let content = std::fs::read_to_string(&path).expect(&format!("Failed to read {}", path));
    // Golden test files use a sentinel to separate input/expected; take only input
    if let Some(pos) = content.find(")))))__SQLFMT_OUTPUT__(((((") {
        content[..pos].to_string()
    } else {
        content
    }
}

fn bench_format_small(c: &mut Criterion) {
    let sql = "SELECT a, b, c FROM my_table WHERE x = 1 AND y > 2 ORDER BY a\n";
    let mode = Mode::default();
    c.bench_function("format_small", |b| {
        b.iter(|| format_string(black_box(sql), black_box(&mode)).unwrap())
    });
}

fn bench_format_medium(c: &mut Criterion) {
    let sql = load_test_file("104_joins.sql");
    let mode = Mode::default();
    c.bench_function("format_medium", |b| {
        b.iter(|| format_string(black_box(&sql), black_box(&mode)).unwrap())
    });
}

fn bench_format_large(c: &mut Criterion) {
    let sql = load_test_file("216_gitlab_zuora_revenue_revenue_contract_line_source.sql");
    let mode = Mode::default();
    c.bench_function("format_large", |b| {
        b.iter(|| format_string(black_box(&sql), black_box(&mode)).unwrap())
    });
}

fn bench_lex_only(c: &mut Criterion) {
    let sql = load_test_file("216_gitlab_zuora_revenue_revenue_contract_line_source.sql");
    let mode = Mode::default();
    let dialect = mode.dialect().unwrap();
    c.bench_function("lex_only", |b| {
        b.iter(|| {
            let mut analyzer = dialect.initialize_analyzer(mode.line_length);
            analyzer.parse_query(black_box(&sql)).unwrap();
        })
    });
}

fn bench_format_no_safety(c: &mut Criterion) {
    let sql = load_test_file("216_gitlab_zuora_revenue_revenue_contract_line_source.sql");
    let mode = Mode {
        fast: true,
        ..Mode::default()
    };
    c.bench_function("format_no_safety", |b| {
        b.iter(|| format_string(black_box(&sql), black_box(&mode)).unwrap())
    });
}

/// Measures the cost of safety check in isolation by computing the difference
/// between format_large (with safety) and format_no_safety (without).
/// Both are benchmarked here side-by-side for easy comparison in Criterion output.
fn bench_safety_check_overhead(c: &mut Criterion) {
    let sql = load_test_file("216_gitlab_zuora_revenue_revenue_contract_line_source.sql");

    let mut group = c.benchmark_group("safety_check_overhead");

    let mode_with = Mode::default();
    group.bench_function("with_safety", |b| {
        b.iter(|| format_string(black_box(&sql), black_box(&mode_with)).unwrap())
    });

    let mode_without = Mode {
        fast: true,
        ..Mode::default()
    };
    group.bench_function("without_safety", |b| {
        b.iter(|| format_string(black_box(&sql), black_box(&mode_without)).unwrap())
    });

    group.finish();
}

/// Benchmark formatting already-formatted output (idempotent pass).
/// This isolates the safety check path since formatting is a near-no-op.
fn bench_format_idempotent(c: &mut Criterion) {
    let sql = load_test_file("216_gitlab_zuora_revenue_revenue_contract_line_source.sql");
    let mode = Mode::default();
    // Pre-format once to get idempotent input
    let formatted = format_string(&sql, &mode).unwrap();

    c.bench_function("format_idempotent", |b| {
        b.iter(|| format_string(black_box(&formatted), black_box(&mode)).unwrap())
    });
}

criterion_group!(
    benches,
    bench_format_small,
    bench_format_medium,
    bench_format_large,
    bench_lex_only,
    bench_format_no_safety,
    bench_safety_check_overhead,
    bench_format_idempotent
);
criterion_main!(benches);
