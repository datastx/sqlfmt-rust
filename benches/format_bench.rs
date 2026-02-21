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

criterion_group!(
    benches,
    bench_format_small,
    bench_format_medium,
    bench_format_large,
    bench_lex_only,
    bench_format_no_safety
);
criterion_main!(benches);
