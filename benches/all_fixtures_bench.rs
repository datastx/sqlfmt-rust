use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sqlfmt::{format_string, Mode};
use std::fs;
use std::path::Path;
use std::time::Duration;

const SENTINEL: &str = ")))))__SQLFMT_OUTPUT__(((((";

fn extract_input(content: &str) -> &str {
    if let Some(pos) = content.find(SENTINEL) {
        &content[..pos]
    } else {
        content
    }
}

fn collect_fixtures(dir: &str) -> Vec<(String, String)> {
    let path = Path::new("tests/data").join(dir);
    let mut fixtures: Vec<(String, String)> = Vec::new();

    if let Ok(entries) = fs::read_dir(&path) {
        for entry in entries.flatten() {
            let file_path = entry.path();
            if file_path.extension().map_or(false, |e| e == "sql") {
                let name = file_path.file_stem().unwrap().to_string_lossy().to_string();
                let content = fs::read_to_string(&file_path).unwrap();
                let input = extract_input(&content).to_string();
                fixtures.push((name, input));
            }
        }
    }

    fixtures.sort_by(|a, b| a.0.cmp(&b.0));
    fixtures
}

fn bench_all_unformatted(c: &mut Criterion) {
    let fixtures = collect_fixtures("unformatted");
    let mode = Mode::default();

    let mut group = c.benchmark_group("unformatted");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    for (name, sql) in &fixtures {
        group.bench_with_input(BenchmarkId::new("format", name), sql, |b, s| {
            b.iter(|| format_string(black_box(s), black_box(&mode)))
        });
    }

    group.finish();
}

fn bench_all_preformatted(c: &mut Criterion) {
    let fixtures = collect_fixtures("preformatted");
    let mode = Mode::default();

    let mut group = c.benchmark_group("preformatted");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    for (name, sql) in &fixtures {
        group.bench_with_input(BenchmarkId::new("format", name), sql, |b, s| {
            b.iter(|| format_string(black_box(s), black_box(&mode)))
        });
    }

    group.finish();
}

fn bench_all_errors(c: &mut Criterion) {
    let fixtures = collect_fixtures("errors");
    let mode = Mode::default();

    let mut group = c.benchmark_group("errors");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    for (name, sql) in &fixtures {
        group.bench_with_input(BenchmarkId::new("format", name), sql, |b, s| {
            b.iter(|| format_string(black_box(s), black_box(&mode)))
        });
    }

    group.finish();
}

fn bench_by_size(c: &mut Criterion) {
    let mut all: Vec<(String, String)> = Vec::new();
    all.extend(collect_fixtures("unformatted"));
    all.extend(collect_fixtures("preformatted"));

    // Sort by input size descending to find the heaviest files
    all.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    let mode = Mode::default();
    let mut group = c.benchmark_group("by_size_top10");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    for (name, sql) in all.iter().take(10) {
        let label = format!("{}_{}b", name, sql.len());
        group.bench_with_input(BenchmarkId::new("format", &label), sql, |b, s| {
            b.iter(|| format_string(black_box(s), black_box(&mode)))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_all_unformatted,
    bench_all_preformatted,
    bench_all_errors,
    bench_by_size,
);
criterion_main!(benches);
