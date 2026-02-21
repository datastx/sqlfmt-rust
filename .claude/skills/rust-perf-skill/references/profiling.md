# Profiling sqlfmt-rust

How to measure, profile, and interpret performance data for this SQL formatter.

## Existing Benchmarks

sqlfmt-rust has criterion benchmarks in `benches/format_bench.rs`:

| Benchmark | What it measures | Typical use |
|-----------|-----------------|-------------|
| `format_small` | ~60 char trivial query | Overhead/startup cost |
| `format_medium` | `104_joins.sql` real query | Typical formatting perf |
| `format_large` | `216_gitlab_zuora` large query | Stress test |
| `lex_only` | Lexing without formatting | Isolate lexer vs formatter |
| `format_no_safety` | Format with `mode.fast=true` | Cost of safety check |

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- format_large

# Save baseline before changes
cargo bench -- --save-baseline before

# Compare after changes
cargo bench -- --baseline before

# Quick comparison of two specific benches
cargo bench -- format_large format_no_safety
```

Results are saved to `target/criterion/`. Open `target/criterion/report/index.html` in a
browser for visual reports with confidence intervals and regression detection.

**Tips for reliable results:**
- Close other applications (browsers, Docker, etc.)
- Run on AC power (laptops throttle on battery)
- Run multiple times — criterion handles statistical significance automatically
- For macOS: `sudo powermetrics --samplers cpu_power` shows if thermal throttling occurs

## Flamegraph Workflow

```bash
# Install (once)
cargo install flamegraph

# Generate flamegraph for the large benchmark
cargo flamegraph --bench format_bench -- --bench format_large

# For the binary directly with a SQL file
cargo flamegraph -- format path/to/large.sql
```

On macOS, you may need to use `dtrace` backend:
```bash
sudo cargo flamegraph --root --bench format_bench -- --bench format_large
```

**What to look for in sqlfmt-rust flamegraphs:**

| Wide bar in... | Means | Action |
|----------------|-------|--------|
| `analyzer::lex` / `lex_one` | Lexer is bottleneck | Optimize regex or token allocation |
| `regex::` functions | Regex matching overhead | Consider caching compiled regex (already done via LazyLock) |
| `alloc::` / `__rust_alloc` | Allocation-heavy | Reduce String/Vec allocations |
| `core::fmt` | `format!()` overhead | Switch to `write!()` with buffers |
| `<str as ToString>` / `to_string` | Per-token String clones | Use Cow/SmolStr/borrowing |
| `node_manager::create_node` | Per-node overhead | Reduce prefix/value computation cost |
| `jinja_formatter::normalize_*` | Jinja String chain | Single-pass normalization |
| `api::safety_check` | Safety verification | Skip with `mode.fast=true`, or optimize |
| `memcpy` / `memmove` | Data copying | Reduce clones, use indices |
| `splitter::maybe_split` | Line splitting | Reduce Vec allocations in split_at_index |
| `merger::maybe_merge_lines` | Line merging | Reduce recursive segment allocation |

## Allocation Profiling

### Using DHAT (recommended)

Add a `dhat` feature to `Cargo.toml`:

```toml
[features]
dhat-heap = ["dhat"]

[dependencies]
dhat = { version = "0.3", optional = true }
```

Add to `src/main.rs`:
```rust
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    // ... existing main code ...
}
```

Run:
```bash
cargo run --release --features dhat-heap -- format tests/data/unformatted/216_gitlab_zuora.sql
```

This produces `dhat-heap.json`. Open it at https://nnethercote.github.io/dh_view/dh_view.html

**Key metrics for sqlfmt-rust:**
- **Total allocations:** Target <10x the number of tokens in the input
- **Total bytes allocated:** Target <10x the input file size
- **Short-lived allocations:** These are optimization targets (allocated then quickly freed)
- **Hot allocation sites:** Sort by "total bytes" to find biggest offenders

### Expected allocation hotspots (in order)

1. `Token::new` — 2 Strings per token (prefix + text)
2. `Node` creation — prefix + value Strings
3. `JinjaFormatter::normalize_*` — 5-6 Strings per Jinja token
4. `Line` creation — `Vec<NodeIndex>` per line
5. `safety_check` — `normalize_token_text` per token
6. `Query::render` — output String building

## Adding New Benchmarks

When optimizing a specific stage, add a targeted benchmark:

```rust
// In benches/format_bench.rs

fn bench_jinja_normalize(c: &mut Criterion) {
    let jinja_input = "{{ some_variable | filter(arg1, arg2) }}";
    c.bench_function("jinja_normalize", |b| {
        b.iter(|| {
            JinjaFormatter::normalize_expression(black_box(jinja_input))
        })
    });
}

fn bench_safety_check(c: &mut Criterion) {
    let source = std::fs::read_to_string("tests/data/unformatted/216_gitlab_zuora.sql").unwrap();
    let mode = Mode::default();
    let formatted = format_string(&source, &mode).unwrap();
    c.bench_function("safety_check", |b| {
        b.iter(|| {
            // Isolate just the safety check cost
            safety_check(black_box(&source), black_box(&formatted))
        })
    });
}
```

## Compile Time

sqlfmt-rust tests take ~4-5 minutes (mostly compilation). To improve:

```bash
# See which crates take longest
cargo build --timings

# Use nextest for parallel test execution
cargo install cargo-nextest
cargo nextest run

# Use mold linker on Linux for faster linking
# In .cargo/config.toml:
# [target.x86_64-unknown-linux-gnu]
# linker = "clang"
# rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

## Interpreting Results for SQL Formatters

### Performance targets (rough ballpark)

| Operation | Target | Current concern |
|-----------|--------|-----------------|
| Lex one token | <500ns | ~200ns (regex) + ~100ns (2 String allocs) |
| Create one Node | <300ns | prefix/value computation + SmallVec clones |
| Split one Line | <1µs | Vec allocations, comment distribution |
| Merge attempt | <2µs | Segment building, recursive merging |
| Render one Line | <500ns | String building, indentation |
| Format small query | <50µs | Baseline overhead |
| Format medium query | <500µs | ~100-500 tokens |
| Format large query | <5ms | ~1000-5000 tokens |

### Optimization priority for sqlfmt-rust

1. **Reduce per-token String allocations** — biggest win, affects everything downstream
2. **Single-pass Jinja normalization** — eliminates 4-5 allocs per Jinja token
3. **Cow/SmolStr in Node** — avoid cloning when value unchanged
4. **Reusable buffers in formatter stages** — avoid per-call Vec allocations
5. **Safety check optimization** — skip re-normalization or cache results
6. **Indentation caching** — small win but easy to implement
