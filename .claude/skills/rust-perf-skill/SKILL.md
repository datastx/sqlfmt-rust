---
name: rust-perf-skill
description: Performance and memory optimization guide specific to sqlfmt-rust. Trigger on performance, benchmarks, allocation, profiling, or making the formatter faster.
---

# sqlfmt-rust Performance & Memory Guide

This skill is tailored to the sqlfmt-rust SQL formatter architecture. It documents the
actual data flow, hotspots, and optimization opportunities in *this* codebase.

## Architecture & Pipeline

```
SOURCE SQL (&str)
    ↓
[Analyzer.lex]          — Regex-based lexer → Token per SQL element
    ↓                     (src/analyzer.rs, src/rules/)
[NodeManager.create_node] — Token → Node with computed prefix/value
    ↓                       (src/node_manager.rs)
[Arena: Vec<Node>]       — All nodes in one contiguous Vec, indexed by NodeIndex (usize)
    ↓
[Query { lines: Vec<Line> }] — Lines hold Vec<NodeIndex>, not Node copies
    ↓
[Formatter Pipeline — 5 stages]  (src/formatter.rs)
  1. LineSplitter.maybe_split     — Split long lines at keywords/operators
  2. JinjaFormatter.format_line   — Normalize Jinja whitespace
  3. Re-split                     — Handle multiline Jinja
  4. LineMerger.maybe_merge_lines — Merge short lines (operator-precedence aware)
  5. Blank line cleanup
    ↓
[Query.render]           — Walk arena, build formatted String
    ↓
[Safety check]           — Token-equivalence verification (skipped when mode.fast=true)
```

**Key design strength:** Arena allocation with `Vec<Node>` and `NodeIndex = usize` references.
Lines are lightweight (`Vec<NodeIndex>`), not copies of node data. SmallVec used for brackets
and Jinja block tracking.

## Hot Path Priority Map

These are the actual hotspots, ordered by impact. Always measure with `cargo bench` before
and after changes.

| Priority | Location | Why It's Hot | Key Cost |
|----------|----------|-------------|----------|
| 1 | `analyzer.rs` — `lex` / `lex_one` | Runs per-token, regex matching | Token String allocs |
| 2 | `node_manager.rs` — `create_node` | Runs per-token, prefix/value computation | String ops, SmallVec clones |
| 3 | `jinja_formatter.rs` — normalize fns | Runs per-Jinja token, 5-6 String allocs each | Chained String allocations |
| 4 | `api.rs` — safety check normalization | Runs per-token when enabled | Full token re-normalization |
| 5 | `splitter.rs` — `maybe_split` / `split_at_index` | Runs per-line, Vec allocations | `to_vec()`, linear comment search |
| 6 | `merger.rs` — `maybe_merge_lines` | Recursive, segment building | Line/Vec cloning |
| 7 | `line.rs` — `render` / `indentation` | Runs per-output-line | String building, `" ".repeat()` |

## Decision Checklist (sqlfmt-specific)

Before writing or reviewing code touching the formatter pipeline:

1. **Does this add a String allocation per-token?** If yes, use `Cow<'_, str>`, `&str`, or write into a reusable buffer.
2. **Does this clone Tokens or Nodes?** Clone `NodeIndex` (a `usize`) instead. Only clone string data when mutation is needed.
3. **Does this add a `.to_string()` or `format!()` inside a loop?** Use `write!(buf, ...)` to a pre-allocated buffer.
4. **Does this use `.contains()` on a `Vec<NodeIndex>`?** For >10 elements, switch to a `HashSet` or sorted+binary_search.
5. **Does this grow the arena without bound?** Arena nodes are never freed during a single format operation — keep added nodes minimal.
6. **Am I re-normalizing strings that were already normalized?** Cache or pass through normalized forms.
7. **Have I measured?** Run `cargo bench` (criterion benchmarks in `benches/format_bench.rs`) before and after.

## Known Allocation Hotspots

### 1. Token Creation (analyzer.rs) — 2 Strings per token

```rust
// Current: allocates prefix + token text from regex captures
Token::new(token_type, prefix.to_string(), token_text.to_string(), spos, epos)
```

**Optimization paths:**
- Store `&str` slices into source buffer (requires lifetime threading through pipeline)
- Use `CompactString` / `SmolStr` for small tokens (most SQL tokens are <24 bytes)
- Intern common keywords (SELECT, FROM, WHERE — `phf` or `string_interner`)

### 2. Jinja Normalization (jinja_formatter.rs) — 5-6 allocs per Jinja token

```rust
// Current chain — each step returns a new String:
let normalized = normalize_inner_whitespace(inner);   // alloc 1
let normalized = normalize_quotes(&normalized);       // alloc 2
let normalized = add_operator_spaces(&normalized);    // alloc 3
let normalized = add_comma_spaces(&normalized);       // alloc 4
let normalized = strip_paren_spaces(&normalized);     // alloc 5
```

**Optimization paths:**
- Single-pass normalization writing into one `String` buffer
- Pass `&mut String` through the chain, mutating in place
- Cache normalized forms if same Jinja expression appears multiple times

### 3. Node Prefix/Value Computation (node_manager.rs)

```rust
// compute_prefix returns Cow<'static, str> (good!)
// But standardize_value always returns owned String
fn standardize_value(&self, token: &Token) -> String { ... }
```

**Optimization path:** Return `Cow<'_, str>` from `standardize_value` — most values don't change.

### 4. SmallVec<Token> Cloning (formatting_disabled)

```rust
// Each Node carries: formatting_disabled: SmallVec<[Token; 2]>
// When cloned, the contained Tokens (each with 2 Strings) are deep-cloned
```

**Optimization path:** Store `Vec<NodeIndex>` pointing to fmt-off/on nodes instead of cloned Tokens.

### 5. Safety Check (api.rs) — re-normalizes every token

```rust
fn normalize_token_text(text: &str, token_type: TokenType) -> String {
    // Allocates new String per token, Jinja tokens get multiple allocs
}
```

**Optimization paths:**
- Skip when `mode.fast = true` (already implemented)
- Compare tokens structurally without re-normalizing when possible
- Cache normalized forms from the formatting pass

### 6. Line Operations (splitter.rs, merger.rs)

```rust
// split_at_index: clones node slices
let new_nodes = line.nodes[head..index].to_vec();  // alloc per split
// Comment distribution uses O(n) linear search
if new_nodes.contains(&prev_idx) { ... }
```

**Optimization paths:**
- Pass slices instead of cloned Vecs where possible
- Use `HashSet<NodeIndex>` for comment distribution lookups

### 7. Indentation (line.rs)

```rust
pub fn indentation(&self, arena: &[Node]) -> String {
    " ".repeat(self.indent_size(arena))  // new String every line
}
```

**Optimization path:** Pre-compute indentation strings for common depths (0-20 spaces).

## Patterns To Follow

### DO: Arena Index References
```rust
// Good — lightweight, no allocation
struct Line {
    nodes: Vec<NodeIndex>,  // Vec of usize
}
```

### DO: SmallVec for Bounded Collections
```rust
// Already used well in the codebase:
type BracketVec = SmallVec<[NodeIndex; 8]>;    // 8 bracket depth covers 99%
type JinjaBlockVec = SmallVec<[NodeIndex; 4]>;  // 4 Jinja nesting levels
```

### DO: Cow for Computed Strings
```rust
// Good pattern (already in compute_prefix):
fn compute_prefix(...) -> Cow<'static, str> {
    if needs_change { Cow::Owned(modified) }
    else { Cow::Borrowed("") }
}
```

### DO: LazyLock for Regex Compilation
```rust
// Already done correctly:
static MAIN_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_main_rules);
```

### DO: Rayon for File-Level Parallelism
```rust
// Already done correctly in api.rs:
matching_paths.par_iter().map(|path| format_file(path, mode)).collect()
```

### AVOID: String Chains
```rust
// Bad — N allocations
let s = step1(input);      // alloc
let s = step2(&s);         // alloc
let s = step3(&s);         // alloc

// Better — single buffer
let mut buf = String::with_capacity(input.len());
step1_into(input, &mut buf);
step2_in_place(&mut buf);
step3_in_place(&mut buf);
```

### AVOID: .clone() on Tokens or Nodes
```rust
// Bad — deep copies 2+ Strings
let t = token.clone();

// Better — reference by index
let idx: NodeIndex = arena.len();
arena.push(node);
line.nodes.push(idx);
```

## Quick Reference: sqlfmt-Specific Transformations

| Current Pattern | Better Alternative | Where |
|----------------|-------------------|-------|
| `token.to_string()` per capture | `SmolStr` or borrowed `&str` | analyzer.rs lexing |
| Chain of `normalize_*` → new String each | Single-pass into `&mut String` | jinja_formatter.rs |
| `standardize_value` → owned String | `Cow<'_, str>` return | node_manager.rs |
| `SmallVec<[Token; 2]>` clone | `SmallVec<[NodeIndex; 2]>` | node.rs formatting_disabled |
| `" ".repeat(n)` per line | Static lookup table for 0..=MAX_INDENT | line.rs indentation |
| `new_nodes.contains(&idx)` O(n) | `HashSet<NodeIndex>` lookup | splitter.rs comment distribution |
| `normalize_token_text` per token | Cache from format pass or skip | api.rs safety check |
| `line.nodes[..n].to_vec()` | Pass `&[NodeIndex]` slice where possible | splitter.rs split_at_index |
| `format!()` in loops | `write!(buf, ...)` with reused buffer | comment.rs rendering |
| `Vec<Rule>` clone per format | `Arc<Vec<Rule>>` or `&'static [Rule]` | rules/mod.rs main_rules() |

## Benchmarking This Project

Existing benchmarks in `benches/format_bench.rs`:

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- format_small
cargo bench -- format_medium
cargo bench -- format_large
cargo bench -- lex_only
cargo bench -- format_no_safety

# Compare before/after a change
cargo bench -- --save-baseline before
# ... make changes ...
cargo bench -- --baseline before
```

Benchmark cases:
- **format_small** — trivial ~60 char query (measures overhead)
- **format_medium** — `104_joins.sql` (typical real query)
- **format_large** — `216_gitlab_zuora` (stress test, large real-world query)
- **lex_only** — just lexing, no formatting (isolates lexer perf)
- **format_no_safety** — formatting with `mode.fast=true` (skip safety check)

Results at `target/criterion/` — open `report/index.html` for plots.

## Profiling Workflow

See [references/profiling.md](references/profiling.md) for detailed tool setup.

Quick workflow:
1. `cargo bench` to establish baseline
2. `cargo flamegraph --bench format_bench -- --bench format_large` for flamegraph
3. Identify wide bars (allocation, formatting, regex, memcpy)
4. Make targeted change
5. `cargo bench` to confirm improvement
6. Run `cargo test` to verify correctness (especially golden tests)

## Verification: Run `make ci` After Every Change

**CRITICAL:** After every change, task, or phase, run `make ci` to ensure nothing is broken.
This runs the full CI pipeline (formatting checks, clippy, tests). Never skip this step.

```bash
make ci
```

If `make ci` fails, fix the issue before moving on to the next optimization. Performance
improvements that break correctness are worthless.

## What NOT to Optimize

- **CLI argument parsing** (`src/main.rs`) — runs once
- **Config loading** (`src/config.rs`) — runs once
- **Rule compilation** (`src/rules/`) — already cached via `LazyLock`
- **File I/O** — already parallelized with rayon
- **Error paths** — rare, clarity matters more than speed

Focus effort on the per-token and per-line code paths listed in the Hot Path Priority Map above.

## Deep Dives

- [references/profiling.md](references/profiling.md) — Profiling tools and sqlfmt-specific benchmarking
- [references/advanced-patterns.md](references/advanced-patterns.md) — String interning, arena patterns, zero-copy techniques for SQL formatters
