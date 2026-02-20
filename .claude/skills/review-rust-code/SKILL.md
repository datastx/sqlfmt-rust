# Review Rust Code - Best Practices & Standards

Apply these standards when working with Rust code in this project. These patterns leverage Rust's type system, ownership model, and trait system to make illegal states unrepresentable and enforce clean code at compile time.

## Core Principles

1. **Never nest**: Max 3 levels of indentation — if you hit 4, refactor
2. **Type-driven design**: Use the type system to make invalid states unrepresentable
3. **Parse, don't validate**: Enforce invariants at construction time
4. **No `unwrap()` outside tests**: Every `unwrap()` is an implicit assertion that something can never fail — use `?` or `.expect("reason")`
5. **No `clone()` to appease the borrow checker**: Restructure ownership, use `Cow<T>`, `Arc`, or references instead
6. **Minimal visibility**: `pub` is a code smell unless you're building a library API — default to private, then `pub(crate)`, then `pub(super)`, then `pub`
7. **Structured representations over string manipulation**: Use tokens/nodes, not direct regex replace on SQL text
8. **No inline comments**: Code should be self-explanatory — if it needs a comment, the code needs rewriting
9. **Prefer iterators over manual loops**: `.iter().filter().map().collect()` over `for` loops with mutable accumulators
10. **Clippy is not optional**: `cargo clippy` is mandatory in CI and local development
11. **Separate test files**: Tests go in `tests/` directory for integration tests

---

## The Never Nester's Rules

**The hard constraint: max 3 levels of indentation.** If you hit 4, refactor. Two fundamental techniques plus Rust-specific superpowers.

### Technique 1: Inversion (Early Returns / Guard Clauses)

Check the unhappy case first, bail out, let the happy path flow downward. Rust gives you three distinct tools for this.

#### The `?` Operator — Inversion for free

The single most powerful de-nesting tool in Rust. It replaces entire `match` trees with a single character:

```rust
// BAD: 3 levels of nesting
fn process_sql(sql: &str) -> Result<String, Error> {
    match parse_sql(sql) {
        Ok(ast) => {
            match format_ast(&ast) {
                Ok(formatted) => {
                    match validate(&formatted) {
                        Ok(result) => Ok(result),
                        Err(e) => Err(e.into()),
                    }
                }
                Err(e) => Err(e.into()),
            }
        }
        Err(e) => Err(e.into()),
    }
}

// GOOD: Zero nesting — each ? is an implicit early return on the error path
fn process_sql(sql: &str) -> Result<String, Error> {
    let ast = parse_sql(sql)?;
    let formatted = format_ast(&ast)?;
    let result = validate(&formatted)?;
    Ok(result)
}
```

Set up proper `From` impls on your error types so `?` chains cleanly. This alone eliminates most nesting in Rust.

#### `let-else` — Guard clauses for pattern matching

When you need to unwrap an `Option` or destructure an enum but bail on the unhappy case:

```rust
// BAD: Nested if-let pyramid
fn get_segment_text(segment: &Segment) -> Result<String, Error> {
    if let Some(token) = segment.get_token() {
        if let Some(raw) = token.raw() {
            if let Some(text) = raw.normalized() {
                Ok(text)
            } else {
                Err(Error::MissingText)
            }
        } else {
            Err(Error::NoRaw)
        }
    } else {
        Err(Error::NotFound)
    }
}

// GOOD: Flat guard clauses — declare requirements up front, then do the real work
fn get_segment_text(segment: &Segment) -> Result<String, Error> {
    let Some(token) = segment.get_token() else {
        return Err(Error::NotFound);
    };
    let Some(raw) = token.raw() else {
        return Err(Error::NoRaw);
    };
    let Some(text) = raw.normalized() else {
        return Err(Error::MissingText);
    };
    Ok(text)
}
```

#### Classic early returns with boolean guards

```rust
fn format_segments(segments: &[Segment]) -> Result<String, Error> {
    if segments.is_empty() {
        return Err(Error::EmptyInput);
    }
    if !segments.iter().all(|s| s.is_valid()) {
        return Err(Error::InvalidSegments);
    }
    process_segments(segments)
}
```

### Technique 2: Extraction (Pull blocks into named functions)

When your loop body or match arm gets complex, extract it:

```rust
// BAD: Deeply nested match-in-a-loop
fn format_nodes(&mut self) {
    for node in &mut self.nodes {
        match node.segment_type() {
            SegmentType::Keyword => {
                match self.config.keyword_case {
                    KeywordCase::Upper => { /* ... */ }
                    KeywordCase::Lower => { /* ... */ }
                }
            }
            SegmentType::Whitespace => {
                match node.context() {
                    Context::Indent => { /* ... */ }
                    Context::Newline => { /* ... */ }
                }
            }
        }
    }
}

// GOOD: format_nodes() is a table of contents
fn format_nodes(&mut self) {
    for node in &mut self.nodes {
        match node.segment_type() {
            SegmentType::Keyword => self.format_keyword(node),
            SegmentType::Whitespace => self.format_whitespace(node),
            _ => {}
        }
    }
}
```

### Rust-Specific De-Nesting Techniques

#### Combinator chains on `Option`/`Result`

Instead of nested `if let`, chain transformations:

```rust
// BAD: Nested
fn get_indent_level(config: &Config) -> usize {
    if let Some(fmt) = &config.format {
        if let Some(indent) = fmt.indent {
            indent
        } else {
            4
        }
    } else {
        4
    }
}

// GOOD: Flat with combinators
fn get_indent_level(config: &Config) -> usize {
    config.format
        .as_ref()
        .and_then(|f| f.indent)
        .unwrap_or(4)
}
```

#### `match` as an expression (not a statement)

Assign directly from `match` to avoid nesting:

```rust
let action = match segment.segment_type() {
    SegmentType::Newline => Action::Preserve,
    SegmentType::Whitespace if segment.is_indent() => Action::Reformat,
    SegmentType::Whitespace => Action::Remove,
    _ => Action::Keep,
};
```

#### State machine enums to eliminate conditional nesting

If you find yourself with nested conditions checking combinations of booleans, encode valid states in an enum:

```rust
// GOOD: Eliminates a whole class of nested if/else
enum FormatterState {
    Initial,
    InSelect { depth: usize },
    InWhere { depth: usize, clause_count: usize },
    InJoin { join_type: JoinType },
}

// BAD: Bag of booleans where half the combinations are nonsensical
struct FormatterState {
    in_select: bool,
    in_where: bool,
    in_join: bool,
    depth: Option<usize>,
    clause_count: Option<usize>,
    join_type: Option<JoinType>,
}
```

### Never Nester Summary

1. **Max 3 levels of indentation.** Period.
2. **Use `?` aggressively.** Set up `From` impls so `?` chains cleanly.
3. **Use `let-else` for guard clauses** on `Option`/`Result`/pattern matches.
4. **Early return for boolean guards** — check the bad case, bail, keep the happy path flowing down.
5. **Extract match arms and loop bodies** into named methods when they grow beyond a few lines.
6. **Use combinators** (`.map`, `.and_then`, `.unwrap_or_else`) to flatten `Option`/`Result` transformations.
7. **Encode state in enums**, not in nested conditionals over booleans.

---

## Rust Discipline

These are community conventions enforced as hard rules in this project. The compiler doesn't require any of them — the discipline does.

### No `unwrap()` outside tests

Every `unwrap()` is an implicit assertion that something can never fail. If you're wrong, you get a panic with a useless message instead of a recoverable error.

```rust
// BAD: unwrap in production code
let segment = segments.first().unwrap();

// ACCEPTABLE: expect() with a reason (grudging compromise)
let segment = segments.first().expect("segments must not be empty after validation");

// GOOD: The true path
let segment = segments.first().ok_or(Error::EmptySegments)?;

// OK: unwrap in tests
#[test]
fn test_something() {
    let result = format_sql("SELECT 1").unwrap();
    assert_eq!(result, expected);
}
```

### No `clone()` to appease the borrow checker

When fighting lifetimes, don't sprinkle `.clone()` everywhere to make it compile. Restructure so data flows naturally.

```rust
// BAD: Cloning to satisfy the borrow checker
fn format_segment(segment: &Segment) -> Result<String, Error> {
    let raw = segment.raw().clone();
    let result = transform(&raw);
    use_result(result, &segment.raw().clone());
}

// GOOD: Restructure ownership or use references
fn format_segment(segment: &Segment) -> Result<String, Error> {
    let raw = segment.raw();
    let result = transform(raw);
    use_result(result, raw);
}

// ACCEPTABLE: When clone is the genuinely pragmatic choice (and you know it)
// e.g., small Copy types, Arc::clone for shared ownership, one-time setup
let config = Arc::clone(&shared_config);
```

### Prefer iterators over manual loops

Iterator chains are more idiomatic, composable, and less error-prone. The compiler generates identical code.

```rust
// BAD: Manual loop with mutable accumulator
fn get_keyword_segments(segments: &[Segment]) -> Vec<String> {
    let mut keywords = Vec::new();
    for segment in segments {
        if segment.is_keyword() {
            keywords.push(segment.raw().to_string());
        }
    }
    keywords
}

// GOOD: Iterator chain
fn get_keyword_segments(segments: &[Segment]) -> Vec<String> {
    segments.iter()
        .filter(|s| s.is_keyword())
        .map(|s| s.raw().to_string())
        .collect()
}
```

### No `Rc<RefCell<T>>` without genuine reason

`Rc<RefCell<T>>` opts out of Rust's compile-time borrow checking. It has legitimate uses (tree structures, graphs like AST nodes) but reaching for it in application code means your architecture needs rethinking.

### No async in library code unless you actually need it

This project is a formatter/CLI tool that processes text synchronously. Making functions `async` without genuine I/O needs adds complexity without benefit.

### Implement `Display` for error types, not just `Debug`

Errors should produce human-readable messages. Use `thiserror` for automatic `Display` impls:

```rust
// GOOD: thiserror gives you Display for free
#[derive(Error, Debug)]
pub enum FormatError {
    #[error("failed to parse SQL: {0}")]
    ParseError(String),

    #[error("invalid segment at position {position}")]
    InvalidSegment { position: usize },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// BAD: Only deriving Debug — errors print as FormatError { kind: ParseError, ... }
#[derive(Debug)]
pub struct FormatError {
    kind: ErrorKind,
    context: Option<String>,
}
```

### Clippy is mandatory

`cargo clippy` is not optional. Run it locally, enforce it in CI. Treat clippy warnings as errors:

```bash
cargo clippy -- -D warnings
```

---

## Comments and Inline Code

**Project standard: No inline comments.** Write self-explanatory code. If the code needs a comment to be understood, the code needs to be rewritten with better names, smaller functions, or clearer structure.

### What is never acceptable

```rust
// BAD: Inline comments that narrate the code
fn format_sql(query: &str) -> Result<String> {
    // Parse the SQL
    let ast = parse(query)?;

    // Format the AST
    let formatted = format_ast(&ast)?;

    // Return the result
    Ok(formatted)
}

// BAD: Comments compensating for unclear code
fn calc(s: &Segment, c: &Config) -> usize {
    // Calculate the indent level based on depth and config
    let x = if s.d < 3 { 2 } else { 4 };
    let y = if c.i == IndentType::Tabs { 8 } else { 4 };
    x * y
}

// Fix: Make the code clear, delete the comment
fn calculate_indent_width(segment: &Segment, config: &Config) -> usize {
    let depth_multiplier = if segment.depth < 3 { 2 } else { 4 };
    let base_width = if config.indent_type == IndentType::Tabs { 8 } else { 4 };
    depth_multiplier * base_width
}

// BAD: Commented-out code (use version control)
pub fn format_query(sql: &str) -> Result<String> {
    // let ast = old_parser::parse(sql)?;
    // ast.format_with_legacy()
    parse_and_format(sql)
}
```

### What is acceptable (sparingly)

```rust
// OK: Explains WHY — non-obvious business logic or external constraints
fn format_whitespace(segment: &Segment) -> Result<String> {
    // SQL standard requires at least one space between keywords
    // See: ISO/IEC 9075-1:2016 Section 5.2
    if segment.is_keyword_boundary() {
        return Ok(" ".to_string());
    }
    Ok(String::new())
}

// OK: Non-obvious business rule
pub fn can_merge_segments(left: &Segment, right: &Segment) -> bool {
    // Per SQL grammar, adjacent string literals are implicitly concatenated
    left.is_string_literal() && right.is_string_literal()
}

// OK: TODO with ticket reference
pub fn format_dialect_specific(sql: &str, dialect: Dialect) -> Result<String> {
    // TODO(#42): Add BigQuery dialect support
    match dialect {
        Dialect::Standard => format_standard(sql),
        Dialect::Postgres => format_postgres(sql),
        _ => Err(Error::UnsupportedDialect),
    }
}
```

### The rule of thumb

If you're tempted to write a comment, first try:
1. Rename the variable/function to be self-describing
2. Extract a helper function whose name explains the operation
3. Use a newtype or enum to encode the meaning in the type system

If none of those work and the "why" still isn't obvious, then a comment is acceptable.

### Doc comments for public APIs

Public items get `///` doc comments. These are documentation, not inline comments — they describe the contract, not the implementation:

```rust
/// Formats SQL query text according to the specified configuration.
///
/// # Errors
///
/// Returns `FormatError::ParseError` if the SQL cannot be parsed.
/// Returns `FormatError::InvalidSegment` if the AST contains invalid nodes.
pub fn format_sql(sql: &str, config: &Config) -> Result<String, FormatError> {
    // ...
}
```

---

## Test File Organization

**Project standard: Integration tests go in `tests/` directory** at the crate root (standard Rust convention).

### Integration tests structure

```
sqlfmt-rust/
  src/
    lib.rs
    formatter.rs
    parser.rs
  tests/
    integration_test.rs
    cli_test.rs
    fixtures/
      simple_select.sql
      complex_case.sql
```

### Why separate test files

- Production files stay focused on production code
- Tests are easy to find — look in `tests/`
- Diffs are cleaner — test changes don't pollute production file history
- Files stay shorter and more navigable

---

## Early Returns and Guard Clauses

(See **The Never Nester's Rules** above for the full treatment with examples.)

### Prefer `ok_or_else` over `ok_or`

Use lazy evaluation to avoid unnecessary error construction:

```rust
// GOOD: Lazy evaluation
get_segment().ok_or_else(|| Error::NotFound("Segment not found".to_string()))?

// BAD: Eager evaluation (constructs error even on success path)
get_segment().ok_or(Error::NotFound("Segment not found".to_string()))?
```

---

## Type-Driven Design

### Newtype pattern for domain types

Wrap primitive types to prevent entire categories of bugs. Zero runtime cost.

```rust
// GOOD: Distinct types prevent mixing
struct LineNumber(usize);
struct ColumnNumber(usize);
struct IndentLevel(usize);

fn set_indent(line: LineNumber, level: IndentLevel) { /* ... */ }

// This won't compile (type safety):
// set_indent(IndentLevel(4), LineNumber(10));
```

### Parse, don't validate

Instead of accepting a `String` and checking if it's valid at every call site, create a validated type whose constructor validates once:

```rust
pub struct ValidSql(String);

impl ValidSql {
    pub fn parse(s: String) -> Result<Self, ParseError> {
        // Validate SQL syntax once
        if is_valid_sql(&s) {
            Ok(ValidSql(s))
        } else {
            Err(ParseError::InvalidSyntax)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// If a ValidSql exists, it's guaranteed to be parseable — no re-validation needed
```

### Make invalid states unrepresentable

Instead of a struct with multiple booleans where half the combinations are nonsensical, use an enum:

```rust
// GOOD: Each state carries exactly the data it needs
enum FormatterMode {
    Pretty { indent: usize, max_line_length: usize },
    Compact,
    Debug { show_whitespace: bool },
}

// BAD: Boolean flags and nullable fields
struct FormatterMode {
    is_pretty: bool,
    is_compact: bool,
    is_debug: bool,
    indent: Option<usize>,
    max_line_length: Option<usize>,
    show_whitespace: Option<bool>,
}
```

### Typestate pattern for builders

Use the type system to enforce required fields at compile time:

```rust
struct ConfigBuilder<State> {
    dialect: Option<Dialect>,
    indent: Option<usize>,
    _state: PhantomData<State>,
}

struct NoDialect;
struct HasDialect;

impl ConfigBuilder<NoDialect> {
    fn dialect(self, dialect: Dialect) -> ConfigBuilder<HasDialect> {
        ConfigBuilder {
            dialect: Some(dialect),
            indent: self.indent,
            _state: PhantomData,
        }
    }
}

impl ConfigBuilder<HasDialect> {
    fn build(self) -> Config {
        Config {
            dialect: self.dialect.unwrap(),
            indent: self.indent.unwrap_or(4),
        }
    }
}
```

---

## Error Handling

### Use `thiserror` for this library

```rust
// Library code with thiserror (gives you Display for free)
#[derive(Error, Debug)]
pub enum FormatError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("invalid segment at line {line}, column {column}")]
    InvalidSegment { line: usize, column: usize },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
```

### Always include error context

```rust
#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to format SQL query")]
    Format {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("configuration error: {0}")]
    Config(String),
}
```

---

## SOLID Principles Through Traits

### Single Responsibility

Small, focused modules with visibility controls:

```rust
pub(crate) mod formatter {
    use super::Segment;

    pub(crate) fn format_segment(segment: &Segment) -> Result<String, Error> {
        // Single responsibility: segment formatting
    }
}
```

### Open/Closed — Extend via trait implementations

```rust
trait Formatter {
    fn format(&self, input: &str) -> Result<String>;
}

fn format_all(queries: &[String], formatter: &impl Formatter) -> Result<Vec<String>> {
    queries.iter().map(|q| formatter.format(q)).collect()
}
```

### Interface Segregation — Small, focused traits

```rust
// GOOD: Types implement only what they need
trait Parser {
    fn parse(&self, sql: &str) -> Result<Ast>;
}

trait Formatter {
    fn format(&self, ast: &Ast) -> Result<String>;
}

// BAD: Monolithic trait
trait SqlProcessor {
    fn parse(&self, sql: &str) -> Result<Ast>;
    fn format(&self, ast: &Ast) -> Result<String>;
    fn validate(&self, ast: &Ast) -> Result<()>;
    fn optimize(&self, ast: &Ast) -> Ast;
}
```

### Dependency Inversion — Trait-based DI

```rust
trait ConfigLoader {
    fn load(&self) -> Result<Config>;
}

struct Formatter<L: ConfigLoader> {
    loader: L,
}

impl<L: ConfigLoader> Formatter<L> {
    fn format_with_config(&self, sql: &str) -> Result<String> {
        let config = self.loader.load()?;
        format_sql(sql, &config)
    }
}
```

Decision matrix:
- **Generics** (`impl Trait`, `<T: Trait>`): compile-time DI, zero cost
- **Trait objects** (`Box<dyn Trait + Send + Sync>`): runtime polymorphism, heterogeneous collections

---

## Trait Design

### Static vs. dynamic dispatch

```rust
// Static dispatch: zero cost, but increases binary size
fn format_segments_static(segments: &[Segment], formatter: &impl Formatter) -> Result<String> {
    segments.iter().map(|s| formatter.format(s)).collect()
}

// Dynamic dispatch: runtime polymorphism, vtable overhead
fn format_segments_dynamic(segments: &[Segment], formatter: &dyn Formatter) -> Result<String> {
    segments.iter().map(|s| formatter.format(s)).collect()
}
```

### Associated types vs. generic parameters

```rust
// Associated types: one logical implementation per type
trait Parser {
    type Output;
    fn parse(&self, input: &str) -> Result<Self::Output>;
}

// Generic parameters: multiple implementations make sense
trait From<T> {
    fn from(value: T) -> Self;
}
```

### Sealed traits

```rust
mod sealed {
    pub trait Sealed {}
}

pub trait Formatter: sealed::Sealed {
    fn format(&self, sql: &str) -> Result<String>;
}

impl sealed::Sealed for StandardFormatter {}
impl Formatter for StandardFormatter {
    fn format(&self, sql: &str) -> Result<String> { /* ... */ }
}
```

---

## Ownership Patterns

### Use `Cow` for flexible borrowing

```rust
use std::borrow::Cow;

fn normalize_whitespace(input: &str) -> Cow<str> {
    if input.contains("  ") {
        Cow::Owned(input.replace("  ", " "))
    } else {
        Cow::Borrowed(input)
    }
}
```

### Multi-threaded patterns (if needed)

```rust
// Arc<T> for immutable shared data
let shared_config = Arc::new(Config::load());

// Arc<RwLock<T>> for read-heavy mutable data
let cache = Arc::new(RwLock::new(HashMap::new()));

// Keep critical sections minimal
{
    let mut data = cache.write().unwrap();
    data.insert(key, value);
}
```

---

## Module Organization

### Visibility hierarchy

Default to private, expose deliberately:

```rust
mod internal {
    pub(crate) struct Helper;

    impl Helper {
        pub(super) fn assist(&self) { /* ... */ }
        fn private_method(&self) { /* ... */ }
    }
}

// lib.rs: Curated public API
pub use crate::internal::Helper;
```

### Prelude pattern

```rust
pub mod prelude {
    pub use crate::{Error, Result};
    pub use crate::formatter::{format_sql, Config};
    pub use crate::dialect::Dialect;
}
```

---

## Lexer-Based Structured Representation

**Project-specific standard**: sqlfmt-rust uses a **regex-based lexer** to create structured tokens and nodes. This is intentional and performant.

### Architecture: Regex Lexer → Tokens/Nodes → Operations

```rust
// GOOD: Regex-based lexer creates structured tokens
fn tokenize(sql: &str) -> Result<Vec<Token>> {
    let mut analyzer = Analyzer::new(sql);
    analyzer.lex() // Uses regex rules to create Token objects
}

// GOOD: Work with structured tokens/nodes
fn extract_table_names(tokens: &[Token]) -> Vec<String> {
    tokens.iter()
        .filter(|t| t.is_keyword() && matches!(t.text.as_str(), "FROM" | "JOIN"))
        .filter_map(|_| tokens.iter().skip_while(|t| t.is_whitespace()).next())
        .filter(|t| t.is_name())
        .map(|t| t.text.clone())
        .collect()
}

// BAD: Direct string manipulation with regex on SQL text
fn format_keywords_bad(sql: &str) -> String {
    let re = Regex::new(r"\b(select|from|where)\b").unwrap();
    re.replace_all(sql, |caps: &regex::Captures| {
        caps[1].to_uppercase()
    }).to_string()
    // Breaks on keywords in strings, comments, identifiers
    // Can't handle context or nested structures
}

// GOOD: Format using structured representation
fn format_keywords(nodes: &mut [Node]) {
    for node in nodes.iter_mut() {
        if node.is_keyword() {
            node.set_text(node.text().to_uppercase());
        }
    }
}
```

### When Regex is Appropriate in sqlfmt-rust

✅ **Lexing rules** in `analyzer.rs` and `rules/` modules:
```rust
// Perfectly fine: regex for lexing
Rule::new(
    r"^(?i)\bSELECT\b",
    TokenType::Keyword,
    Some(Action::HandleKeyword),
)
```

✅ **Simple pattern matching** that doesn't require context:
```rust
// OK: Extract Jinja template variables
let re = Regex::new(r"\{\{\s*([\w.]+)\s*\}\}").unwrap();
```

❌ **Direct SQL text transformation** without tokenizing first:
```rust
// BAD: Don't do string operations on raw SQL
let formatted = sql.replace("select", "SELECT"); // Too naive, breaks in strings/comments
```

### Key Principle

**Always lex first, then operate on tokens/nodes.** Never manipulate SQL as raw strings.

---

## Testing

### Integration test structure

Tests go in the `tests/` directory:

```rust
// tests/integration_test.rs
use sqlfmt::{format_sql, Config};

#[test]
fn test_format_simple_select() {
    let input = "select * from users";
    let expected = "SELECT *\nFROM users";
    let result = format_sql(input, &Config::default()).unwrap();
    assert_eq!(result, expected);
}
```

### Table-based testing

Use table-based testing to avoid duplicating test logic:

```rust
#[test]
fn test_keyword_capitalization() {
    let cases = [
        ("select", "SELECT"),
        ("from", "FROM"),
        ("where", "WHERE"),
        ("join", "JOIN"),
    ];

    for (input, expected) in cases {
        let result = capitalize_keyword(input);
        assert_eq!(result, expected, "Failed for input: {}", input);
    }
}
```

### Loading test fixtures

```rust
#[test]
fn test_complex_query() {
    let input = std::fs::read_to_string("tests/fixtures/complex_case.sql").unwrap();
    let expected = std::fs::read_to_string("tests/fixtures/complex_case_expected.sql").unwrap();
    
    let result = format_sql(&input, &Config::default()).unwrap();
    assert_eq!(result.trim(), expected.trim());
}
```

---

## Code Review Checklist

When reviewing Rust code, check:

- **Nesting**: No more than 3 levels of indentation anywhere
- **No `unwrap()`**: Only in tests — production code uses `?` or `.expect("reason")`
- **No gratuitous `clone()`**: Ownership restructured, not cloned away
- **Type safety**: Domain types using newtypes? Invalid states unrepresentable?
- **Error handling**: `thiserror` for errors? `Display` implemented? Context provided?
- **Early returns**: Functions use `?`, `let-else`, and guard clauses instead of nesting
- **Iterator chains**: Prefer `.iter().filter().map().collect()` over manual loops
- **Visibility**: Everything private by default, `pub(crate)` where needed, `pub` only for true API
- **Structured operations**: Work with Token/Node objects, not raw SQL string manipulation
- **Regex for lexing**: Regex in lexer rules is fine; direct regex replace on SQL text is not
- **No inline comments**: Code is self-explanatory; only "why" comments survive review
- **Test separation**: Tests in `tests/` directory with fixtures
- **Clippy clean**: `cargo clippy -- -D warnings` passes
- **No `Rc<RefCell<T>>`** in application code without justification

---

## Summary

These patterns make clean code the path of least resistance in Rust:

1. **Never nest beyond 3 levels** — use `?`, `let-else`, extraction, and combinators
2. **No `unwrap()` in production** — `?` is the true path
3. **No `clone()` to fight the borrow checker** — restructure ownership
4. **Make invalid states unrepresentable** with newtypes and state-machine enums
5. **No inline comments** — write self-explanatory code or refactor until it is
6. **Tests in `tests/` directory** — keep production code clean
7. **Iterators over manual loops** — more idiomatic, composable, same performance
8. **Clippy is law** — no exceptions
9. **Visibility is minimal** — private by default, `pub` only when genuinely public API
10. **Structured tokens/nodes over raw string manipulation** — lex first, then transform

For detailed examples and supporting documentation, see [examples.md](examples.md).

---

## Verification

**After every unit of work, run local tests before moving on.** Use:
- `cargo test` — run all tests
- `cargo clippy -- -D warnings` — enforce clippy
- `cargo fmt` — format code

Do not proceed to the next task until all checks pass.

---
name: review-rust-code
description: Review and enforce Rust best practices, clean code principles, and idiomatic patterns for sqlfmt-rust SQL formatter. Use when reviewing code, writing new Rust code, or refactoring existing implementations. Covers type-driven design, error handling, AST parsing, testing, and project-specific standards.
---
