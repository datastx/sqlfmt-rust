# Advanced Patterns for sqlfmt-rust

Optimization patterns specifically applicable to the sqlfmt-rust SQL formatter architecture.

## Table of Contents

1. [Arena Allocation Patterns](#arena-allocation-patterns)
2. [String Interning for SQL Keywords](#string-interning-for-sql-keywords)
3. [Zero-Copy Token Pipeline](#zero-copy-token-pipeline)
4. [Jinja Normalization Optimization](#jinja-normalization-optimization)
5. [SmallVec and Inline Storage](#smallvec-and-inline-storage)
6. [Reusable Buffer Patterns](#reusable-buffer-patterns)

---

## Arena Allocation Patterns

sqlfmt-rust already uses a `Vec<Node>` arena with `NodeIndex = usize` references. Here are
patterns to extend and improve this approach.

### Current architecture (good foundation)

```rust
// All nodes in one contiguous allocation
let arena: Vec<Node> = Vec::new();
// Lines reference by index, not by pointer
struct Line { nodes: Vec<NodeIndex> }
```

### Avoid arena growth during formatting

The formatter pipeline (split/merge) adds nodes to the arena (e.g., newline nodes for
multiline Jinja). These grow the arena unboundedly. Pre-reserve capacity:

```rust
// Before entering the formatter pipeline, estimate growth
let estimated_new_nodes = query.lines.len() * 2; // splits rarely double
arena.reserve(estimated_new_nodes);
```

### Use indices for formatting_disabled instead of Token clones

Current: `formatting_disabled: SmallVec<[Token; 2]>` stores cloned Tokens (expensive — each
Token has 2 Strings).

Better:
```rust
// Store indices to the fmt:off/on nodes in the arena
formatting_disabled: SmallVec<[NodeIndex; 2]>

// Access token data through the arena when needed
fn is_formatting_disabled(&self, arena: &[Node]) -> bool {
    !self.formatting_disabled.is_empty()
}

fn get_fmt_off_token<'a>(&self, arena: &'a [Node]) -> Option<&'a Token> {
    self.formatting_disabled.first().map(|&idx| &arena[idx].token)
}
```

### Sentinel nodes instead of Option<NodeIndex>

For `previous_node: Option<NodeIndex>`, consider a sentinel index (e.g., `usize::MAX`) to
avoid the Option overhead in tight loops:

```rust
const NO_NODE: NodeIndex = usize::MAX;

// Before: branches on Option
if let Some(prev) = node.previous_node { ... }

// After: simple comparison
if node.previous_node != NO_NODE { ... }
```

This saves 8 bytes per Node (no Option discriminant + padding) and avoids branch mispredictions.

---

## String Interning for SQL Keywords

SQL has ~200 reserved keywords that appear repeatedly. Currently each occurrence allocates a
new String.

### Compile-time keyword map with phf

```rust
use phf::phf_map;

static SQL_KEYWORDS: phf::Map<&'static str, &'static str> = phf_map! {
    "select" => "SELECT",
    "from" => "FROM",
    "where" => "WHERE",
    "join" => "JOIN",
    "inner" => "INNER",
    "left" => "LEFT",
    "right" => "RIGHT",
    "outer" => "OUTER",
    "on" => "ON",
    "and" => "AND",
    "or" => "OR",
    "not" => "NOT",
    "in" => "IN",
    "as" => "AS",
    "order" => "ORDER",
    "by" => "BY",
    "group" => "GROUP",
    "having" => "HAVING",
    "limit" => "LIMIT",
    "offset" => "OFFSET",
    "union" => "UNION",
    "insert" => "INSERT",
    "update" => "UPDATE",
    "delete" => "DELETE",
    "create" => "CREATE",
    "alter" => "ALTER",
    "drop" => "DROP",
    "table" => "TABLE",
    "index" => "INDEX",
    "into" => "INTO",
    "values" => "VALUES",
    "set" => "SET",
    "null" => "NULL",
    "true" => "TRUE",
    "false" => "FALSE",
    "case" => "CASE",
    "when" => "WHEN",
    "then" => "THEN",
    "else" => "ELSE",
    "end" => "END",
    "exists" => "EXISTS",
    "between" => "BETWEEN",
    "like" => "LIKE",
    "is" => "IS",
    "distinct" => "DISTINCT",
    "with" => "WITH",
    // ... add remaining keywords
};

fn standardize_keyword(token_text: &str) -> Cow<'static, str> {
    let lower = token_text.to_ascii_lowercase();
    match SQL_KEYWORDS.get(lower.as_str()) {
        Some(&canonical) => Cow::Borrowed(canonical),  // zero alloc — static str
        None => Cow::Owned(token_text.to_uppercase()), // rare — only non-standard keywords
    }
}
```

### Runtime interning for identifiers

For table/column names that repeat within a query:

```rust
use string_interner::{StringInterner, DefaultSymbol};

struct FormatContext {
    interner: StringInterner,
    // ... other fields
}

impl FormatContext {
    fn intern(&mut self, s: &str) -> DefaultSymbol {
        self.interner.get_or_intern(s)  // alloc only on first occurrence
    }

    fn resolve(&self, sym: DefaultSymbol) -> &str {
        self.interner.resolve(sym).unwrap()
    }
}
```

### SmolStr for small tokens

Most SQL tokens fit in 23 bytes (keywords, operators, short identifiers). `SmolStr` stores
these inline without heap allocation:

```rust
use smol_str::SmolStr;

struct Token {
    token_type: TokenType,
    prefix: SmolStr,     // whitespace — almost always < 23 bytes
    token: SmolStr,      // token text — keywords/operators always < 23 bytes
    spos: Pos,
    epos: Pos,
}
// SmolStr: 24 bytes on stack, no heap alloc for strings ≤ 23 bytes
// String:  24 bytes on stack + heap alloc for ANY non-empty string
```

---

## Zero-Copy Token Pipeline

The ideal: lex into `&str` slices referencing the original SQL source, avoiding per-token
String allocations entirely.

### Lifetime-threaded approach

```rust
struct Token<'src> {
    token_type: TokenType,
    prefix: &'src str,      // slice into source
    token: &'src str,       // slice into source
    spos: Pos,
    epos: Pos,
}

struct Node<'src> {
    token: Token<'src>,
    prefix: Cow<'src, str>,    // computed — Borrowed when same as token prefix
    value: Cow<'src, str>,     // computed — Borrowed when no normalization needed
    // ...
}

struct Analyzer<'src> {
    source: &'src str,
    arena: Vec<Node<'src>>,
    // ...
}
```

**Trade-off:** This requires threading `'src` through the entire pipeline (Analyzer, Query,
Line, Formatter). It's a significant refactor but eliminates the #1 allocation hotspot
(2 Strings per token × thousands of tokens).

### Incremental approach: Cow in Node

A smaller step — keep Token as-is but use `Cow` in Node where prefix/value often equal
the token's original text:

```rust
struct Node {
    token: Token,
    prefix: Cow<'static, str>,  // already done — returns "" or " " etc.
    value: Cow<'static, str>,   // TODO: return Borrowed when value == token.token
    // ...
}

fn standardize_value(token: &Token) -> Cow<'static, str> {
    match token.token_type {
        // Keywords: look up in static map (borrowed)
        TokenType::ReservedKeyword => {
            match SQL_KEYWORDS.get(token.token.to_ascii_lowercase().as_str()) {
                Some(&canonical) => Cow::Borrowed(canonical),
                None => Cow::Owned(token.token.to_uppercase()),
            }
        }
        // Most tokens: value == token text (no change needed)
        _ => Cow::Borrowed(&token.token), // needs lifetime adjustment
    }
}
```

---

## Jinja Normalization Optimization

`jinja_formatter.rs` is the most allocation-heavy file relative to its frequency of execution.
Each Jinja token goes through 5-6 normalization passes, each creating a new String.

### Single-pass normalization

Replace the chain of `normalize_*` → new String with a single scan:

```rust
fn normalize_jinja(input: &str, buf: &mut String) {
    buf.clear();
    buf.reserve(input.len());

    let inner = input.trim();
    let bytes = inner.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    let mut quote_char: u8 = 0;

    while i < bytes.len() {
        let b = bytes[i];

        // Handle string literals (preserve internal whitespace)
        if in_string {
            if b == quote_char && (i == 0 || bytes[i - 1] != b'\\') {
                in_string = false;
            }
            buf.push(b as char);
            i += 1;
            continue;
        }

        if b == b'"' || b == b'\'' {
            in_string = true;
            quote_char = b;
            // Optionally normalize to preferred quote style here
            buf.push(b as char);
            i += 1;
            continue;
        }

        // Collapse whitespace
        if b.is_ascii_whitespace() {
            // Skip consecutive whitespace
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            buf.push(' ');
            continue;
        }

        // Handle operators: add spaces around =, !=, >=, etc.
        // Handle commas: ensure space after comma
        // Handle parens: strip internal padding
        // ... all in one pass

        buf.push(b as char);
        i += 1;
    }
}
```

One allocation (the buffer, reused across calls) instead of 5-6.

### Reusable buffer across Jinja tokens

```rust
struct JinjaFormatter {
    buf: String,  // reused across normalize calls
}

impl JinjaFormatter {
    fn normalize_expression(&mut self, value: &str) -> &str {
        normalize_jinja(value, &mut self.buf);
        &self.buf
    }
}
```

---

## SmallVec and Inline Storage

sqlfmt-rust already uses SmallVec effectively. Here are refinements:

### Current usage (good)

```rust
type BracketVec = SmallVec<[NodeIndex; 8]>;     // 8 covers 99% of bracket depth
type JinjaBlockVec = SmallVec<[NodeIndex; 4]>;   // 4 covers typical Jinja nesting
type FmtDisabledVec = SmallVec<[Token; 2]>;      // Problem: Token is large
```

### Fix: FmtDisabledVec with indices

```rust
// Token is ~56 bytes (2 Strings + enum + 2 usizes)
// SmallVec<[Token; 2]> = ~112 bytes inline, plus heap alloc for Token Strings
// NodeIndex is 8 bytes
// SmallVec<[NodeIndex; 2]> = ~16 bytes inline, no additional heap alloc
type FmtDisabledVec = SmallVec<[NodeIndex; 2]>;  // 7x smaller, no String clones
```

### Right-size Vec allocations

For `Line.nodes`, most lines have 1-20 nodes. Consider:

```rust
// Instead of Vec<NodeIndex> (24 bytes + heap alloc)
// For most lines, 16 inline slots covers everything:
type NodeVec = SmallVec<[NodeIndex; 16]>;

struct Line {
    nodes: NodeVec,  // inline for typical lines, spills to heap for very long lines
}
```

Measure first — if most lines are <16 nodes, this eliminates one heap alloc per Line.

---

## Reusable Buffer Patterns

Several formatter stages repeatedly allocate and discard temporary Vecs and Strings.

### Formatter with reusable scratch space

```rust
struct QueryFormatter {
    // Scratch buffers — cleared and reused per format call
    new_lines: Vec<Line>,
    merged_lines: Vec<Line>,
    segments: Vec<Segment>,
    render_buf: String,
}

impl QueryFormatter {
    fn format(&mut self, query: &mut Query, arena: &mut Vec<Node>) {
        // Stage 1: split
        self.new_lines.clear();
        for line in &query.lines {
            self.new_lines.extend(self.splitter.maybe_split(line, arena));
        }
        std::mem::swap(&mut query.lines, &mut self.new_lines);

        // Stage 4: merge
        self.merged_lines.clear();
        self.segments.clear();
        // ... reuse buffers instead of allocating new ones
    }

    fn render(&mut self, query: &Query, arena: &[Node]) -> &str {
        self.render_buf.clear();
        // write into self.render_buf
        &self.render_buf
    }
}
```

### Indentation cache

```rust
const MAX_CACHED_INDENT: usize = 40;  // 10 indent levels × 4 spaces

static INDENT_CACHE: LazyLock<Vec<String>> = LazyLock::new(|| {
    (0..=MAX_CACHED_INDENT).map(|n| " ".repeat(n)).collect()
});

fn indentation(depth: usize) -> &'static str {
    if depth <= MAX_CACHED_INDENT {
        &INDENT_CACHE[depth]
    } else {
        // Fallback for deeply nested (rare)
        // Caller should use a buffer for this case
        panic!("indent depth {} exceeds cache", depth);
    }
}
```

### Comment rendering buffer

```rust
// Instead of building a new String per comment:
fn render_standalone(&self, prefix: &str, max_len: usize, buf: &mut String) {
    buf.clear();
    for line in &self.cleaned_lines {
        buf.push_str(prefix);
        buf.push_str(line);
        buf.push_str(" \n");
    }
}
```
