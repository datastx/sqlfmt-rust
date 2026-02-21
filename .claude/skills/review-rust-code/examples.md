# Review Rust Code - Extended Examples

This file contains additional detailed examples for reference. The main SKILL.md provides the core patterns; refer to this file when you need more context or advanced scenarios specific to sqlfmt-rust.

## Never Nester: Real-World SQL Formatting Examples

### Before/After: Segment processing pipeline

```rust
// BAD: 5 levels deep
fn process_segments(segments: &[Segment]) -> Result<Vec<FormattedSegment>> {
    let mut results = Vec::new();
    if !segments.is_empty() {
        for segment in segments {
            if segment.is_code() {
                match segment.get_token() {
                    Some(token) => {
                        if token.is_keyword() {
                            match format_keyword(token) {
                                Ok(formatted) => results.push(formatted),
                                Err(e) => return Err(e),
                            }
                        }
                    }
                    None => return Err(Error::MissingToken),
                }
            }
        }
    }
    Ok(results)
}

// GOOD: Max 2 levels — inversion + extraction + iterators
fn process_segments(segments: &[Segment]) -> Result<Vec<FormattedSegment>> {
    if segments.is_empty() {
        return Ok(Vec::new());
    }

    segments.iter()
        .filter(|s| s.is_code())
        .map(|s| process_code_segment(s))
        .collect()
}

fn process_code_segment(segment: &Segment) -> Result<FormattedSegment> {
    let token = segment.get_token().ok_or(Error::MissingToken)?;
    
    if token.is_keyword() {
        format_keyword(token)
    } else {
        Ok(FormattedSegment::from(token))
    }
}
```

### Before/After: Config loading with dialect fallbacks

```rust
// BAD: Nested option checks
fn load_dialect(config: &Config, env: &Environment) -> Result<Dialect> {
    if let Some(dialect_config) = &config.dialect {
        if let Some(name) = &dialect_config.name {
            Ok(Dialect::from_str(name)?)
        } else {
            if let Ok(env_dialect) = env.get("SQLFMT_DIALECT") {
                Ok(Dialect::from_str(&env_dialect)?)
            } else {
                Ok(Dialect::default())
            }
        }
    } else {
        if let Ok(env_dialect) = env.get("SQLFMT_DIALECT") {
            Ok(Dialect::from_str(&env_dialect)?)
        } else {
            Ok(Dialect::default())
        }
    }
}

// GOOD: Combinator chain with fallback
fn load_dialect(config: &Config, env: &Environment) -> Result<Dialect> {
    config.dialect
        .as_ref()
        .and_then(|d| d.name.as_ref())
        .map(|s| s.as_str())
        .or_else(|| env.get("SQLFMT_DIALECT").ok().as_deref())
        .map(Dialect::from_str)
        .transpose()?
        .ok_or(Error::NoDialect)
}
```

### Before/After: Node visitor pattern

```rust
// BAD: Complex logic inside visitor methods
impl Visitor for FormattingVisitor {
    fn visit_node(&mut self, node: &Node) -> Result<()> {
        match node.node_type() {
            NodeType::SelectStatement => {
                let select = node.as_select().unwrap();
                if let Some(columns) = select.columns() {
                    for col in columns {
                        if col.is_aggregated() {
                            if let Some(func) = col.aggregate_function() {
                                if func.name() == "COUNT" {
                                    self.count_aggregates += 1;
                                    if self.count_aggregates > 3 {
                                        return Err(Error::TooManyAggregates);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            NodeType::WhereClause => {
                // ... similarly complex
            }
            _ => {}
        }
        Ok(())
    }
}

// GOOD: Each node type delegates to a focused function
impl Visitor for FormattingVisitor {
    fn visit_node(&mut self, node: &Node) -> Result<()> {
        match node.node_type() {
            NodeType::SelectStatement => self.visit_select(node),
            NodeType::WhereClause => self.visit_where(node),
            _ => Ok(()),
        }
    }
}

impl FormattingVisitor {
    fn visit_select(&mut self, node: &Node) -> Result<()> {
        let select = node.as_select().ok_or(Error::InvalidNodeType)?;
        let Some(columns) = select.columns() else {
            return Ok(());
        };

        for col in columns.iter().filter(|c| c.is_aggregated()) {
            self.check_aggregate_function(col)?;
        }
        Ok(())
    }

    fn check_aggregate_function(&mut self, col: &Column) -> Result<()> {
        let Some(func) = col.aggregate_function() else {
            return Ok(());
        };

        if func.name() == "COUNT" {
            self.count_aggregates += 1;
            if self.count_aggregates > 3 {
                return Err(Error::TooManyAggregates);
            }
        }
        Ok(())
    }
}
```

---

## Iterator Chains vs Manual Loops

### Collecting keywords from AST

```rust
// BAD: Manual loop
fn collect_keywords(segments: &[Segment]) -> Vec<String> {
    let mut keywords = Vec::new();
    for segment in segments {
        if segment.is_keyword() {
            if let Some(token) = segment.get_token() {
                let normalized = token.raw().to_uppercase();
                if !keywords.contains(&normalized) {
                    keywords.push(normalized);
                }
            }
        }
    }
    keywords
}

// GOOD: Iterator methods
fn collect_keywords(segments: &[Segment]) -> Vec<String> {
    segments.iter()
        .filter(|s| s.is_keyword())
        .filter_map(|s| s.get_token())
        .map(|t| t.raw().to_uppercase())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}
```

### Computing indentation statistics

```rust
// BAD: Manual accumulation
fn analyze_indentation(lines: &[Line]) -> IndentStats {
    let mut total = 0;
    let mut count = 0;
    let mut max_indent = 0;
    
    for line in lines {
        if line.has_code() {
            let indent = line.indent_level();
            total += indent;
            count += 1;
            if indent > max_indent {
                max_indent = indent;
            }
        }
    }
    
    IndentStats {
        average: if count > 0 { total / count } else { 0 },
        max: max_indent,
        count,
    }
}

// GOOD: fold for single-pass
fn analyze_indentation(lines: &[Line]) -> IndentStats {
    lines.iter()
        .filter(|l| l.has_code())
        .map(|l| l.indent_level())
        .fold(IndentStats::default(), |mut stats, indent| {
            stats.total += indent;
            stats.count += 1;
            stats.max = stats.max.max(indent);
            stats
        })
        .with_computed_average()
}
```

### Grouping segments by type

```rust
// BAD: Manual grouping
fn group_by_type(segments: &[Segment]) -> HashMap<SegmentType, Vec<&Segment>> {
    let mut groups: HashMap<SegmentType, Vec<&Segment>> = HashMap::new();
    for segment in segments {
        groups.entry(segment.segment_type()).or_default().push(segment);
    }
    groups
}

// GOOD: itertools or fold
use itertools::Itertools;

fn group_by_type(segments: &[Segment]) -> HashMap<SegmentType, Vec<&Segment>> {
    segments.iter()
        .into_group_map_by(|s| s.segment_type())
}
```

---

## Token/Node Operations (Regex Lexer Architecture)

### Extract table references from token stream

```rust
use crate::token::{Token, TokenType};

struct TableExtractor {
    tables: Vec<String>,
}

impl TableExtractor {
    fn new() -> Self {
        Self { tables: Vec::new() }
    }

    fn extract(&mut self, tokens: &[Token]) -> Vec<String> {
        for (i, token) in tokens.iter().enumerate() {
            // Look for FROM or JOIN keywords followed by table names
            if self.is_table_keyword(token) {
                if let Some(table) = self.extract_next_table(tokens, i + 1) {
                    self.tables.push(table);
                }
            }
        }
        
        self.tables.clone()
    }

    fn is_table_keyword(&self, token: &Token) -> bool {
        token.is_keyword() && 
            matches!(
                token.text.to_uppercase().as_str(),
                "FROM" | "JOIN" | "INTO" | "UPDATE"
            )
    }

    fn extract_next_table(&self, tokens: &[Token], start: usize) -> Option<String> {
        tokens.get(start..)?
            .iter()
            .skip_while(|t| t.is_whitespace())
            .find(|t| t.is_name())
            .map(|t| t.text.clone())
    }
}

// Usage: Always lex first, then extract
fn get_table_dependencies(sql: &str) -> Result<Vec<String>> {
    let tokens = lex_sql(sql)?; // Regex-based lexer creates tokens
    let mut extractor = TableExtractor::new();
    Ok(extractor.extract(&tokens))
}
```

### Transform SQL by modifying token/node structure

```rust
// Regex lexer creates tokens
use crate::analyzer::Analyzer;
use crate::token::{Token, TokenType};

struct KeywordFormatter {
    case: KeywordCase,
}

impl KeywordFormatter {
    fn format_tokens(&self, tokens: &mut [Token]) {
        for token in tokens.iter_mut() {
            if token.is_keyword() {
                self.format_keyword(token);
            }
        }
    }

    fn format_keyword(&self, token: &mut Token) {
        let formatted = match self.case {
            KeywordCase::Upper => token.text.to_uppercase(),
            KeywordCase::Lower => token.text.to_lowercase(),
            KeywordCase::Capitalize => self.capitalize(&token.text),
        };
        token.text = formatted;
    }

    fn capitalize(&self, s: &str) -> String {
        let mut chars = s.chars();
        match chars.next() {
            None => String::new(),
            Some(first) => first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
        }
    }
}

// Usage: lex → transform → render
fn format_keywords_in_sql(sql: &str, case: KeywordCase) -> Result<String> {
    let mut tokens = Analyzer::new(sql).lex()?; // Regex-based lexing
    let formatter = KeywordFormatter { case };
    formatter.format_tokens(&mut tokens);
    Ok(tokens.iter().map(|t| t.text.as_str()).collect())
}
```

### Analyzing query complexity via node traversal

```rust
use crate::node::{Node, NodeManager};

#[derive(Default)]
struct ComplexityAnalyzer {
    depth: usize,
    max_depth: usize,
    subquery_count: usize,
    join_count: usize,
}

impl ComplexityAnalyzer {
    fn analyze(&mut self, nodes: &[Node]) -> ComplexityReport {
        self.visit_nodes(nodes);
        
        ComplexityReport {
            max_nesting_depth: self.max_depth,
            subquery_count: self.subquery_count,
            join_count: self.join_count,
            complexity_score: self.calculate_score(),
        }
    }

    fn visit_nodes(&mut self, nodes: &[Node]) {
        for node in nodes {
            if node.is_bracketed() {
                self.depth += 1;
                self.max_depth = self.max_depth.max(self.depth);
                
                if self.contains_select(node) {
                    self.subquery_count += 1;
                }
                
                // Recurse into nested nodes
                if let Some(children) = node.children() {
                    self.visit_nodes(children);
                }
                
                self.depth -= 1;
            } else if node.is_keyword() && self.is_join_keyword(node) {
                self.join_count += 1;
            }
        }
    }

    fn is_join_keyword(&self, node: &Node) -> bool {
        node.token.text.to_uppercase().contains("JOIN")
    }

    fn contains_select(&self, node: &Node) -> bool {
        node.children()
            .map(|children| {
                children.iter().any(|c| 
                    c.is_keyword() && c.token.text.to_uppercase() == "SELECT"
                )
            })
            .unwrap_or(false)
    }

    fn calculate_score(&self) -> f64 {
        (self.max_depth as f64 * 2.0) +
        (self.subquery_count as f64 * 3.0) +
        (self.join_count as f64 * 1.5)
    }
}
```

---

## Error Handling in SQL Formatting Context

### Error hierarchies for parse/format operations

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("unexpected token at line {line}, column {column}: {token}")]
    UnexpectedToken {
        token: String,
        line: usize,
        column: usize,
    },

    #[error("unclosed bracket at line {line}")]
    UnclosedBracket { line: usize },

    #[error("invalid SQL syntax: {0}")]
    InvalidSyntax(String),
}

#[derive(Error, Debug)]
pub enum FormatError {
    #[error("parse error")]
    Parse(#[from] ParseError),

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

// Usage with context
fn format_file(&self, path: &Path) -> Result<String, FormatError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| FormatError::Io(e))?;
    
    let segments = parse_sql(&content)
        .map_err(FormatError::Parse)?;
    
    let formatted = self.format_segments(&segments)?;
    
    Ok(formatted)
}
```

---

## Test File Organization Examples

### Integration test with fixtures

```rust
// tests/integration_test.rs

use sqlfmt::{format_sql, Config, Dialect};
use std::fs;
use std::path::Path;

#[test]
fn test_format_simple_select() {
    let input = load_fixture("simple_select.sql");
    let expected = load_fixture("simple_select_expected.sql");
    
    let result = format_sql(&input, &Config::default()).unwrap();
    
    assert_eq!(result.trim(), expected.trim());
}

#[test]
fn test_format_complex_case() {
    let input = load_fixture("complex_case.sql");
    let config = Config {
        dialect: Dialect::Postgres,
        indent: 2,
        max_line_length: 80,
        ..Default::default()
    };
    
    let result = format_sql(&input, &config).unwrap();
    
    // Verify key properties
    assert!(result.contains("SELECT"));
    assert!(!result.contains("select")); // Keywords should be uppercase
    assert!(result.lines().all(|line| line.len() <= 80));
}

#[test]
fn test_error_on_invalid_sql() {
    let input = "SELECT FROM WHERE";
    let result = format_sql(input, &Config::default());
    
    assert!(result.is_err());
    match result {
        Err(e) => assert!(e.to_string().contains("parse error")),
        Ok(_) => panic!("Expected error"),
    }
}

fn load_fixture(name: &str) -> String {
    let path = Path::new("tests/fixtures").join(name);
    fs::read_to_string(path).unwrap()
}
```

### Table-based dialect testing

```rust
#[test]
fn test_dialect_specific_keywords() {
    let test_cases = vec![
        (Dialect::Standard, "FETCH FIRST 10 ROWS ONLY", true),
        (Dialect::Postgres, "LIMIT 10", true),
        (Dialect::Postgres, "FETCH FIRST 10 ROWS ONLY", true),
        (Dialect::Standard, "LIMIT 10", false), // LIMIT not in SQL standard
    ];

    for (dialect, keyword, should_parse) in test_cases {
        let query = format!("SELECT * FROM users {}", keyword);
        let result = parse_with_dialect(&query, dialect);
        
        assert_eq!(
            result.is_ok(),
            should_parse,
            "Dialect: {:?}, Keyword: {}, Expected: {}",
            dialect,
            keyword,
            should_parse
        );
    }
}
```

### Snapshot testing pattern

```rust
#[test]
fn test_format_snapshot() {
    let input = load_fixture("complex_query.sql");
    let result = format_sql(&input, &Config::default()).unwrap();
    
    // For manual review, write to snapshot file
    #[cfg(feature = "update-snapshots")]
    {
        std::fs::write("tests/snapshots/complex_query.txt", &result).unwrap();
    }
    
    // Compare against snapshot
    let snapshot = std::fs::read_to_string("tests/snapshots/complex_query.txt")
        .expect("Snapshot file missing");
    
    assert_eq!(result, snapshot, "Output doesn't match snapshot. Run with --features update-snapshots to update.");
}
```

---

## Table-Based Testing Patterns

### Parameterized whitespace normalization tests

```rust
#[test]
fn test_whitespace_normalization() {
    let cases = [
        ("SELECT  *", "SELECT *"),                    // Double space
        ("SELECT\t*", "SELECT *"),                     // Tab
        ("SELECT\n*", "SELECT\n*"),                    // Newline preserved
        ("  SELECT *", "SELECT *"),                    // Leading space
        ("SELECT *  ", "SELECT *"),                    // Trailing space
        ("SELECT * FROM  users", "SELECT * FROM users"), // Multiple spaces
    ];

    for (input, expected) in cases {
        let result = normalize_whitespace(input);
        assert_eq!(
            result,
            expected,
            "Failed to normalize: {:?}",
            input
        );
    }
}
```

### Keyword case conversion matrix

```rust
#[test]
fn test_keyword_case_conversion() {
    let keywords = ["select", "SELECT", "Select", "SeLeCt"];
    let expected = [
        (KeywordCase::Upper, "SELECT"),
        (KeywordCase::Lower, "select"),
        (KeywordCase::Capitalize, "Select"),
    ];

    for keyword in keywords {
        for (case_rule, expected) in &expected {
            let result = apply_case(*case_rule, keyword);
            assert_eq!(
                result,
                *expected,
                "Failed: {:?}({}) != {}",
                case_rule,
                keyword,
                expected
            );
        }
    }
}
```

---

## Type-Driven Design Patterns

### Newtype for line/column positions

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Line(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Column(usize);

#[derive(Debug, Clone, Copy)]
pub struct Position {
    line: Line,
    column: Column,
}

impl Position {
    pub fn new(line: usize, column: usize) -> Self {
        Self {
            line: Line(line),
            column: Column(column),
        }
    }

    pub fn line(&self) -> usize {
        self.line.0
    }

    pub fn column(&self) -> usize {
        self.column.0
    }
}

// This won't compile (type safety prevents bugs):
// fn set_position(pos: Position, line: Column, col: Line) { }
```

### State machine for parser

```rust
enum ParserState {
    Initial,
    InSelect { depth: usize },
    InFrom { table_count: usize },
    InWhere { condition_depth: usize },
    InJoin { join_type: JoinType, depth: usize },
    Error { message: String },
}

impl ParserState {
    fn handle_keyword(self, keyword: &str) -> Result<Self, ParseError> {
        use ParserState::*;
        
        match (self, keyword.to_uppercase().as_str()) {
            (Initial, "SELECT") => Ok(InSelect { depth: 0 }),
            (InSelect { depth }, "FROM") => Ok(InFrom { table_count: 0 }),
            (InFrom { table_count }, "WHERE") => Ok(InWhere { condition_depth: 0 }),
            (InFrom { table_count }, "JOIN") => Ok(InJoin { 
                join_type: JoinType::Inner,
                depth: 0,
            }),
            (state, keyword) => Err(ParseError::UnexpectedKeyword {
                keyword: keyword.to_string(),
                state: format!("{:?}", state),
            }),
        }
    }
}
```

---

## Performance Patterns for SQL Formatting

### Cow for conditional string transformation

```rust
use std::borrow::Cow;

fn normalize_keyword<'a>(input: &'a str, case: KeywordCase) -> Cow<'a, str> {
    match case {
        KeywordCase::Upper if input.chars().all(|c| c.is_uppercase()) => {
            Cow::Borrowed(input)
        }
        KeywordCase::Upper => Cow::Owned(input.to_uppercase()),
        KeywordCase::Lower if input.chars().all(|c| c.is_lowercase()) => {
            Cow::Borrowed(input)
        }
        KeywordCase::Lower => Cow::Owned(input.to_lowercase()),
        KeywordCase::AsIs => Cow::Borrowed(input),
    }
}
```

### String building with capacity pre-allocation

```rust
fn format_sql_optimized(segments: &[Segment]) -> String {
    // Pre-calculate approximate capacity
    let capacity = segments.iter()
        .map(|s| s.raw().len() + 1) // +1 for potential space
        .sum();
    
    let mut output = String::with_capacity(capacity);
    
    for segment in segments {
        output.push_str(segment.raw());
        if segment.needs_trailing_space() {
            output.push(' ');
        }
    }
    
    output
}
```

This examples file provides SQL-formatting-specific patterns and real-world scenarios from the sqlfmt-rust project for reference when implementing or reviewing code.
