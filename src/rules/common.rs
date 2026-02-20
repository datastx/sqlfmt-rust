/// Common regex pattern building blocks used across rule definitions.
/// These mirror the Python sqlfmt `group()` and `any()` pattern helpers.
///
/// Join alternatives into a regex group: `(alt1|alt2|...)`.
pub fn group(alternatives: &[&str]) -> String {
    format!("({})", alternatives.join("|"))
}

/// Join alternatives without grouping.
pub fn any(alternatives: &[&str]) -> String {
    alternatives.join("|")
}

/// Build a word-boundary-terminated keyword pattern.
/// Matches `keyword` followed by a non-word char or end of string.
pub fn keyword(kw: &str) -> String {
    format!(r"({})\b", kw)
}

/// Build a multi-keyword pattern (e.g., "order by", "group by").
/// Allows flexible whitespace between the words.
pub fn multi_keyword(words: &[&str]) -> String {
    let pattern = words.join(r"\s+");
    format!(r"({})\b", pattern)
}

// ---- Bracket patterns ----

/// Opening brackets: (, [
pub const BRACKET_OPEN: &str = r"(\(|\[)";

/// Closing brackets: ), ]
pub const BRACKET_CLOSE: &str = r"(\)|\])";

// ---- String / Quoted Name patterns ----

/// Single-quoted string: 'text', with escaped quotes handled.
pub const SINGLE_QUOTED_STRING: &str = r"('(?:[^'\\]|\\.)*')";

/// Double-quoted name: "name"
pub const DOUBLE_QUOTED_NAME: &str = r#"("(?:[^"\\]|\\.)*")"#;

/// Backtick-quoted name: `name`
pub const BACKTICK_QUOTED_NAME: &str = r"(`(?:[^`\\]|\\.)*`)";

/// Dollar-quoted string (PostgreSQL): $$text$$ or $tag$text$tag$
pub const DOLLAR_QUOTED_STRING: &str = r"(\$[^$]*\$.*?\$[^$]*\$)";

// ---- Number patterns ----

/// Integer: digits, possibly with _ separators
pub const INTEGER: &str = r"(\d[\d_]*)";

/// Decimal: digits.digits
pub const DECIMAL: &str = r"(\d[\d_]*\.[\d_]*)";

/// Scientific notation: digits[.digits]e[+-]digits
pub const SCIENTIFIC: &str = r"(\d[\d_]*(?:\.[\d_]*)?[eE][+-]?\d[\d_]*)";

/// Hex literal: 0x...
pub const HEX_LITERAL: &str = r"(0[xX][0-9a-fA-F]+)";

// ---- Comment patterns ----

/// Line comment: -- or # or //
pub const LINE_COMMENT: &str = r"(--[^\n]*|#[^\n]*|//[^\n]*)";

/// Block comment: /* ... */
pub const BLOCK_COMMENT: &str = r"(/\*[\s\S]*?\*/)";

// ---- Jinja patterns ----

/// Jinja expression: {{ ... }}
pub const JINJA_EXPRESSION: &str = r"(\{\{[\s\S]*?\}\})";

/// Jinja statement: {% ... %}
pub const JINJA_STATEMENT: &str = r"(\{%[\s\S]*?%\})";

/// Jinja comment: {# ... #}
pub const JINJA_COMMENT: &str = r"(\{#[\s\S]*?#\})";

// ---- Operator patterns ----

/// Comparison operators
pub const COMPARISON_OPERATORS: &str =
    r"(<>|!=|>=|<=|=>|<=>|!~\*|!~|~\*|~|>>|<<|->|->>|#>>|#>|\|\||\*\*|[+\-*/%&|^=<>])";

/// The star character (needs special handling as it can be SELECT * or multiplication)
pub const STAR: &str = r"(\*)";

/// Double colon (PostgreSQL cast)
pub const DOUBLE_COLON: &str = r"(::)";

/// Single colon (used in various contexts)
pub const COLON: &str = r"(:)(?!:)";

// ---- Misc patterns ----

/// Dot
pub const DOT: &str = r"(\.)";

/// Comma
pub const COMMA: &str = r"(,)";

/// Semicolon
pub const SEMICOLON: &str = r"(;)";

/// Newline
pub const NEWLINE: &str = r"(\n)";

/// Any name (identifier): letters, digits, underscores, starting with letter or _
pub const NAME: &str = r"([a-zA-Z_]\w*)";

/// Whitespace that is not a newline (for skipping)
pub const NON_NEWLINE_WHITESPACE: &str = r"([^\S\n]+)";
