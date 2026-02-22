use memchr::{memchr, memchr2};

use crate::action::Action;
use crate::token::TokenType;

/// Lexer context states, replacing the rule_stack of Vec<&'static [Rule]>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LexState {
    Main,
    FmtOff,
    JinjaSetBlock,
    JinjaCallBlock,
    Unsupported,
    Grant,
    Function,
    Warehouse,
    Clone,
}

/// Result of a single lex step: what action to take, how far to advance,
/// and the prefix/token_text slices (byte offsets into `remaining`).
pub struct LexResult<'a> {
    pub action: &'static Action,
    pub match_len: usize,
    pub prefix: &'a str,
    pub token_text: &'a str,
}

// ---- Static action constants ----
// These replace the Box::leak'd actions from rules/mod.rs.

static A_UNTERM: Action = Action::AddNode {
    token_type: TokenType::UntermKeyword,
};
static A_RESERVED_UNTERM: Action = Action::HandleReservedKeyword { inner: &A_UNTERM };
static A_NONRESERVED_UNTERM: Action = Action::HandleNonreservedTopLevelKeyword { inner: &A_UNTERM };
static A_RESERVED_NONRESERVED_UNTERM: Action = Action::HandleReservedKeyword {
    inner: &A_NONRESERVED_UNTERM,
};

static A_NAME: Action = Action::AddNode {
    token_type: TokenType::Name,
};

static A_QUOTED_NAME: Action = Action::AddNode {
    token_type: TokenType::QuotedName,
};

static A_WORD_OP: Action = Action::AddNode {
    token_type: TokenType::WordOperator,
};
static A_RESERVED_WORD_OP: Action = Action::HandleReservedKeyword { inner: &A_WORD_OP };

static A_BOOL_OP: Action = Action::AddNode {
    token_type: TokenType::BooleanOperator,
};
static A_RESERVED_BOOL_OP: Action = Action::HandleReservedKeyword { inner: &A_BOOL_OP };

static A_ON: Action = Action::AddNode {
    token_type: TokenType::On,
};
static A_RESERVED_ON: Action = Action::HandleReservedKeyword { inner: &A_ON };

static A_OPERATOR: Action = Action::AddNode {
    token_type: TokenType::Operator,
};

static A_STAR: Action = Action::AddNode {
    token_type: TokenType::Star,
};

static A_COMMA: Action = Action::AddNode {
    token_type: TokenType::Comma,
};

static A_DOT: Action = Action::AddNode {
    token_type: TokenType::Dot,
};

static A_BRACKET_OPEN: Action = Action::AddNode {
    token_type: TokenType::BracketOpen,
};

static A_BRACKET_CLOSE: Action = Action::AddNode {
    token_type: TokenType::BracketClose,
};

static A_DOUBLE_COLON: Action = Action::AddNode {
    token_type: TokenType::DoubleColon,
};

static A_COLON: Action = Action::AddNode {
    token_type: TokenType::Colon,
};

static A_FMT_OFF: Action = Action::AddNode {
    token_type: TokenType::FmtOff,
};

static A_FMT_ON: Action = Action::AddNode {
    token_type: TokenType::FmtOn,
};

static A_DATA: Action = Action::AddNode {
    token_type: TokenType::Data,
};
static A_RESERVED_DATA: Action = Action::HandleReservedKeyword { inner: &A_DATA };

static A_NUMBER: Action = Action::HandleNumber;
static A_NEWLINE: Action = Action::HandleNewline;
static A_SEMICOLON: Action = Action::HandleSemicolon;
static A_COMMENT: Action = Action::AddComment;

static A_ANGLE_CLOSE: Action = Action::HandleClosingAngleBracket;

static A_JINJA_BLOCK_START: Action = Action::HandleJinjaBlockStart;
static A_JINJA_BLOCK_KEYWORD: Action = Action::HandleJinjaBlockKeyword;
static A_JINJA_BLOCK_END: Action = Action::HandleJinjaBlockEnd;
static A_JINJA_EXPR: Action = Action::HandleJinja {
    token_type: TokenType::JinjaExpression,
};
static A_JINJA_STMT: Action = Action::HandleJinja {
    token_type: TokenType::JinjaStatement,
};

static A_SET_OP: Action = Action::HandleSetOperator;
static A_RESERVED_SET_OP: Action = Action::HandleReservedKeyword { inner: &A_SET_OP };

static A_STATEMENT_START: Action = Action::AddNode {
    token_type: TokenType::StatementStart,
};
static A_RESERVED_STATEMENT_START: Action = Action::HandleReservedKeyword {
    inner: &A_STATEMENT_START,
};

static A_SAFE_STATEMENT_END: Action = Action::SafeAddNode {
    token_type: TokenType::StatementEnd,
    alt_token_type: TokenType::Name,
};
static A_RESERVED_STATEMENT_END: Action = Action::HandleReservedKeyword {
    inner: &A_SAFE_STATEMENT_END,
};

static A_DDL_AS: Action = Action::HandleDdlAs;
static A_RESERVED_DDL_AS: Action = Action::HandleReservedKeyword { inner: &A_DDL_AS };

static A_ANGLE_BRACKET_OPEN: Action = Action::SafeAddNode {
    token_type: TokenType::BracketOpen,
    alt_token_type: TokenType::Name,
};

static A_LEX_GRANT: Action = Action::LexRuleset {
    ruleset_name: "grant",
};
static A_NONRESERVED_GRANT: Action = Action::HandleNonreservedTopLevelKeyword {
    inner: &A_LEX_GRANT,
};
static A_RESERVED_NONRESERVED_GRANT: Action = Action::HandleReservedKeyword {
    inner: &A_NONRESERVED_GRANT,
};

static A_LEX_FUNCTION: Action = Action::LexRuleset {
    ruleset_name: "function",
};
static A_NONRESERVED_FUNCTION: Action = Action::HandleNonreservedTopLevelKeyword {
    inner: &A_LEX_FUNCTION,
};
static A_RESERVED_NONRESERVED_FUNCTION: Action = Action::HandleReservedKeyword {
    inner: &A_NONRESERVED_FUNCTION,
};

static A_LEX_WAREHOUSE: Action = Action::LexRuleset {
    ruleset_name: "warehouse",
};
static A_NONRESERVED_WAREHOUSE: Action = Action::HandleNonreservedTopLevelKeyword {
    inner: &A_LEX_WAREHOUSE,
};
static A_RESERVED_NONRESERVED_WAREHOUSE: Action = Action::HandleReservedKeyword {
    inner: &A_NONRESERVED_WAREHOUSE,
};

static A_LEX_CLONE: Action = Action::LexRuleset {
    ruleset_name: "clone",
};
static A_NONRESERVED_CLONE: Action = Action::HandleNonreservedTopLevelKeyword {
    inner: &A_LEX_CLONE,
};
static A_RESERVED_NONRESERVED_CLONE: Action = Action::HandleReservedKeyword {
    inner: &A_NONRESERVED_CLONE,
};

static A_LEX_UNSUPPORTED: Action = Action::LexRuleset {
    ruleset_name: "unsupported",
};
static A_NONRESERVED_UNSUPPORTED: Action = Action::HandleNonreservedTopLevelKeyword {
    inner: &A_LEX_UNSUPPORTED,
};
static A_RESERVED_NONRESERVED_UNSUPPORTED: Action = Action::HandleReservedKeyword {
    inner: &A_NONRESERVED_UNSUPPORTED,
};

static A_KW_BEFORE_PAREN_NAME: Action = Action::HandleKeywordBeforeParen {
    token_type: TokenType::Name,
};

static A_KW_BEFORE_PAREN_WORD_OP: Action = Action::HandleKeywordBeforeParen {
    token_type: TokenType::WordOperator,
};
static A_RESERVED_KW_BEFORE_PAREN_WORD_OP: Action = Action::HandleReservedKeyword {
    inner: &A_KW_BEFORE_PAREN_WORD_OP,
};

static A_DELETE_FROM: Action = Action::AddNode {
    token_type: TokenType::UntermKeyword,
};

// ---- Helper: skip leading non-newline whitespace ----

/// Returns the byte length of leading non-newline whitespace.
#[inline]
fn skip_prefix_whitespace(bytes: &[u8]) -> usize {
    let mut i = 0;
    while i < bytes.len() && bytes[i] != b'\n' && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// Scan an identifier (word characters: alphanumeric + underscore).
/// Returns byte length of the identifier.
#[inline]
fn scan_word(bytes: &[u8]) -> usize {
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_alphanumeric() || b == b'_' {
            i += 1;
        } else if b >= 0x80 {
            // Non-ASCII: could be unicode identifier. Accept it.
            i += 1;
        } else {
            break;
        }
    }
    i
}

/// Scan a number starting at bytes[0]. Handles:
/// - Hex (0x...), binary (0b...), octal (0o...)
/// - Decimal with optional fractional part and scientific notation
/// - Spark suffixes (L, S, K, Y, bd, d, f)
///
/// Returns byte length.
fn scan_number(bytes: &[u8]) -> usize {
    let len = bytes.len();
    if len == 0 {
        return 0;
    }

    let mut i = 0;

    // Check for 0x, 0b, 0o prefixed literals
    if bytes[0] == b'0' && len > 1 {
        match bytes[1] {
            b'x' | b'X' => {
                i = 2;
                while i < len && (bytes[i].is_ascii_hexdigit() || bytes[i] == b'_') {
                    i += 1;
                }
                return i;
            }
            b'b' | b'B' => {
                i = 2;
                while i < len && (bytes[i] == b'0' || bytes[i] == b'1' || bytes[i] == b'_') {
                    i += 1;
                }
                return i;
            }
            b'o' | b'O' => {
                i = 2;
                while i < len && ((bytes[i] >= b'0' && bytes[i] <= b'7') || bytes[i] == b'_') {
                    i += 1;
                }
                return i;
            }
            _ => {}
        }
    }

    // Integer part: digits and underscores
    while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'_') {
        i += 1;
    }

    // Decimal part
    if i < len && bytes[i] == b'.' {
        i += 1;
        while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'_') {
            i += 1;
        }
    }

    // Scientific notation
    if i < len && (bytes[i] == b'e' || bytes[i] == b'E') {
        let mut j = i + 1;
        if j < len && (bytes[j] == b'+' || bytes[j] == b'-') {
            j += 1;
        }
        if j < len && bytes[j].is_ascii_digit() {
            i = j;
            while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'_') {
                i += 1;
            }
        }
    }

    // Spark suffixes: bd, d, f, L, S, K, Y (case-insensitive single char after digits)
    if i < len {
        if i + 1 < len
            && (bytes[i] == b'b' || bytes[i] == b'B')
            && (bytes[i + 1] == b'd' || bytes[i + 1] == b'D')
        {
            i += 2;
        } else {
            match bytes[i] {
                b'd' | b'D' | b'f' | b'F' | b'l' | b'L' | b's' | b'S' | b'k' | b'K' | b'y'
                | b'Y' => {
                    // Only consume if followed by word boundary
                    if i + 1 >= len || !bytes[i + 1].is_ascii_alphanumeric() {
                        i += 1;
                    }
                }
                _ => {}
            }
        }
    }

    i
}

/// Scan a string literal (single or double quoted). Handles backslash escapes.
/// Returns the byte length including delimiters.
fn scan_string(bytes: &[u8]) -> usize {
    let quote = bytes[0];
    let mut i = 1;
    while i < bytes.len() {
        if let Some(offset) = memchr2(quote, b'\\', &bytes[i..]) {
            let pos = i + offset;
            if bytes[pos] == b'\\' && pos + 1 < bytes.len() {
                i = pos + 2;
                continue;
            }
            // Found closing quote
            return pos + 1;
        } else {
            return bytes.len();
        }
    }
    bytes.len()
}

/// Scan a triple-quoted string (''' or """). Returns byte length including delimiters.
fn scan_triple_string(bytes: &[u8], quote: u8) -> usize {
    // bytes starts at the first quote char; first 3 are the opening delimiter
    let mut i = 3;
    while i + 2 < bytes.len() {
        if bytes[i] == quote && bytes[i + 1] == quote && bytes[i + 2] == quote {
            return i + 3;
        }
        i += 1;
    }
    bytes.len()
}

/// Scan a line comment (-- or // or #). Returns byte length including prefix marker.
fn scan_line_comment(bytes: &[u8]) -> usize {
    if let Some(offset) = memchr(b'\n', bytes) {
        offset
    } else {
        bytes.len()
    }
}

/// Scan a block comment. `bytes` starts at `/*`. Returns byte length including delimiters.
fn scan_block_comment(bytes: &[u8]) -> usize {
    let mut i = 2;
    while i + 1 < bytes.len() {
        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            return i + 2;
        }
        i += 1;
    }
    bytes.len()
}

/// Scan a dollar-quoted string ($tag$...$tag$). `bytes` starts at `$`.
fn scan_dollar_string(bytes: &[u8]) -> usize {
    // Find the end of the opening tag
    let mut tag_end = 1;
    while tag_end < bytes.len()
        && (bytes[tag_end].is_ascii_alphanumeric() || bytes[tag_end] == b'_')
    {
        tag_end += 1;
    }
    if tag_end >= bytes.len() || bytes[tag_end] != b'$' {
        return 0; // Not a dollar-quoted string
    }
    let tag = &bytes[..tag_end + 1]; // e.g. $$ or $tag$
    let tag_len = tag.len();

    // Find matching closing tag
    let mut i = tag_len;
    while i + tag_len <= bytes.len() {
        if bytes[i] == b'$' && bytes[i..].starts_with(tag) {
            return i + tag_len;
        }
        i += 1;
    }
    bytes.len()
}

/// Scan a Jinja tag starting with `{`. Returns (tag_len, action).
/// Handles {#...#}, {{...}}, {%...%}.
fn scan_jinja_tag(bytes: &[u8]) -> Option<(usize, &'static Action)> {
    if bytes.len() < 2 {
        return None;
    }
    let second = bytes[1];
    match second {
        b'#' => {
            // Jinja comment {# ... #}
            let mut i = 2;
            while i + 1 < bytes.len() {
                if bytes[i] == b'#' && bytes[i + 1] == b'}' {
                    return Some((i + 2, &A_COMMENT));
                }
                i += 1;
            }
            Some((bytes.len(), &A_COMMENT))
        }
        b'{' => {
            // Jinja expression {{ ... }}
            // The depth-aware scanning is handled by analyzer's handle_jinja.
            // Here we just find the simple end marker.
            let mut i = 2;
            // skip optional -
            if i < bytes.len() && bytes[i] == b'-' {
                i += 1;
            }
            while i + 1 < bytes.len() {
                if bytes[i] == b'\'' || bytes[i] == b'"' {
                    // Skip string literals inside jinja
                    let end = scan_string(&bytes[i..]);
                    i += end;
                    continue;
                }
                if bytes[i] == b'-'
                    && i + 2 < bytes.len()
                    && bytes[i + 1] == b'}'
                    && bytes[i + 2] == b'}'
                {
                    return Some((i + 3, &A_JINJA_EXPR));
                }
                if bytes[i] == b'}' && bytes[i + 1] == b'}' {
                    return Some((i + 2, &A_JINJA_EXPR));
                }
                i += 1;
            }
            Some((bytes.len(), &A_JINJA_EXPR))
        }
        b'%' => {
            // Jinja statement {% ... %}
            // Find end marker %}
            let mut i = 2;
            // skip optional -
            if i < bytes.len() && bytes[i] == b'-' {
                i += 1;
            }
            while i + 1 < bytes.len() {
                if bytes[i] == b'\'' || bytes[i] == b'"' {
                    let end = scan_string(&bytes[i..]);
                    i += end;
                    continue;
                }
                if bytes[i] == b'-'
                    && i + 2 < bytes.len()
                    && bytes[i + 1] == b'%'
                    && bytes[i + 2] == b'}'
                {
                    let tag_len = i + 3;
                    let action = classify_jinja_block(&bytes[..tag_len]);
                    return Some((tag_len, action));
                }
                if bytes[i] == b'%' && bytes[i + 1] == b'}' {
                    let tag_len = i + 2;
                    let action = classify_jinja_block(&bytes[..tag_len]);
                    return Some((tag_len, action));
                }
                i += 1;
            }
            let tag_len = bytes.len();
            Some((tag_len, &A_JINJA_STMT))
        }
        _ => None,
    }
}

/// Classify a {% ... %} block as start/keyword/end/generic statement.
fn classify_jinja_block(tag_bytes: &[u8]) -> &'static Action {
    // Strip {%- or {% and whitespace to get the keyword
    let s = unsafe { std::str::from_utf8_unchecked(tag_bytes) };
    let inner = s.trim_start_matches(['{', '%', '-']).trim_start();

    // Extract first word (the keyword)
    let keyword_end = inner
        .find(|c: char| c.is_ascii_whitespace() || c == '-' || c == '%')
        .unwrap_or(inner.len());
    let keyword = &inner[..keyword_end];

    if keyword.eq_ignore_ascii_case("if")
        || keyword.eq_ignore_ascii_case("for")
        || keyword.eq_ignore_ascii_case("macro")
        || keyword.eq_ignore_ascii_case("test")
        || keyword.eq_ignore_ascii_case("snapshot")
        || keyword.eq_ignore_ascii_case("materialization")
    {
        return &A_JINJA_BLOCK_START;
    }

    if keyword.eq_ignore_ascii_case("set") {
        // {% set x %} (block set - no = sign) vs {% set x = y %} (assignment)
        // Block set has no = in the tag
        if !s.contains('=') {
            return &A_JINJA_BLOCK_START;
        }
        return &A_JINJA_STMT;
    }

    if keyword.eq_ignore_ascii_case("call") {
        // {% call ... %} but NOT {% call statement ... %}
        let rest = inner[keyword_end..].trim_start();
        if !rest.to_ascii_lowercase().starts_with("statement") {
            return &A_JINJA_BLOCK_START;
        }
        return &A_JINJA_STMT;
    }

    if keyword.eq_ignore_ascii_case("elif") || keyword.eq_ignore_ascii_case("else") {
        return &A_JINJA_BLOCK_KEYWORD;
    }

    if keyword.eq_ignore_ascii_case("endif")
        || keyword.eq_ignore_ascii_case("endfor")
        || keyword.eq_ignore_ascii_case("endmacro")
        || keyword.eq_ignore_ascii_case("endtest")
        || keyword.eq_ignore_ascii_case("endsnapshot")
        || keyword.eq_ignore_ascii_case("endmaterialization")
        || keyword.eq_ignore_ascii_case("endset")
        || keyword.eq_ignore_ascii_case("endcall")
    {
        return &A_JINJA_BLOCK_END;
    }

    &A_JINJA_STMT
}

/// Check if a line comment text is a fmt:off or fmt:on marker.
/// Returns Some(action) if it is, None otherwise.
fn check_fmt_marker(comment_bytes: &[u8]) -> Option<&'static Action> {
    // Skip the comment prefix (-- or # or //)
    let mut i = if comment_bytes.len() >= 2 && comment_bytes[0] == b'-' && comment_bytes[1] == b'-'
    {
        2
    } else if comment_bytes[0] == b'#' {
        1
    } else if comment_bytes.len() >= 2 && comment_bytes[0] == b'/' && comment_bytes[1] == b'/' {
        2
    } else {
        return None;
    };
    // Skip whitespace
    while i < comment_bytes.len() && comment_bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    // Check for "fmt:" followed by "off" or "on"
    let rest = &comment_bytes[i..];
    if rest.len() >= 7 && rest[0..4].eq_ignore_ascii_case(b"fmt:") {
        let after_colon = &rest[4..];
        let trimmed = after_colon
            .iter()
            .position(|&b| !b.is_ascii_whitespace())
            .map(|p| &after_colon[p..])
            .unwrap_or(after_colon);
        if trimmed.len() >= 3 && trimmed[0..3].eq_ignore_ascii_case(b"off") {
            return Some(&A_FMT_OFF);
        }
        if trimmed.len() >= 2 && trimmed[0..2].eq_ignore_ascii_case(b"on") {
            return Some(&A_FMT_ON);
        }
    }
    None
}

/// Try to match a multi-word sequence starting with `first_word` (already lowercased).
/// `after_word` is the remaining bytes after the first word.
/// Returns (total_match_len_from_after_word, matched_text_including_first_word_len) on success.
///
/// This function peeks ahead past whitespace for continuation words to greedily
/// match the longest multi-word keyword (e.g., "LEFT OUTER JOIN" before "LEFT JOIN").
fn try_multi_word(first_lower: &str, after_word: &[u8], state: LexState) -> Option<usize> {
    // Returns additional bytes consumed AFTER the first word

    // State-specific multi-word patterns (take longest match)
    let state_match = match state {
        LexState::Grant => try_multi_word_grant(first_lower, after_word),
        LexState::Function => try_multi_word_function(first_lower, after_word),
        LexState::Warehouse => try_multi_word_warehouse(first_lower, after_word),
        _ => None,
    };

    // Base (state-independent) multi-word patterns
    let base_match = try_multi_word_base(first_lower, after_word);

    // Return the longer match
    match (base_match, state_match) {
        (Some(b), Some(s)) => Some(b.max(s)),
        (Some(b), None) => Some(b),
        (None, Some(s)) => Some(s),
        (None, None) => None,
    }
}

/// State-independent multi-word patterns.
fn try_multi_word_base(first_lower: &str, after_word: &[u8]) -> Option<usize> {
    match first_lower {
        // JOINs (most specific first)
        "global" => try_sequence_any(
            after_word,
            &[
                &["inner", "join"],
                &["left", "outer", "join"],
                &["left", "join"],
                &["right", "outer", "join"],
                &["right", "join"],
                &["full", "outer", "join"],
                &["full", "join"],
                &["any", "join"],
                &["join"],
            ],
        ),
        "any" => try_sequence_any(
            after_word,
            &[
                &["left", "outer", "join"],
                &["left", "join"],
                &["right", "outer", "join"],
                &["right", "join"],
                &["inner", "join"],
                &["full", "outer", "join"],
                &["full", "join"],
                &["join"],
            ],
        ),
        "natural" => try_sequence_any(
            after_word,
            &[
                &["full", "outer", "join"],
                &["full", "join"],
                &["left", "outer", "join"],
                &["left", "join"],
                &["right", "outer", "join"],
                &["right", "join"],
                &["inner", "join"],
                &["join"],
            ],
        ),
        "cross" => try_sequence_any(after_word, &[&["lateral", "join"], &["join"]]),
        "left" => try_sequence_any(
            after_word,
            &[
                &["outer", "join"],
                &["semi", "join"],
                &["anti", "join"],
                &["asof", "join"],
                &["join"],
            ],
        ),
        "right" => try_sequence_any(
            after_word,
            &[
                &["outer", "join"],
                &["semi", "join"],
                &["anti", "join"],
                &["join"],
            ],
        ),
        "full" => try_sequence_any(after_word, &[&["outer", "join"], &["join"]]),
        "inner" | "semi" | "anti" | "positional" | "paste" => try_sequence(after_word, &["join"]),
        "asof" => try_sequence_any(after_word, &[&["left", "join"], &["join"]]),

        // SELECT modifiers
        "select" => try_sequence_any(
            after_word,
            &[
                &["as", "struct"],
                &["as", "value"],
                &["into"], // select into
                &["all"],
                &["distinct"],
                // "select top N" handled specially below
            ],
        )
        .or_else(|| try_select_top(after_word)),

        // WITH
        "with" => try_sequence(after_word, &["recursive"]),

        // DELETE FROM
        "delete" => try_sequence(after_word, &["from"]),

        // Standard multi-word clauses
        "group" | "order" | "cluster" | "distribute" | "sort" | "partition" => {
            try_sequence(after_word, &["by"])
        }
        "lateral" => try_sequence_any(after_word, &[&["view", "outer"], &["view"], &["join"]]),
        "fetch" => try_sequence_any(after_word, &[&["first"], &["next"]]),
        "start" => try_sequence(after_word, &["with"]),

        // FOR variants
        "for" => try_sequence_any(
            after_word,
            &[
                &["no", "key", "update"],
                &["key", "share"],
                &["update"],
                &["share"],
            ],
        ),

        // Word operators (multi-word)
        "is" => try_sequence_any(
            after_word,
            &[
                &["not", "distinct", "from"],
                &["distinct", "from"],
                &["not"],
            ],
        ),
        "not" => try_sequence_any(
            after_word,
            &[
                &["similar", "to"],
                &["ilike", "all"],
                &["ilike", "any"],
                &["like", "all"],
                &["like", "any"],
                &["between"],
                &["ilike"],
                &["like"],
                &["rlike"],
                &["regexp"],
                &["exists"],
                &["in"],
            ],
        ),
        "ilike" | "like" => try_sequence_any(after_word, &[&["all"], &["any"]]),
        "similar" => try_sequence(after_word, &["to"]),
        "grouping" => try_sequence(after_word, &["sets"]),
        "within" => try_sequence(after_word, &["group"]),
        "respect" | "ignore" => try_sequence(after_word, &["nulls"]),
        "nulls" => try_sequence_any(after_word, &[&["first"], &["last"]]),

        // SET operators (multi-word)
        "union" => try_sequence_any(
            after_word,
            &[
                &["all", "by", "name"],
                &["by", "name"],
                &["all", "corresponding", "by"],
                &["corresponding", "by"],
                &["strict", "corresponding"],
                &["corresponding"],
                &["all"],
                &["distinct"],
            ],
        ),
        "intersect" => try_sequence_any(
            after_word,
            &[
                &["all", "corresponding"],
                &["corresponding"],
                &["all"],
                &["distinct"],
            ],
        ),
        "except" | "minus" => {
            // except has special handling: it can be a set operator or star modifier
            // We'll handle "except all", "except distinct", "except corresponding" here
            // "except(" is handled by the paren lookahead in classify_keyword
            try_sequence_any(
                after_word,
                &[
                    &["all", "corresponding"],
                    &["corresponding"],
                    &["all"],
                    &["distinct"],
                ],
            )
        }

        // DDL multi-word
        "create" => try_create_extension(after_word),
        "alter" => try_alter_extension(after_word),
        "drop" => try_sequence_any(after_word, &[&["function", "if", "exists"], &["function"]]),
        "insert" => try_sequence_any(
            after_word,
            &[&["overwrite", "into"], &["overwrite"], &["into"]],
        ),
        "merge" => try_sequence(after_word, &["into"]),
        "rename" => try_sequence(after_word, &["table"]),
        "cache" => try_sequence(after_word, &["table"]),
        "clear" => try_sequence(after_word, &["cache"]),
        "reassign" => try_sequence(after_word, &["owned"]),
        "import" => try_sequence_any(after_word, &[&["foreign", "schema"], &["table"]]),
        "security" => try_sequence(after_word, &["label"]),

        // EXPLAIN modifiers
        "explain" => try_sequence_any(after_word, &[&["analyze"], &["verbose"], &["using"]]),

        _ => None,
    }
}

/// Extend "create" with DDL-specific patterns.
/// Handles: create [or replace] [temp[orary]] [secure] [external] [table] function [if not exists]
///          create [or replace] warehouse [if not exists]
///          create [or replace] (fallback)
fn try_create_extension(bytes: &[u8]) -> Option<usize> {
    let mut pos = 0;

    // Optional: "or replace"
    if let Some(extra) = try_sequence(bytes, &["or", "replace"]) {
        pos = extra;
    }

    // Try to find "function" after optional modifiers
    if let Some(extra) = scan_past_modifiers_to_function(&bytes[pos..]) {
        return Some(pos + extra);
    }

    // Try warehouse (no modifiers between create [or replace] and warehouse)
    if let Some(extra) = try_sequence(&bytes[pos..], &["warehouse"]) {
        let wpos = pos + extra;
        if let Some(ine) = try_sequence(&bytes[wpos..], &["if", "not", "exists"]) {
            return Some(wpos + ine);
        }
        return Some(wpos);
    }

    // Fallback: just "or replace" if matched
    if pos > 0 {
        Some(pos)
    } else {
        None
    }
}

/// Scan past optional function modifiers (temporary, temp, secure, external, table)
/// to find "function" keyword. Returns extra bytes consumed from start on success.
fn scan_past_modifiers_to_function(bytes: &[u8]) -> Option<usize> {
    let modifiers: &[&str] = &["temporary", "temp", "secure", "external", "table"];
    let mut pos = 0;

    for _ in 0..modifiers.len() {
        // Skip whitespace
        let mut ws = pos;
        while ws < bytes.len() && bytes[ws].is_ascii_whitespace() {
            ws += 1;
        }
        if ws >= bytes.len() {
            return None;
        }

        // Check for "function"
        if ws + 8 <= bytes.len()
            && bytes[ws..ws + 8].eq_ignore_ascii_case(b"function")
            && (ws + 8 >= bytes.len()
                || !(bytes[ws + 8].is_ascii_alphanumeric() || bytes[ws + 8] == b'_'))
        {
            pos = ws + 8;
            // Optional "if not exists"
            if let Some(ine) = try_sequence(&bytes[pos..], &["if", "not", "exists"]) {
                return Some(pos + ine);
            }
            return Some(pos);
        }

        // Check if current word is a known modifier
        let mut found = false;
        for &modifier in modifiers {
            let mlen = modifier.len();
            if ws + mlen <= bytes.len()
                && bytes[ws..ws + mlen].eq_ignore_ascii_case(modifier.as_bytes())
                && (ws + mlen >= bytes.len()
                    || !(bytes[ws + mlen].is_ascii_alphanumeric() || bytes[ws + mlen] == b'_'))
            {
                pos = ws + mlen;
                found = true;
                break;
            }
        }
        if !found {
            return None;
        }
    }
    None
}

/// Extend "alter" with DDL-specific patterns.
fn try_alter_extension(bytes: &[u8]) -> Option<usize> {
    // Try "function [if exists]"
    if let Some(extra) = try_sequence(bytes, &["function"]) {
        if let Some(ie) = try_sequence(&bytes[extra..], &["if", "exists"]) {
            return Some(extra + ie);
        }
        return Some(extra);
    }
    // Try "warehouse [if exists]"
    if let Some(extra) = try_sequence(bytes, &["warehouse"]) {
        if let Some(ie) = try_sequence(&bytes[extra..], &["if", "exists"]) {
            return Some(extra + ie);
        }
        return Some(extra);
    }
    None
}

/// Grant-state multi-word patterns.
fn try_multi_word_grant(first_lower: &str, after_word: &[u8]) -> Option<usize> {
    match first_lower {
        "revoke" => try_sequence(after_word, &["grant", "option", "for"]),
        "with" => try_sequence(after_word, &["grant", "option"]),
        "granted" => try_sequence(after_word, &["by"]),
        _ => None,
    }
}

/// Function-state multi-word patterns.
fn try_multi_word_function(first_lower: &str, after_word: &[u8]) -> Option<usize> {
    match first_lower {
        "called" => try_sequence(after_word, &["on", "null", "input"]),
        "returns" => try_sequence(after_word, &["null", "on", "null", "input"]),
        "remote" => try_sequence(after_word, &["with", "connection"]),
        "rename" => try_sequence(after_word, &["to"]),
        "owner" => try_sequence(after_word, &["to"]),
        "depends" => try_sequence(after_word, &["on", "extension"]),
        "no" => try_sequence(after_word, &["depends", "on", "extension"]),
        "not" => try_sequence(after_word, &["leakproof"]),
        "parallel" => try_sequence_any(after_word, &[&["safe"], &["unsafe"], &["restricted"]]),
        "security" => try_sequence_any(after_word, &[&["definer"], &["invoker"]]),
        "set" => try_sequence_any(
            after_word,
            &[
                &["api_integration"],
                &["headers"],
                &["context_headers"],
                &["max_batch_rows"],
                &["compression"],
                &["request_translator"],
                &["response_translator"],
                &["comment"],
                &["schema"],
                &["secure"],
            ],
        ),
        "unset" => try_sequence_any(after_word, &[&["comment"], &["secure"]]),
        _ => None,
    }
}

/// Warehouse-state multi-word patterns.
fn try_multi_word_warehouse(first_lower: &str, after_word: &[u8]) -> Option<usize> {
    match first_lower {
        "abort" => try_sequence(after_word, &["all", "queries"]),
        "rename" => try_sequence(after_word, &["to"]),
        "resume" => try_sequence(after_word, &["if", "suspended"]),
        "with" | "set" | "unset" => {
            let params: &[&[&str]] = &[
                &["warehouse_type"],
                &["warehouse_size"],
                &["max_cluster_count"],
                &["min_cluster_count"],
                &["scaling_policy"],
                &["auto_suspend"],
                &["auto_resume"],
                &["initially_suspended"],
                &["resource_monitor"],
                &["comment"],
                &["enable_query_acceleration"],
                &["query_acceleration_max_scale_factor"],
                &["max_concurrency_level"],
                &["statement_queued_timeout_in_seconds"],
                &["statement_timeout_in_seconds"],
                &["tag"],
            ];
            try_sequence_any(after_word, params)
        }
        _ => None,
    }
}

/// Scan ahead in remaining text to detect a clone pattern:
/// (database|schema|table|stage|file format|sequence|stream|task) [if not exists] <name> clone
fn scan_rest_for_clone(rest: &[u8]) -> bool {
    let mut pos = 0;

    // Skip whitespace
    while pos < rest.len() && rest[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if pos >= rest.len() {
        return false;
    }

    // Object type: database|schema|table|stage|sequence|stream|task
    let object_types: &[&str] = &[
        "database", "schema", "table", "stage", "sequence", "stream", "task",
    ];
    let mut found_type = false;

    // Check for "file format" (two words)
    if let Some(extra) = try_sequence(&rest[pos..], &["file", "format"]) {
        pos += extra;
        found_type = true;
    }

    if !found_type {
        for &obj_type in object_types {
            let olen = obj_type.len();
            if pos + olen <= rest.len()
                && rest[pos..pos + olen].eq_ignore_ascii_case(obj_type.as_bytes())
                && (pos + olen >= rest.len()
                    || !(rest[pos + olen].is_ascii_alphanumeric() || rest[pos + olen] == b'_'))
            {
                pos += olen;
                found_type = true;
                break;
            }
        }
    }

    if !found_type {
        return false;
    }

    // Optional: "if not exists"
    if let Some(extra) = try_sequence(&rest[pos..], &["if", "not", "exists"]) {
        pos += extra;
    }

    // Object name: any word (skip whitespace first)
    while pos < rest.len() && rest[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if pos >= rest.len() {
        return false;
    }

    // The name could be a quoted identifier or a regular word
    if rest[pos] == b'"' || rest[pos] == b'`' || rest[pos] == b'\'' {
        let quote = rest[pos];
        pos += 1;
        while pos < rest.len() && rest[pos] != quote {
            pos += 1;
        }
        if pos < rest.len() {
            pos += 1; // skip closing quote
        } else {
            return false;
        }
    } else {
        let start = pos;
        while pos < rest.len()
            && (rest[pos].is_ascii_alphanumeric() || rest[pos] == b'_' || rest[pos] == b'.')
        {
            pos += 1;
        }
        if pos == start {
            return false;
        }
    }

    // Check for "clone"
    try_sequence(&rest[pos..], &["clone"]).is_some()
}

/// Scan ahead for "function" keyword (past optional DDL modifiers).
/// Used as a fallback when multi-word matching didn't capture the DDL object type.
fn scan_rest_for_function(rest: &[u8]) -> bool {
    // Scan past optional modifiers to find "function"
    let modifiers: &[&str] = &["temporary", "temp", "secure", "external", "table"];
    let mut pos = 0;

    for _ in 0..=modifiers.len() {
        while pos < rest.len() && rest[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= rest.len() {
            return false;
        }
        // Check for "function"
        if pos + 8 <= rest.len()
            && rest[pos..pos + 8].eq_ignore_ascii_case(b"function")
            && (pos + 8 >= rest.len()
                || !(rest[pos + 8].is_ascii_alphanumeric() || rest[pos + 8] == b'_'))
        {
            return true;
        }
        // Check if current word is a modifier
        let mut found = false;
        for &modifier in modifiers {
            let mlen = modifier.len();
            if pos + mlen <= rest.len()
                && rest[pos..pos + mlen].eq_ignore_ascii_case(modifier.as_bytes())
                && (pos + mlen >= rest.len()
                    || !(rest[pos + mlen].is_ascii_alphanumeric() || rest[pos + mlen] == b'_'))
            {
                pos += mlen;
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    false
}

/// Scan ahead for "warehouse" keyword.
fn scan_rest_for_warehouse(rest: &[u8]) -> bool {
    try_sequence(rest, &["warehouse"]).is_some()
}

/// Try to match a sequence of words after the current position.
/// Skips all whitespace including newlines between words (matching regex `\s+` behavior).
/// Returns extra bytes consumed on success.
fn try_sequence(bytes: &[u8], words: &[&str]) -> Option<usize> {
    let mut pos = 0;
    for &word in words {
        // Skip whitespace (including newlines — regex \s+ matches across lines)
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        // Match word (case-insensitive)
        let word_bytes = word.as_bytes();
        if pos + word_bytes.len() > bytes.len() {
            return None;
        }
        if !bytes[pos..pos + word_bytes.len()].eq_ignore_ascii_case(word_bytes) {
            return None;
        }
        pos += word_bytes.len();
        // Check word boundary
        if pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
            return None;
        }
    }
    Some(pos)
}

/// Try multiple sequences, return the first (longest) match.
fn try_sequence_any(bytes: &[u8], sequences: &[&[&str]]) -> Option<usize> {
    for seq in sequences {
        if let Some(extra) = try_sequence(bytes, seq) {
            return Some(extra);
        }
    }
    None
}

/// Try "select top N" pattern
fn try_select_top(bytes: &[u8]) -> Option<usize> {
    // Skip whitespace (including newlines)
    let mut pos = 0;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    // Match "top"
    if pos + 3 > bytes.len() || !bytes[pos..pos + 3].eq_ignore_ascii_case(b"top") {
        return None;
    }
    pos += 3;
    if pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
        return None;
    }
    // Skip whitespace (including newlines)
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    // Match digits
    let digit_start = pos;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == digit_start {
        return None;
    }
    Some(pos)
}

/// Scan a compound operator starting at bytes[0].
/// Returns byte length of the operator, or 0 if no compound operator.
fn scan_compound_operator(bytes: &[u8]) -> usize {
    let len = bytes.len();
    if len == 0 {
        return 0;
    }
    let b0 = bytes[0];
    match b0 {
        b'>' => {
            if len >= 2 {
                match bytes[1] {
                    b'=' => 2, // >=
                    b'>' => 2, // >>
                    _ => 0,
                }
            } else {
                0
            }
        }
        b'<' => {
            if len >= 3 && bytes[1] == b'=' && bytes[2] == b'>' {
                return 3; // <=>
            }
            if len >= 3 && bytes[1] == b'-' && bytes[2] == b'>' {
                return 3; // <->
            }
            if len >= 3 && bytes[1] == b'#' && bytes[2] == b'>' {
                return 3; // <#>
            }
            if len >= 2 {
                match bytes[1] {
                    b'>' => 2, // <>
                    b'=' => 2, // <=
                    b'<' => 2, // <<
                    b'@' => 2, // <@
                    _ => 0,
                }
            } else {
                0
            }
        }
        b'=' => {
            if len >= 2 {
                match bytes[1] {
                    b'>' => 2, // =>
                    b'=' => 2, // ==
                    _ => 0,
                }
            } else {
                0
            }
        }
        b'!' => {
            if len >= 3 && bytes[1] == b'!' && bytes[2] == b'=' {
                return 3; // !!=
            }
            if len >= 3 && bytes[1] == b'~' && bytes[2] == b'*' {
                return 3; // !~*
            }
            if len >= 2 {
                match bytes[1] {
                    b'=' => 2, // !=
                    b'~' => 2, // !~
                    _ => 0,
                }
            } else {
                0
            }
        }
        b'-' => {
            if len >= 4 && bytes[1] == b'>' && bytes[2] == b'-' && bytes[3] == b'>' {
                return 4; // ->->
            }
            if len >= 3 && bytes[1] == b'>' && bytes[2] == b'>' {
                return 3; // ->>
            }
            if len >= 3 && bytes[1] == b'|' && bytes[2] == b'-' {
                return 3; // -|-
            }
            if len >= 2 && bytes[1] == b'>' {
                return 2; // ->
            }
            0
        }
        b'|' => {
            if len >= 3 && bytes[1] == b'|' && bytes[2] == b'/' {
                return 3; // ||/
            }
            if len >= 2 {
                match bytes[1] {
                    b'|' => 2, // ||
                    b'/' => 2, // |/
                    _ => 0,
                }
            } else {
                0
            }
        }
        b'&' => {
            if len >= 2 && bytes[1] == b'&' {
                return 2; // &&
            }
            0
        }
        b'*' => {
            if len >= 2 && bytes[1] == b'*' {
                return 2; // **
            }
            0
        }
        b'~' => {
            if len >= 2 && bytes[1] == b'*' {
                return 2; // ~*
            }
            0
        }
        b'@' => {
            if len >= 3 && bytes[1] == b'-' && bytes[2] == b'@' {
                return 3; // @-@
            }
            if len >= 2 && bytes[1] == b'>' {
                return 2; // @>
            }
            if len >= 2 && bytes[1] == b'@' {
                return 2; // @@
            }
            0
        }
        b'?' => {
            if len >= 2 {
                match bytes[1] {
                    b'|' => 2, // ?|
                    b'&' => 2, // ?&
                    _ => 0,
                }
            } else {
                0
            }
        }
        b'#' => {
            if len >= 3 && bytes[1] == b'>' && bytes[2] == b'>' {
                return 3; // #>>
            }
            if len >= 2 {
                match bytes[1] {
                    b'>' => 2, // #>
                    b'-' => 2, // #-
                    _ => 0,
                }
            } else {
                0
            }
        }
        b'%' => {
            if len >= 2 && bytes[1] == b'%' {
                return 2; // %%
            }
            0
        }
        _ => 0,
    }
}

// ---- Main dispatch ----

/// Lex one token from `remaining` (source[pos..]) using byte dispatch.
/// Returns a LexResult with the action, match_len, prefix, and token_text.
pub fn lex_one<'a>(remaining: &'a str, state: LexState) -> Option<LexResult<'a>> {
    let bytes = remaining.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    // 1. Skip leading non-newline whitespace → prefix
    let prefix_len = skip_prefix_whitespace(bytes);
    let prefix = &remaining[..prefix_len];
    let after_prefix = &bytes[prefix_len..];
    let after_prefix_str = &remaining[prefix_len..];

    if after_prefix.is_empty() {
        return None;
    }

    // 2. Handle data-mode states (FmtOff, JinjaSetBlock, JinjaCallBlock, Unsupported)
    match state {
        LexState::FmtOff => {
            return lex_fmt_off(
                remaining,
                prefix,
                prefix_len,
                after_prefix,
                after_prefix_str,
            );
        }
        LexState::JinjaSetBlock => {
            return lex_jinja_set_block(
                remaining,
                prefix,
                prefix_len,
                after_prefix,
                after_prefix_str,
            );
        }
        LexState::JinjaCallBlock => {
            return lex_jinja_call_block(
                remaining,
                prefix,
                prefix_len,
                after_prefix,
                after_prefix_str,
            );
        }
        LexState::Unsupported => {
            return lex_unsupported(
                remaining,
                prefix,
                prefix_len,
                after_prefix,
                after_prefix_str,
            );
        }
        _ => {}
    }

    // 3. Dispatch on first byte after prefix
    let b0 = after_prefix[0];

    match b0 {
        b'\n' => {
            let match_len = prefix_len + 1;
            Some(LexResult {
                action: &A_NEWLINE,
                match_len,
                prefix,
                token_text: "\n",
            })
        }

        // Comments: -- or -other
        b'-' => {
            if after_prefix.len() >= 2 && after_prefix[1] == b'-' {
                // Line comment --
                let comment_len = scan_line_comment(after_prefix);
                let token_text = &after_prefix_str[..comment_len];
                let match_len = prefix_len + comment_len;
                // Check for fmt:off/on
                if let Some(action) = check_fmt_marker(token_text.as_bytes()) {
                    return Some(LexResult {
                        action,
                        match_len,
                        prefix,
                        token_text,
                    });
                }
                Some(LexResult {
                    action: &A_COMMENT,
                    match_len,
                    prefix,
                    token_text,
                })
            } else {
                // Operator: check compound operators first
                let comp = scan_compound_operator(after_prefix);
                if comp > 0 {
                    Some(LexResult {
                        action: &A_OPERATOR,
                        match_len: prefix_len + comp,
                        prefix,
                        token_text: &after_prefix_str[..comp],
                    })
                } else {
                    Some(LexResult {
                        action: &A_OPERATOR,
                        match_len: prefix_len + 1,
                        prefix,
                        token_text: &after_prefix_str[..1],
                    })
                }
            }
        }

        // Comments: / (block comment /* */ or line comment //)
        b'/' => {
            if after_prefix.len() >= 2 && after_prefix[1] == b'*' {
                let comment_len = scan_block_comment(after_prefix);
                let token_text = &after_prefix_str[..comment_len];
                let match_len = prefix_len + comment_len;
                Some(LexResult {
                    action: &A_COMMENT,
                    match_len,
                    prefix,
                    token_text,
                })
            } else if after_prefix.len() >= 2 && after_prefix[1] == b'/' {
                let comment_len = scan_line_comment(after_prefix);
                let token_text = &after_prefix_str[..comment_len];
                let match_len = prefix_len + comment_len;
                if let Some(action) = check_fmt_marker(token_text.as_bytes()) {
                    return Some(LexResult {
                        action,
                        match_len,
                        prefix,
                        token_text,
                    });
                }
                Some(LexResult {
                    action: &A_COMMENT,
                    match_len,
                    prefix,
                    token_text,
                })
            } else {
                // Division operator
                Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + 1,
                    prefix,
                    token_text: "/",
                })
            }
        }

        // Hash: line comment or fmt marker or PG operator
        b'#' => {
            // Check for PG operators first: #>>, #>, #-
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            // Line comment
            let comment_len = scan_line_comment(after_prefix);
            let token_text = &after_prefix_str[..comment_len];
            let match_len = prefix_len + comment_len;
            if let Some(action) = check_fmt_marker(token_text.as_bytes()) {
                return Some(LexResult {
                    action,
                    match_len,
                    prefix,
                    token_text,
                });
            }
            Some(LexResult {
                action: &A_COMMENT,
                match_len,
                prefix,
                token_text,
            })
        }

        // Single-quoted string
        b'\'' => {
            // Check for triple quote '''
            if after_prefix.len() >= 3 && after_prefix[1] == b'\'' && after_prefix[2] == b'\'' {
                let len = scan_triple_string(after_prefix, b'\'');
                Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + len,
                    prefix,
                    token_text: &after_prefix_str[..len],
                })
            } else {
                let len = scan_string(after_prefix);
                Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + len,
                    prefix,
                    token_text: &after_prefix_str[..len],
                })
            }
        }

        // Double-quoted name
        b'"' => {
            if after_prefix.len() >= 3 && after_prefix[1] == b'"' && after_prefix[2] == b'"' {
                let len = scan_triple_string(after_prefix, b'"');
                Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + len,
                    prefix,
                    token_text: &after_prefix_str[..len],
                })
            } else {
                let len = scan_string(after_prefix);
                Some(LexResult {
                    action: &A_QUOTED_NAME,
                    match_len: prefix_len + len,
                    prefix,
                    token_text: &after_prefix_str[..len],
                })
            }
        }

        // Backtick-quoted name
        b'`' => {
            let len = scan_string(after_prefix);
            Some(LexResult {
                action: &A_QUOTED_NAME,
                match_len: prefix_len + len,
                prefix,
                token_text: &after_prefix_str[..len],
            })
        }

        // Dollar: dollar-quoted string, $identifier, or $N
        b'$' => {
            let ds_len = scan_dollar_string(after_prefix);
            if ds_len > 0 {
                return Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + ds_len,
                    prefix,
                    token_text: &after_prefix_str[..ds_len],
                });
            }
            // $identifier or $N
            let word_len = scan_word(&after_prefix[1..]);
            if word_len > 0 {
                let total = 1 + word_len;
                return Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + total,
                    prefix,
                    token_text: &after_prefix_str[..total],
                });
            }
            // Just $ alone — treat as operator
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "$",
            })
        }

        // Jinja: {#, {{, {%, or bracket_open {
        b'{' => {
            if let Some((tag_len, action)) = scan_jinja_tag(after_prefix) {
                return Some(LexResult {
                    action,
                    match_len: prefix_len + tag_len,
                    prefix,
                    token_text: &after_prefix_str[..tag_len],
                });
            }
            // Regular brace: bracket_open
            Some(LexResult {
                action: &A_BRACKET_OPEN,
                match_len: prefix_len + 1,
                prefix,
                token_text: "{",
            })
        }

        // Semicolon
        b';' => Some(LexResult {
            action: &A_SEMICOLON,
            match_len: prefix_len + 1,
            prefix,
            token_text: ";",
        }),

        // Comma
        b',' => Some(LexResult {
            action: &A_COMMA,
            match_len: prefix_len + 1,
            prefix,
            token_text: ",",
        }),

        // Dot: could be leading-dot number (.5) or just dot
        b'.' => {
            if after_prefix.len() >= 2 && after_prefix[1].is_ascii_digit() {
                // Leading-dot number: .5, .123, .5e10
                let num_len = scan_number(after_prefix);
                if num_len > 1 {
                    return Some(LexResult {
                        action: &A_NUMBER,
                        match_len: prefix_len + num_len,
                        prefix,
                        token_text: &after_prefix_str[..num_len],
                    });
                }
            }
            Some(LexResult {
                action: &A_DOT,
                match_len: prefix_len + 1,
                prefix,
                token_text: ".",
            })
        }

        // Colon: ::, :=, or :
        b':' => {
            if after_prefix.len() >= 2 && after_prefix[1] == b':' {
                Some(LexResult {
                    action: &A_DOUBLE_COLON,
                    match_len: prefix_len + 2,
                    prefix,
                    token_text: "::",
                })
            } else if after_prefix.len() >= 2 && after_prefix[1] == b'=' {
                Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + 2,
                    prefix,
                    token_text: ":=",
                })
            } else {
                Some(LexResult {
                    action: &A_COLON,
                    match_len: prefix_len + 1,
                    prefix,
                    token_text: ":",
                })
            }
        }

        // Star
        b'*' => {
            // Check compound operator ** first
            let comp = scan_compound_operator(after_prefix);
            if comp > 1 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            Some(LexResult {
                action: &A_STAR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "*",
            })
        }

        // Opening brackets
        b'(' => Some(LexResult {
            action: &A_BRACKET_OPEN,
            match_len: prefix_len + 1,
            prefix,
            token_text: "(",
        }),

        b'[' => Some(LexResult {
            action: &A_BRACKET_OPEN,
            match_len: prefix_len + 1,
            prefix,
            token_text: "[",
        }),

        // Closing brackets
        b')' => Some(LexResult {
            action: &A_BRACKET_CLOSE,
            match_len: prefix_len + 1,
            prefix,
            token_text: ")",
        }),

        b']' => Some(LexResult {
            action: &A_BRACKET_CLOSE,
            match_len: prefix_len + 1,
            prefix,
            token_text: "]",
        }),

        b'}' => Some(LexResult {
            action: &A_BRACKET_CLOSE,
            match_len: prefix_len + 1,
            prefix,
            token_text: "}",
        }),

        // Numbers: 0-9
        b'0'..=b'9' => {
            let num_len = scan_number(after_prefix);
            Some(LexResult {
                action: &A_NUMBER,
                match_len: prefix_len + num_len,
                prefix,
                token_text: &after_prefix_str[..num_len],
            })
        }

        // Operators and special characters
        b'>' => {
            // Check compound operators first: >=, >>
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            // Single > needs angle bracket handling
            Some(LexResult {
                action: &A_ANGLE_CLOSE,
                match_len: prefix_len + 1,
                prefix,
                token_text: ">",
            })
        }

        b'<' => {
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            // Single < is an operator
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "<",
            })
        }

        b'=' => {
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "=",
            })
        }

        b'!' => {
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "!",
            })
        }

        b'~' => {
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "~",
            })
        }

        b'@' => {
            // Check for @variable first
            if after_prefix.len() >= 2
                && (after_prefix[1].is_ascii_alphanumeric() || after_prefix[1] == b'_')
            {
                let word_len = scan_word(&after_prefix[1..]);
                let total = 1 + word_len;
                return Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + total,
                    prefix,
                    token_text: &after_prefix_str[..total],
                });
            }
            // Compound operators: @-@, @>, @@
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            // Fallback: single-char operator
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "@",
            })
        }

        b'?' => {
            // Check for ?N (positional param)
            if after_prefix.len() >= 2 && after_prefix[1].is_ascii_digit() {
                let mut i = 1;
                while i < after_prefix.len() && after_prefix[i].is_ascii_digit() {
                    i += 1;
                }
                return Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + i,
                    prefix,
                    token_text: &after_prefix_str[..i],
                });
            }
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "?",
            })
        }

        b'%' => {
            // Check for %(name)s psycopg param
            if after_prefix.len() >= 2 && after_prefix[1] == b'(' {
                if let Some(close) = memchr(b')', &after_prefix[2..]) {
                    let close_pos = 2 + close;
                    if close_pos + 1 < after_prefix.len() && after_prefix[close_pos + 1] == b's' {
                        let total = close_pos + 2;
                        return Some(LexResult {
                            action: &A_NAME,
                            match_len: prefix_len + total,
                            prefix,
                            token_text: &after_prefix_str[..total],
                        });
                    }
                }
            }
            // Check for %s psycopg param
            if after_prefix.len() >= 2 && after_prefix[1] == b's' {
                return Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + 2,
                    prefix,
                    token_text: "%s",
                });
            }
            // %% operator
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            // Single % operator
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: "%",
            })
        }

        b'+' | b'^' | b'&' | b'|' => {
            let comp = scan_compound_operator(after_prefix);
            if comp > 0 {
                return Some(LexResult {
                    action: &A_OPERATOR,
                    match_len: prefix_len + comp,
                    prefix,
                    token_text: &after_prefix_str[..comp],
                });
            }
            Some(LexResult {
                action: &A_OPERATOR,
                match_len: prefix_len + 1,
                prefix,
                token_text: &after_prefix_str[..1],
            })
        }

        // r/R prefix for raw strings (r"""...""", r'''...''')
        b'r' | b'R' => {
            if after_prefix.len() >= 4 {
                if after_prefix[1] == b'"' && after_prefix[2] == b'"' && after_prefix[3] == b'"' {
                    let len = 1 + scan_triple_string(&after_prefix[1..], b'"');
                    return Some(LexResult {
                        action: &A_NAME,
                        match_len: prefix_len + len,
                        prefix,
                        token_text: &after_prefix_str[..len],
                    });
                }
                if after_prefix[1] == b'\'' && after_prefix[2] == b'\'' && after_prefix[3] == b'\''
                {
                    let len = 1 + scan_triple_string(&after_prefix[1..], b'\'');
                    return Some(LexResult {
                        action: &A_NAME,
                        match_len: prefix_len + len,
                        prefix,
                        token_text: &after_prefix_str[..len],
                    });
                }
            }
            // Fall through to identifier scanning
            lex_identifier(
                remaining,
                prefix,
                prefix_len,
                after_prefix,
                after_prefix_str,
                state,
            )
        }

        // Identifiers: a-z, A-Z, _
        b'a'..=b'z' | b'A'..=b'Z' | b'_' => lex_identifier(
            remaining,
            prefix,
            prefix_len,
            after_prefix,
            after_prefix_str,
            state,
        ),

        // Non-ASCII (unicode identifiers)
        _ if b0 >= 0x80 => {
            let word_len = scan_word(after_prefix);
            if word_len > 0 {
                Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + word_len,
                    prefix,
                    token_text: &after_prefix_str[..word_len],
                })
            } else {
                // Skip unknown byte
                Some(LexResult {
                    action: &A_NAME,
                    match_len: prefix_len + 1,
                    prefix,
                    token_text: &after_prefix_str[..1],
                })
            }
        }

        // Catch-all: treat as single-char token
        _ => Some(LexResult {
            action: &A_NAME,
            match_len: prefix_len + 1,
            prefix,
            token_text: &after_prefix_str[..1],
        }),
    }
}

/// Lex an identifier and classify it as a keyword, operator, etc.
fn lex_identifier<'a>(
    _remaining: &'a str,
    prefix: &'a str,
    prefix_len: usize,
    after_prefix: &[u8],
    after_prefix_str: &'a str,
    state: LexState,
) -> Option<LexResult<'a>> {
    let word_len = scan_word(after_prefix);
    if word_len == 0 {
        return None;
    }

    let word = &after_prefix_str[..word_len];
    let after_word = &after_prefix[word_len..];

    // Lowercase for keyword comparison (stack-allocate for short words)
    let mut lower_buf = [0u8; 64];
    let lower: &str = if word_len <= 64 {
        for (i, &b) in after_prefix[..word_len].iter().enumerate() {
            lower_buf[i] = b.to_ascii_lowercase();
        }
        unsafe { std::str::from_utf8_unchecked(&lower_buf[..word_len]) }
    } else {
        // Very long identifier — not a keyword
        return Some(LexResult {
            action: &A_NAME,
            match_len: prefix_len + word_len,
            prefix,
            token_text: word,
        });
    };

    // Try multi-word extension
    let (total_word_len, full_text) = if let Some(extra) = try_multi_word(lower, after_word, state)
    {
        (word_len + extra, &after_prefix_str[..word_len + extra])
    } else {
        (word_len, word)
    };

    // Build lowercase of full multi-word keyword for classification.
    // Normalize all whitespace (including newlines) to single spaces so
    // keyword matching works regardless of original whitespace.
    let full_lower: String;
    let classify_key: &str = if total_word_len == word_len {
        lower
    } else {
        // Lowercase and normalize whitespace to single spaces
        let raw_lower = full_text.to_ascii_lowercase();
        full_lower = raw_lower.split_whitespace().collect::<Vec<_>>().join(" ");
        &full_lower
    };

    // Check if followed by ( for keyword-before-paren handling
    let rest_after_keyword = &after_prefix[total_word_len..];
    let has_paren = has_trailing_paren(rest_after_keyword);

    // Classify the keyword based on state
    let (action, text) = classify_keyword(
        classify_key,
        lower,
        full_text,
        total_word_len,
        has_paren,
        state,
        after_prefix_str,
    );

    Some(LexResult {
        action,
        match_len: prefix_len + text.len(),
        prefix,
        token_text: text,
    })
}

/// Check if there's a `(` after optional whitespace.
#[inline]
fn has_trailing_paren(bytes: &[u8]) -> bool {
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() && bytes[i] != b'\n' {
        i += 1;
    }
    i < bytes.len() && bytes[i] == b'('
}

/// Classify a keyword (already lowercased) and return the appropriate action and text slice.
fn classify_keyword<'a>(
    full_lower: &str,
    first_word_lower: &str,
    full_text: &'a str,
    _total_len: usize,
    has_paren: bool,
    state: LexState,
    after_prefix_str: &'a str,
) -> (&'static Action, &'a str) {
    // Angle bracket types: array<, struct<, map<, table<
    // Check if identifier is followed by < (possibly with whitespace)
    // Must be before state-specific dispatch so it works in all states (e.g., Function)
    if matches!(first_word_lower, "array" | "struct" | "map" | "table") {
        let rest = &after_prefix_str[full_text.len()..];
        let mut i = 0;
        while i < rest.len()
            && rest.as_bytes()[i].is_ascii_whitespace()
            && rest.as_bytes()[i] != b'\n'
        {
            i += 1;
        }
        if i < rest.len() && rest.as_bytes()[i] == b'<' {
            // Include whitespace and < in the match
            let angle_text = &after_prefix_str[..full_text.len() + i + 1];
            return (&A_ANGLE_BRACKET_OPEN, angle_text);
        }
    }

    // State-specific keyword handling
    match state {
        LexState::Grant => return classify_grant_keyword(full_lower, full_text, has_paren),
        LexState::Function => return classify_function_keyword(full_lower, full_text, has_paren),
        LexState::Warehouse => return classify_warehouse_keyword(full_lower, full_text, has_paren),
        LexState::Clone => return classify_clone_keyword(full_lower, full_text, has_paren),
        _ => {} // Main state — fall through
    }

    // Main state keyword classification

    // CASE/END (statement start/end)
    if full_lower == "case" {
        return (&A_RESERVED_STATEMENT_START, full_text);
    }
    if full_lower == "end" {
        return (&A_RESERVED_STATEMENT_END, full_text);
    }

    // Functions that overlap with word operators: filter(), isnull(), offset()
    if has_paren && matches!(first_word_lower, "filter" | "isnull" | "offset") {
        // Include up through the ( in the match
        let rest = &after_prefix_str[full_text.len()..];
        let paren_pos = rest.find('(').unwrap();
        let text_with_paren = &after_prefix_str[..full_text.len() + paren_pos + 1];
        return (&A_KW_BEFORE_PAREN_NAME, text_with_paren);
    }

    // Star modifiers: except(), exclude(), replace() — before paren, if preceded by *
    if has_paren && matches!(first_word_lower, "except" | "exclude" | "replace") {
        let rest = &after_prefix_str[full_text.len()..];
        let paren_pos = rest.find('(').unwrap();
        let text_with_paren = &after_prefix_str[..full_text.len() + paren_pos + 1];
        return (&A_RESERVED_KW_BEFORE_PAREN_WORD_OP, text_with_paren);
    }

    // DDL functions before paren: get(), comment(), add(), remove(), list()
    if has_paren
        && matches!(
            first_word_lower,
            "get" | "comment" | "add" | "remove" | "list"
        )
    {
        let rest = &after_prefix_str[full_text.len()..];
        let paren_pos = rest.find('(').unwrap();
        let text_with_paren = &after_prefix_str[..full_text.len() + paren_pos + 1];
        return (&A_KW_BEFORE_PAREN_NAME, text_with_paren);
    }

    // SELECT INTO (special: must be before generic select)
    if full_lower == "select into" {
        return (&A_RESERVED_NONRESERVED_UNTERM, full_text);
    }

    // DELETE FROM (no HandleReservedKeyword wrapper!)
    if full_lower == "delete from" {
        return (&A_DELETE_FROM, full_text);
    }

    // FROM — always UntermKeyword
    if full_lower == "from" {
        return (&A_RESERVED_UNTERM, full_text);
    }

    // USING
    if full_lower == "using" {
        return (&A_RESERVED_UNTERM, full_text);
    }

    // ON
    if full_lower == "on" {
        return (&A_RESERVED_ON, full_text);
    }

    // SELECT TOP N — dynamic pattern (the N is a number, not a fixed keyword)
    if full_lower.starts_with("select top ") {
        return (&A_RESERVED_UNTERM, full_text);
    }

    // Unterminated keywords (SELECT, JOIN variants, WHERE, etc.)
    if is_unterm_keyword(full_lower) {
        return (&A_RESERVED_UNTERM, full_text);
    }

    // Word operators
    if is_word_operator(full_lower) {
        return (&A_RESERVED_WORD_OP, full_text);
    }

    // Boolean operators
    if matches!(full_lower, "and" | "or" | "not") {
        // "not" can be part of multi-word operators like "not in" — but those
        // are handled by try_multi_word already. If we get here, it's standalone.
        return (&A_RESERVED_BOOL_OP, full_text);
    }

    // Frame clause keywords (rows/range/groups followed by between/unbounded/current/N preceding/following)
    // This is complex — keep regex for now? No, let's handle it.
    if matches!(first_word_lower, "rows" | "range" | "groups") {
        if let Some(frame_len) = try_scan_frame_clause(after_prefix_str) {
            let frame_text = &after_prefix_str[..frame_len];
            return (&A_RESERVED_UNTERM, frame_text);
        }
    }

    // OFFSET (not before paren — that's handled above)
    if full_lower == "offset" {
        return (&A_RESERVED_UNTERM, full_text);
    }

    // Set operators
    if is_set_operator(full_lower) {
        return (&A_RESERVED_SET_OP, full_text);
    }

    // EXPLAIN (exact matches only — don't match explain_output etc.)
    if matches!(
        full_lower,
        "explain" | "explain analyze" | "explain verbose" | "explain using"
    ) {
        return (&A_RESERVED_NONRESERVED_UNTERM, full_text);
    }

    // GRANT/REVOKE → GRANT ruleset
    if matches!(first_word_lower, "grant" | "revoke") {
        return (&A_RESERVED_NONRESERVED_GRANT, full_text);
    }

    // DDL detection: create/alter/drop → specific or generic DDL state
    if matches!(first_word_lower, "create" | "alter" | "drop") {
        let rest_bytes = &after_prefix_str.as_bytes()[full_text.len()..];

        // Clone detection (most specific: create ... <obj_type> <name> clone)
        if first_word_lower == "create" && scan_rest_for_clone(rest_bytes) {
            return (&A_RESERVED_NONRESERVED_CLONE, full_text);
        }

        // Function DDL (multi-word matching already extends to include "function")
        if is_function_ddl(full_lower) {
            return (&A_RESERVED_NONRESERVED_FUNCTION, full_text);
        }

        // Warehouse DDL (multi-word matching already extends to include "warehouse")
        if is_warehouse_ddl(full_lower) {
            return (&A_RESERVED_NONRESERVED_WAREHOUSE, full_text);
        }

        // Look ahead for function/warehouse that multi-word matching missed
        if scan_rest_for_function(rest_bytes) {
            return (&A_RESERVED_NONRESERVED_FUNCTION, full_text);
        }
        if first_word_lower != "drop" && scan_rest_for_warehouse(rest_bytes) {
            return (&A_RESERVED_NONRESERVED_WAREHOUSE, full_text);
        }

        // Generic unsupported DDL
        return (&A_RESERVED_NONRESERVED_UNSUPPORTED, full_text);
    }

    // Other unsupported DDL keywords
    if is_unsupported_ddl(first_word_lower) {
        return (&A_RESERVED_NONRESERVED_UNSUPPORTED, full_text);
    }

    // Default: Name
    (&A_NAME, full_text)
}

/// Check if a (lowercased) keyword is an unterminated keyword.
fn is_unterm_keyword(kw: &str) -> bool {
    matches!(
        kw,
        "with recursive" | "with"
        | "select as struct" | "select as value" | "select all"
        | "select distinct" | "select"
        // select top N handled by multi-word
        | "global inner join" | "global left outer join" | "global left join"
        | "global right outer join" | "global right join" | "global full outer join"
        | "global full join" | "global any join" | "global join"
        | "any left outer join" | "any left join" | "any right outer join" | "any right join"
        | "any inner join" | "any full outer join" | "any full join"
        | "paste join"
        | "natural full outer join" | "natural full join" | "natural left outer join"
        | "natural left join" | "natural right outer join" | "natural right join"
        | "natural inner join" | "natural join"
        | "cross lateral join" | "cross join"
        | "left outer join" | "left semi join" | "left anti join" | "left asof join" | "left join"
        | "right outer join" | "right semi join" | "right anti join" | "right join"
        | "full outer join" | "full join"
        | "inner join" | "semi join" | "anti join" | "asof left join" | "asof join"
        | "positional join" | "any join" | "lateral join" | "join"
        | "lateral view outer" | "lateral view" | "lateral"
        | "prewhere" | "where"
        | "group by" | "cluster by" | "distribute by" | "sort by"
        | "having" | "qualify" | "window"
        | "order by" | "limit"
        | "fetch first" | "fetch next"
        | "for no key update" | "for key share" | "for update" | "for share"
        | "when" | "then" | "else"
        | "partition by"
        | "values" | "returning" | "into"
        | "match_recognize" | "connect" | "start with"
    )
}

/// Check if it's a word operator.
fn is_word_operator(kw: &str) -> bool {
    matches!(
        kw,
        "is not distinct from"
            | "is distinct from"
            | "not similar to"
            | "similar to"
            | "not ilike all"
            | "not ilike any"
            | "not like all"
            | "not like any"
            | "ilike all"
            | "ilike any"
            | "like all"
            | "like any"
            | "not between"
            | "not ilike"
            | "not like"
            | "not rlike"
            | "not regexp"
            | "not exists"
            | "global not in"
            | "global in"
            | "not in"
            | "is not"
            | "grouping sets"
            | "within group"
            | "respect nulls"
            | "ignore nulls"
            | "nulls first"
            | "nulls last"
            | "as"
            | "between"
            | "cube"
            | "exists"
            | "filter"
            | "ilike"
            | "isnull"
            | "in"
            | "interval"
            | "is"
            | "like"
            | "notnull"
            | "over"
            | "pivot"
            | "regexp"
            | "rlike"
            | "rollup"
            | "some"
            | "tablesample"
            | "unpivot"
            | "asc"
            | "desc"
    )
}

/// Check if it's a set operator.
fn is_set_operator(kw: &str) -> bool {
    matches!(
        kw,
        "union all by name"
            | "union by name"
            | "union all"
            | "union distinct"
            | "intersect all"
            | "intersect distinct"
            | "except all"
            | "except distinct"
            | "union all corresponding by"
            | "union corresponding by"
            | "union strict corresponding"
            | "union corresponding"
            | "intersect all corresponding"
            | "intersect corresponding"
            | "except all corresponding"
            | "except corresponding"
            | "union"
            | "intersect"
            | "except"
            | "minus"
    )
}

/// Check if it's a CREATE/ALTER FUNCTION DDL keyword.
fn is_function_ddl(kw: &str) -> bool {
    kw.starts_with("create") && kw.contains("function")
        || kw.starts_with("alter") && kw.contains("function")
        || kw.starts_with("drop") && kw.contains("function")
}

/// Check if it's a CREATE/ALTER WAREHOUSE DDL keyword.
fn is_warehouse_ddl(kw: &str) -> bool {
    kw.starts_with("create") && kw.contains("warehouse")
        || kw.starts_with("alter") && kw.contains("warehouse")
}

/// Check if first word starts an unsupported DDL.
/// Note: create/alter/drop and grant/revoke are handled separately above.
fn is_unsupported_ddl(first_word: &str) -> bool {
    matches!(
        first_word,
        "delete"
            | "insert"
            | "update"
            | "merge"
            | "truncate"
            | "rename"
            | "unset"
            | "use"
            | "execute"
            | "begin"
            | "commit"
            | "rollback"
            | "copy"
            | "clone"
            | "cluster"
            | "deallocate"
            | "declare"
            | "discard"
            | "do"
            | "export"
            | "handler"
            | "import"
            | "lock"
            | "move"
            | "prepare"
            | "reassign"
            | "repair"
            | "security"
            | "unload"
            | "validate"
            | "vacuum"
            | "analyze"
            | "refresh"
            | "list"
            | "remove"
            | "get"
            | "put"
            | "describe"
            | "show"
            | "comment"
            | "add"
            | "undrop"
            | "cache"
            | "clear"
    )
}

/// Try to scan a frame clause: "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" etc.
fn try_scan_frame_clause(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let len = bytes.len();

    // We know first word is rows/range/groups (already consumed in caller).
    // Scan it again here for simplicity.
    let mut pos = 0;
    // Skip first word
    while pos < len && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
        pos += 1;
    }

    // Skip whitespace
    while pos < len && bytes[pos].is_ascii_whitespace() && bytes[pos] != b'\n' {
        pos += 1;
    }

    // Optional "between"
    let has_between = pos + 7 <= len
        && bytes[pos..pos + 7].eq_ignore_ascii_case(b"between")
        && (pos + 7 >= len || !bytes[pos + 7].is_ascii_alphanumeric());
    if has_between {
        pos += 7;
        while pos < len && bytes[pos].is_ascii_whitespace() && bytes[pos] != b'\n' {
            pos += 1;
        }
    }

    // First bound: "unbounded preceding", "N preceding", "current row"
    if let Some(bound_len) = scan_frame_bound(&bytes[pos..]) {
        pos += bound_len;
    } else {
        return None;
    }

    // If we had "between", expect "and" + second bound
    if has_between {
        while pos < len && bytes[pos].is_ascii_whitespace() && bytes[pos] != b'\n' {
            pos += 1;
        }
        if pos + 3 <= len
            && bytes[pos..pos + 3].eq_ignore_ascii_case(b"and")
            && (pos + 3 >= len || !bytes[pos + 3].is_ascii_alphanumeric())
        {
            pos += 3;
            while pos < len && bytes[pos].is_ascii_whitespace() && bytes[pos] != b'\n' {
                pos += 1;
            }
            if let Some(bound_len) = scan_frame_bound(&bytes[pos..]) {
                pos += bound_len;
            } else {
                return None;
            }
        }
    }

    Some(pos)
}

/// Scan a single frame bound: "unbounded preceding", "N preceding", "unbounded following",
/// "N following", "current row". Returns byte length or None.
fn scan_frame_bound(bytes: &[u8]) -> Option<usize> {
    let len = bytes.len();
    // "current row"
    if len >= 11 && bytes[..7].eq_ignore_ascii_case(b"current") {
        let mut pos = 7;
        while pos < len && bytes[pos].is_ascii_whitespace() && bytes[pos] != b'\n' {
            pos += 1;
        }
        if pos + 3 <= len
            && bytes[pos..pos + 3].eq_ignore_ascii_case(b"row")
            && (pos + 3 >= len || !bytes[pos + 3].is_ascii_alphanumeric())
        {
            return Some(pos + 3);
        }
        return None;
    }
    // "unbounded preceding/following"
    if len >= 9 && bytes[..9].eq_ignore_ascii_case(b"unbounded") {
        let mut pos = 9;
        while pos < len && bytes[pos].is_ascii_whitespace() && bytes[pos] != b'\n' {
            pos += 1;
        }
        if pos + 9 <= len
            && bytes[pos..pos + 9].eq_ignore_ascii_case(b"preceding")
            && (pos + 9 >= len || !bytes[pos + 9].is_ascii_alphanumeric())
        {
            return Some(pos + 9);
        }
        if pos + 9 <= len
            && bytes[pos..pos + 9].eq_ignore_ascii_case(b"following")
            && (pos + 9 >= len || !bytes[pos + 9].is_ascii_alphanumeric())
        {
            return Some(pos + 9);
        }
        return None;
    }
    // "N preceding/following"
    let mut pos = 0;
    while pos < len && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == 0 {
        return None;
    }
    while pos < len && bytes[pos].is_ascii_whitespace() && bytes[pos] != b'\n' {
        pos += 1;
    }
    if pos + 9 <= len
        && bytes[pos..pos + 9].eq_ignore_ascii_case(b"preceding")
        && (pos + 9 >= len || !bytes[pos + 9].is_ascii_alphanumeric())
    {
        return Some(pos + 9);
    }
    if pos + 9 <= len
        && bytes[pos..pos + 9].eq_ignore_ascii_case(b"following")
        && (pos + 9 >= len || !bytes[pos + 9].is_ascii_alphanumeric())
    {
        return Some(pos + 9);
    }
    None
}

// ---- Data-mode lexers ----

/// Lex in FmtOff mode: only match fmt:on, data, or newline.
fn lex_fmt_off<'a>(
    _remaining: &'a str,
    prefix: &'a str,
    prefix_len: usize,
    after_prefix: &[u8],
    after_prefix_str: &'a str,
) -> Option<LexResult<'a>> {
    if after_prefix[0] == b'\n' {
        return Some(LexResult {
            action: &A_NEWLINE,
            match_len: prefix_len + 1,
            prefix,
            token_text: "\n",
        });
    }

    // Check for fmt:on (-- fmt: on or # fmt: on)
    if (after_prefix.len() >= 2 && after_prefix[0] == b'-' && after_prefix[1] == b'-')
        || after_prefix[0] == b'#'
    {
        let comment_len = scan_line_comment(after_prefix);
        let comment_text = &after_prefix[..comment_len];
        if let Some(action) = check_fmt_marker(comment_text) {
            if std::ptr::eq(action, &A_FMT_ON) {
                return Some(LexResult {
                    action: &A_FMT_ON,
                    match_len: prefix_len + comment_len,
                    prefix,
                    token_text: &after_prefix_str[..comment_len],
                });
            }
        }
    }

    // Data: everything up to newline
    let data_len = if let Some(nl_pos) = memchr(b'\n', after_prefix) {
        nl_pos
    } else {
        after_prefix.len()
    };
    if data_len > 0 {
        Some(LexResult {
            action: &A_DATA,
            match_len: prefix_len + data_len,
            prefix,
            token_text: &after_prefix_str[..data_len],
        })
    } else {
        None
    }
}

/// Lex in JinjaSetBlock mode: only match endset, data, or newline.
fn lex_jinja_set_block<'a>(
    _remaining: &'a str,
    prefix: &'a str,
    prefix_len: usize,
    after_prefix: &[u8],
    after_prefix_str: &'a str,
) -> Option<LexResult<'a>> {
    if after_prefix[0] == b'\n' {
        return Some(LexResult {
            action: &A_NEWLINE,
            match_len: prefix_len + 1,
            prefix,
            token_text: "\n",
        });
    }

    // Check for {% endset %}
    if after_prefix[0] == b'{' && after_prefix.len() >= 2 && after_prefix[1] == b'%' {
        if let Some((tag_len, action)) = scan_jinja_tag(after_prefix) {
            if std::ptr::eq(action, &A_JINJA_BLOCK_END) {
                // Verify it's actually endset
                let tag = &after_prefix_str[..tag_len];
                let lower = tag.to_ascii_lowercase();
                if lower.contains("endset") {
                    return Some(LexResult {
                        action: &A_JINJA_BLOCK_END,
                        match_len: prefix_len + tag_len,
                        prefix,
                        token_text: tag,
                    });
                }
            }
        }
    }

    // Data: non-whitespace followed by rest of line (matching regex `\S[^\n]*`)
    if !after_prefix[0].is_ascii_whitespace() {
        let data_len = if let Some(nl_pos) = memchr(b'\n', after_prefix) {
            nl_pos
        } else {
            after_prefix.len()
        };
        return Some(LexResult {
            action: &A_DATA,
            match_len: prefix_len + data_len,
            prefix,
            token_text: &after_prefix_str[..data_len],
        });
    }

    None
}

/// Lex in JinjaCallBlock mode: match endcall, nested call, data, or newline.
fn lex_jinja_call_block<'a>(
    _remaining: &'a str,
    prefix: &'a str,
    prefix_len: usize,
    after_prefix: &[u8],
    after_prefix_str: &'a str,
) -> Option<LexResult<'a>> {
    if after_prefix[0] == b'\n' {
        return Some(LexResult {
            action: &A_NEWLINE,
            match_len: prefix_len + 1,
            prefix,
            token_text: "\n",
        });
    }

    // Check for {% endcall %} or nested {% call ... %}
    if after_prefix[0] == b'{' && after_prefix.len() >= 2 && after_prefix[1] == b'%' {
        if let Some((tag_len, action)) = scan_jinja_tag(after_prefix) {
            let tag = &after_prefix_str[..tag_len];
            let lower = tag.to_ascii_lowercase();
            if lower.contains("endcall") {
                return Some(LexResult {
                    action: &A_JINJA_BLOCK_END,
                    match_len: prefix_len + tag_len,
                    prefix,
                    token_text: tag,
                });
            }
            if lower.contains("call") && !lower.contains("endcall") {
                return Some(LexResult {
                    action: &A_JINJA_BLOCK_START,
                    match_len: prefix_len + tag_len,
                    prefix,
                    token_text: tag,
                });
            }
            // Other jinja tags in call block → treat as data, fall through
            let _ = action;
        }
    }

    // Data: non-whitespace followed by rest of line
    if !after_prefix[0].is_ascii_whitespace() {
        let data_len = if let Some(nl_pos) = memchr(b'\n', after_prefix) {
            nl_pos
        } else {
            after_prefix.len()
        };
        return Some(LexResult {
            action: &A_DATA,
            match_len: prefix_len + data_len,
            prefix,
            token_text: &after_prefix_str[..data_len],
        });
    }

    None
}

/// Lex in Unsupported DDL mode: always rules + data.
fn lex_unsupported<'a>(
    _remaining: &'a str,
    prefix: &'a str,
    prefix_len: usize,
    after_prefix: &[u8],
    after_prefix_str: &'a str,
) -> Option<LexResult<'a>> {
    let b0 = after_prefix[0];

    // Newline
    if b0 == b'\n' {
        return Some(LexResult {
            action: &A_NEWLINE,
            match_len: prefix_len + 1,
            prefix,
            token_text: "\n",
        });
    }

    // Semicolon
    if b0 == b';' {
        return Some(LexResult {
            action: &A_SEMICOLON,
            match_len: prefix_len + 1,
            prefix,
            token_text: ";",
        });
    }

    // fmt:off/on comments
    if (b0 == b'-' && after_prefix.len() >= 2 && after_prefix[1] == b'-')
        || b0 == b'#'
        || (b0 == b'/' && after_prefix.len() >= 2 && after_prefix[1] == b'/')
    {
        let comment_len = scan_line_comment(after_prefix);
        let comment_text = &after_prefix[..comment_len];
        if let Some(action) = check_fmt_marker(comment_text) {
            return Some(LexResult {
                action,
                match_len: prefix_len + comment_len,
                prefix,
                token_text: &after_prefix_str[..comment_len],
            });
        }
        return Some(LexResult {
            action: &A_COMMENT,
            match_len: prefix_len + comment_len,
            prefix,
            token_text: &after_prefix_str[..comment_len],
        });
    }

    // Block comment
    if b0 == b'/' && after_prefix.len() >= 2 && after_prefix[1] == b'*' {
        let comment_len = scan_block_comment(after_prefix);
        return Some(LexResult {
            action: &A_COMMENT,
            match_len: prefix_len + comment_len,
            prefix,
            token_text: &after_prefix_str[..comment_len],
        });
    }

    // Jinja tags
    if b0 == b'{' {
        if let Some((tag_len, action)) = scan_jinja_tag(after_prefix) {
            return Some(LexResult {
                action,
                match_len: prefix_len + tag_len,
                prefix,
                token_text: &after_prefix_str[..tag_len],
            });
        }
    }

    // Quoted strings in unsupported mode → Data
    if b0 == b'"' || b0 == b'`' {
        let len = scan_string(after_prefix);
        return Some(LexResult {
            action: &A_DATA,
            match_len: prefix_len + len,
            prefix,
            token_text: &after_prefix_str[..len],
        });
    }

    // Single-quoted strings → Name (preserving standard string behavior)
    if b0 == b'\'' {
        if after_prefix.len() >= 3 && after_prefix[1] == b'\'' && after_prefix[2] == b'\'' {
            let len = scan_triple_string(after_prefix, b'\'');
            return Some(LexResult {
                action: &A_NAME,
                match_len: prefix_len + len,
                prefix,
                token_text: &after_prefix_str[..len],
            });
        }
        let len = scan_string(after_prefix);
        return Some(LexResult {
            action: &A_NAME,
            match_len: prefix_len + len,
            prefix,
            token_text: &after_prefix_str[..len],
        });
    }

    // Data: everything up to ; or newline
    let data_len = after_prefix
        .iter()
        .position(|&b| b == b';' || b == b'\n')
        .unwrap_or(after_prefix.len());
    if data_len > 0 {
        Some(LexResult {
            action: &A_RESERVED_DATA,
            match_len: prefix_len + data_len,
            prefix,
            token_text: &after_prefix_str[..data_len],
        })
    } else {
        None
    }
}

// ---- State-specific keyword classifiers ----

fn classify_grant_keyword<'a>(
    kw: &str,
    text: &'a str,
    _has_paren: bool,
) -> (&'static Action, &'a str) {
    if matches!(kw, "grant" | "on" | "to" | "from" | "cascade" | "restrict")
        || kw.starts_with("revoke")
        || kw.starts_with("with grant")
        || kw.starts_with("granted by")
    {
        return (&A_RESERVED_UNTERM, text);
    }
    // Semicolon resets handled by analyzer
    (&A_NAME, text)
}

fn classify_function_keyword<'a>(
    kw: &str,
    text: &'a str,
    _has_paren: bool,
) -> (&'static Action, &'a str) {
    // AS in function context → HandleDdlAs
    if kw == "as" {
        return (&A_RESERVED_DDL_AS, text);
    }
    // Word operators in function context
    if matches!(kw, "to" | "from" | "runtime_version") {
        return (&A_RESERVED_WORD_OP, text);
    }
    // Function-specific unterm keywords
    if is_function_unterm_keyword(kw) {
        return (&A_RESERVED_UNTERM, text);
    }
    (&A_NAME, text)
}

fn is_function_unterm_keyword(kw: &str) -> bool {
    matches!(
        kw,
        "language"
            | "transform"
            | "immutable"
            | "stable"
            | "volatile"
            | "strict"
            | "cost"
            | "rows"
            | "support"
            | "imports"
            | "packages"
            | "handler"
            | "target_path"
            | "options"
            | "cascade"
            | "restrict"
    ) || kw.starts_with("create") && kw.contains("function")
        || kw.starts_with("alter") && kw.contains("function")
        || kw.starts_with("drop") && kw.contains("function")
        || kw.starts_with("return")
        || kw.starts_with("leakproof")
        || kw.starts_with("not leakproof")
        || kw.starts_with("called on null")
        || kw.starts_with("returns null on null")
        || kw.starts_with("security")
        || kw.starts_with("parallel")
        || matches!(kw, "comment" | "set comment" | "unset comment")
        || matches!(kw, "api_integration" | "set api_integration")
        || matches!(kw, "headers" | "set headers")
        || matches!(kw, "context_headers" | "set context_headers")
        || matches!(kw, "max_batch_rows" | "set max_batch_rows")
        || matches!(kw, "compression" | "set compression")
        || matches!(kw, "request_translator" | "set request_translator")
        || matches!(kw, "response_translator" | "set response_translator")
        || kw == "remote with connection"
        || kw == "rename to"
        || kw == "owner to"
        || kw == "set schema"
        || matches!(kw, "depends on extension" | "no depends on extension")
        || matches!(kw, "set secure" | "unset secure")
        || kw == "not null"
        || kw == "null"
        || matches!(kw, "set" | "reset")
}

fn classify_warehouse_keyword<'a>(
    kw: &str,
    text: &'a str,
    _has_paren: bool,
) -> (&'static Action, &'a str) {
    if is_warehouse_unterm_keyword(kw) {
        return (&A_RESERVED_UNTERM, text);
    }
    (&A_NAME, text)
}

fn is_warehouse_unterm_keyword(kw: &str) -> bool {
    kw.starts_with("create") && kw.contains("warehouse")
        || kw.starts_with("alter") && kw.contains("warehouse")
        || matches!(kw, "suspend" | "rename to")
        || kw.starts_with("resume")
        || kw.starts_with("abort all queries")
        || kw.contains("warehouse_type")
        || kw.contains("warehouse_size")
        || kw.contains("max_cluster_count")
        || kw.contains("min_cluster_count")
        || kw.contains("scaling_policy")
        || kw.contains("auto_suspend")
        || kw.contains("auto_resume")
        || kw.contains("initially_suspended")
        || kw.contains("resource_monitor")
        || kw.contains("comment")
        || kw.contains("enable_query_acceleration")
        || kw.contains("query_acceleration_max_scale_factor")
        || kw.contains("tag")
        || kw.contains("max_concurrency_level")
        || kw.contains("statement_queued_timeout_in_seconds")
        || kw.contains("statement_timeout_in_seconds")
}

fn classify_clone_keyword<'a>(
    kw: &str,
    text: &'a str,
    _has_paren: bool,
) -> (&'static Action, &'a str) {
    if kw == "clone" || (kw.starts_with("create") && kw.contains("clone")) {
        return (&A_RESERVED_UNTERM, text);
    }
    // Word operators in clone context: at, before
    if matches!(kw, "at" | "before") {
        return (&A_RESERVED_WORD_OP, text);
    }
    (&A_NAME, text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_prefix_whitespace() {
        assert_eq!(skip_prefix_whitespace(b"  hello"), 2);
        assert_eq!(skip_prefix_whitespace(b"\thello"), 1);
        assert_eq!(skip_prefix_whitespace(b"\nhello"), 0); // newline is NOT whitespace prefix
        assert_eq!(skip_prefix_whitespace(b"hello"), 0);
    }

    #[test]
    fn test_scan_word() {
        assert_eq!(scan_word(b"select"), 6);
        assert_eq!(scan_word(b"my_table"), 8);
        assert_eq!(scan_word(b"foo123 bar"), 6);
        assert_eq!(scan_word(b"123abc"), 6); // numbers are word chars
    }

    #[test]
    fn test_scan_number() {
        assert_eq!(scan_number(b"42"), 2);
        assert_eq!(scan_number(b"3.14"), 4);
        assert_eq!(scan_number(b"1e10"), 4);
        assert_eq!(scan_number(b"0xFF"), 4);
        assert_eq!(scan_number(b"0b1010"), 6);
        assert_eq!(scan_number(b"0o777"), 5);
        assert_eq!(scan_number(b"1_000"), 5);
        assert_eq!(scan_number(b"10L"), 3);
    }

    #[test]
    fn test_scan_string() {
        assert_eq!(scan_string(b"'hello'"), 7);
        assert_eq!(scan_string(b"'it\\'s'"), 7);
        assert_eq!(scan_string(b"\"world\""), 7);
    }

    #[test]
    fn test_lex_simple_tokens() {
        let r = lex_one("  SELECT", LexState::Main).unwrap();
        assert_eq!(r.prefix, "  ");
        assert_eq!(r.token_text.to_ascii_lowercase(), "select");

        let r = lex_one(",", LexState::Main).unwrap();
        assert_eq!(r.token_text, ",");

        let r = lex_one(";", LexState::Main).unwrap();
        assert_eq!(r.token_text, ";");

        let r = lex_one("\n", LexState::Main).unwrap();
        assert_eq!(r.token_text, "\n");
    }

    #[test]
    fn test_lex_keyword_classification() {
        let r = lex_one("select", LexState::Main).unwrap();
        // Should be classified as HandleReservedKeyword wrapping UntermKeyword
        assert!(matches!(r.action, Action::HandleReservedKeyword { .. }));

        let r = lex_one("from", LexState::Main).unwrap();
        assert!(matches!(r.action, Action::HandleReservedKeyword { .. }));
    }

    #[test]
    fn test_lex_multi_word() {
        let r = lex_one("left outer join", LexState::Main).unwrap();
        assert_eq!(r.token_text.to_ascii_lowercase(), "left outer join");

        let r = lex_one("order by", LexState::Main).unwrap();
        assert_eq!(r.token_text.to_ascii_lowercase(), "order by");

        let r = lex_one("union all", LexState::Main).unwrap();
        assert_eq!(r.token_text.to_ascii_lowercase(), "union all");
    }

    #[test]
    fn test_lex_string_literals() {
        let r = lex_one("'hello'", LexState::Main).unwrap();
        assert_eq!(r.token_text, "'hello'");

        let r = lex_one("\"my_table\"", LexState::Main).unwrap();
        assert_eq!(r.token_text, "\"my_table\"");
    }

    #[test]
    fn test_lex_comments() {
        let r = lex_one("-- comment\n", LexState::Main).unwrap();
        assert_eq!(r.token_text, "-- comment");
        assert!(matches!(r.action, Action::AddComment));

        let r = lex_one("/* block */", LexState::Main).unwrap();
        assert_eq!(r.token_text, "/* block */");
    }

    #[test]
    fn test_lex_fmt_markers() {
        let r = lex_one("-- fmt: off", LexState::Main).unwrap();
        assert!(matches!(
            r.action,
            Action::AddNode {
                token_type: TokenType::FmtOff
            }
        ));

        let r = lex_one("-- fmt: on", LexState::Main).unwrap();
        assert!(matches!(
            r.action,
            Action::AddNode {
                token_type: TokenType::FmtOn
            }
        ));
    }

    #[test]
    fn test_lex_jinja() {
        let r = lex_one("{{ x }}", LexState::Main).unwrap();
        assert_eq!(r.token_text, "{{ x }}");

        let r = lex_one("{% if x %}", LexState::Main).unwrap();
        assert_eq!(r.token_text, "{% if x %}");
        assert!(matches!(r.action, Action::HandleJinjaBlockStart));
    }

    #[test]
    fn test_lex_operators() {
        let r = lex_one(">=", LexState::Main).unwrap();
        assert_eq!(r.token_text, ">=");

        let r = lex_one("::text", LexState::Main).unwrap();
        assert_eq!(r.token_text, "::");
    }

    #[test]
    fn test_lex_fmt_off_mode() {
        let r = lex_one("anything here", LexState::FmtOff).unwrap();
        assert_eq!(r.token_text, "anything here");
        assert!(matches!(
            r.action,
            Action::AddNode {
                token_type: TokenType::Data
            }
        ));

        let r = lex_one("-- fmt: on", LexState::FmtOff).unwrap();
        assert!(matches!(
            r.action,
            Action::AddNode {
                token_type: TokenType::FmtOn
            }
        ));
    }

    #[test]
    fn test_check_fmt_marker() {
        assert!(check_fmt_marker(b"-- fmt: off").is_some());
        assert!(check_fmt_marker(b"--fmt:off").is_some());
        assert!(check_fmt_marker(b"# fmt: on").is_some());
        assert!(check_fmt_marker(b"-- regular comment").is_none());
    }

    #[test]
    fn test_compound_operators() {
        assert_eq!(scan_compound_operator(b">="), 2);
        assert_eq!(scan_compound_operator(b"->"), 2);
        assert_eq!(scan_compound_operator(b"->>"), 3);
        assert_eq!(scan_compound_operator(b"->->"), 4);
        assert_eq!(scan_compound_operator(b"||"), 2);
        assert_eq!(scan_compound_operator(b"<=>"), 3);
        assert_eq!(scan_compound_operator(b"!="), 2);
    }

    #[test]
    fn test_dollar_string() {
        assert_eq!(scan_dollar_string(b"$$hello$$"), 9);
        assert_eq!(scan_dollar_string(b"$tag$hello$tag$"), 15);
    }

    #[test]
    fn test_frame_clause() {
        let r = lex_one(
            "rows between unbounded preceding and current row",
            LexState::Main,
        );
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(
            r.token_text.to_ascii_lowercase(),
            "rows between unbounded preceding and current row"
        );
    }

    #[test]
    fn test_select_top_multiline() {
        let r = lex_one("select\ntop\n25\n*\n", LexState::Main).unwrap();
        eprintln!("token_text: {:?}, match_len: {}", r.token_text, r.match_len);
        assert_eq!(r.token_text, "select\ntop\n25");
    }

    #[test]
    fn test_union_all_multiline() {
        let r = lex_one("union\nall\n", LexState::Main).unwrap();
        eprintln!("token_text: {:?}, match_len: {}", r.token_text, r.match_len);
        assert_eq!(r.token_text, "union\nall");
    }
}
