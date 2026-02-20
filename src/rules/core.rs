/// Core lexing rules. These define the base rule set that all dialects extend.
/// Priority ordering: lower numbers match first.
///
/// Rule categories and priority ranges:
///   0-1:     fmt:off / fmt:on
///   100-120: Jinja start markers
///   200:     Quoted names / strings
///   299-320: Comments, PG operators
///   350:     Semicolons
///   400-450: Numbers, stars, operators, punctuation
///   500-510: Brackets
///   600:     Other identifiers
///   790-800: Angle brackets, operators
///   5000:    Fallback name
///   9000:    Newline
use crate::action::Action;
use crate::rule::Rule;
use crate::token::TokenType;

/// Build the ALWAYS rules — applied in every context.
pub fn always_rules() -> Vec<Rule> {
    vec![
        // fmt: off (use [^\S\n] instead of \s to avoid consuming newlines)
        Rule::new(
            "fmt_off",
            0,
            r"((--|#)[^\S\n]*fmt:[^\S\n]*off[^\S\n]*)",
            Action::AddNode {
                token_type: TokenType::FmtOff,
            },
        ),
        // fmt: on
        Rule::new(
            "fmt_on",
            1,
            r"((--|#)[^\S\n]*fmt:[^\S\n]*on[^\S\n]*)",
            Action::AddNode {
                token_type: TokenType::FmtOn,
            },
        ),
        // Jinja comment: {# ... #}
        Rule::new(
            "jinja_comment",
            110,
            r"(\{#[\s\S]*?#\})",
            Action::AddComment,
        ),
        // Jinja expression: {{ ... }}
        Rule::new(
            "jinja_expression",
            115,
            r"(\{\{-?[\s\S]*?-?\}\})",
            Action::HandleJinja {
                token_type: TokenType::JinjaExpression,
            },
        ),
        // Jinja block tags: specific patterns matched BEFORE the generic jinja_statement.
        // These enable Jinja block indentation tracking (open_jinja_blocks depth).
        // {% set x %} (block set - no = sign)
        Rule::new(
            "jinja_set_block_start",
            116,
            r"(\{%-?\s*set\s+[^=]+?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endset %}
        Rule::new(
            "jinja_endset",
            116,
            r"(\{%-?\s*endset\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% if ... %}
        Rule::new(
            "jinja_if_block_start",
            117,
            r"(\{%-?\s*if\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% elif ... %}
        Rule::new(
            "jinja_elif",
            117,
            r"(\{%-?\s*elif\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockKeyword,
        ),
        // {% else %}
        Rule::new(
            "jinja_else",
            117,
            r"(\{%-?\s*else\s*-?%\})",
            Action::HandleJinjaBlockKeyword,
        ),
        // {% endif %}
        Rule::new(
            "jinja_endif",
            117,
            r"(\{%-?\s*endif\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% for ... %}
        Rule::new(
            "jinja_for_block_start",
            118,
            r"(\{%-?\s*for\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endfor %}
        Rule::new(
            "jinja_endfor",
            118,
            r"(\{%-?\s*endfor\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% macro ... %}
        Rule::new(
            "jinja_macro_start",
            118,
            r"(\{%-?\s*macro\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endmacro %}
        Rule::new(
            "jinja_endmacro",
            118,
            r"(\{%-?\s*endmacro\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% test ... %}
        Rule::new(
            "jinja_test_start",
            118,
            r"(\{%-?\s*test\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endtest %}
        Rule::new(
            "jinja_endtest",
            118,
            r"(\{%-?\s*endtest\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% snapshot ... %}
        Rule::new(
            "jinja_snapshot_start",
            119,
            r"(\{%-?\s*snapshot\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endsnapshot %}
        Rule::new(
            "jinja_endsnapshot",
            119,
            r"(\{%-?\s*endsnapshot\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% call ... %}
        Rule::new(
            "jinja_call_start",
            119,
            r"(\{%-?\s*call(?:\(.*?\))?\s+\w+[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endcall %}
        Rule::new(
            "jinja_endcall",
            119,
            r"(\{%-?\s*endcall\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% materialization ... %}
        Rule::new(
            "jinja_materialization_start",
            119,
            r"(\{%-?\s*materialization\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endmaterialization %}
        Rule::new(
            "jinja_endmaterialization",
            119,
            r"(\{%-?\s*endmaterialization\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // Jinja statement: {% ... %} (generic fallback for all other {% %} tags)
        Rule::new(
            "jinja_statement",
            120,
            r"(\{%-?[\s\S]*?-?%\})",
            Action::HandleJinja {
                token_type: TokenType::JinjaStatement,
            },
        ),
        // Single-quoted string
        Rule::new(
            "single_quoted_string",
            200,
            r"('(?:[^'\\]|\\.)*')",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        // Double-quoted name
        Rule::new(
            "double_quoted_name",
            201,
            r#"("(?:[^"\\]|\\.)*")"#,
            Action::AddNode {
                token_type: TokenType::QuotedName,
            },
        ),
        // Backtick-quoted name
        Rule::new(
            "backtick_quoted_name",
            202,
            r"(`(?:[^`\\]|\\.)*`)",
            Action::AddNode {
                token_type: TokenType::QuotedName,
            },
        ),
        // Dollar-quoted string (PostgreSQL)
        Rule::new(
            "dollar_quoted_string",
            205,
            r"(\$[a-zA-Z_]*\$[\s\S]*?\$[a-zA-Z_]*\$)",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        // Line comment: --, //, or # (but not fmt:off/on)
        Rule::new(
            "line_comment",
            300,
            r"(--[^\n]*|//[^\n]*|#[^\n]*)",
            Action::AddComment,
        ),
        // Block comment start: /*
        Rule::new(
            "block_comment",
            310,
            r"(/\*[\s\S]*?\*/)",
            Action::AddComment,
        ),
        // Semicolon
        Rule::new("semicolon", 350, r"(;)", Action::HandleSemicolon),
        // Newline
        Rule::new("newline", 9000, r"(\n)", Action::HandleNewline),
    ]
}

/// Build the CORE rules — applied in standard SQL contexts.
/// Extends ALWAYS rules.
pub fn core_rules() -> Vec<Rule> {
    let mut rules = always_rules();

    rules.extend(vec![
        // PostgreSQL hash operators: #>>, #>, #-
        Rule::new(
            "pg_operator",
            299,
            r"(#>>|#>|#-)",
            Action::AddNode {
                token_type: TokenType::Operator,
            },
        ),
        // Binary literals: 0b1010
        Rule::new(
            "binary_literal",
            396,
            r"(0[bB][01]+)\b",
            Action::HandleNumber,
        ),
        // Octal literals: 0o777
        Rule::new(
            "octal_literal",
            397,
            r"(0[oO][0-7]+)\b",
            Action::HandleNumber,
        ),
        // Leading-dot decimals: .5, .123
        Rule::new(
            "leading_dot_number",
            398,
            r"(\.\d[\d_]*(?:[eE][+-]?\d[\d_]*)?(?:bd|d|f)?)\b",
            Action::HandleNumber,
        ),
        // Hex literals: 0x...
        Rule::new(
            "hex_literal",
            399,
            r"(0[xX][0-9a-fA-F]+)\b",
            Action::HandleNumber,
        ),
        // Spark integer literal suffixes: 10L, 5S, 3Y
        Rule::new(
            "spark_int_literal",
            400,
            r"(\d[\d_]*[lLsSkKyY])\b",
            Action::HandleNumber,
        ),
        // Scientific notation numbers (with optional Spark suffix)
        Rule::new(
            "scientific_number",
            401,
            r"(\d[\d_]*(?:\.[\d_]*)?[eE][+-]?\d[\d_]*(?:bd|d|f)?)\b",
            Action::HandleNumber,
        ),
        // Decimal numbers (with optional Spark suffix)
        // No \b required — the dot naturally separates from identifiers,
        // and this handles trailing-dot decimals like "123." (Spark syntax).
        Rule::new(
            "decimal_number",
            402,
            r"(\d[\d_]*\.[\d_]*(?:bd|d|f)?)",
            Action::HandleNumber,
        ),
        // Integer numbers
        Rule::new(
            "integer_number",
            404,
            r"(\d[\d_]*)\b",
            Action::HandleNumber,
        ),
        // Star (SELECT *, multiplication, etc.)
        Rule::new(
            "star",
            410,
            r"(\*)",
            Action::AddNode {
                token_type: TokenType::Star,
            },
        ),
        // Double colon (PostgreSQL cast)
        Rule::new(
            "double_colon",
            420,
            r"(::)",
            Action::AddNode {
                token_type: TokenType::DoublColon,
            },
        ),
        // Walrus operator :=
        Rule::new(
            "walrus",
            421,
            r"(:=)",
            Action::AddNode {
                token_type: TokenType::Operator,
            },
        ),
        // Colon (single : only — :: is matched by double_colon at priority 420,
        // walrus := at priority 421, so this only matches lone colons)
        Rule::new(
            "colon",
            430,
            r"(:)",
            Action::AddNode {
                token_type: TokenType::Colon,
            },
        ),
        // Comma
        Rule::new(
            "comma",
            440,
            r"(,)",
            Action::AddNode {
                token_type: TokenType::Comma,
            },
        ),
        // Dot
        Rule::new(
            "dot",
            450,
            r"(\.)",
            Action::AddNode {
                token_type: TokenType::Dot,
            },
        ),
        // Opening brackets: (, [, {
        Rule::new(
            "bracket_open",
            500,
            r"(\(|\[|\{)",
            Action::AddNode {
                token_type: TokenType::BracketOpen,
            },
        ),
        // Closing brackets: ), ], }
        Rule::new(
            "bracket_close",
            510,
            r"(\)|\]|\})",
            Action::AddNode {
                token_type: TokenType::BracketClose,
            },
        ),
        // Angle bracket: array<, struct<, map<
        Rule::new(
            "angle_bracket_open",
            505,
            r"(array|struct|map)\s*(<)",
            Action::SafeAddNode {
                token_type: TokenType::BracketOpen,
                alt_token_type: TokenType::Name,
            },
        ),
        // Other identifiers: @variable, $variable, $1, %(name)s, ?1
        Rule::new(
            "at_identifier",
            600,
            r"(@\w+)",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        Rule::new(
            "dollar_identifier",
            601,
            r"(\$\w+)",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        // PostgreSQL positional param: $1, $23
        Rule::new(
            "pg_positional_param",
            602,
            r"(\$\d+)\b",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        // psycopg format param: %(name)s, %s
        Rule::new(
            "psycopg_param",
            603,
            r"(%(?:\([^%()]+\))?s)",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        // Bun/JDBC positional param: ?1, ?23
        Rule::new(
            "bun_param",
            604,
            r"(\?\d+)",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        // Multi-char operators (must come before angle_bracket_close and single-char ops)
        // Includes PostgreSQL geometric, JSON, text-search, and containment operators
        // Also includes %% (psycopg escaped percent)
        Rule::new(
            "compound_operator",
            785,
            r"(>=|=>|>>|<>|<=|<=>|!=|==|<<|->->|->|->>|<->|@-@|<#>|@>|<@|@@|\?\||\?&|-\|-|\|\|/|\|/|\|\||&&|\*\*|!~\*|!~|~\*|!!=|%%)",
            Action::AddNode {
                token_type: TokenType::Operator,
            },
        ),
        // Closing angle bracket: > (needs special handling - could be bracket or operator)
        Rule::new(
            "angle_bracket_close",
            790,
            r"(>)",
            Action::HandleClosingAngleBracket,
        ),
        // Operators (single-char and remaining multi-char)
        Rule::new(
            "operator",
            800,
            r"(~|[+\-/%&|^?!])",
            Action::AddNode {
                token_type: TokenType::Operator,
            },
        ),
        // Comparison: < (only if not already consumed by angle bracket rules)
        Rule::new(
            "less_than",
            801,
            r"(<)",
            Action::AddNode {
                token_type: TokenType::Operator,
            },
        ),
        // Equals sign
        Rule::new(
            "equals",
            802,
            r"(=)",
            Action::AddNode {
                token_type: TokenType::Operator,
            },
        ),
        // Fallback: any identifier
        Rule::new(
            "name",
            5000,
            r"(\w+)",
            Action::AddNode {
                token_type: TokenType::Name,
            },
        ),
        // Note: Non-newline whitespace is captured by group 1 of every rule's
        // pattern prefix `([^\S\n]*)`, so no explicit whitespace rule is needed.
    ]);

    rules
}

/// Build the FMT_OFF rules — applied when `-- fmt: off` is active.
/// Only matches `-- fmt: on` to re-enable, otherwise treats all content as Data.
pub fn fmt_off_rules() -> Vec<Rule> {
    vec![
        // fmt: on (exits fmt:off mode)
        Rule::new(
            "fmt_on",
            1,
            r"((--|#)[^\S\n]*fmt:[^\S\n]*on[^\S\n]*)",
            Action::AddNode {
                token_type: TokenType::FmtOn,
            },
        ),
        // Data: everything else up to newline (preserves original text verbatim)
        Rule::new(
            "data",
            5000,
            r"([^\n]+)",
            Action::AddNode {
                token_type: TokenType::Data,
            },
        ),
        // Newline
        Rule::new("newline", 9000, r"(\n)", Action::HandleNewline),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_rules_created() {
        let rules = always_rules();
        assert!(!rules.is_empty());
        // Should have fmt_off, fmt_on, jinja, strings, comments, semicolon, newline
        assert!(rules.iter().any(|r| r.name == "fmt_off"));
        assert!(rules.iter().any(|r| r.name == "newline"));
    }

    #[test]
    fn test_core_rules_created() {
        let rules = core_rules();
        assert!(rules.len() > 20);
        assert!(rules.iter().any(|r| r.name == "star"));
        assert!(rules.iter().any(|r| r.name == "comma"));
        assert!(rules.iter().any(|r| r.name == "bracket_open"));
        assert!(rules.iter().any(|r| r.name == "name"));
    }

    #[test]
    fn test_rule_priority_ordering() {
        let mut rules = core_rules();
        rules.sort_by_key(|r| r.priority);
        // Verify fmt_off has lowest priority
        assert_eq!(rules[0].name, "fmt_off");
    }

    #[test]
    fn test_number_patterns() {
        let rules = core_rules();
        let hex_rule = rules.iter().find(|r| r.name == "hex_literal").unwrap();
        assert!(hex_rule.pattern.is_match("0xFF"));
        assert!(hex_rule.pattern.is_match("0x1A2B"));

        let int_rule = rules.iter().find(|r| r.name == "integer_number").unwrap();
        assert!(int_rule.pattern.is_match("42"));
        assert!(int_rule.pattern.is_match("1_000"));
    }

    #[test]
    fn test_string_patterns() {
        let rules = core_rules();
        let sq = rules
            .iter()
            .find(|r| r.name == "single_quoted_string")
            .unwrap();
        assert!(sq.pattern.is_match("'hello'"));
        assert!(sq.pattern.is_match("'it\\'s'"));

        let dq = rules
            .iter()
            .find(|r| r.name == "double_quoted_name")
            .unwrap();
        assert!(dq.pattern.is_match("\"my_table\""));
    }
}
