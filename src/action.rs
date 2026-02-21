use crate::token::TokenType;

/// All possible actions the lexer can take when a rule matches.
/// Uses enum dispatch for zero-cost abstraction instead of Python closures/partial.
///
/// Inner actions use `&'static Action` instead of `Box<Action>` because all rules
/// are constructed once in `LazyLock` statics and live for the entire program.
/// This eliminates heap allocation for every nested action and removes the need
/// to clone actions on each token match in the lexer hot path.
#[derive(Debug)]
pub enum Action {
    /// Add a node of the given type to the buffer.
    AddNode { token_type: TokenType },

    /// Add a node; fall back to alt_type on bracket error.
    SafeAddNode {
        token_type: TokenType,
        alt_token_type: TokenType,
    },

    /// Add a comment token.
    AddComment,

    /// Add a Jinja comment and signal end of ruleset.
    AddJinjaComment,

    /// Handle newline: flush node buffer into a Line.
    HandleNewline,

    /// Handle semicolon: reset rule stack and flush.
    HandleSemicolon,

    /// Handle number: disambiguate unary operator prefix.
    HandleNumber,

    /// Reserved keyword: check if preceded by DOT (then treat as NAME).
    HandleReservedKeyword { inner: &'static Action },

    /// Non-reserved top-level keyword: check bracket depth.
    HandleNonreservedTopLevelKeyword { inner: &'static Action },

    /// Handle SET operators (UNION, INTERSECT, EXCEPT, MINUS).
    HandleSetOperator,

    /// Handle DDL AS keyword.
    HandleDdlAs,

    /// Handle closing angle bracket (bracket vs comparison operator).
    HandleClosingAngleBracket,

    /// Handle Jinja block start (`{% if %}`, `{% for %}`, etc.).
    HandleJinjaBlockStart,

    /// Handle Jinja block keyword (`{% elif %}`, `{% else %}`).
    HandleJinjaBlockKeyword,

    /// Handle Jinja block end (`{% endif %}`, `{% endfor %}`).
    HandleJinjaBlockEnd,

    /// Handle simple Jinja expression/statement.
    HandleJinja { token_type: TokenType },

    /// Handle keyword that appears before `(`. The regex includes the trailing `(`
    /// but we only consume the keyword portion, leaving `(` for the bracket_open rule.
    /// Used for functions_that_overlap_with_word_operators, star_replace_exclude, etc.
    HandleKeywordBeforeParen { token_type: TokenType },

    /// Lex with an alternate ruleset (for nested constructs like CREATE FUNCTION body).
    LexRuleset { ruleset_name: &'static str },
}
