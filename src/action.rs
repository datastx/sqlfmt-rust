use crate::token::TokenType;

/// All possible actions the lexer can take when a rule matches.
/// Uses enum dispatch for zero-cost abstraction instead of Python closures/partial.
#[derive(Debug, Clone)]
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
    HandleReservedKeyword { inner: Box<Action> },

    /// Non-reserved top-level keyword: check bracket depth.
    HandleNonreservedTopLevelKeyword { inner: Box<Action> },

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

    /// Lex with an alternate ruleset (for nested constructs like CREATE FUNCTION body).
    LexRuleset { ruleset_name: String },
}
