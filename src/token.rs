use compact_str::CompactString;

/// Position in source string (byte offset). u32 is sufficient (SQL files < 4GB)
/// and saves 8 bytes per Token vs usize on 64-bit platforms.
pub(crate) type Pos = u32;

/// All token types recognized by the lexer.
/// Mirrors Python sqlfmt's TokenType (31 variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TokenType {
    FmtOff,
    FmtOn,
    Data,
    JinjaStatement,
    JinjaExpression,
    JinjaBlockStart,
    JinjaBlockEnd,
    JinjaBlockKeyword,
    QuotedName,
    Comment,
    #[allow(dead_code)] // Defined for API completeness; not yet constructed by lexer
    CommentStart,
    #[allow(dead_code)] // Defined for API completeness; not yet constructed by lexer
    CommentEnd,
    Semicolon,
    StatementStart,
    StatementEnd,
    Star,
    Number,
    BracketOpen,
    BracketClose,
    DoubleColon,
    Colon,
    Operator,
    WordOperator,
    On,
    BooleanOperator,
    Comma,
    Dot,
    Newline,
    UntermKeyword,
    SetOperator,
    Name,
}

impl TokenType {
    pub(crate) fn is_jinja(self) -> bool {
        matches!(
            self,
            Self::JinjaStatement
                | Self::JinjaExpression
                | Self::JinjaBlockStart
                | Self::JinjaBlockEnd
                | Self::JinjaBlockKeyword
        )
    }

    pub(crate) fn divides_queries(self) -> bool {
        matches!(self, Self::Semicolon | Self::SetOperator)
    }

    pub(crate) fn is_opening_bracket(self) -> bool {
        matches!(self, Self::BracketOpen | Self::StatementStart)
    }

    pub(crate) fn is_closing_bracket(self) -> bool {
        matches!(self, Self::BracketClose | Self::StatementEnd)
    }

    pub(crate) fn is_always_operator(self) -> bool {
        matches!(
            self,
            Self::Operator | Self::WordOperator | Self::On | Self::DoubleColon | Self::Colon
        )
    }

    /// Tokens that should be lowercased in the formatted output.
    pub(crate) fn is_always_lowercased(self) -> bool {
        matches!(
            self,
            Self::UntermKeyword
                | Self::SetOperator
                | Self::StatementStart
                | Self::StatementEnd
                | Self::WordOperator
                | Self::On
                | Self::BooleanOperator
        )
    }

    /// Tokens that never have a space before them.
    pub(crate) fn is_never_preceded_by_space(self) -> bool {
        matches!(
            self,
            Self::Comma
                | Self::Dot
                | Self::Semicolon
                | Self::Newline
                | Self::BracketClose
                | Self::CommentEnd
                | Self::DoubleColon
                | Self::Colon
        )
    }

    /// Tokens preceded by a space unless after an opening bracket.
    pub(crate) fn is_preceded_by_space_except_after_open_bracket(self) -> bool {
        matches!(
            self,
            Self::Operator
                | Self::WordOperator
                | Self::On
                | Self::BooleanOperator
                | Self::SetOperator
                | Self::Star
                | Self::Number
                | Self::Comment
                | Self::CommentStart
                | Self::UntermKeyword
                | Self::FmtOff
                | Self::FmtOn
                | Self::Data
        )
    }

    pub(crate) fn is_possible_name(self) -> bool {
        matches!(self, Self::Name | Self::QuotedName | Self::Star)
    }

    /// Tokens that do not affect the "previous SQL context" for whitespace decisions.
    /// Matches Python's `does_not_set_prev_sql_context` which includes all jinja
    /// statement types (block start/keyword/end) plus newlines.
    pub(crate) fn does_not_set_prev_sql_context(self) -> bool {
        matches!(
            self,
            Self::Newline
                | Self::JinjaStatement
                | Self::JinjaBlockStart
                | Self::JinjaBlockKeyword
                | Self::JinjaBlockEnd
        )
    }
}

/// An immutable token produced by the lexer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) token_type: TokenType,
    pub(crate) prefix: CompactString,
    pub(crate) text: CompactString,
    pub(crate) spos: Pos,
    pub(crate) epos: Pos,
}

impl Token {
    pub(crate) fn new(
        token_type: TokenType,
        prefix: &str,
        text: &str,
        spos: Pos,
        epos: Pos,
    ) -> Self {
        Self {
            token_type,
            prefix: CompactString::from(prefix),
            text: CompactString::from(text),
            spos,
            epos,
        }
    }
}

#[cfg(test)]
#[path = "token_test.rs"]
mod tests;
