/// Position in source string (byte offset).
pub type Pos = usize;

/// All token types recognized by the lexer.
/// Mirrors Python sqlfmt's TokenType (31 variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenType {
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
    CommentStart,
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
    pub fn is_jinja_statement(self) -> bool {
        matches!(self, Self::JinjaStatement | Self::JinjaExpression)
    }

    pub fn is_jinja(self) -> bool {
        matches!(
            self,
            Self::JinjaStatement
                | Self::JinjaExpression
                | Self::JinjaBlockStart
                | Self::JinjaBlockEnd
                | Self::JinjaBlockKeyword
        )
    }

    pub fn divides_queries(self) -> bool {
        matches!(self, Self::Semicolon | Self::SetOperator)
    }

    pub fn is_opening_bracket(self) -> bool {
        matches!(self, Self::BracketOpen | Self::StatementStart)
    }

    pub fn is_closing_bracket(self) -> bool {
        matches!(self, Self::BracketClose | Self::StatementEnd)
    }

    pub fn is_always_operator(self) -> bool {
        matches!(
            self,
            Self::Operator | Self::WordOperator | Self::On | Self::DoubleColon | Self::Colon
        )
    }

    /// Tokens that should be lowercased in the formatted output.
    pub fn is_always_lowercased(self) -> bool {
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
    pub fn is_never_preceded_by_space(self) -> bool {
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
    pub fn is_preceded_by_space_except_after_open_bracket(self) -> bool {
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

    pub fn is_possible_name(self) -> bool {
        matches!(self, Self::Name | Self::QuotedName | Self::Star)
    }

    /// Tokens that do not affect the "previous SQL context" for whitespace decisions.
    /// Matches Python's `does_not_set_prev_sql_context` which includes all jinja
    /// statement types (block start/keyword/end) plus newlines.
    pub fn does_not_set_prev_sql_context(self) -> bool {
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
pub struct Token {
    pub token_type: TokenType,
    pub prefix: String,
    pub text: String,
    pub spos: Pos,
    pub epos: Pos,
}

impl Token {
    pub fn new(token_type: TokenType, prefix: &str, text: &str, spos: Pos, epos: Pos) -> Self {
        Self {
            token_type,
            prefix: prefix.to_string(),
            text: text.to_string(),
            spos,
            epos,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jinja_classification() {
        assert!(TokenType::JinjaStatement.is_jinja());
        assert!(TokenType::JinjaExpression.is_jinja());
        assert!(TokenType::JinjaBlockStart.is_jinja());
        assert!(TokenType::JinjaBlockEnd.is_jinja());
        assert!(TokenType::JinjaBlockKeyword.is_jinja());
        assert!(!TokenType::Name.is_jinja());
    }

    #[test]
    fn test_divides_queries() {
        assert!(TokenType::Semicolon.divides_queries());
        assert!(TokenType::SetOperator.divides_queries());
        assert!(!TokenType::Name.divides_queries());
    }

    #[test]
    fn test_bracket_classification() {
        assert!(TokenType::BracketOpen.is_opening_bracket());
        assert!(TokenType::StatementStart.is_opening_bracket());
        assert!(!TokenType::BracketClose.is_opening_bracket());

        assert!(TokenType::BracketClose.is_closing_bracket());
        assert!(TokenType::StatementEnd.is_closing_bracket());
        assert!(!TokenType::BracketOpen.is_closing_bracket());
    }

    #[test]
    fn test_always_lowercased() {
        assert!(TokenType::UntermKeyword.is_always_lowercased());
        assert!(TokenType::BooleanOperator.is_always_lowercased());
        assert!(!TokenType::Name.is_always_lowercased());
        assert!(!TokenType::Operator.is_always_lowercased());
    }

    #[test]
    fn test_token_creation() {
        let tok = Token::new(TokenType::Name, " ", "foo", 5, 8);
        assert_eq!(tok.token_type, TokenType::Name);
        assert_eq!(tok.prefix, " ");
        assert_eq!(tok.text, "foo");
        assert_eq!(tok.spos, 5);
        assert_eq!(tok.epos, 8);
    }
}
