use compact_str::CompactString;
use smallvec::SmallVec;

use crate::token::{Token, TokenType};

/// Index into the node arena (Vec<Node>).
pub type NodeIndex = usize;

/// SmallVec type aliases used by NodeManager for internal bracket tracking.
pub type BracketVec = SmallVec<[NodeIndex; 8]>;
pub type JinjaBlockVec = SmallVec<[NodeIndex; 4]>;

/// A Node wraps a Token with formatting metadata: depth, open brackets,
/// open Jinja blocks, and a link to the previous node.
#[derive(Debug, Clone)]
pub struct Node {
    pub token: Token,
    pub previous_node: Option<NodeIndex>,
    pub prefix: CompactString,
    pub value: CompactString,
    /// SQL bracket depth (number of open brackets + unterm keywords).
    pub bracket_depth: u16,
    /// Jinja block nesting depth.
    pub jinja_depth: u16,
    /// Whether formatting is disabled (fmt:off region).
    pub formatting_disabled: bool,
}

impl Node {
    pub fn new(
        token: Token,
        previous_node: Option<NodeIndex>,
        prefix: CompactString,
        value: CompactString,
        bracket_depth: u16,
        jinja_depth: u16,
    ) -> Self {
        Self {
            token,
            previous_node,
            prefix,
            value,
            bracket_depth,
            jinja_depth,
            formatting_disabled: false,
        }
    }

    /// Depth is (sql_bracket_depth, jinja_block_depth).
    pub fn depth(&self) -> (usize, usize) {
        (self.bracket_depth as usize, self.jinja_depth as usize)
    }

    /// Formatted string: prefix + value.
    pub fn to_formatted_string(&self) -> String {
        let mut s = String::with_capacity(self.prefix.len() + self.value.len());
        s.push_str(&self.prefix);
        s.push_str(&self.value);
        s
    }

    /// Push formatted string (prefix + value) directly into the given buffer.
    /// Avoids allocating a temporary String.
    #[inline]
    pub fn push_formatted_to(&self, buf: &mut String) {
        buf.push_str(&self.prefix);
        buf.push_str(&self.value);
    }

    /// Character length of the formatted string.
    pub fn len(&self) -> usize {
        self.prefix.len() + self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // --- Token type classification ---

    pub fn is_unterm_keyword(&self) -> bool {
        self.token.token_type == TokenType::UntermKeyword
    }

    pub fn is_comma(&self) -> bool {
        self.token.token_type == TokenType::Comma
    }

    pub fn is_opening_bracket(&self) -> bool {
        self.token.token_type.is_opening_bracket()
    }

    pub fn is_closing_bracket(&self) -> bool {
        self.token.token_type.is_closing_bracket()
    }

    pub fn is_opening_jinja_block(&self) -> bool {
        matches!(
            self.token.token_type,
            TokenType::JinjaBlockStart | TokenType::JinjaBlockKeyword
        )
    }

    pub fn is_closing_jinja_block(&self) -> bool {
        self.token.token_type == TokenType::JinjaBlockEnd
    }

    pub fn is_jinja(&self) -> bool {
        self.token.token_type.is_jinja()
    }

    pub fn is_jinja_statement(&self) -> bool {
        self.token.token_type.is_jinja_statement()
    }

    pub fn is_boolean_operator(&self) -> bool {
        self.token.token_type == TokenType::BooleanOperator
    }

    pub fn is_newline(&self) -> bool {
        self.token.token_type == TokenType::Newline
    }

    pub fn is_multiline_jinja(&self) -> bool {
        self.token.token_type.is_jinja() && self.value.contains('\n')
    }

    pub fn divides_queries(&self) -> bool {
        self.token.token_type.divides_queries()
    }

    pub fn is_set_operator(&self) -> bool {
        self.token.token_type == TokenType::SetOperator
    }

    pub fn is_semicolon(&self) -> bool {
        self.token.token_type == TokenType::Semicolon
    }

    pub fn is_star(&self) -> bool {
        self.token.token_type == TokenType::Star
    }

    pub fn is_name(&self) -> bool {
        self.token.token_type == TokenType::Name
    }

    pub fn is_quoted_name(&self) -> bool {
        self.token.token_type == TokenType::QuotedName
    }

    pub fn is_dot(&self) -> bool {
        self.token.token_type == TokenType::Dot
    }

    pub fn is_comment(&self) -> bool {
        self.token.token_type == TokenType::Comment
    }

    pub fn is_fmt_off(&self) -> bool {
        self.token.token_type == TokenType::FmtOff
    }

    pub fn is_fmt_on(&self) -> bool {
        self.token.token_type == TokenType::FmtOn
    }

    // --- Context-dependent classification ---

    /// True if this STAR token acts as multiplication (not SELECT *).
    pub fn is_multiplication_star(&self, arena: &[Node]) -> bool {
        if self.token.token_type != TokenType::Star {
            return false;
        }
        match self.get_previous_sql_token(arena) {
            None => false,
            Some(t) => !matches!(
                t.token_type,
                TokenType::UntermKeyword
                    | TokenType::Comma
                    | TokenType::Dot
                    | TokenType::BracketOpen
                    | TokenType::StatementStart
            ),
        }
    }

    /// True if this bracket acts as an operator (e.g., array indexing with `[`).
    pub fn is_bracket_operator(&self, arena: &[Node]) -> bool {
        if self.token.token_type != TokenType::BracketOpen {
            return false;
        }
        match self.get_previous_sql_token(arena) {
            None => false,
            Some(t) => {
                if self.value == "[" {
                    matches!(
                        t.token_type,
                        TokenType::Name | TokenType::QuotedName | TokenType::BracketClose
                    )
                } else {
                    self.value == "("
                        && t.token_type == TokenType::BracketClose
                        && t.text.contains('>')
                }
            }
        }
    }

    /// True if this node acts as an operator in context.
    pub fn is_operator(&self, arena: &[Node]) -> bool {
        self.token.token_type.is_always_operator()
            || self.is_multiplication_star(arena)
            || self.is_bracket_operator(arena)
    }

    /// Walk backward through previous_node links, skipping NEWLINE and
    /// JINJA_STATEMENT, to find the previous "meaningful" SQL token.
    pub fn get_previous_sql_token<'a>(&self, arena: &'a [Node]) -> Option<&'a Token> {
        let mut idx = self.previous_node;
        while let Some(i) = idx {
            let node = &arena[i];
            if node.token.token_type.does_not_set_prev_sql_context() {
                idx = node.previous_node;
            } else {
                return Some(&node.token);
            }
        }
        None
    }

    /// Checks for a preceding BETWEEN operator at the same depth (for AND disambiguation).
    pub fn has_preceding_between_operator(&self, arena: &[Node]) -> bool {
        let my_depth = self.depth();
        let mut idx = self.previous_node;
        while let Some(i) = idx {
            let node = &arena[i];
            if node.depth() < my_depth {
                break;
            }
            if node.depth() == my_depth {
                if node.token.token_type == TokenType::WordOperator
                    && node.value.eq_ignore_ascii_case("between")
                {
                    return true;
                }
                if node.is_boolean_operator() {
                    break;
                }
            }
            idx = node.previous_node;
        }
        false
    }

    /// True if this is the AND that follows a BETWEEN (i.e., BETWEEN x AND y).
    pub fn is_the_and_after_between(&self, arena: &[Node]) -> bool {
        self.is_boolean_operator()
            && self.value.eq_ignore_ascii_case("and")
            && self.has_preceding_between_operator(arena)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Token;

    fn make_node(token_type: TokenType, value: &str, prev: Option<NodeIndex>) -> Node {
        Node::new(
            Token::new(token_type, "", value, 0, value.len() as u32),
            prev,
            CompactString::new(""),
            CompactString::from(value),
            0,
            0,
        )
    }

    #[test]
    fn test_depth_empty() {
        let node = make_node(TokenType::Name, "foo", None);
        assert_eq!(node.depth(), (0, 0));
    }

    #[test]
    fn test_depth_with_brackets() {
        let mut node = make_node(TokenType::Name, "foo", None);
        node.bracket_depth = 2;
        node.jinja_depth = 1;
        assert_eq!(node.depth(), (2, 1));
    }

    #[test]
    fn test_formatted_string() {
        let mut node = make_node(TokenType::Name, "foo", None);
        node.prefix = CompactString::from(" ");
        assert_eq!(node.to_formatted_string(), " foo");
        assert_eq!(node.len(), 4);
    }

    #[test]
    fn test_is_multiplication_star() {
        // Star after a name => multiplication
        let mut arena = Vec::new();
        arena.push(make_node(TokenType::Name, "a", None));
        let mut star = make_node(TokenType::Star, "*", Some(0));
        star.token = Token::new(TokenType::Star, "", "*", 2, 3);
        assert!(star.is_multiplication_star(&arena));

        // Star after SELECT => not multiplication
        let mut arena2 = Vec::new();
        arena2.push(make_node(TokenType::UntermKeyword, "select", None));
        let star2 = make_node(TokenType::Star, "*", Some(0));
        assert!(!star2.is_multiplication_star(&arena2));
    }

    #[test]
    fn test_get_previous_sql_token_skips_newline() {
        let mut arena = Vec::new();
        arena.push(make_node(TokenType::Name, "a", None));
        arena.push(make_node(TokenType::Newline, "\n", Some(0)));
        let node = make_node(TokenType::Name, "b", Some(1));
        let prev = node.get_previous_sql_token(&arena);
        assert!(prev.is_some());
        assert_eq!(prev.unwrap().text, "a");
    }

    #[test]
    fn test_is_the_between_operator() {
        // A WordOperator "between" should be identified correctly
        let node = make_node(TokenType::WordOperator, "between", None);
        assert_eq!(node.token.token_type, TokenType::WordOperator);
        assert!(node.value.eq_ignore_ascii_case("between"));

        // Case-insensitive check
        let node2 = make_node(TokenType::WordOperator, "BETWEEN", None);
        assert!(node2.value.eq_ignore_ascii_case("between"));

        // "like" is NOT between
        let node3 = make_node(TokenType::WordOperator, "like", None);
        assert!(!node3.value.eq_ignore_ascii_case("between"));
    }

    #[test]
    fn test_is_square_bracket_operator() {
        // Square bracket after a Name => bracket operator (array indexing)
        let mut arena = Vec::new();
        arena.push(make_node(TokenType::Name, "arr", None));
        let mut bracket = make_node(TokenType::BracketOpen, "[", Some(0));
        bracket.value = CompactString::from("[");
        assert!(bracket.is_bracket_operator(&arena));

        // Square bracket after QuotedName => bracket operator
        let mut arena2 = Vec::new();
        arena2.push(make_node(TokenType::QuotedName, "\"my_col\"", None));
        let mut bracket2 = make_node(TokenType::BracketOpen, "[", Some(0));
        bracket2.value = CompactString::from("[");
        assert!(bracket2.is_bracket_operator(&arena2));

        // Square bracket after BracketClose => bracket operator
        let mut arena3 = Vec::new();
        arena3.push(make_node(TokenType::BracketClose, "]", None));
        let mut bracket3 = make_node(TokenType::BracketOpen, "[", Some(0));
        bracket3.value = CompactString::from("[");
        assert!(bracket3.is_bracket_operator(&arena3));

        // Square bracket with no previous node => NOT bracket operator
        let arena4: Vec<Node> = Vec::new();
        let mut bracket4 = make_node(TokenType::BracketOpen, "[", None);
        bracket4.value = CompactString::from("[");
        assert!(!bracket4.is_bracket_operator(&arena4));
    }

    #[test]
    fn test_is_the_and_after_the_between_operator() {
        // Build: BETWEEN x AND y
        let mut arena = Vec::new();
        // index 0: between
        arena.push(make_node(TokenType::WordOperator, "between", None));
        // index 1: x (name)
        arena.push(make_node(TokenType::Name, "x", Some(0)));
        // index 2: and (boolean operator)
        arena.push(make_node(TokenType::BooleanOperator, "and", Some(1)));

        // The AND at index 2 should be recognized as "the AND after BETWEEN"
        assert!(arena[2].is_the_and_after_between(&arena));

        // A standalone AND without BETWEEN should NOT be
        let mut arena2 = Vec::new();
        arena2.push(make_node(TokenType::Name, "a", None));
        arena2.push(make_node(TokenType::BooleanOperator, "and", Some(0)));
        assert!(!arena2[1].is_the_and_after_between(&arena2));
    }

    #[test]
    fn test_is_operator_context() {
        // A WordOperator is always an operator
        let arena: Vec<Node> = Vec::new();
        let node = make_node(TokenType::WordOperator, "in", None);
        assert!(node.is_operator(&arena));

        // An Operator token is always an operator
        let node2 = make_node(TokenType::Operator, "+", None);
        assert!(node2.is_operator(&arena));

        // A Name is NOT an operator
        let node3 = make_node(TokenType::Name, "foo", None);
        assert!(!node3.is_operator(&arena));
    }

    #[test]
    fn test_node_classification_methods() {
        assert!(make_node(TokenType::UntermKeyword, "select", None).is_unterm_keyword());
        assert!(make_node(TokenType::Comma, ",", None).is_comma());
        assert!(make_node(TokenType::BracketOpen, "(", None).is_opening_bracket());
        assert!(make_node(TokenType::BracketClose, ")", None).is_closing_bracket());
        assert!(make_node(TokenType::Newline, "\n", None).is_newline());
        assert!(make_node(TokenType::Semicolon, ";", None).is_semicolon());
        assert!(make_node(TokenType::SetOperator, "union", None).is_set_operator());
        assert!(make_node(TokenType::Star, "*", None).is_star());
        assert!(make_node(TokenType::Name, "foo", None).is_name());
        assert!(make_node(TokenType::QuotedName, "\"bar\"", None).is_quoted_name());
        assert!(make_node(TokenType::Dot, ".", None).is_dot());
        assert!(make_node(TokenType::BooleanOperator, "and", None).is_boolean_operator());
    }

    #[test]
    fn test_divides_queries() {
        assert!(make_node(TokenType::Semicolon, ";", None).divides_queries());
        assert!(make_node(TokenType::SetOperator, "union all", None).divides_queries());
        assert!(!make_node(TokenType::Name, "foo", None).divides_queries());
    }

    #[test]
    fn test_is_multiline_jinja() {
        let mut node = make_node(TokenType::JinjaExpression, "{{ foo }}", None);
        assert!(!node.is_multiline_jinja());

        node.value = CompactString::from("{{ foo\n  bar }}");
        assert!(node.is_multiline_jinja());
    }

    #[test]
    fn test_is_opening_closing_jinja_block() {
        assert!(make_node(TokenType::JinjaBlockStart, "{% if x %}", None).is_opening_jinja_block());
        assert!(
            make_node(TokenType::JinjaBlockKeyword, "{% elif y %}", None).is_opening_jinja_block()
        );
        assert!(make_node(TokenType::JinjaBlockEnd, "{% endif %}", None).is_closing_jinja_block());
        assert!(!make_node(TokenType::JinjaExpression, "{{ x }}", None).is_opening_jinja_block());
        assert!(!make_node(TokenType::JinjaExpression, "{{ x }}", None).is_closing_jinja_block());
    }
}
