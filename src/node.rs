use compact_str::CompactString;
use smallvec::SmallVec;

use crate::token::{Token, TokenType};

/// Index into the node arena (Vec<Node>).
pub(crate) type NodeIndex = usize;

/// SmallVec type aliases used by NodeManager for internal bracket tracking.
pub(crate) type BracketVec = SmallVec<[NodeIndex; 8]>;
pub(crate) type JinjaBlockVec = SmallVec<[NodeIndex; 4]>;

/// A Node wraps a Token with formatting metadata: depth, open brackets,
/// open Jinja blocks, and a link to the previous node.
#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub(crate) token: Token,
    pub(crate) previous_node: Option<NodeIndex>,
    pub(crate) prefix: CompactString,
    pub(crate) value: CompactString,
    /// SQL bracket depth (number of open brackets + unterm keywords).
    pub(crate) bracket_depth: u16,
    /// Jinja block nesting depth.
    pub(crate) jinja_depth: u16,
    /// Whether formatting is disabled (fmt:off region).
    pub(crate) formatting_disabled: bool,
    /// Cached: whether this node acts as an operator in context.
    /// Computed once at creation to avoid repeated backward arena walks.
    pub(crate) is_operator: bool,
    /// Cached: whether this bracket acts as an operator (e.g., array indexing).
    pub(crate) is_bracket_operator: bool,
    /// Cached: whether this STAR token acts as multiplication.
    pub(crate) is_multiplication_star: bool,
}

impl Node {
    pub(crate) fn new(
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
            is_operator: false,
            is_bracket_operator: false,
            is_multiplication_star: false,
        }
    }

    /// Depth is (sql_bracket_depth, jinja_block_depth).
    pub(crate) fn depth(&self) -> (usize, usize) {
        (self.bracket_depth as usize, self.jinja_depth as usize)
    }

    /// Push formatted string (prefix + value) directly into the given buffer.
    /// Avoids allocating a temporary String.
    #[inline]
    pub(crate) fn push_formatted_to(&self, buf: &mut String) {
        buf.push_str(&self.prefix);
        buf.push_str(&self.value);
    }

    /// Character length of the formatted string.
    pub(crate) fn len(&self) -> usize {
        self.prefix.len() + self.value.len()
    }

    // --- Token type classification ---

    pub(crate) fn is_unterm_keyword(&self) -> bool {
        self.token.token_type == TokenType::UntermKeyword
    }

    pub(crate) fn is_comma(&self) -> bool {
        self.token.token_type == TokenType::Comma
    }

    pub(crate) fn is_opening_bracket(&self) -> bool {
        self.token.token_type.is_opening_bracket()
    }

    pub(crate) fn is_closing_bracket(&self) -> bool {
        self.token.token_type.is_closing_bracket()
    }

    pub(crate) fn is_opening_jinja_block(&self) -> bool {
        matches!(
            self.token.token_type,
            TokenType::JinjaBlockStart | TokenType::JinjaBlockKeyword
        )
    }

    pub(crate) fn is_closing_jinja_block(&self) -> bool {
        self.token.token_type == TokenType::JinjaBlockEnd
    }

    pub(crate) fn is_boolean_operator(&self) -> bool {
        self.token.token_type == TokenType::BooleanOperator
    }

    pub(crate) fn is_newline(&self) -> bool {
        self.token.token_type == TokenType::Newline
    }

    pub(crate) fn is_multiline_jinja(&self) -> bool {
        self.token.token_type.is_jinja() && self.value.contains('\n')
    }

    pub(crate) fn divides_queries(&self) -> bool {
        self.token.token_type.divides_queries()
    }

    // --- Context-dependent classification ---

    /// Compute is_multiplication_star (used during node creation to cache).
    pub(crate) fn compute_is_multiplication_star(&self, arena: &[Node]) -> bool {
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

    /// Compute is_bracket_operator (used during node creation to cache).
    pub(crate) fn compute_is_bracket_operator(&self, arena: &[Node]) -> bool {
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

    /// Walk backward through previous_node links, skipping NEWLINE and
    /// JINJA_STATEMENT, to find the previous "meaningful" SQL token.
    pub(crate) fn get_previous_sql_token<'a>(&self, arena: &'a [Node]) -> Option<&'a Token> {
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
    pub(crate) fn has_preceding_between_operator(&self, arena: &[Node]) -> bool {
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
    pub(crate) fn is_the_and_after_between(&self, arena: &[Node]) -> bool {
        self.is_boolean_operator()
            && self.value.eq_ignore_ascii_case("and")
            && self.has_preceding_between_operator(arena)
    }
}

#[cfg(test)]
#[path = "node_test.rs"]
mod tests;
