use crate::error::SqlfmtError;
use crate::node::{Node, NodeIndex};
use crate::token::{Token, TokenType};

/// NodeManager creates Nodes from Tokens, tracking bracket depth,
/// whitespace rules, and formatting state.
#[derive(Debug, Clone)]
pub struct NodeManager {
    pub case_sensitive_names: bool,
    pub open_brackets: Vec<NodeIndex>,
    pub open_jinja_blocks: Vec<NodeIndex>,
    pub formatting_disabled: Vec<Token>,
}

impl NodeManager {
    pub fn new(case_sensitive_names: bool) -> Self {
        Self {
            case_sensitive_names,
            open_brackets: Vec::new(),
            open_jinja_blocks: Vec::new(),
            formatting_disabled: Vec::new(),
        }
    }

    /// Create a Node from a Token, applying whitespace, casing, and depth rules.
    pub fn create_node(
        &mut self,
        token: Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> Node {
        let prefix = self.compute_prefix(&token, previous_node, arena);
        let value = self.standardize_value(&token);
        let open_brackets = self.open_brackets.clone();
        let open_jinja_blocks = self.open_jinja_blocks.clone();

        let mut node = Node::new(
            token.clone(),
            previous_node,
            prefix,
            value,
            open_brackets,
            open_jinja_blocks,
        );

        // Track bracket state changes
        if token.token_type.is_opening_bracket() {
            // Will be tracked by caller after adding to arena
        }
        if token.token_type.is_closing_bracket() {
            let _ = self.close_bracket(&token);
        }

        // Track formatting disabled state
        if !self.formatting_disabled.is_empty() {
            node.formatting_disabled = self.formatting_disabled.clone();
        }

        node
    }

    /// Open a bracket (called after node is added to arena with its index).
    pub fn push_bracket(&mut self, node_idx: NodeIndex) {
        self.open_brackets.push(node_idx);
    }

    /// Close the most recent bracket, validating it matches.
    fn close_bracket(&mut self, _token: &Token) -> std::result::Result<(), SqlfmtError> {
        if self.open_brackets.is_empty() {
            // Silently handle — error will be caught by analyzer
            return Ok(());
        }
        self.open_brackets.pop();
        Ok(())
    }

    /// Open a Jinja block.
    pub fn push_jinja_block(&mut self, node_idx: NodeIndex) {
        self.open_jinja_blocks.push(node_idx);
    }

    /// Close the most recent Jinja block.
    pub fn pop_jinja_block(&mut self) {
        self.open_jinja_blocks.pop();
    }

    /// Compute the whitespace prefix for a token.
    fn compute_prefix(
        &self,
        token: &Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> String {
        let tt = token.token_type;

        // No prefix for the very first token
        if previous_node.is_none() {
            return String::new();
        }

        // Tokens that are never preceded by a space
        if tt.is_never_preceded_by_space() {
            return String::new();
        }

        // Look at the previous meaningful token
        let prev = previous_node.and_then(|idx| {
            let mut i = Some(idx);
            while let Some(ii) = i {
                let n = &arena[ii];
                if n.token.token_type.does_not_set_prev_sql_context() {
                    i = n.previous_node;
                } else {
                    return Some(n);
                }
            }
            None
        });

        let prev_type = prev.map(|n| n.token.token_type);

        // After a DOT, most tokens don't get a space
        if prev_type == Some(TokenType::Dot) {
            return String::new();
        }

        // After an actual opening bracket ( [ <: certain tokens get no space.
        // Note: StatementStart (CASE) is a logical bracket for depth tracking
        // but should NOT suppress spaces like literal brackets do.
        if prev_type == Some(TokenType::BracketOpen)
            && tt.is_preceded_by_space_except_after_open_bracket()
        {
            return String::new();
        }

        // DoubleColon is tight-bound (no space before or after)
        if tt == TokenType::DoublColon || prev_type == Some(TokenType::DoublColon) {
            return String::new();
        }

        // Colon: no space before, one space after
        if tt == TokenType::Colon {
            return String::new();
        }

        // Name/QuotedName after actual opening bracket ( [ get no space
        if matches!(tt, TokenType::Name | TokenType::QuotedName | TokenType::Star | TokenType::Number)
            && prev_type == Some(TokenType::BracketOpen)
        {
            return String::new();
        }

        // Opening bracket after a name/quoted_name is a function call — no space
        if tt == TokenType::BracketOpen
            && matches!(
                prev_type,
                Some(TokenType::Name)
                    | Some(TokenType::QuotedName)
                    | Some(TokenType::StatementEnd) // END(...)
            )
        {
            return String::new();
        }

        // Default: one space
        " ".to_string()
    }

    /// Standardize the token value: lowercase keywords, preserve names.
    fn standardize_value(&self, token: &Token) -> String {
        let tt = token.token_type;

        if tt.is_always_lowercased() {
            return token.token.to_ascii_lowercase();
        }

        // For non-quoted names in case-insensitive mode, lowercase
        if !self.case_sensitive_names && tt == TokenType::Name {
            return token.token.to_ascii_lowercase();
        }

        token.token.clone()
    }

    /// Enable formatting (handle fmt:on).
    pub fn enable_formatting(&mut self) {
        self.formatting_disabled.clear();
    }

    /// Disable formatting (handle fmt:off).
    pub fn disable_formatting(&mut self, token: Token) {
        self.formatting_disabled.push(token);
    }

    /// Check if formatting is currently disabled.
    pub fn is_formatting_disabled(&self) -> bool {
        !self.formatting_disabled.is_empty()
    }

    /// Reset state (for new query).
    pub fn reset(&mut self) {
        self.open_brackets.clear();
        self.open_jinja_blocks.clear();
        self.formatting_disabled.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standardize_value_lowercases_keywords() {
        let nm = NodeManager::new(false);
        let token = Token::new(TokenType::UntermKeyword, "", "SELECT", 0, 6);
        assert_eq!(nm.standardize_value(&token), "select");
    }

    #[test]
    fn test_standardize_value_lowercases_names_when_insensitive() {
        let nm = NodeManager::new(false);
        let token = Token::new(TokenType::Name, "", "MyTable", 0, 7);
        assert_eq!(nm.standardize_value(&token), "mytable");
    }

    #[test]
    fn test_standardize_value_preserves_names_when_sensitive() {
        let nm = NodeManager::new(true);
        let token = Token::new(TokenType::Name, "", "MyTable", 0, 7);
        assert_eq!(nm.standardize_value(&token), "MyTable");
    }

    #[test]
    fn test_bracket_tracking() {
        let mut nm = NodeManager::new(false);
        assert!(nm.open_brackets.is_empty());
        nm.push_bracket(0);
        assert_eq!(nm.open_brackets.len(), 1);
        nm.push_bracket(5);
        assert_eq!(nm.open_brackets.len(), 2);

        let close_token = Token::new(TokenType::BracketClose, "", ")", 10, 11);
        nm.close_bracket(&close_token).unwrap();
        assert_eq!(nm.open_brackets.len(), 1);
    }

    #[test]
    fn test_formatting_disabled() {
        let mut nm = NodeManager::new(false);
        assert!(!nm.is_formatting_disabled());

        let off_token = Token::new(TokenType::FmtOff, "", "-- fmt: off", 0, 11);
        nm.disable_formatting(off_token);
        assert!(nm.is_formatting_disabled());

        nm.enable_formatting();
        assert!(!nm.is_formatting_disabled());
    }
}
