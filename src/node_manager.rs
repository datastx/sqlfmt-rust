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
    /// Mirrors Python's NodeManager.create_node() which computes depth from
    /// previous_node's brackets, not from NodeManager state.
    pub fn create_node(
        &mut self,
        token: Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> Node {
        // Compute open_brackets and open_jinja_blocks from previous_node chain
        // This matches the Python pattern where depth propagates through nodes
        let (open_brackets, open_jinja_blocks) =
            self.compute_open_brackets(&token, previous_node, arena);

        // Compute formatting_disabled from previous_node
        let formatting_disabled = self.compute_formatting_disabled(&token, previous_node, arena);

        let (prefix, value) = if !formatting_disabled.is_empty() {
            // When formatting is disabled, preserve original whitespace and value
            (token.prefix.clone(), token.token.clone())
        } else {
            let prefix = self.compute_prefix(&token, previous_node, arena);
            let value = self.standardize_value(&token);
            (prefix, value)
        };

        Node {
            token,
            previous_node,
            prefix,
            value,
            open_brackets,
            open_jinja_blocks,
            formatting_disabled,
        }
    }

    /// Compute the list of open brackets for a new node.
    ///
    /// In Python sqlfmt, the Node's open_brackets includes both actual brackets
    /// AND unterm keywords (for depth tracking). But the NodeManager's open_brackets
    /// only contains actual brackets (BracketOpen, StatementStart), used by actions
    /// like HandleNonreservedTopLevelKeyword to decide behavior.
    fn compute_open_brackets(
        &mut self,
        token: &Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> (Vec<NodeIndex>, Vec<NodeIndex>) {
        // NODE's open_brackets: includes unterm keywords for depth
        let (mut node_brackets, mut node_jinja) = match previous_node {
            None => (Vec::new(), Vec::new()),
            Some(prev_idx) => {
                let prev = &arena[prev_idx];
                let mut ob = prev.open_brackets.clone();
                let mut oj = prev.open_jinja_blocks.clone();

                // Add previous node to brackets if it opens a scope
                if prev.is_unterm_keyword() || prev.is_opening_bracket() {
                    ob.push(prev_idx);
                } else if prev.is_opening_jinja_block() {
                    oj.push(prev_idx);
                }

                (ob, oj)
            }
        };

        // Handle tokens that reduce depth
        match token.token_type {
            TokenType::UntermKeyword | TokenType::SetOperator => {
                // Pop last unterm keyword if any (unterm keywords at same depth replace each other)
                if let Some(last) = node_brackets.last() {
                    if arena[*last].is_unterm_keyword() {
                        node_brackets.pop();
                    }
                }
            }
            TokenType::BracketClose | TokenType::StatementEnd => {
                // Pop until we find the matching opening bracket
                // First pop unterm keyword on top if any
                while let Some(last) = node_brackets.last() {
                    if arena[*last].is_unterm_keyword() {
                        node_brackets.pop();
                    } else {
                        break;
                    }
                }
                // Now pop the actual bracket
                node_brackets.pop();
            }
            TokenType::JinjaBlockEnd => {
                node_jinja.pop();
            }
            TokenType::Semicolon => {
                node_brackets.clear();
            }
            _ => {}
        }

        // NodeManager's open_brackets: ONLY actual brackets, not unterm keywords.
        // This is used by HandleNonreservedTopLevelKeyword to decide if FROM/USING
        // should be treated as keywords or names.
        self.open_brackets = node_brackets
            .iter()
            .filter(|&&idx| !arena[idx].is_unterm_keyword())
            .copied()
            .collect();
        self.open_jinja_blocks = node_jinja.clone();

        (node_brackets, node_jinja)
    }

    /// Compute formatting_disabled state from previous node.
    fn compute_formatting_disabled(
        &mut self,
        token: &Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> Vec<Token> {
        let mut formatting_disabled = match previous_node {
            None => Vec::new(),
            Some(prev_idx) => arena[prev_idx].formatting_disabled.clone(),
        };

        if matches!(token.token_type, TokenType::FmtOff | TokenType::Data) {
            formatting_disabled.push(token.clone());
        }

        if !formatting_disabled.is_empty() {
            if let Some(prev_idx) = previous_node {
                if matches!(
                    arena[prev_idx].token.token_type,
                    TokenType::FmtOn | TokenType::Data
                ) {
                    formatting_disabled.pop();
                }
            }
        }

        // Keep NodeManager state in sync
        self.formatting_disabled = formatting_disabled.clone();

        formatting_disabled
    }

    /// Open a bracket (called after node is added to arena with its index).
    pub fn push_bracket(&mut self, node_idx: NodeIndex) {
        self.open_brackets.push(node_idx);
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
    /// Mirrors Python's NodeManager.whitespace() exactly.
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

        // Look at the previous meaningful SQL token (skipping newlines/jinja statements)
        let (prev, extra_whitespace) = Self::get_previous_token(previous_node, arena);
        let prev_type = prev.map(|n| n.token.token_type);

        // No space after an open bracket or cast operator (::)
        if matches!(
            prev_type,
            Some(TokenType::BracketOpen) | Some(TokenType::DoublColon)
        ) {
            return String::new();
        }

        // Always a space before keywords/operators/etc. except after open bracket
        if tt.is_preceded_by_space_except_after_open_bracket() {
            return " ".to_string();
        }

        // Names preceded by dots or colons are namespaced identifiers — no space
        if tt.is_possible_name()
            && matches!(prev_type, Some(TokenType::Dot) | Some(TokenType::Colon))
        {
            return String::new();
        }

        // Numbers preceded by colons are simple slices — no space
        if tt == TokenType::Number && prev_type == Some(TokenType::Colon) {
            return String::new();
        }

        // Open brackets with `<` (BQ type definitions like array<int64>)
        if tt == TokenType::BracketOpen && token.token.contains('<') {
            if prev_type.is_some() && prev_type != Some(TokenType::BracketOpen) {
                return " ".to_string();
            } else {
                return String::new();
            }
        }

        // Open brackets that follow names/quoted names are function calls
        // Open brackets that follow closing brackets are array indexes
        // Open brackets that follow open brackets are nested brackets
        if tt == TokenType::BracketOpen
            && matches!(
                prev_type,
                Some(TokenType::Name)
                    | Some(TokenType::QuotedName)
                    | Some(TokenType::BracketOpen)
                    | Some(TokenType::BracketClose)
            )
        {
            return String::new();
        }

        // Open square brackets after colons are Databricks variant cols
        if tt == TokenType::BracketOpen && token.token == "[" && prev_type == Some(TokenType::Colon)
        {
            return String::new();
        }

        // Need a space before any other open bracket
        if tt == TokenType::BracketOpen {
            return " ".to_string();
        }

        // Jinja: respect original whitespace
        if tt.is_jinja() {
            if !token.prefix.is_empty() || extra_whitespace {
                return " ".to_string();
            } else {
                return String::new();
            }
        }

        // After a jinja expression, respect original whitespace
        if prev_type == Some(TokenType::JinjaExpression) {
            if !token.prefix.is_empty() || extra_whitespace {
                return " ".to_string();
            } else {
                return String::new();
            }
        }

        // Default: one space
        " ".to_string()
    }

    /// Walk backward through previous_node links, skipping tokens that
    /// don't set SQL context (newlines, jinja statements).
    /// Returns (previous_token, extra_whitespace).
    fn get_previous_token(prev_node: Option<NodeIndex>, arena: &[Node]) -> (Option<&Node>, bool) {
        match prev_node {
            None => (None, false),
            Some(idx) => {
                let node = &arena[idx];
                if node.token.token_type.does_not_set_prev_sql_context() {
                    let (prev, _) = Self::get_previous_token(node.previous_node, arena);
                    (prev, true) // extra_whitespace = true because we skipped
                } else {
                    (Some(node), false)
                }
            }
        }
    }

    /// Standardize the token value: lowercase keywords, normalize whitespace, preserve names.
    /// Mirrors Python's standardize_value which also normalizes internal whitespace
    /// in multi-word keywords (e.g., "ORDER  BY" => "order by").
    fn standardize_value(&self, token: &Token) -> String {
        let tt = token.token_type;

        if tt.is_always_lowercased() {
            // Normalize internal whitespace for multi-word keywords
            return token
                .token
                .to_ascii_lowercase()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
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

        // Brackets are now tracked via compute_open_brackets in create_node
        nm.open_brackets.pop();
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
