use crate::action::Action;
use crate::comment::Comment;
use crate::error::SqlfmtError;
use crate::line::Line;
use crate::node::{Node, NodeIndex};
use crate::node_manager::NodeManager;
use crate::query::Query;
use crate::rule::Rule;
use crate::token::{Token, TokenType};

/// The regex-based lexer. Parses SQL source strings into Queries.
/// Maintains buffers and a rule stack for nested lexing contexts.
pub struct Analyzer {
    pub line_length: usize,
    pub node_manager: NodeManager,
    pub arena: Vec<Node>,

    rule_stack: Vec<Vec<Rule>>,
    node_buffer: Vec<NodeIndex>,
    comment_buffer: Vec<Comment>,
    line_buffer: Vec<Line>,
    pos: usize,
}

impl Analyzer {
    pub fn new(rules: Vec<Rule>, node_manager: NodeManager, line_length: usize) -> Self {
        Self {
            line_length,
            node_manager,
            rule_stack: vec![rules],
            node_buffer: Vec::new(),
            comment_buffer: Vec::new(),
            line_buffer: Vec::new(),
            arena: Vec::new(),
            pos: 0,
        }
    }

    /// Main entry point: parse source string into a Query.
    pub fn parse_query(&mut self, source: &str) -> Result<Query, SqlfmtError> {
        self.clear_buffers();
        self.lex(source)?;
        self.flush_line_buffer();
        Ok(self.build_query(source))
    }

    /// Lex the source string by trying rules until all input is consumed.
    fn lex(&mut self, source: &str) -> Result<(), SqlfmtError> {
        while self.pos < source.len() {
            self.lex_one(source)?;
        }
        Ok(())
    }

    /// Try each rule in priority order; execute the first match.
    fn lex_one(&mut self, source: &str) -> Result<(), SqlfmtError> {
        let remaining = &source[self.pos..];
        if remaining.is_empty() {
            return Ok(());
        }

        let rules = self.rule_stack.last().unwrap().clone();
        for rule in &rules {
            if let Some(captures) = rule.pattern.captures(remaining) {
                self.execute_action(&rule.action, &captures, source)?;
                return Ok(());
            }
        }

        Err(SqlfmtError::Parsing {
            position: self.pos,
            message: format!(
                "No rule matched near: {:?}",
                &remaining[..remaining.len().min(40)]
            ),
        })
    }

    /// Dispatch an action based on what the rule matched.
    fn execute_action(
        &mut self,
        action: &Action,
        captures: &regex::Captures,
        _source: &str,
    ) -> Result<(), SqlfmtError> {
        let full_match = captures.get(0).unwrap();
        let prefix = captures.get(1).map(|m| m.as_str()).unwrap_or("");
        let token_text = captures.get(2).map(|m| m.as_str()).unwrap_or("");
        let match_len = full_match.as_str().len();

        match action {
            Action::AddNode { token_type } => {
                self.add_node(prefix, token_text, *token_type);
                self.pos += match_len;
            }

            Action::SafeAddNode {
                token_type,
                alt_token_type,
            } => {
                // Try primary type; fall back to alt_token_type on mismatch
                if *token_type == TokenType::BracketOpen && token_text.contains('<') {
                    // Handle "array<", "struct<", "map<" — split into name + bracket
                    let angle_pos = token_text.find('<').unwrap();
                    let name_part = &token_text[..angle_pos].trim();
                    let bracket_part = "<";
                    if !name_part.is_empty() {
                        self.add_node(prefix, name_part, TokenType::Name);
                    }
                    self.add_node("", bracket_part, TokenType::BracketOpen);
                    let last_idx = self.arena.len() - 1;
                    self.node_manager.push_bracket(last_idx);
                } else if *token_type == TokenType::StatementEnd {
                    // END: check if there is a matching CASE (StatementStart) in open brackets
                    let has_matching_case = self
                        .node_manager
                        .open_brackets
                        .iter()
                        .any(|&idx| self.arena[idx].token.token_type == TokenType::StatementStart);
                    if has_matching_case {
                        self.add_node(prefix, token_text, TokenType::StatementEnd);
                    } else {
                        self.add_node(prefix, token_text, *alt_token_type);
                    }
                } else {
                    self.add_node(prefix, token_text, *token_type);
                    if token_type.is_opening_bracket() {
                        let last_idx = self.arena.len() - 1;
                        self.node_manager.push_bracket(last_idx);
                    }
                }
                self.pos += match_len;
            }

            Action::AddComment => {
                self.add_comment(prefix, token_text);
                self.pos += match_len;
            }

            Action::AddJinjaComment => {
                self.add_comment(prefix, token_text);
                self.pos += match_len;
            }

            Action::HandleNewline => {
                self.flush_line_buffer();
                self.pos += match_len;
            }

            Action::HandleSemicolon => {
                self.add_node(prefix, token_text, TokenType::Semicolon);
                self.flush_line_buffer();
                // Reset rule stack to base
                while self.rule_stack.len() > 1 {
                    self.rule_stack.pop();
                }
                self.node_manager.reset();
                self.pos += match_len;
            }

            Action::HandleNumber => {
                // Check if preceded by a unary +/- operator
                // If the previous token is +/- and before that is an operator/keyword/comma,
                // then the +/- is unary and should be part of the number
                self.add_node(prefix, token_text, TokenType::Number);
                self.pos += match_len;
            }

            Action::HandleReservedKeyword { inner } => {
                // If preceded by DOT, treat as a NAME instead
                let prev_is_dot = self.previous_node_index().map_or(false, |idx| {
                    let node = &self.arena[idx];
                    let _prev = node.get_previous_sql_token(&self.arena);
                    // Actually check if the *direct* prev node (skipping newlines) is DOT
                    self.get_prev_sql_type() == Some(TokenType::Dot)
                });
                if prev_is_dot {
                    self.add_node(prefix, token_text, TokenType::Name);
                    self.pos += match_len;
                } else {
                    self.execute_action(inner, captures, _source)?;
                }
            }

            Action::HandleNonreservedTopLevelKeyword { inner } => {
                // If inside brackets, treat as NAME
                if !self.node_manager.open_brackets.is_empty() {
                    self.add_node(prefix, token_text, TokenType::Name);
                    self.pos += match_len;
                } else {
                    self.execute_action(inner, captures, _source)?;
                }
            }

            Action::HandleSetOperator => {
                self.flush_line_buffer();
                self.add_node(prefix, token_text, TokenType::SetOperator);
                self.flush_line_buffer();
                self.node_manager.reset();
                self.pos += match_len;
            }

            Action::HandleDdlAs => {
                self.add_node(prefix, token_text, TokenType::WordOperator);
                self.pos += match_len;
            }

            Action::HandleClosingAngleBracket => {
                // Check if we have an open angle bracket to close
                let has_open_angle = self
                    .node_manager
                    .open_brackets
                    .last()
                    .map(|&idx| self.arena[idx].value == "<")
                    .unwrap_or(false);
                if has_open_angle {
                    self.add_node(prefix, token_text, TokenType::BracketClose);
                    self.node_manager.open_brackets.pop();
                } else {
                    self.add_node(prefix, token_text, TokenType::Operator);
                }
                self.pos += match_len;
            }

            Action::HandleJinjaBlockStart => {
                self.add_node(prefix, token_text, TokenType::JinjaBlockStart);
                let last_idx = self.arena.len() - 1;
                self.node_manager.push_jinja_block(last_idx);
                self.pos += match_len;
            }

            Action::HandleJinjaBlockKeyword => {
                // Pop current jinja block, add keyword, push new block
                self.node_manager.pop_jinja_block();
                self.add_node(prefix, token_text, TokenType::JinjaBlockKeyword);
                let last_idx = self.arena.len() - 1;
                self.node_manager.push_jinja_block(last_idx);
                self.pos += match_len;
            }

            Action::HandleJinjaBlockEnd => {
                self.node_manager.pop_jinja_block();
                self.add_node(prefix, token_text, TokenType::JinjaBlockEnd);
                self.pos += match_len;
            }

            Action::HandleJinja { token_type } => {
                self.add_node(prefix, token_text, *token_type);
                self.pos += match_len;
            }

            Action::HandleKeywordBeforeParen { token_type } => {
                // The matched text includes the trailing `(`, but we only consume the keyword.
                // Strip trailing `(` and any whitespace before it to get the keyword.
                let keyword = token_text.trim_end_matches('(').trim_end();
                self.add_node(prefix, keyword, *token_type);
                // Only advance past prefix + keyword (leave `(` for bracket_open)
                self.pos += prefix.len() + keyword.len();
            }

            Action::LexRuleset { ruleset_name: _ } => {
                // For now, treat as UntermKeyword and advance
                self.add_node(prefix, token_text, TokenType::UntermKeyword);
                self.pos += match_len;
            }
        }

        Ok(())
    }

    /// Add a node to the node buffer.
    fn add_node(&mut self, prefix: &str, token_text: &str, token_type: TokenType) {
        let spos = self.pos;
        let epos = spos + prefix.len() + token_text.len();
        let token = Token::new(token_type, prefix, token_text, spos, epos);

        let prev = self.previous_node_index();
        let node = self.node_manager.create_node(token, prev, &self.arena);
        let idx = self.arena.len();
        self.arena.push(node);

        // Track opening brackets
        if token_type.is_opening_bracket() && !matches!(token_type, TokenType::BracketOpen if token_text.contains('<')) {
            self.node_manager.push_bracket(idx);
        }

        self.node_buffer.push(idx);
    }

    /// Add a comment to the comment buffer.
    fn add_comment(&mut self, _prefix: &str, token_text: &str) {
        let spos = self.pos;
        let epos = spos + token_text.len();
        let token = Token::new(TokenType::Comment, "", token_text, spos, epos);
        let is_standalone = self.node_buffer.is_empty();
        let prev = self.previous_node_index();
        let comment = Comment::new(token, is_standalone, prev);
        self.comment_buffer.push(comment);
    }

    /// Flush node and comment buffers into a Line, append to line_buffer.
    fn flush_line_buffer(&mut self) {
        if self.node_buffer.is_empty() && self.comment_buffer.is_empty() {
            // Still create a newline-only line if we have nothing
            let prev = self.previous_node_index();

            // Create a newline node
            let token = Token::new(TokenType::Newline, "", "\n", self.pos, self.pos + 1);
            let node = self.node_manager.create_node(token, prev, &self.arena);
            let idx = self.arena.len();
            self.arena.push(node);

            let mut line = Line::new(prev);
            line.append_node(idx);
            self.line_buffer.push(line);
            return;
        }

        let prev = if let Some(&first) = self.node_buffer.first() {
            self.arena[first].previous_node
        } else {
            self.previous_node_index()
        };

        let mut line = Line::new(prev);

        // Add all buffered nodes
        for &idx in &self.node_buffer {
            line.append_node(idx);
        }

        // Add a newline node at the END (mirrors Python behavior)
        let nl_prev = self.node_buffer.last().copied().or_else(|| self.previous_node_index());
        let nl_token = Token::new(TokenType::Newline, "", "\n", self.pos, self.pos + 1);
        let nl_node = self.node_manager.create_node(nl_token, nl_prev, &self.arena);
        let nl_idx = self.arena.len();
        self.arena.push(nl_node);
        line.append_node(nl_idx);

        // Add all buffered comments
        for comment in self.comment_buffer.drain(..) {
            line.append_comment(comment);
        }

        self.node_buffer.clear();
        self.line_buffer.push(line);
    }

    /// Get the index of the most recently created node.
    fn previous_node_index(&self) -> Option<NodeIndex> {
        if self.arena.is_empty() {
            None
        } else {
            Some(self.arena.len() - 1)
        }
    }

    /// Get the token type of the previous meaningful SQL token.
    fn get_prev_sql_type(&self) -> Option<TokenType> {
        let mut idx = self.previous_node_index();
        while let Some(i) = idx {
            let node = &self.arena[i];
            if !node.token.token_type.does_not_set_prev_sql_context() {
                return Some(node.token.token_type);
            }
            idx = node.previous_node;
        }
        None
    }

    /// Clear all buffers for a fresh parse.
    fn clear_buffers(&mut self) {
        self.node_buffer.clear();
        self.comment_buffer.clear();
        self.line_buffer.clear();
        self.arena.clear();
        self.pos = 0;
        self.node_manager.reset();
    }

    /// Build the Query from the accumulated line buffer.
    fn build_query(&mut self, source: &str) -> Query {
        Query::new(
            source.to_string(),
            self.line_length,
            std::mem::take(&mut self.line_buffer),
        )
    }

    pub fn push_rules(&mut self, rules: Vec<Rule>) {
        self.rule_stack.push(rules);
    }

    pub fn pop_rules(&mut self) {
        if self.rule_stack.len() > 1 {
            self.rule_stack.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::main_rules;

    fn create_analyzer() -> Analyzer {
        let rules = main_rules();
        let nm = NodeManager::new(false);
        Analyzer::new(rules, nm, 88)
    }

    #[test]
    fn test_parse_select_one() {
        let mut analyzer = create_analyzer();
        let query = analyzer.parse_query("select 1\n").unwrap();
        assert!(!query.lines.is_empty());

        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("select"));
        assert!(rendered.contains("1"));
    }

    #[test]
    fn test_parse_simple_query() {
        let mut analyzer = create_analyzer();
        let source = "SELECT a, b FROM my_table WHERE x = 1\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);

        // Should contain lowercased keywords
        assert!(rendered.contains("select"));
        assert!(rendered.contains("from"));
        assert!(rendered.contains("where"));
    }

    #[test]
    fn test_parse_with_comments() {
        let mut analyzer = create_analyzer();
        let source = "-- this is a comment\nSELECT 1\n";
        let query = analyzer.parse_query(source).unwrap();
        assert!(!query.lines.is_empty());
    }

    #[test]
    fn test_parse_multiple_statements() {
        let mut analyzer = create_analyzer();
        let source = "SELECT 1;\nSELECT 2;\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("select"));
    }

    #[test]
    fn test_parse_with_brackets() {
        let mut analyzer = create_analyzer();
        let source = "SELECT count(*) FROM t\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("count"));
        assert!(rendered.contains("*"));
    }

    #[test]
    fn test_parse_case_expression() {
        let mut analyzer = create_analyzer();
        let source = "SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("case"));
        assert!(rendered.contains("when"));
        assert!(rendered.contains("end"));
    }

    #[test]
    fn test_parse_jinja_expression() {
        let mut analyzer = create_analyzer();
        let source = "SELECT {{ column_name }} FROM t\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("{{"));
        assert!(rendered.contains("}}"));
    }
}
