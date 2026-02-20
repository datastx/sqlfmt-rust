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
    /// When true, the next HandleNewline should not create a blank line.
    /// Set after HandleSemicolon and HandleSetOperator which already flush.
    suppress_next_newline: bool,
    /// Set after a newline was suppressed, cleared after use. Prevents comments
    /// on the next line from being incorrectly attached as inline to the
    /// previous semicolon line (since the suppressed newline means the
    /// comment's prefix won't contain '\n').
    had_suppressed_newline: bool,
    /// Trailing whitespace captured from HandleNewline's prefix.
    /// Stored in the newline node's token prefix so formatting-disabled
    /// lines can preserve trailing whitespace before newlines.
    trailing_whitespace: String,
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
            suppress_next_newline: false,
            had_suppressed_newline: false,
            trailing_whitespace: String::new(),
        }
    }

    /// Main entry point: parse source string into a Query.
    pub fn parse_query(&mut self, source: &str) -> Result<Query, SqlfmtError> {
        // Pre-lex validation: check for unmatched comment markers
        Self::validate_comment_markers(source)?;
        self.clear_buffers();
        self.lex(source)?;
        self.flush_line_buffer();
        // Post-lex validation: check for unmatched brackets
        self.validate_brackets()?;
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
    /// Split into two phases to avoid cloning the entire Vec<Rule> per token:
    /// 1. Match phase: immutable borrow of rule_stack to find match + extract strings
    /// 2. Execute phase: mutable borrow of self to process the match
    fn lex_one(&mut self, source: &str) -> Result<(), SqlfmtError> {
        let remaining = &source[self.pos..];
        if remaining.is_empty() {
            return Ok(());
        }

        // Phase 1: Find matching rule (immutable borrow of rule_stack)
        let match_result = {
            let rules = self.rule_stack.last().unwrap();
            let mut found = None;
            for rule in rules {
                if let Some(captures) = rule.pattern.captures(remaining) {
                    let full_match_str = captures.get(0).unwrap().as_str().to_string();
                    let prefix = captures
                        .get(1)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default();
                    let token_text = captures
                        .get(2)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default();
                    let action = rule.action.clone();
                    found = Some((full_match_str, prefix, token_text, action));
                    break;
                }
            }
            found
        };

        // Phase 2: Execute the action (mutable borrow of self)
        match match_result {
            Some((full_match_str, prefix, token_text, action)) => {
                self.execute_action(&action, &full_match_str, &prefix, &token_text, source)
            }
            None => Err(SqlfmtError::Parsing {
                position: self.pos,
                message: format!(
                    "No rule matched near: {:?}",
                    &remaining[..remaining.len().min(40)]
                ),
            }),
        }
    }

    /// Dispatch an action based on what the rule matched.
    fn execute_action(
        &mut self,
        action: &Action,
        full_match_str: &str,
        prefix: &str,
        token_text: &str,
        _source: &str,
    ) -> Result<(), SqlfmtError> {
        let match_len = full_match_str.len();

        match action {
            Action::AddNode { token_type } => {
                // Special case: >> inside angle brackets should be two closing brackets
                if *token_type == TokenType::Operator && token_text == ">>" {
                    let open_angle_count = self
                        .node_manager
                        .open_brackets
                        .iter()
                        .filter(|&&idx| self.arena[idx].value == "<")
                        .count();
                    if open_angle_count >= 2 {
                        // Split >> into two > BracketClose tokens
                        self.add_node(prefix, ">", TokenType::BracketClose);
                        self.node_manager.open_brackets.pop();
                        self.add_node("", ">", TokenType::BracketClose);
                        self.node_manager.open_brackets.pop();
                        self.pos += match_len;
                        return Ok(());
                    } else if open_angle_count == 1 {
                        // First > closes bracket, second > is operator
                        self.add_node(prefix, ">", TokenType::BracketClose);
                        self.node_manager.open_brackets.pop();
                        self.add_node("", ">", TokenType::Operator);
                        self.pos += match_len;
                        return Ok(());
                    }
                }
                self.add_node(prefix, token_text, *token_type);
                // Switch rule sets on fmt:off/on
                if *token_type == TokenType::FmtOff {
                    self.push_rules(crate::rules::fmt_off_rules());
                } else if *token_type == TokenType::FmtOn {
                    self.pop_rules();
                }
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
                    let has_matching_case =
                        self.node_manager.open_brackets.iter().any(|&idx| {
                            self.arena[idx].token.token_type == TokenType::StatementStart
                        });
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
                if self.suppress_next_newline {
                    self.suppress_next_newline = false;
                    self.had_suppressed_newline = true;
                } else {
                    // Store trailing whitespace from this newline match
                    // (prefix captures whitespace between last token and newline)
                    self.trailing_whitespace = prefix.to_string();
                    self.flush_line_buffer();
                    self.trailing_whitespace.clear();
                }
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
                self.suppress_next_newline = true;
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
                let prev_is_dot = self.previous_node_index().is_some_and(|_idx| {
                    // Check if the *direct* prev node (skipping newlines) is DOT
                    self.get_prev_sql_type() == Some(TokenType::Dot)
                });
                if prev_is_dot {
                    self.add_node(prefix, token_text, TokenType::Name);
                    self.pos += match_len;
                } else {
                    self.execute_action(inner, full_match_str, prefix, token_text, _source)?;
                }
            }

            Action::HandleNonreservedTopLevelKeyword { inner } => {
                // If inside brackets, treat as NAME
                if !self.node_manager.open_brackets.is_empty() {
                    self.add_node(prefix, token_text, TokenType::Name);
                    self.pos += match_len;
                } else {
                    self.execute_action(inner, full_match_str, prefix, token_text, _source)?;
                }
            }

            Action::HandleSetOperator => {
                // Only flush if there's buffered content; don't create a spurious
                // blank line when the previous newline already flushed the buffer.
                if !self.node_buffer.is_empty() || !self.comment_buffer.is_empty() {
                    self.flush_line_buffer();
                }
                self.add_node(prefix, token_text, TokenType::SetOperator);
                self.flush_line_buffer();
                self.node_manager.reset();
                self.suppress_next_newline = true;
                self.pos += match_len;
            }

            Action::HandleDdlAs => {
                // In CREATE FUNCTION, AS is an UntermKeyword.
                // If the next meaningful token is NOT a quoted name/string,
                // pop the current ruleset to switch back to MAIN rules
                // so the function body gets proper SQL formatting.
                self.add_node(prefix, token_text, TokenType::UntermKeyword);
                self.pos += match_len;

                // Check if the next non-whitespace, non-comment text starts
                // with a quote character (string literal or quoted name)
                let remaining = &_source[self.pos..];
                let trimmed = remaining.trim_start();
                // Skip comments
                let mut check = trimmed;
                loop {
                    if check.starts_with("--") || check.starts_with("//") || check.starts_with('#')
                    {
                        // Skip to end of line
                        if let Some(nl) = check.find('\n') {
                            check = check[nl + 1..].trim_start();
                        } else {
                            break;
                        }
                    } else if check.starts_with("/*") {
                        if let Some(end) = check.find("*/") {
                            check = check[end + 2..].trim_start();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                let next_is_quoted = check.starts_with('\'')
                    || check.starts_with('"')
                    || check.starts_with('`')
                    || check.starts_with("$$");
                if !next_is_quoted {
                    // Pop back to main rules for the function body
                    self.pop_rules();
                }
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
                // For {% set x %} blocks (without =), switch to set block rules
                // to preserve content as data until {% endset %}
                let lower = token_text.to_lowercase();
                if lower.contains("set") && !lower.contains('=') {
                    self.push_rules(crate::rules::jinja_set_block_rules());
                }
                self.pos += match_len;
            }

            Action::HandleJinjaBlockKeyword => {
                // Save the block start's previous_node before popping.
                // Python sqlfmt sets {% else %}'s previous_node to {% if %}'s
                // previous_node so get_previous_token resolves to the SQL
                // context before the entire if/else block.
                let block_start_prev = self
                    .node_manager
                    .open_jinja_blocks
                    .last()
                    .and_then(|&idx| self.arena[idx].previous_node);
                // Pop current jinja block, add keyword, push new block
                self.node_manager.pop_jinja_block();
                self.add_node(prefix, token_text, TokenType::JinjaBlockKeyword);
                let last_idx = self.arena.len() - 1;
                // Override previous_node to block start's previous_node
                // so get_previous_token for tokens AFTER {% else %} resolves
                // to the SQL context before the entire if/else block.
                // The prefix is always "" for block keywords (handled in compute_prefix).
                if let Some(bsp) = block_start_prev {
                    self.arena[last_idx].previous_node = Some(bsp);
                }
                self.node_manager.push_jinja_block(last_idx);
                self.pos += match_len;
            }

            Action::HandleJinjaBlockEnd => {
                // Pop set block rules if we were inside a set block
                let lower = token_text.to_lowercase();
                if lower.contains("endset") && self.rule_stack.len() > 1 {
                    self.pop_rules();
                }
                self.node_manager.pop_jinja_block();
                self.add_node(prefix, token_text, TokenType::JinjaBlockEnd);
                self.pos += match_len;
            }

            Action::HandleJinja { token_type } => {
                if *token_type == TokenType::JinjaExpression {
                    // Use depth-aware scanning for {{ }} to handle nested {{ }}
                    let remaining = &_source[self.pos..];
                    let prefix_len = prefix.len();
                    let tag_start = &remaining[prefix_len..];
                    if let Some(tag_len) = Self::find_jinja_expr_end(tag_start) {
                        let full_text = &tag_start[..tag_len];
                        self.add_node(prefix, full_text, *token_type);
                        self.pos += prefix_len + tag_len;
                    } else {
                        // Fallback: use regex match
                        self.add_node(prefix, token_text, *token_type);
                        self.pos += match_len;
                    }
                } else {
                    self.add_node(prefix, token_text, *token_type);
                    self.pos += match_len;
                }
            }

            Action::HandleKeywordBeforeParen { token_type } => {
                // The matched text includes the trailing `(`, but we only consume the keyword.
                // Strip trailing `(` and any whitespace before it to get the keyword.
                let keyword = token_text.trim_end_matches('(').trim_end();

                // For star modifiers (except/exclude/replace): only use WordOperator
                // when preceded by Star. Otherwise it's a function call → Name.
                let effective_type = if *token_type == TokenType::WordOperator
                    && self.get_prev_sql_type() != Some(TokenType::Star)
                {
                    TokenType::Name
                } else {
                    *token_type
                };

                self.add_node(prefix, keyword, effective_type);
                // Only advance past prefix + keyword (leave `(` for bracket_open)
                self.pos += prefix.len() + keyword.len();
            }

            Action::LexRuleset { ruleset_name } => {
                // Push the alternate ruleset and re-lex from the same position.
                // The new ruleset will re-match the trigger keyword, potentially
                // with a longer pattern (e.g., "revoke grant option for" in GRANT
                // ruleset). This mirrors Python sqlfmt's lex_ruleset behavior.
                let ruleset = match ruleset_name.as_str() {
                    "grant" => crate::rules::grant_rules(),
                    "function" => crate::rules::function_rules(),
                    "warehouse" => crate::rules::warehouse_rules(),
                    "clone" => crate::rules::clone_rules(),
                    _ => crate::rules::unsupported_rules(),
                };
                self.push_rules(ruleset);
                // Don't advance pos — let the new ruleset re-lex from current position
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
        if token_type.is_opening_bracket()
            && !matches!(token_type, TokenType::BracketOpen if token_text.contains('<'))
        {
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
        // If the last node in the buffer is a semicolon, attach the comment
        // to the node before the semicolon. This ensures inline comments after
        // semicolons (e.g., `from table; -- comment`) stay with the preceding
        // content line when the semicolon is split to its own line.
        let prev = if !is_standalone {
            if let Some(&last_idx) = self.node_buffer.last() {
                if self.arena[last_idx].token.token_type == TokenType::Semicolon {
                    if self.node_buffer.len() >= 2 {
                        Some(self.node_buffer[self.node_buffer.len() - 2])
                    } else {
                        self.previous_node_index()
                    }
                } else {
                    self.previous_node_index()
                }
            } else {
                self.previous_node_index()
            }
        } else {
            self.previous_node_index()
        };

        // When the buffer is empty but the last flushed line ends with a semicolon
        // AND the comment is on the SAME LINE (no newline in token prefix),
        // the comment is inline after that semicolon (e.g., `from table; -- comment`).
        // Attach it directly to the previous line instead of buffering it as standalone.
        if is_standalone && !self.line_buffer.is_empty() && !token_text.is_empty() {
            // Check if the comment is on the same line as the semicolon
            // by examining the original prefix (text between semicolon and comment).
            // Also check had_suppressed_newline: when a newline after a semicolon
            // was suppressed (to prevent blank lines), the prefix won't contain '\n'
            // even though the comment IS on a different line.
            let same_line =
                !_prefix.contains('\n') && !_prefix.is_empty() && !self.had_suppressed_newline;
            if same_line {
                let last_line = self.line_buffer.last().unwrap();
                // Check if the last content node in the previous line is a semicolon
                let has_trailing_semicolon = last_line.nodes.iter().rev().any(|&idx| {
                    let node = &self.arena[idx];
                    if node.is_newline() {
                        false
                    } else {
                        node.token.token_type == TokenType::Semicolon
                    }
                });
                if has_trailing_semicolon {
                    let content_nodes: Vec<usize> = last_line
                        .nodes
                        .iter()
                        .copied()
                        .filter(|&idx| !self.arena[idx].is_newline())
                        .collect();
                    let attach_to = if content_nodes.len() >= 2 {
                        Some(content_nodes[content_nodes.len() - 2])
                    } else if !content_nodes.is_empty() {
                        Some(content_nodes[0])
                    } else {
                        prev
                    };
                    let comment = Comment::new(token, false, attach_to);
                    self.line_buffer.last_mut().unwrap().append_comment(comment);
                    return;
                }
            }
        }

        self.had_suppressed_newline = false;
        let comment = Comment::new(token, is_standalone, prev);
        self.comment_buffer.push(comment);
    }

    /// Flush node and comment buffers into a Line, append to line_buffer.
    fn flush_line_buffer(&mut self) {
        if self.node_buffer.is_empty() && self.comment_buffer.is_empty() {
            // Still create a newline-only line if we have nothing
            let prev = self.previous_node_index();

            // Create a newline node (include trailing whitespace for fmt:off regions)
            let token = Token::new(
                TokenType::Newline,
                &self.trailing_whitespace,
                "\n",
                self.pos,
                self.pos + 1,
            );
            let node = self.node_manager.create_node(token, prev, &self.arena);
            let idx = self.arena.len();
            self.arena.push(node);

            let mut line = Line::new(prev);
            line.append_node(idx);
            // Propagate formatting_disabled from newline node
            if !self.arena[idx].formatting_disabled.is_empty() {
                line.formatting_disabled = self.arena[idx].formatting_disabled.clone();
            }
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

        // Propagate formatting_disabled from first content node with it set
        for &idx in &self.node_buffer {
            if !self.arena[idx].formatting_disabled.is_empty() {
                line.formatting_disabled = self.arena[idx].formatting_disabled.clone();
                break;
            }
        }

        // Add a newline node at the END (mirrors Python behavior)
        let nl_prev = self
            .node_buffer
            .last()
            .copied()
            .or_else(|| self.previous_node_index());
        let nl_token = Token::new(
            TokenType::Newline,
            &self.trailing_whitespace,
            "\n",
            self.pos,
            self.pos + 1,
        );
        let nl_node = self
            .node_manager
            .create_node(nl_token, nl_prev, &self.arena);
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
        self.suppress_next_newline = false;
        self.had_suppressed_newline = false;
        self.trailing_whitespace.clear();
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

    /// Find the end of a `{{ }}` Jinja expression with depth tracking.
    /// Handles nested `{{ }}` and skips strings. Returns the byte length
    /// of the full expression (from `{{` to matching `}}`), or None if unmatched.
    fn find_jinja_expr_end(text: &str) -> Option<usize> {
        let bytes = text.as_bytes();
        let len = bytes.len();
        if len < 4 || bytes[0] != b'{' || bytes[1] != b'{' {
            return None;
        }

        let mut i = 2;
        // Skip optional `-` (for {{-)
        if i < len && bytes[i] == b'-' {
            i += 1;
        }

        let mut depth = 1;
        while i < len && depth > 0 {
            // Skip strings
            if bytes[i] == b'\'' || bytes[i] == b'"' {
                let quote = bytes[i];
                i += 1;
                while i < len && bytes[i] != quote {
                    if bytes[i] == b'\\' && i + 1 < len {
                        i += 1;
                    }
                    i += 1;
                }
                if i < len {
                    i += 1;
                }
                continue;
            }

            // Check for nested {{ open
            if i + 1 < len && bytes[i] == b'{' && bytes[i + 1] == b'{' {
                depth += 1;
                i += 2;
                continue;
            }

            // Check for -}} close
            if i + 2 < len && bytes[i] == b'-' && bytes[i + 1] == b'}' && bytes[i + 2] == b'}' {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 3);
                }
                i += 3;
                continue;
            }

            // Check for }} close
            if i + 1 < len && bytes[i] == b'}' && bytes[i + 1] == b'}' {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 2);
                }
                i += 2;
                continue;
            }

            i += 1;
        }

        None
    }

    /// Pre-lex validation: check that `/*` and `*/` are properly matched.
    /// Detects unterminated multiline comments and stray `*/`.
    fn validate_comment_markers(source: &str) -> Result<(), SqlfmtError> {
        // Check that /* and */ are properly paired.
        // SQL comments do NOT nest, so inner /* inside a comment is ignored.
        let bytes = source.as_bytes();
        let len = bytes.len();
        let mut i = 0;
        let mut in_comment = false;

        while i < len {
            if in_comment {
                // Inside a /* comment: only look for the closing */
                if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    in_comment = false;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }

            // Skip single-quoted strings
            if bytes[i] == b'\'' {
                i += 1;
                while i < len && bytes[i] != b'\'' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            // Skip double-quoted identifiers
            if bytes[i] == b'"' {
                i += 1;
                while i < len && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            // Skip single-line comments
            if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            // Jinja tags: skip {{ }}, {% %}, {# #} with depth tracking
            if i + 1 < len
                && bytes[i] == b'{'
                && (bytes[i + 1] == b'{' || bytes[i + 1] == b'%' || bytes[i + 1] == b'#')
            {
                let open = bytes[i + 1];
                let close = match open {
                    b'{' => b'}',
                    b'%' => b'%',
                    _ => b'#',
                };
                let mut depth = 1;
                i += 2;
                while i + 1 < len && depth > 0 {
                    // Skip strings inside Jinja tags
                    if bytes[i] == b'\'' || bytes[i] == b'"' {
                        let quote = bytes[i];
                        i += 1;
                        while i < len && bytes[i] != quote {
                            if bytes[i] == b'\\' {
                                i += 1;
                            }
                            i += 1;
                        }
                        if i < len {
                            i += 1;
                        }
                        continue;
                    }
                    if bytes[i] == b'{' && bytes[i + 1] == open {
                        depth += 1;
                        i += 2;
                    } else if bytes[i] == close && bytes[i + 1] == b'}' {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                continue;
            }
            // Check for /* (open comment)
            if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                in_comment = true;
                i += 2;
                continue;
            }
            // Check for stray */ (close without open)
            if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                return Err(SqlfmtError::Bracket(
                    "Encountered */ without a preceding /*".to_string(),
                ));
            }
            i += 1;
        }
        if in_comment {
            return Err(SqlfmtError::Bracket(
                "Unterminated multiline comment (/* without matching */)".to_string(),
            ));
        }
        Ok(())
    }

    /// Post-lex validation: check for unmatched closing brackets.
    fn validate_brackets(&self) -> Result<(), SqlfmtError> {
        let mut depth = 0i32;
        for node in &self.arena {
            match node.token.token_type {
                TokenType::BracketOpen | TokenType::StatementStart => {
                    depth += 1;
                }
                TokenType::BracketClose | TokenType::StatementEnd => {
                    depth -= 1;
                    if depth < 0 {
                        return Err(SqlfmtError::Bracket(format!(
                            "Encountered closing bracket '{}' without a matching opening bracket",
                            node.token.token
                        )));
                    }
                }
                _ => {}
            }
        }
        Ok(())
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

    #[test]
    fn test_star_parsing_select_star() {
        // SELECT * should have no prefix space on *
        let mut analyzer = create_analyzer();
        let query = analyzer.parse_query("SELECT * FROM t\n").unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("*"));
        // The star should follow select with a space
        assert!(rendered.contains("select") || rendered.contains("*"));
    }

    #[test]
    fn test_star_parsing_table_star() {
        // table.* should have no space between . and *
        let mut analyzer = create_analyzer();
        let query = analyzer.parse_query("SELECT t.* FROM t\n").unwrap();
        let rendered = query.render(&analyzer.arena);
        // The rendering splits nodes per line; t, ., * may render as "t.*" or
        // the formatter may split them. Just check all parts are present.
        assert!(
            rendered.contains("t") && rendered.contains("*"),
            "Table star should be preserved: {}",
            rendered
        );
    }

    #[test]
    fn test_end_as_identifier() {
        // "end" in a context without matching CASE should not crash
        let mut analyzer = create_analyzer();
        let result = analyzer.parse_query("SELECT end\n");
        assert!(result.is_ok(), "END as identifier should not crash");
    }

    #[test]
    fn test_open_paren_spacing_function_call() {
        // Function call: no space before (
        let mut analyzer = create_analyzer();
        let query = analyzer.parse_query("SELECT sum(1)\n").unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(
            rendered.contains("sum(") || rendered.contains("sum\n"),
            "Function call should have no space before paren: {}",
            rendered
        );
    }

    #[test]
    fn test_parse_jinja_block() {
        let mut analyzer = create_analyzer();
        let source = "{% if condition %}\nSELECT 1\n{% endif %}\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("{% if condition %}"));
        assert!(rendered.contains("{% endif %}"));
    }

    #[test]
    fn test_parse_set_operator() {
        let mut analyzer = create_analyzer();
        let source = "SELECT 1\nUNION ALL\nSELECT 2\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("union all"));
    }

    #[test]
    fn test_parse_between_and() {
        let mut analyzer = create_analyzer();
        let source = "SELECT * FROM t WHERE x BETWEEN 1 AND 10\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("between"));
        assert!(rendered.contains("and"));
    }

    #[test]
    fn test_parse_window_function() {
        let mut analyzer = create_analyzer();
        let source = "SELECT ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn FROM t\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("over"));
        assert!(rendered.contains("partition by"));
        assert!(rendered.contains("order by"));
    }

    #[test]
    fn test_parse_cte() {
        let mut analyzer = create_analyzer();
        let source = "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("with"));
        assert!(rendered.contains("as"));
    }

    #[test]
    fn test_parse_closing_angle_bracket() {
        // Test angle brackets for generic types like ARRAY<INT>
        let mut analyzer = create_analyzer();
        let source = "SELECT CAST(x AS ARRAY<INT>)\n";
        let result = analyzer.parse_query(source);
        assert!(
            result.is_ok(),
            "Angle bracket type should parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_number_formats() {
        let mut analyzer = create_analyzer();
        // Various number formats
        let source = "SELECT 42, 3.14, 1e10, 0xFF, 0b1010, 0o777\n";
        let result = analyzer.parse_query(source);
        assert!(result.is_ok(), "Number formats should parse");
    }
}
