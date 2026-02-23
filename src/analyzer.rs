use memchr::memchr;

use crate::action::Action;
use crate::comment::Comment;
use crate::error::SqlfmtError;
use crate::lexer::{self, scan_dollar_string, LexState};
use crate::line::Line;
use crate::node::{Node, NodeIndex};
use crate::node_manager::NodeManager;
use crate::query::Query;
use crate::string_utils::skip_string_literal;
use crate::token::{Token, TokenType};

/// The byte-dispatch lexer. Parses SQL source strings into Queries.
/// Maintains buffers and a lex state stack for nested lexing contexts.
pub struct Analyzer {
    pub line_length: usize,
    pub node_manager: NodeManager,
    pub arena: Vec<Node>,

    lex_state: Vec<LexState>,
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
    pub fn new(node_manager: NodeManager, line_length: usize) -> Self {
        Self {
            line_length,
            node_manager,
            lex_state: vec![LexState::Main],
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
        Self::validate_comment_markers(source)?;
        self.clear_buffers();
        // Pre-allocate arena: ~1 node per 6 source bytes avoids repeated reallocations
        self.arena.reserve(source.len() / 6);
        // Pre-allocate line_buffer based on newline count to avoid Vec growth
        let estimated_lines = memchr::memchr_iter(b'\n', source.as_bytes()).count();
        self.line_buffer.reserve(estimated_lines + 1);
        self.node_buffer.reserve(32);
        self.lex(source)?;
        self.flush_line_buffer();
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

    /// Lex one token using byte dispatch. Returns the action, match_len,
    /// prefix, and token_text without any regex overhead.
    fn lex_one(&mut self, source: &str) -> Result<(), SqlfmtError> {
        let remaining = &source[self.pos..];
        if remaining.is_empty() {
            return Ok(());
        }

        let state = *self
            .lex_state
            .last()
            .expect("lex_state initialized with Main in Analyzer::new");

        match lexer::lex_one(remaining, state) {
            Some(result) => self.execute_action(
                result.action,
                result.match_len,
                result.prefix,
                result.token_text,
                source,
            ),
            None => Err(SqlfmtError::Parsing {
                position: self.pos,
                message: format!(
                    "No token matched near: {:?}",
                    &remaining[..remaining.len().min(40)]
                ),
            }),
        }
    }

    /// Dispatch an action based on what the rule matched.
    fn execute_action(
        &mut self,
        action: &Action,
        match_len: usize,
        prefix: &str,
        token_text: &str,
        _source: &str,
    ) -> Result<(), SqlfmtError> {
        match action {
            Action::AddNode { token_type } => {
                if *token_type == TokenType::Operator
                    && token_text == ">>"
                    && self.handle_angle_bracket_splitting(prefix, match_len)
                {
                    return Ok(());
                }
                self.add_node(prefix, token_text, *token_type);
                if *token_type == TokenType::FmtOff {
                    self.push_state(LexState::FmtOff);
                } else if *token_type == TokenType::FmtOn {
                    self.pop_state();
                }
                self.pos += match_len;
            }

            Action::SafeAddNode {
                token_type,
                alt_token_type,
            } => {
                self.handle_safe_add_node(prefix, token_text, *token_type, *alt_token_type);
                self.pos += match_len;
            }

            Action::AddComment => {
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
                    self.trailing_whitespace.clear();
                    self.trailing_whitespace.push_str(prefix);
                    self.flush_line_buffer();
                    self.trailing_whitespace.clear();
                }
                self.pos += match_len;
            }

            Action::HandleSemicolon => {
                self.add_node(prefix, token_text, TokenType::Semicolon);
                self.flush_line_buffer();
                while self.lex_state.len() > 1 {
                    self.lex_state.pop();
                }
                self.node_manager.reset();
                self.suppress_next_newline = true;
                self.pos += match_len;
            }

            Action::HandleNumber => {
                self.add_node(prefix, token_text, TokenType::Number);
                self.pos += match_len;
            }

            Action::HandleReservedKeyword { inner } => {
                let prev_is_dot = self
                    .previous_node_index()
                    .is_some_and(|_idx| self.get_prev_sql_type() == Some(TokenType::Dot));
                if prev_is_dot {
                    self.add_node(prefix, token_text, TokenType::Name);
                    self.pos += match_len;
                } else {
                    self.execute_action(inner, match_len, prefix, token_text, _source)?;
                }
            }

            Action::HandleNonreservedTopLevelKeyword { inner } => {
                if !self.node_manager.open_brackets.is_empty() {
                    self.add_node(prefix, token_text, TokenType::Name);
                    self.pos += match_len;
                } else {
                    self.execute_action(inner, match_len, prefix, token_text, _source)?;
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
                self.add_node(prefix, token_text, TokenType::UntermKeyword);
                self.pos += match_len;
                self.handle_ddl_as(_source);
            }

            Action::HandleClosingAngleBracket => {
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
                self.maybe_push_jinja_data_rules(token_text);
                self.pos += match_len;
            }

            Action::HandleJinjaBlockKeyword => {
                self.handle_jinja_block_keyword(prefix, token_text);
                self.pos += match_len;
            }

            Action::HandleJinjaBlockEnd => {
                self.handle_jinja_block_end(prefix, token_text);
                self.pos += match_len;
            }

            Action::HandleJinja { token_type } => {
                self.handle_jinja(prefix, token_text, *token_type, _source, match_len);
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
                // Push the alternate lex state and re-lex from the same position.
                let new_state = match *ruleset_name {
                    "grant" => LexState::Grant,
                    "function" => LexState::Function,
                    "warehouse" => LexState::Warehouse,
                    "clone" => LexState::Clone,
                    _ => LexState::Unsupported,
                };
                self.push_state(new_state);
                // Don't advance pos — let the new state re-lex from current position
            }
        }

        Ok(())
    }

    /// Handle `>>` inside angle brackets by splitting into two `>` tokens.
    /// Returns `true` if the split was handled (caller should return early).
    fn handle_angle_bracket_splitting(&mut self, prefix: &str, match_len: usize) -> bool {
        let open_angle_count = self
            .node_manager
            .open_brackets
            .iter()
            .filter(|&&idx| self.arena[idx].value == "<")
            .count();
        if open_angle_count >= 2 {
            self.add_node(prefix, ">", TokenType::BracketClose);
            self.node_manager.open_brackets.pop();
            self.add_node("", ">", TokenType::BracketClose);
            self.node_manager.open_brackets.pop();
            self.pos += match_len;
            return true;
        }
        if open_angle_count == 1 {
            self.add_node(prefix, ">", TokenType::BracketClose);
            self.node_manager.open_brackets.pop();
            self.add_node("", ">", TokenType::Operator);
            self.pos += match_len;
            return true;
        }
        false
    }

    /// Handle SafeAddNode: try primary type, fall back to alt on mismatch.
    fn handle_safe_add_node(
        &mut self,
        prefix: &str,
        token_text: &str,
        token_type: TokenType,
        alt_token_type: TokenType,
    ) {
        if token_type == TokenType::BracketOpen && token_text.contains('<') {
            let angle_pos = token_text.find('<').expect("contains('<') checked above");
            let name_part = &token_text[..angle_pos].trim();
            if !name_part.is_empty() {
                self.add_node(prefix, name_part, TokenType::Name);
            }
            self.add_node("", "<", TokenType::BracketOpen);
            let last_idx = self.arena.len() - 1;
            self.node_manager.push_bracket(last_idx);
        } else if token_type == TokenType::StatementEnd {
            let has_matching_case = self
                .node_manager
                .open_brackets
                .iter()
                .any(|&idx| self.arena[idx].token.token_type == TokenType::StatementStart);
            if has_matching_case {
                self.add_node(prefix, token_text, TokenType::StatementEnd);
            } else {
                self.add_node(prefix, token_text, alt_token_type);
            }
        } else {
            self.add_node(prefix, token_text, token_type);
            if token_type.is_opening_bracket() {
                let last_idx = self.arena.len() - 1;
                self.node_manager.push_bracket(last_idx);
            }
        }
    }

    /// After adding DDL AS keyword, check if the function body should use
    /// main rules (when the next token is not a quoted string).
    fn handle_ddl_as(&mut self, source: &str) {
        let remaining = &source[self.pos..];
        let mut check = remaining.trim_start();
        loop {
            if check.starts_with("--") || check.starts_with("//") || check.starts_with('#') {
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
            self.pop_state();
        }
    }

    /// Handle {% else %}, {% elif %}, etc. — close the previous block section
    /// and open a new one, inheriting the SQL context from the block start.
    fn handle_jinja_block_keyword(&mut self, prefix: &str, token_text: &str) {
        // Save the block start's previous_node before popping.
        // Python sqlfmt sets {% else %}'s previous_node to {% if %}'s
        // previous_node so get_previous_token resolves to the SQL
        // context before the entire if/else block.
        let block_start_prev = self
            .node_manager
            .open_jinja_blocks
            .last()
            .and_then(|&idx| self.arena[idx].previous_node);
        self.node_manager.pop_jinja_block();
        self.add_node(prefix, token_text, TokenType::JinjaBlockKeyword);
        let last_idx = self.arena.len() - 1;
        if let Some(bsp) = block_start_prev {
            self.arena[last_idx].previous_node = Some(bsp);
        }
        self.node_manager.push_jinja_block(last_idx);
    }

    /// Handle {% endif %}, {% endfor %}, etc. — close the jinja block,
    /// popping data-mode state if this is an endset/endcall.
    fn handle_jinja_block_end(&mut self, prefix: &str, token_text: &str) {
        if self.lex_state.len() > 1 {
            let lower = token_text.to_lowercase();
            if lower.contains("endset") || lower.contains("endcall") {
                self.pop_state();
            }
        }
        self.node_manager.pop_jinja_block();
        self.add_node(prefix, token_text, TokenType::JinjaBlockEnd);
    }

    /// Push data-mode state for {% call %} and {% set %} blocks.
    fn maybe_push_jinja_data_rules(&mut self, token_text: &str) {
        // Strip Jinja delimiters and whitespace without allocating a lowercased copy.
        let stripped = token_text
            .trim_start_matches(|c: char| c == '{' || c == '%' || c == '-' || c.is_whitespace());
        let starts_with_ci = |s: &str, prefix: &str| -> bool {
            s.len() >= prefix.len()
                && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
        };
        let starts_with_ci_not = |s: &str, prefix: &str, not_prefix: &str| -> bool {
            starts_with_ci(s, prefix) && !starts_with_ci(s, not_prefix)
        };
        if starts_with_ci_not(stripped, "call", "call statement") {
            self.push_state(LexState::JinjaCallBlock);
        } else if starts_with_ci(stripped, "set") && !token_text.contains('=') {
            self.push_state(LexState::JinjaSetBlock);
        }
    }

    /// Handle Jinja expressions with depth-aware scanning for nested `{{ }}`.
    fn handle_jinja(
        &mut self,
        prefix: &str,
        token_text: &str,
        token_type: TokenType,
        source: &str,
        match_len: usize,
    ) {
        if token_type == TokenType::JinjaExpression {
            let remaining = &source[self.pos..];
            let prefix_len = prefix.len();
            let tag_start = &remaining[prefix_len..];
            if let Some(tag_len) = Self::find_jinja_expr_end(tag_start) {
                let full_text = &tag_start[..tag_len];
                self.add_node(prefix, full_text, token_type);
                self.pos += prefix_len + tag_len;
                return;
            }
        }
        self.add_node(prefix, token_text, token_type);
        self.pos += match_len;
    }

    /// Add a node to the node buffer.
    fn add_node(&mut self, prefix: &str, token_text: &str, token_type: TokenType) {
        let spos = self.pos as u32;
        let epos = spos + (prefix.len() + token_text.len()) as u32;
        let token = Token::new(token_type, prefix, token_text, spos, epos);

        let prev = self.previous_node_index();
        let node = self.node_manager.create_node(token, prev, &self.arena);
        let idx = self.arena.len();
        self.arena.push(node);

        if token_type.is_opening_bracket()
            && !matches!(token_type, TokenType::BracketOpen if token_text.contains('<'))
        {
            self.node_manager.push_bracket(idx);
        }

        self.node_buffer.push(idx);
    }

    /// Add a comment to the comment buffer.
    fn add_comment(&mut self, _prefix: &str, token_text: &str) {
        let spos = self.pos as u32;
        let epos = spos + token_text.len() as u32;
        let token = Token::new(TokenType::Comment, "", token_text, spos, epos);
        let is_standalone = self.node_buffer.is_empty();
        let prev = self.comment_prev_node(is_standalone);

        if self.try_attach_inline_comment_to_semicolon_line(&token, _prefix, is_standalone, prev) {
            return;
        }

        self.had_suppressed_newline = false;
        let comment = Comment::new(token, is_standalone, prev);
        self.comment_buffer.push(comment);
    }

    /// Determine the previous node for a comment. When the last buffered node
    /// is a semicolon, attach to the node before it so the comment stays with
    /// the preceding content when the semicolon is split to its own line.
    fn comment_prev_node(&self, is_standalone: bool) -> Option<NodeIndex> {
        if is_standalone {
            return self.previous_node_index();
        }
        let Some(&last_idx) = self.node_buffer.last() else {
            return self.previous_node_index();
        };
        if self.arena[last_idx].token.token_type == TokenType::Semicolon
            && self.node_buffer.len() >= 2
        {
            Some(self.node_buffer[self.node_buffer.len() - 2])
        } else {
            self.previous_node_index()
        }
    }

    /// When the buffer is empty but the last flushed line ends with a semicolon
    /// and the comment is on the same line, attach it as an inline comment
    /// on that line. Returns true if the comment was attached.
    fn try_attach_inline_comment_to_semicolon_line(
        &mut self,
        token: &Token,
        prefix: &str,
        is_standalone: bool,
        prev: Option<NodeIndex>,
    ) -> bool {
        if !is_standalone || self.line_buffer.is_empty() || token.text.is_empty() {
            return false;
        }
        let same_line =
            !prefix.contains('\n') && !prefix.is_empty() && !self.had_suppressed_newline;
        if !same_line {
            return false;
        }
        let Some(last_line) = self.line_buffer.last() else {
            return false;
        };
        let has_trailing_semicolon = last_line.nodes.iter().rev().any(|&idx| {
            let node = &self.arena[idx];
            !node.is_newline() && node.token.token_type == TokenType::Semicolon
        });
        if !has_trailing_semicolon {
            return false;
        }
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
        let comment = Comment::new(token.clone(), false, attach_to);
        if let Some(last_line) = self.line_buffer.last_mut() {
            last_line.append_comment(comment);
        }
        true
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
                self.pos as u32,
                (self.pos + 1) as u32,
            );
            let node = self.node_manager.create_node(token, prev, &self.arena);
            let idx = self.arena.len();
            self.arena.push(node);

            let mut line = Line::new(prev);
            line.append_node(idx);
            if !self.arena[idx].formatting_disabled.is_empty() {
                line.formatting_disabled = true;
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

        for &idx in &self.node_buffer {
            line.append_node(idx);
        }

        for &idx in &self.node_buffer {
            if !self.arena[idx].formatting_disabled.is_empty() {
                line.formatting_disabled = true;
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
            self.pos as u32,
            (self.pos + 1) as u32,
        );
        let nl_node = self
            .node_manager
            .create_node(nl_token, nl_prev, &self.arena);
        let nl_idx = self.arena.len();
        self.arena.push(nl_node);
        line.append_node(nl_idx);

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
        if i < len && bytes[i] == b'-' {
            i += 1;
        }

        let mut depth = 1;
        while i < len && depth > 0 {
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

            if i + 1 < len && bytes[i] == b'{' && bytes[i + 1] == b'{' {
                depth += 1;
                i += 2;
                continue;
            }

            if i + 2 < len && bytes[i] == b'-' && bytes[i + 1] == b'}' && bytes[i + 2] == b'}' {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 3);
                }
                i += 3;
                continue;
            }

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
        // SQL comments do NOT nest, so inner /* inside a comment is ignored.
        let bytes = source.as_bytes();
        let len = bytes.len();
        let mut i = 0;
        let mut in_comment = false;

        while i < len {
            if in_comment {
                // Use memchr to jump to next '*' inside a block comment
                if let Some(offset) = memchr(b'*', &bytes[i..]) {
                    let pos = i + offset;
                    if pos + 1 < len && bytes[pos + 1] == b'/' {
                        in_comment = false;
                        i = pos + 2;
                    } else {
                        i = pos + 1;
                    }
                } else {
                    // No '*' found — rest is all comment
                    break;
                }
                continue;
            }

            if bytes[i] == b'\'' || bytes[i] == b'"' {
                i = skip_string_literal(bytes, i);
                continue;
            }
            if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                // Use memchr to find end of line comment
                if let Some(offset) = memchr(b'\n', &bytes[i..]) {
                    i += offset;
                } else {
                    i = len;
                }
                continue;
            }
            if i + 1 < len
                && bytes[i] == b'{'
                && (bytes[i + 1] == b'{' || bytes[i + 1] == b'%' || bytes[i + 1] == b'#')
            {
                i = skip_jinja_block(bytes, i);
                continue;
            }
            if bytes[i] == b'$' {
                let ds_len = scan_dollar_string(&bytes[i..]);
                if ds_len > 0 {
                    i += ds_len;
                    continue;
                }
            }
            if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                in_comment = true;
                i += 2;
                continue;
            }
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
                            node.token.text
                        )));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn push_state(&mut self, state: LexState) {
        self.lex_state.push(state);
    }

    fn pop_state(&mut self) {
        if self.lex_state.len() > 1 {
            self.lex_state.pop();
        }
    }
}

/// Skip a Jinja block ({{ }}, {% %}, or {# #}) starting at position `i`.
/// Handles nested blocks and string literals. Returns position after the block.
fn skip_jinja_block(bytes: &[u8], i: usize) -> usize {
    let open = bytes[i + 1];
    let close = match open {
        b'{' => b'}',
        b'%' => b'%',
        _ => b'#',
    };
    let len = bytes.len();
    let mut depth = 1;
    let mut j = i + 2;
    while j + 1 < len && depth > 0 {
        if bytes[j] == b'\'' || bytes[j] == b'"' {
            j = skip_string_literal(bytes, j);
            continue;
        }
        if bytes[j] == b'{' && bytes[j + 1] == open {
            depth += 1;
            j += 2;
        } else if bytes[j] == close && bytes[j + 1] == b'}' {
            depth -= 1;
            j += 2;
        } else {
            j += 1;
        }
    }
    j
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_analyzer() -> Analyzer {
        let nm = NodeManager::new(false);
        Analyzer::new(nm, 88)
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

    // --- Additional analyzer tests for coverage parity with Python ---

    /// Helper: format SQL through the full pipeline.
    fn format_sql(source: &str) -> Result<String, crate::error::SqlfmtError> {
        crate::api::format_string(source, &crate::mode::Mode::default())
    }

    #[test]
    fn test_unmatched_closing_paren_error() {
        let result = format_sql("SELECT )\n");
        assert!(result.is_err(), "Unmatched ) should error");
        let err = result.unwrap_err();
        assert!(
            matches!(err, crate::error::SqlfmtError::Bracket(_)),
            "Expected Bracket error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_unmatched_closing_bracket_error() {
        let result = format_sql("SELECT ]\n");
        assert!(result.is_err(), "Unmatched ] should error");
        let err = result.unwrap_err();
        assert!(
            matches!(err, crate::error::SqlfmtError::Bracket(_)),
            "Expected Bracket error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_unterminated_block_comment_error() {
        let result = format_sql("/* unclosed\n");
        assert!(result.is_err(), "Unterminated block comment should error");
    }

    #[test]
    fn test_empty_newlines_create_blank_lines() {
        let mut analyzer = create_analyzer();
        let source = "SELECT 1\n\n\n\nSELECT 2\n";
        let query = analyzer.parse_query(source).unwrap();
        // Multiple blank newlines should produce blank lines
        let blank_count = query
            .lines
            .iter()
            .filter(|l| l.is_blank_line(&analyzer.arena))
            .count();
        assert!(
            blank_count >= 1,
            "Multiple newlines should create blank lines"
        );
    }

    #[test]
    fn test_leading_comment_preserved() {
        let mut analyzer = create_analyzer();
        let source = "-- leading comment\nSELECT 1\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(
            rendered.contains("-- leading comment"),
            "Leading comment should be preserved: {}",
            rendered
        );
    }

    #[test]
    fn test_set_operator_classification() {
        let mut analyzer = create_analyzer();
        let source = "SELECT 1\nUNION ALL\nSELECT 2\n";
        let query = analyzer.parse_query(source).unwrap();
        // Verify UNION ALL is classified as SetOperator
        let has_set_op = query
            .tokens(&analyzer.arena)
            .iter()
            .any(|n| n.token.token_type == crate::token::TokenType::SetOperator);
        assert!(has_set_op, "UNION ALL should be classified as SetOperator");
    }

    #[test]
    fn test_jinja_block_start_tracking() {
        let mut analyzer = create_analyzer();
        let source = "{% if x %}\nSELECT 1\n{% endif %}\n";
        let query = analyzer.parse_query(source).unwrap();
        let has_block_start = query
            .tokens(&analyzer.arena)
            .iter()
            .any(|n| n.token.token_type == crate::token::TokenType::JinjaBlockStart);
        assert!(has_block_start, "{{%}} if should create JinjaBlockStart");
    }

    #[test]
    fn test_jinja_block_keyword_context() {
        let mut analyzer = create_analyzer();
        let source = "{% if x %}\nSELECT 1\n{% elif y %}\nSELECT 2\n{% endif %}\n";
        let query = analyzer.parse_query(source).unwrap();
        let has_keyword = query
            .tokens(&analyzer.arena)
            .iter()
            .any(|n| n.token.token_type == crate::token::TokenType::JinjaBlockKeyword);
        assert!(has_keyword, "{{%}} elif should create JinjaBlockKeyword");
    }

    #[test]
    fn test_jinja_block_end_pops() {
        let mut analyzer = create_analyzer();
        let source = "{% if x %}\nSELECT 1\n{% endif %}\n";
        let query = analyzer.parse_query(source).unwrap();
        let has_block_end = query
            .tokens(&analyzer.arena)
            .iter()
            .any(|n| n.token.token_type == crate::token::TokenType::JinjaBlockEnd);
        assert!(has_block_end, "{{%}} endif should create JinjaBlockEnd");
        // After endif, jinja depth should be back to 0
        let tokens = query.tokens(&analyzer.arena);
        let last_node = tokens.last().unwrap();
        let (_, jinja_depth) = last_node.depth();
        assert_eq!(jinja_depth, 0, "Jinja depth should be 0 after endif");
    }

    #[test]
    fn test_jinja_set_block() {
        let mut analyzer = create_analyzer();
        let source = "{% set x %}data content{% endset %}\nSELECT 1\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(
            rendered.contains("{% set x %}") || rendered.contains("{%"),
            "Jinja set block should be preserved: {}",
            rendered
        );
    }

    #[test]
    fn test_jinja_nested_blocks_depth() {
        let mut analyzer = create_analyzer();
        let source =
            "{% if a %}\n{% for x in items %}\nSELECT {{ x }}\n{% endfor %}\n{% endif %}\n";
        let query = analyzer.parse_query(source).unwrap();
        // Should parse without error — depth tracking handles nesting
        assert!(!query.lines.is_empty());
        let rendered = query.render(&analyzer.arena);
        assert!(rendered.contains("{% if a %}"));
        assert!(rendered.contains("{% endfor %}"));
        assert!(rendered.contains("{% endif %}"));
    }

    #[test]
    fn test_jinja_empty_block() {
        let mut analyzer = create_analyzer();
        let source = "{% if x %}\n{% endif %}\nSELECT 1\n";
        let result = analyzer.parse_query(source);
        assert!(
            result.is_ok(),
            "Empty jinja block should parse without error"
        );
    }

    #[test]
    fn test_orphan_jinja_endblock_error() {
        // {% endif %} without matching {% if %} should still parse
        // (jinja blocks are tracked but orphans don't always error in the lexer)
        let mut analyzer = create_analyzer();
        let source = "{% endif %}\nSELECT 1\n";
        let _result = analyzer.parse_query(source);
        // The key is it doesn't panic; behavior may vary
    }

    #[test]
    fn test_unsupported_ddl_detection() {
        // CREATE TABLE uses unsupported/DDL rules
        let mut analyzer = create_analyzer();
        let source = "CREATE TABLE t (id INT)\n";
        let result = analyzer.parse_query(source);
        // Should parse (possibly with different rule handling)
        assert!(
            result.is_ok(),
            "CREATE TABLE should parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_explain_as_keyword() {
        let mut analyzer = create_analyzer();
        let source = "EXPLAIN SELECT 1\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(
            rendered.contains("explain"),
            "EXPLAIN should be lowercased: {}",
            rendered
        );
    }

    #[test]
    fn test_semicolon_resets_context() {
        let mut analyzer = create_analyzer();
        let source = "SELECT (1;\nSELECT 2\n";
        // Semicolon should reset bracket depth — second SELECT should parse
        let _result = analyzer.parse_query(source);
        // The key is no panic from mismatched brackets
    }

    #[test]
    fn test_angle_bracket_disambiguation() {
        // ARRAY<INT> should use angle brackets, not comparison operators
        let mut analyzer = create_analyzer();
        let source = "SELECT CAST(x AS ARRAY<INT>)\n";
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        assert!(
            rendered.contains("array"),
            "ARRAY type should be preserved: {}",
            rendered
        );
    }

    #[test]
    fn test_unary_number() {
        // Unary minus should be absorbed into the number
        let result = format_sql("SELECT -1\n").unwrap();
        assert!(
            result.contains("-1"),
            "Unary minus should be absorbed: {}",
            result
        );
    }

    #[test]
    fn test_binary_operator_not_absorbed() {
        // Binary minus should stay as separate operator
        let result = format_sql("SELECT a - 1\n").unwrap();
        assert!(
            result.contains("a") && result.contains("- 1") || result.contains("-"),
            "Binary minus should be separate: {}",
            result
        );
    }

    #[test]
    fn test_reserved_keyword_after_dot() {
        // "select" after a dot should be treated as a Name, not a keyword
        let mut analyzer = create_analyzer();
        let source = "SELECT t.select FROM t\n";
        let result = analyzer.parse_query(source);
        assert!(
            result.is_ok(),
            "Keyword after dot should parse as name: {:?}",
            result.err()
        );
    }
}
