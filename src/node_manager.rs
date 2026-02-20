use std::borrow::Cow;

use smallvec::SmallVec;

use crate::node::{BracketVec, FmtDisabledVec, JinjaBlockVec, Node, NodeIndex};
use crate::token::{Token, TokenType};

/// NodeManager creates Nodes from Tokens, tracking bracket depth,
/// whitespace rules, and formatting state.
#[derive(Debug, Clone)]
pub struct NodeManager {
    pub case_sensitive_names: bool,
    pub open_brackets: BracketVec,
    pub open_jinja_blocks: JinjaBlockVec,
    pub formatting_disabled: FmtDisabledVec,
}

impl NodeManager {
    pub fn new(case_sensitive_names: bool) -> Self {
        Self {
            case_sensitive_names,
            open_brackets: SmallVec::new(),
            open_jinja_blocks: SmallVec::new(),
            formatting_disabled: SmallVec::new(),
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
            let prefix = self.compute_prefix(&token, previous_node, arena).into_owned();
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
    ) -> (BracketVec, JinjaBlockVec) {
        // NODE's open_brackets: includes unterm keywords for depth
        let (mut node_brackets, mut node_jinja) = match previous_node {
            None => (SmallVec::new(), SmallVec::new()),
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
                // Pop the jinja block and restore SQL brackets to the state
                // at the time the jinja block was opened. SQL scope inside a
                // jinja block doesn't leak out to the closing tag.
                if let Some(jinja_start_idx) = node_jinja.pop() {
                    node_brackets = arena[jinja_start_idx].open_brackets.clone();
                }
            }
            TokenType::JinjaBlockKeyword => {
                // {% else %}, {% elif %}, etc. close the previous block section
                // and open a new one. Restore SQL brackets to the block start's state.
                if let Some(jinja_start_idx) = node_jinja.pop() {
                    node_brackets = arena[jinja_start_idx].open_brackets.clone();
                }
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
    ) -> FmtDisabledVec {
        let mut formatting_disabled = match previous_node {
            None => SmallVec::new(),
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
    ) -> Cow<'static, str> {
        let tt = token.token_type;

        // No prefix for the very first token
        if previous_node.is_none() {
            return Cow::Borrowed("");
        }

        // Tokens that are never preceded by a space
        if tt.is_never_preceded_by_space() {
            return Cow::Borrowed("");
        }

        // Look at the previous meaningful SQL token (skipping newlines/jinja statements)
        let (prev, extra_whitespace) = Self::get_previous_token(previous_node, arena);
        let prev_type = prev.map(|n| n.token.token_type);

        // No space after an open bracket, cast operator (::), or colon
        if matches!(
            prev_type,
            Some(TokenType::BracketOpen) | Some(TokenType::DoublColon) | Some(TokenType::Colon)
        ) {
            return Cow::Borrowed("");
        }

        // Names/stars preceded by dots or colons are namespaced identifiers — no space
        // This must come BEFORE the is_preceded_by_space check so that
        // `table.*` renders as `table.*` not `table. *`
        if tt.is_possible_name()
            && matches!(prev_type, Some(TokenType::Dot) | Some(TokenType::Colon))
        {
            return Cow::Borrowed("");
        }

        // Numbers preceded by colons are simple slices — no space
        if tt == TokenType::Number && prev_type == Some(TokenType::Colon) {
            return Cow::Borrowed("");
        }

        // No space between unary +/- and the following number/dot
        // Unary context: the +/- follows an operator, keyword, comma, or open bracket
        if matches!(tt, TokenType::Number | TokenType::Dot | TokenType::Name)
            && prev_type == Some(TokenType::Operator)
        {
            if let Some(prev_node) = prev {
                if prev_node.value == "+" || prev_node.value == "-" {
                    let (prev_prev, _) =
                        Self::get_previous_token(prev_node.previous_node, arena);
                    let is_unary = match prev_prev {
                        None => true,
                        Some(pp) => matches!(
                            pp.token.token_type,
                            TokenType::Operator
                                | TokenType::WordOperator
                                | TokenType::BooleanOperator
                                | TokenType::UntermKeyword
                                | TokenType::Comma
                                | TokenType::BracketOpen
                                | TokenType::StatementStart
                                | TokenType::SetOperator
                                | TokenType::On
                                | TokenType::DoublColon
                        ),
                    };
                    if is_unary {
                        return Cow::Borrowed("");
                    }
                }
            }
        }

        // No space after a select-star when followed by "columns"
        // (DuckDB *COLUMNS pattern — *REPLACE and *EXCLUDE are handled by star_replace_exclude rule)
        if tt == TokenType::Name && prev_type == Some(TokenType::Star) {
            if let Some(prev_node) = prev {
                if !prev_node.is_multiplication_star(arena)
                    && token.token.eq_ignore_ascii_case("columns")
                {
                    return Cow::Borrowed("");
                }
            }
        }

        // No space between single-char string prefix (r, b, f, u, x, e) and following quoted string
        // Handles raw strings: r'...', binary strings: b'...', etc.
        if tt == TokenType::Name && prev_type == Some(TokenType::Name) {
            if let Some(prev_node) = prev {
                let pv = &prev_node.value;
                if pv.len() == 1
                    && matches!(
                        pv.as_bytes()[0],
                        b'r' | b'b' | b'f' | b'u' | b'x' | b'e'
                            | b'R' | b'B' | b'F' | b'U' | b'X' | b'E'
                    )
                    && token.token.starts_with('\'')
                {
                    return Cow::Borrowed("");
                }
            }
        }

        // Always a space before keywords/operators/etc. except after open bracket
        if tt.is_preceded_by_space_except_after_open_bracket() {
            return Cow::Borrowed(" ");
        }

        // Open brackets with `<` (BQ type definitions like array<int64>)
        if tt == TokenType::BracketOpen && token.token.contains('<') {
            // No space after array/struct/map (these are type constructors)
            if let Some(prev_node) = prev {
                let lv = prev_node.value.to_ascii_lowercase();
                if lv == "array" || lv == "struct" || lv == "map" {
                    return Cow::Borrowed("");
                }
            }
            if prev_type.is_some() && prev_type != Some(TokenType::BracketOpen) {
                return Cow::Borrowed(" ");
            } else {
                return Cow::Borrowed("");
            }
        }

        // "lateral(" — no space (DuckDB/Postgres lateral subquery)
        if tt == TokenType::BracketOpen && prev_type == Some(TokenType::UntermKeyword) {
            if let Some(prev_node) = prev {
                if prev_node.value.eq_ignore_ascii_case("lateral") {
                    return Cow::Borrowed("");
                }
            }
        }

        // Open brackets that follow names/quoted names are function calls
        // Open brackets that follow closing brackets are array indexes
        // Open brackets that follow open brackets are nested brackets
        // EXCEPTION: "filter(" and "offset(" after a closing bracket are clause keywords,
        // not function calls, and need a space: e.g., count(*) filter (where ...)
        if tt == TokenType::BracketOpen
            && matches!(
                prev_type,
                Some(TokenType::Name)
                    | Some(TokenType::QuotedName)
                    | Some(TokenType::BracketOpen)
                    | Some(TokenType::BracketClose)
            )
        {
            if prev_type == Some(TokenType::Name) {
                if let Some(prev_node) = prev {
                    let lv = prev_node.value.to_ascii_lowercase();
                    // Snowflake DDL: before(, at( always need a space
                    if lv == "before" || lv == "at" {
                        return Cow::Borrowed(" ");
                    }
                    if lv == "filter" || lv == "offset" {
                        let (pp, _) =
                            Self::get_previous_token(prev_node.previous_node, arena);
                        if let Some(pp_node) = pp {
                            if matches!(
                                pp_node.token.token_type,
                                TokenType::BracketClose | TokenType::StatementEnd
                            ) {
                                return Cow::Borrowed(" ");
                            }
                        }
                    }
                }
            }
            return Cow::Borrowed("");
        }

        // Open square brackets after colons are Databricks variant cols
        if tt == TokenType::BracketOpen && token.token == "[" && prev_type == Some(TokenType::Colon)
        {
            return Cow::Borrowed("");
        }

        // Need a space before any other open bracket
        if tt == TokenType::BracketOpen {
            return Cow::Borrowed(" ");
        }

        // Jinja: respect original whitespace, except block keywords like {% else %}/{% elif %}
        // which attach directly to preceding content (no space)
        if tt.is_jinja() {
            if Self::is_jinja_block_keyword(&token.token) {
                return Cow::Borrowed("");
            }
            if !token.prefix.is_empty() || extra_whitespace {
                return Cow::Borrowed(" ");
            } else {
                return Cow::Borrowed("");
            }
        }

        // After a jinja expression, respect original whitespace
        if prev_type == Some(TokenType::JinjaExpression) {
            if !token.prefix.is_empty() || extra_whitespace {
                return Cow::Borrowed(" ");
            } else {
                return Cow::Borrowed("");
            }
        }

        // Default: one space
        Cow::Borrowed(" ")
    }

    /// Check if a Jinja statement is a block keyword (else/elif) that should
    /// attach directly to preceding content without a space.
    fn is_jinja_block_keyword(token_text: &str) -> bool {
        let inner = token_text
            .trim_start_matches("{%-")
            .trim_start_matches("{%")
            .trim_end_matches("-%}")
            .trim_end_matches("%}")
            .trim();
        inner == "else" || inner.starts_with("elif ")
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

        // Lowercase number literals (hex 0xFF→0xff, octal 0O→0o, scientific 1E9→1e9)
        if tt == TokenType::Number {
            return token.token.to_ascii_lowercase();
        }

        // For non-quoted names in case-insensitive mode, lowercase —
        // but preserve case for string literals (single-quoted) and dollar-quoted strings
        if !self.case_sensitive_names && tt == TokenType::Name {
            let first = token.token.as_bytes().first().copied();
            if matches!(first, Some(b'\'') | Some(b'$')) {
                return token.token.clone();
            }
            return token.token.to_ascii_lowercase();
        }

        // Jinja tokens: preserve original text; quote normalization happens
        // in JinjaFormatter where we can selectively apply it (e.g., skip
        // multiline-preserved content like {% extends ... else 'default.html' %})
        if tt.is_jinja() {
            return token.token.clone();
        }

        token.token.clone()
    }

    /// Convert single-quoted strings to double-quoted strings inside Jinja tags.
    /// Matches Python sqlfmt's jinjafmt behavior (black's quote normalization).
    /// Skips existing double-quoted strings to avoid corrupting their content.
    fn convert_jinja_quotes(text: &str) -> String {
        let bytes = text.as_bytes();
        let len = bytes.len();
        let mut result = Vec::with_capacity(len);
        let mut i = 0;

        while i < len {
            // Skip double-quoted strings entirely (preserve as-is)
            if bytes[i] == b'"' {
                // Check for triple-double-quote (""")
                if i + 2 < len && bytes[i + 1] == b'"' && bytes[i + 2] == b'"' {
                    result.extend_from_slice(b"\"\"\"");
                    i += 3;
                    while i < len {
                        if i + 2 < len
                            && bytes[i] == b'"'
                            && bytes[i + 1] == b'"'
                            && bytes[i + 2] == b'"'
                        {
                            result.extend_from_slice(b"\"\"\"");
                            i += 3;
                            break;
                        }
                        result.push(bytes[i]);
                        i += 1;
                    }
                    continue;
                }
                result.push(b'"');
                i += 1;
                while i < len && bytes[i] != b'"' {
                    if bytes[i] == b'\\' && i + 1 < len {
                        result.push(bytes[i]);
                        result.push(bytes[i + 1]);
                        i += 2;
                        continue;
                    }
                    result.push(bytes[i]);
                    i += 1;
                }
                if i < len {
                    result.push(bytes[i]);
                    i += 1;
                }
                continue;
            }
            if bytes[i] == b'\'' {
                // Check for triple-single-quote (''')
                if i + 2 < len && bytes[i + 1] == b'\'' && bytes[i + 2] == b'\'' {
                    let start = i;
                    i += 3;
                    let mut contains_double_quote = false;
                    let mut end = None;
                    while i < len {
                        if i + 2 < len
                            && bytes[i] == b'\''
                            && bytes[i + 1] == b'\''
                            && bytes[i + 2] == b'\''
                        {
                            end = Some(i + 2);
                            break;
                        }
                        if bytes[i] == b'"' {
                            contains_double_quote = true;
                        }
                        i += 1;
                    }
                    if let Some(end_pos) = end {
                        if contains_double_quote {
                            result.extend_from_slice(&bytes[start..=end_pos]);
                        } else {
                            result.extend_from_slice(b"\"\"\"");
                            result.extend_from_slice(&bytes[start + 3..end_pos - 2]);
                            result.extend_from_slice(b"\"\"\"");
                        }
                        i = end_pos + 1;
                    } else {
                        result.extend_from_slice(&bytes[start..]);
                        break;
                    }
                    continue;
                }
                // Found a single-quoted string. Scan to find the closing quote.
                let start = i;
                i += 1;
                let mut contains_double_quote = false;
                let mut content_end = None;
                while i < len {
                    if bytes[i] == b'\\' && i + 1 < len {
                        if bytes[i + 1] == b'"' {
                            contains_double_quote = true;
                        }
                        i += 2;
                        continue;
                    }
                    if bytes[i] == b'"' {
                        contains_double_quote = true;
                    }
                    if bytes[i] == b'\'' {
                        content_end = Some(i);
                        i += 1;
                        break;
                    }
                    i += 1;
                }

                if let Some(end) = content_end {
                    if contains_double_quote {
                        // Keep single quotes if content contains double quotes
                        result.extend_from_slice(&bytes[start..=end]);
                    } else {
                        // Convert to double quotes
                        result.push(b'"');
                        result.extend_from_slice(&bytes[start + 1..end]);
                        result.push(b'"');
                    }
                } else {
                    // Unterminated string, keep as-is
                    result.extend_from_slice(&bytes[start..i]);
                }
            } else {
                result.push(bytes[i]);
                i += 1;
            }
        }

        String::from_utf8(result).unwrap_or_else(|_| text.to_string())
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
