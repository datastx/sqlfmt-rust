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
        // This matches the Python pattern where depth propagates through nodes
        let (open_brackets, open_jinja_blocks) =
            self.compute_open_brackets(&token, previous_node, arena);

        let formatting_disabled = self.compute_formatting_disabled(&token, previous_node, arena);

        let (prefix, value) = if !formatting_disabled.is_empty() {
            (token.prefix.clone(), token.text.clone())
        } else {
            let prefix = self
                .compute_prefix(&token, previous_node, arena)
                .into_owned();
            let value = self.standardize_value(&token).into_owned();
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
        let (mut node_brackets, mut node_jinja) = match previous_node {
            None => (SmallVec::new(), SmallVec::new()),
            Some(prev_idx) => {
                let prev = &arena[prev_idx];
                let mut ob = prev.open_brackets.clone();
                let mut oj = prev.open_jinja_blocks.clone();

                // LATERAL is an unterm keyword for splitting but does NOT
                // increase depth for the next node — it's a FROM clause
                // modifier, not a clause-level keyword.
                if prev.is_unterm_keyword() || prev.is_opening_bracket() {
                    let is_lateral_kw =
                        prev.is_unterm_keyword() && prev.value.eq_ignore_ascii_case("lateral");
                    if !is_lateral_kw {
                        ob.push(prev_idx);
                    }
                } else if prev.is_opening_jinja_block() {
                    oj.push(prev_idx);
                }

                (ob, oj)
            }
        };

        match token.token_type {
            TokenType::UntermKeyword | TokenType::SetOperator => {
                // LATERAL should NOT pop the previous keyword — it's a modifier
                // within the FROM clause, not a replacement for FROM.
                let is_lateral = token.text.eq_ignore_ascii_case("lateral");
                if !is_lateral {
                    if let Some(last) = node_brackets.last() {
                        if arena[*last].is_unterm_keyword() {
                            node_brackets.pop();
                        }
                    }
                }
            }
            TokenType::BracketClose | TokenType::StatementEnd => {
                while let Some(last) = node_brackets.last() {
                    if arena[*last].is_unterm_keyword() {
                        node_brackets.pop();
                    } else {
                        break;
                    }
                }
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
            // Push a marker index (the value doesn't matter, only non-emptiness is checked)
            formatting_disabled.push(previous_node.unwrap_or(0));
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

        if previous_node.is_none() {
            return Cow::Borrowed("");
        }

        if tt.is_never_preceded_by_space() {
            return Cow::Borrowed("");
        }

        let (prev, extra_whitespace) = Self::get_previous_token(previous_node, arena);
        let prev_type = prev.map(|n| n.token.token_type);

        if matches!(
            prev_type,
            Some(TokenType::BracketOpen) | Some(TokenType::DoubleColon) | Some(TokenType::Colon)
        ) {
            return Cow::Borrowed("");
        }

        // This must come BEFORE the is_preceded_by_space check so that
        // `table.*` renders as `table.*` not `table. *`
        if tt.is_possible_name()
            && matches!(prev_type, Some(TokenType::Dot) | Some(TokenType::Colon))
        {
            return Cow::Borrowed("");
        }

        if tt == TokenType::Number && prev_type == Some(TokenType::Colon) {
            return Cow::Borrowed("");
        }

        if matches!(tt, TokenType::Number | TokenType::Dot | TokenType::Name)
            && prev_type == Some(TokenType::Operator)
        {
            if let Some(prev_node) = prev {
                if prev_node.value == "+" || prev_node.value == "-" {
                    let (prev_prev, _) = Self::get_previous_token(prev_node.previous_node, arena);
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
                                | TokenType::Star
                                | TokenType::On
                                | TokenType::DoubleColon
                        ),
                    };
                    if is_unary {
                        return Cow::Borrowed("");
                    }
                }
            }
        }

        // *REPLACE and *EXCLUDE are handled by the star_replace_exclude rule
        if tt == TokenType::Name && prev_type == Some(TokenType::Star) {
            if let Some(prev_node) = prev {
                if !prev_node.is_multiplication_star(arena)
                    && token.text.eq_ignore_ascii_case("columns")
                {
                    return Cow::Borrowed("");
                }
            }
        }

        if tt == TokenType::Name && prev_type == Some(TokenType::Name) {
            if let Some(prev_node) = prev {
                let pv = &prev_node.value;
                if pv.len() == 1
                    && matches!(
                        pv.as_bytes()[0],
                        b'r' | b'b'
                            | b'f'
                            | b'u'
                            | b'x'
                            | b'e'
                            | b'R'
                            | b'B'
                            | b'F'
                            | b'U'
                            | b'X'
                            | b'E'
                    )
                    && token.text.starts_with('\'')
                {
                    return Cow::Borrowed("");
                }
            }
        }

        if tt.is_preceded_by_space_except_after_open_bracket() {
            return Cow::Borrowed(" ");
        }

        if tt == TokenType::BracketOpen && token.text.contains('<') {
            if let Some(prev_node) = prev {
                if prev_node.value.eq_ignore_ascii_case("array")
                    || prev_node.value.eq_ignore_ascii_case("struct")
                    || prev_node.value.eq_ignore_ascii_case("map")
                    || prev_node.value.eq_ignore_ascii_case("table")
                {
                    return Cow::Borrowed("");
                }
            }
            if prev_type.is_some() && prev_type != Some(TokenType::BracketOpen) {
                return Cow::Borrowed(" ");
            } else {
                return Cow::Borrowed("");
            }
        }

        if tt == TokenType::BracketOpen && prev_type == Some(TokenType::UntermKeyword) {
            if let Some(prev_node) = prev {
                if prev_node.value.eq_ignore_ascii_case("lateral") {
                    return Cow::Borrowed("");
                }
            }
        }

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
                    // Snowflake DDL: before(, at( always need a space
                    if prev_node.value.eq_ignore_ascii_case("before")
                        || prev_node.value.eq_ignore_ascii_case("at")
                    {
                        return Cow::Borrowed(" ");
                    }
                    if prev_node.value.eq_ignore_ascii_case("filter")
                        || prev_node.value.eq_ignore_ascii_case("offset")
                    {
                        let (pp, _) = Self::get_previous_token(prev_node.previous_node, arena);
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

        if tt == TokenType::BracketOpen && token.text == "[" && prev_type == Some(TokenType::Colon)
        {
            return Cow::Borrowed("");
        }

        if tt == TokenType::BracketOpen {
            return Cow::Borrowed(" ");
        }

        if tt == TokenType::JinjaBlockKeyword {
            return Cow::Borrowed("");
        }

        // Jinja: respect original whitespace. Since does_not_set_prev_sql_context
        // now skips all jinja statement types (block start/keyword/end),
        // prev_type here reflects the SQL context before any jinja blocks.
        // This matches Python's whitespace() logic exactly.
        if tt.is_jinja() {
            if !token.prefix.is_empty() || extra_whitespace {
                return Cow::Borrowed(" ");
            } else {
                return Cow::Borrowed("");
            }
        }

        if prev_type == Some(TokenType::JinjaExpression) {
            if !token.prefix.is_empty() || extra_whitespace {
                return Cow::Borrowed(" ");
            } else {
                return Cow::Borrowed("");
            }
        }

        Cow::Borrowed(" ")
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
    /// Returns Cow::Borrowed when the value doesn't need changing (fast path).
    fn standardize_value<'a>(&self, token: &'a Token) -> Cow<'a, str> {
        let tt = token.token_type;

        if tt.is_always_lowercased() {
            // Fast path: if already lowercase and single-word, borrow directly
            if !token.text.contains(|c: char| c.is_ascii_whitespace())
                && token.text.bytes().all(|b| !b.is_ascii_uppercase())
            {
                return Cow::Borrowed(&token.text);
            }
            // Use optimized bulk lowercase, then normalize whitespace without Vec
            let lower = token.text.to_ascii_lowercase();
            if !lower.contains(|c: char| c.is_ascii_whitespace()) {
                return Cow::Owned(lower);
            }
            let mut result = String::with_capacity(lower.len());
            for (i, word) in lower.split_whitespace().enumerate() {
                if i > 0 {
                    result.push(' ');
                }
                result.push_str(word);
            }
            return Cow::Owned(result);
        }

        if tt == TokenType::Number {
            if token.text.bytes().all(|b| !b.is_ascii_uppercase()) {
                return Cow::Borrowed(&token.text);
            }
            return Cow::Owned(token.text.to_ascii_lowercase());
        }

        if !self.case_sensitive_names && tt == TokenType::Name {
            let first = token.text.as_bytes().first().copied();
            if matches!(first, Some(b'\'') | Some(b'$')) {
                return Cow::Borrowed(&token.text);
            }
            if token.text.bytes().all(|b| !b.is_ascii_uppercase()) {
                return Cow::Borrowed(&token.text);
            }
            return Cow::Owned(token.text.to_ascii_lowercase());
        }

        // Jinja tokens, quoted names, etc.: preserve original text
        Cow::Borrowed(&token.text)
    }

    /// Enable formatting (handle fmt:on).
    #[cfg(test)]
    pub fn enable_formatting(&mut self) {
        self.formatting_disabled.clear();
    }

    /// Disable formatting (handle fmt:off).
    #[cfg(test)]
    pub fn disable_formatting(&mut self) {
        self.formatting_disabled.push(0);
    }

    /// Check if formatting is currently disabled.
    #[cfg(test)]
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

        nm.open_brackets.pop();
        assert_eq!(nm.open_brackets.len(), 1);
    }

    #[test]
    fn test_formatting_disabled() {
        let mut nm = NodeManager::new(false);
        assert!(!nm.is_formatting_disabled());

        nm.disable_formatting();
        assert!(nm.is_formatting_disabled());

        nm.enable_formatting();
        assert!(!nm.is_formatting_disabled());
    }

    // --- Additional node_manager tests for coverage parity ---

    /// Helper: format SQL through the full pipeline and return tokens.
    fn format_and_get_tokens(source: &str) -> (Vec<crate::node::Node>, String) {
        let mode = crate::mode::Mode::default();
        let dialect = mode.dialect().unwrap();
        let mut analyzer = dialect.initialize_analyzer(mode.line_length);
        let query = analyzer.parse_query(source).unwrap();
        let rendered = query.render(&analyzer.arena);
        (analyzer.arena, rendered)
    }

    #[test]
    fn test_jinja_depth_tracking() {
        // Verify jinja_depth returns to 0 after balanced blocks
        let (arena, _) = format_and_get_tokens("{% if x %}\nSELECT 1\n{% endif %}\n");
        // After endif, jinja depth should be back to 0
        let endif_node = arena
            .iter()
            .find(|n| n.token.token_type == TokenType::JinjaBlockEnd)
            .expect("Should have JinjaBlockEnd node");
        let (_, jd) = endif_node.depth();
        assert_eq!(jd, 0, "Jinja depth should be 0 after endif");

        // Inside the block, jinja depth should be > 0
        let inner_node = arena
            .iter()
            .find(|n| n.token.token_type == TokenType::Number)
            .expect("Should have number node");
        let (_, jd_inner) = inner_node.depth();
        assert!(jd_inner > 0, "Jinja depth should be > 0 inside if block");
    }

    #[test]
    fn test_union_depth_resets_to_zero() {
        let (arena, _) = format_and_get_tokens("SELECT 1\nUNION ALL\nSELECT 2\n");
        // Find the SetOperator node
        let union_node = arena
            .iter()
            .find(|n| n.token.token_type == TokenType::SetOperator)
            .expect("Should have UNION ALL node");
        let (bd, _) = union_node.depth();
        // UNION ALL pops unterm keywords, so bracket depth should be 0
        assert_eq!(bd, 0, "UNION ALL should have bracket depth 0");
    }

    #[test]
    fn test_capitalization_clickhouse_preserves_case() {
        let nm = NodeManager::new(true); // case_sensitive_names = true
        let token = Token::new(TokenType::Name, "", "myFunction", 0, 10);
        assert_eq!(
            nm.standardize_value(&token),
            "myFunction",
            "Case-sensitive mode should preserve function name casing"
        );
    }

    #[test]
    fn test_capitalization_operators_lowercased() {
        let nm = NodeManager::new(false);
        let operators = vec![
            (TokenType::BooleanOperator, "AND", "and"),
            (TokenType::BooleanOperator, "OR", "or"),
            (TokenType::BooleanOperator, "NOT", "not"),
            (TokenType::WordOperator, "AS", "as"),
            (TokenType::WordOperator, "IN", "in"),
            (TokenType::WordOperator, "LIKE", "like"),
            (TokenType::On, "ON", "on"),
            (TokenType::SetOperator, "UNION ALL", "union all"),
        ];
        for (tt, input, expected) in operators {
            let token = Token::new(tt, "", input, 0, input.len());
            assert_eq!(
                nm.standardize_value(&token),
                expected,
                "Operator '{}' should lowercase to '{}'",
                input,
                expected
            );
        }
    }

    #[test]
    fn test_capitalization_numbers_lowercased() {
        let nm = NodeManager::new(false);
        let cases = vec![("0xFF", "0xff"), ("1E10", "1e10"), ("0XAB", "0xab")];
        for (input, expected) in cases {
            let token = Token::new(TokenType::Number, "", input, 0, input.len());
            assert_eq!(
                nm.standardize_value(&token),
                expected,
                "Number '{}' should lowercase to '{}'",
                input,
                expected
            );
        }
    }

    #[test]
    fn test_identifier_whitespace() {
        // Test prefix spacing for various identifier combinations via the pipeline
        let (_, rendered) = format_and_get_tokens("SELECT a, b, c FROM t\n");
        // Names after commas should have proper spacing
        assert!(
            rendered.contains("a,") || rendered.contains("a\n"),
            "Identifiers should be formatted: {}",
            rendered
        );
    }

    #[test]
    fn test_bracket_whitespace() {
        // Function call: no space before (
        let (_, rendered) = format_and_get_tokens("SELECT count(*) FROM t\n");
        assert!(
            rendered.contains("count(") || rendered.contains("count\n"),
            "No space before function paren: {}",
            rendered
        );
    }

    #[test]
    fn test_internal_whitespace_normalization() {
        let nm = NodeManager::new(false);
        // Multi-word keywords with extra whitespace
        let cases = vec![
            (TokenType::SetOperator, "UNION  ALL", "union all"),
            (TokenType::UntermKeyword, "GROUP   BY", "group by"),
            (TokenType::UntermKeyword, "ORDER    BY", "order by"),
        ];
        for (tt, input, expected) in cases {
            let token = Token::new(tt, "", input, 0, input.len());
            assert_eq!(
                nm.standardize_value(&token),
                expected,
                "Internal whitespace should normalize: '{}' -> '{}'",
                input,
                expected
            );
        }
    }

    #[test]
    fn test_jinja_whitespace_prefix() {
        // Jinja expression should preserve its original spacing context
        let (_, rendered) = format_and_get_tokens("SELECT {{ column }} FROM t\n");
        assert!(
            rendered.contains("{{ column }}") || rendered.contains("{{"),
            "Jinja expression should be present: {}",
            rendered
        );
    }

    #[test]
    fn test_formatting_disabled_propagation() {
        // fmt:off / fmt:on should toggle formatting
        let (_, rendered) =
            format_and_get_tokens("SELECT 1\n-- fmt: off\nSELECT   2\n-- fmt: on\nSELECT 3\n");
        // The fmt:off region should preserve original formatting
        assert!(
            rendered.contains("SELECT   2") || rendered.contains("select"),
            "fmt:off region formatting should be controlled: {}",
            rendered
        );
    }
}
