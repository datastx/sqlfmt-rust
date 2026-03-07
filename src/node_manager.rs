use std::borrow::Cow;

use compact_str::CompactString;
use smallvec::SmallVec;

use crate::node::{BracketVec, JinjaBlockVec, Node, NodeIndex};
use crate::token::{Token, TokenType};

/// NodeManager creates Nodes from Tokens, tracking bracket depth,
/// whitespace rules, and formatting state.
#[derive(Debug, Clone)]
pub(crate) struct NodeManager {
    pub(crate) case_sensitive_names: bool,
    /// Filtered brackets (no unterm keywords) — used by actions like HandleNonreservedTopLevelKeyword.
    pub(crate) open_brackets: BracketVec,
    /// Current jinja block stack — used by handle_jinja_block_keyword etc.
    pub(crate) open_jinja_blocks: JinjaBlockVec,
    /// Formatting-disabled nesting depth. >0 means formatting is disabled.
    /// Uses a counter instead of bool to handle nested Data token push/pop.
    formatting_disabled_depth: u16,
    /// Running node-level brackets (including unterm keywords). Mutated in place
    /// to avoid cloning from arena on every node creation.
    node_open_brackets: BracketVec,
    /// Running node-level jinja block stack. Separate from open_jinja_blocks
    /// because external push/pop calls can temporarily diverge them.
    node_open_jinja: JinjaBlockVec,
    /// Bracket snapshots parallel to node_open_jinja. When a jinja block is
    /// opened, we snapshot node_open_brackets so we can restore it when the
    /// block closes (without needing to store brackets in each Node).
    node_open_jinja_bracket_snapshots: Vec<BracketVec>,
}

impl NodeManager {
    pub(crate) fn new(case_sensitive_names: bool) -> Self {
        Self {
            case_sensitive_names,
            open_brackets: SmallVec::new(),
            open_jinja_blocks: SmallVec::new(),
            formatting_disabled_depth: 0,
            node_open_brackets: SmallVec::new(),
            node_open_jinja: SmallVec::new(),
            node_open_jinja_bracket_snapshots: Vec::new(),
        }
    }

    /// Create a Node from a Token, applying whitespace, casing, and depth rules.
    /// Mirrors Python's NodeManager.create_node() which computes depth from
    /// previous_node's brackets, not from NodeManager state.
    pub(crate) fn create_node(
        &mut self,
        token: Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> Node {
        // This matches the Python pattern where depth propagates through nodes
        let (bracket_depth, jinja_depth) = self.compute_open_brackets(&token, previous_node, arena);

        let formatting_disabled = self.compute_formatting_disabled(&token, previous_node, arena);

        let (prefix, value) = if formatting_disabled {
            (token.prefix.clone(), token.text.clone())
        } else {
            let prefix_cow = self.compute_prefix(&token, previous_node, arena);
            let value_cow = self.standardize_value(&token);
            (
                CompactString::from(prefix_cow.as_ref()),
                CompactString::from(value_cow.as_ref()),
            )
        };

        let mut node = Node {
            token,
            previous_node,
            prefix,
            value,
            bracket_depth,
            jinja_depth,
            formatting_disabled,
            is_operator: false,
            is_bracket_operator: false,
            is_multiplication_star: false,
        };
        // Cache operator classification to avoid repeated backward arena walks during merging.
        node.is_bracket_operator = node.compute_is_bracket_operator(arena);
        node.is_multiplication_star = node.compute_is_multiplication_star(arena);
        node.is_operator = node.token.token_type.is_always_operator()
            || node.is_multiplication_star
            || node.is_bracket_operator;
        node
    }

    /// Compute the bracket and jinja depths for a new node.
    ///
    /// In Python sqlfmt, the Node's open_brackets includes both actual brackets
    /// AND unterm keywords (for depth tracking). But the NodeManager's open_brackets
    /// only contains actual brackets (BracketOpen, StatementStart), used by actions
    /// like HandleNonreservedTopLevelKeyword to decide behavior.
    ///
    /// Returns (bracket_depth, jinja_depth) as u16 counts instead of cloned SmallVecs.
    fn compute_open_brackets(
        &mut self,
        token: &Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> (u16, u16) {
        // Mutate running state in place instead of cloning from arena each time.
        // self.node_open_brackets always equals the previous node's brackets
        // at this point (set at end of previous call).
        match previous_node {
            None => {
                self.node_open_brackets.clear();
                self.node_open_jinja.clear();
                self.node_open_jinja_bracket_snapshots.clear();
            }
            Some(prev_idx) => {
                self.push_opener_from_previous_node(prev_idx, arena);
            }
        };

        self.adjust_brackets_for_token_type(token, arena);

        // NodeManager's open_brackets: ONLY actual brackets, not unterm keywords.
        // This is used by HandleNonreservedTopLevelKeyword to decide if FROM/USING
        // should be treated as keywords or names.
        self.open_brackets = self
            .node_open_brackets
            .iter()
            .filter(|&&idx| !arena[idx].is_unterm_keyword())
            .copied()
            .collect();
        self.open_jinja_blocks = self.node_open_jinja.clone();

        (
            self.node_open_brackets.len() as u16,
            self.node_open_jinja.len() as u16,
        )
    }

    /// If the previous node is an opening bracket, unterm keyword, or jinja block
    /// opener, push it onto the appropriate running bracket/jinja stack.
    /// LATERAL is excluded because it's a FROM clause modifier, not a depth-opener.
    fn push_opener_from_previous_node(&mut self, prev_idx: NodeIndex, arena: &[Node]) {
        let prev = &arena[prev_idx];
        if prev.is_unterm_keyword() || prev.is_opening_bracket() {
            let is_lateral_kw =
                prev.is_unterm_keyword() && prev.value.eq_ignore_ascii_case("lateral");
            if !is_lateral_kw {
                self.node_open_brackets.push(prev_idx);
            }
        } else if prev.is_opening_jinja_block() {
            // Snapshot brackets before pushing jinja block so we can
            // restore them when the block closes.
            self.node_open_jinja_bracket_snapshots
                .push(self.node_open_brackets.clone());
            self.node_open_jinja.push(prev_idx);
        }
    }

    /// Adjust the running bracket and jinja stacks based on the current token type.
    /// Handles popping brackets for closing tokens, unterm keyword replacement,
    /// jinja block transitions, and semicolon resets.
    fn adjust_brackets_for_token_type(&mut self, token: &Token, arena: &[Node]) {
        match token.token_type {
            TokenType::UntermKeyword | TokenType::SetOperator => {
                // LATERAL should NOT pop the previous keyword — it's a modifier
                // within the FROM clause, not a replacement for FROM.
                let is_lateral = token.text.eq_ignore_ascii_case("lateral");
                if !is_lateral {
                    if let Some(last) = self.node_open_brackets.last() {
                        if arena[*last].is_unterm_keyword() {
                            self.node_open_brackets.pop();
                        }
                    }
                }
            }
            TokenType::BracketClose | TokenType::StatementEnd => {
                while let Some(last) = self.node_open_brackets.last() {
                    if arena[*last].is_unterm_keyword() {
                        self.node_open_brackets.pop();
                    } else {
                        break;
                    }
                }
                self.node_open_brackets.pop();
            }
            TokenType::JinjaBlockEnd => {
                self.restore_jinja_bracket_snapshot();
            }
            TokenType::JinjaBlockKeyword => {
                // {% else %}, {% elif %}, etc. close the previous block section
                // and open a new one. Restore SQL brackets to the block start's state.
                self.restore_jinja_bracket_snapshot();
            }
            TokenType::Semicolon => {
                self.node_open_brackets.clear();
            }
            _ => {}
        }
    }

    /// Pop the most recent jinja block and restore SQL brackets to the state
    /// at the time the jinja block was opened.
    fn restore_jinja_bracket_snapshot(&mut self) {
        if self.node_open_jinja.pop().is_some() {
            if let Some(snapshot) = self.node_open_jinja_bracket_snapshots.pop() {
                self.node_open_brackets = snapshot;
            }
        }
    }

    /// Compute formatting_disabled state from previous node.
    /// Uses self.formatting_disabled_depth as running state, mutating in place.
    /// Returns bool (true = disabled) for the Node's formatting_disabled field.
    fn compute_formatting_disabled(
        &mut self,
        token: &Token,
        previous_node: Option<NodeIndex>,
        arena: &[Node],
    ) -> bool {
        if previous_node.is_none() {
            self.formatting_disabled_depth = 0;
        }

        if matches!(token.token_type, TokenType::FmtOff | TokenType::Data) {
            self.formatting_disabled_depth += 1;
        }

        if self.formatting_disabled_depth > 0 {
            if let Some(prev_idx) = previous_node {
                if matches!(
                    arena[prev_idx].token.token_type,
                    TokenType::FmtOn | TokenType::Data
                ) {
                    self.formatting_disabled_depth -= 1;
                }
            }
        }

        self.formatting_disabled_depth > 0
    }

    /// Open a bracket (called after node is added to arena with its index).
    pub(crate) fn push_bracket(&mut self, node_idx: NodeIndex) {
        self.open_brackets.push(node_idx);
    }

    /// Open a Jinja block.
    pub(crate) fn push_jinja_block(&mut self, node_idx: NodeIndex) {
        self.open_jinja_blocks.push(node_idx);
    }

    /// Close the most recent Jinja block.
    pub(crate) fn pop_jinja_block(&mut self) {
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
            && Self::is_unary_sign_context(prev, arena)
        {
            return Cow::Borrowed("");
        }

        // *REPLACE and *EXCLUDE are handled by the star_replace_exclude rule
        if tt == TokenType::Name && prev_type == Some(TokenType::Star) {
            if let Some(prev_node) = prev {
                if !prev_node.is_multiplication_star && token.text.eq_ignore_ascii_case("columns") {
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
            if Self::bracket_open_needs_space_after_name(prev_type, prev, arena) {
                return Cow::Borrowed(" ");
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
                    // extra_whitespace = true because we skipped
                    (prev, true)
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
                return Cow::Borrowed(&*token.text);
            }
            // Lowercase in-place on a String buffer (avoids str::to_ascii_lowercase
            // which allocates a separate String)
            let mut lower = String::from(&*token.text);
            lower.make_ascii_lowercase();
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
                return Cow::Borrowed(&*token.text);
            }
            let mut s = String::from(&*token.text);
            s.make_ascii_lowercase();
            return Cow::Owned(s);
        }

        if !self.case_sensitive_names && tt == TokenType::Name {
            let first = token.text.as_bytes().first().copied();
            if matches!(first, Some(b'\'') | Some(b'$')) {
                return Cow::Borrowed(&*token.text);
            }
            if token.text.bytes().all(|b| !b.is_ascii_uppercase()) {
                return Cow::Borrowed(&*token.text);
            }
            let mut s = String::from(&*token.text);
            s.make_ascii_lowercase();
            return Cow::Owned(s);
        }

        // Jinja tokens, quoted names, etc.: preserve original text
        Cow::Borrowed(&*token.text)
    }

    /// Enable formatting (handle fmt:on).
    #[cfg(test)]
    pub fn enable_formatting(&mut self) {
        self.formatting_disabled_depth = 0;
    }

    /// Disable formatting (handle fmt:off).
    #[cfg(test)]
    pub fn disable_formatting(&mut self) {
        self.formatting_disabled_depth = 1;
    }

    /// Check if formatting is currently disabled.
    #[cfg(test)]
    pub fn is_formatting_disabled(&self) -> bool {
        self.formatting_disabled_depth > 0
    }

    /// Check if a `+` or `-` operator is being used as a unary sign based on
    /// the token that precedes it. Returns true when the sign should be glued
    /// to the following number/name/dot (no space).
    fn is_unary_sign_context(prev: Option<&Node>, arena: &[Node]) -> bool {
        let prev_node = match prev {
            Some(n) => n,
            None => return false,
        };
        if prev_node.value != "+" && prev_node.value != "-" {
            return false;
        }
        let (prev_prev, _) = Self::get_previous_token(prev_node.previous_node, arena);
        match prev_prev {
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
        }
    }

    /// When a BracketOpen follows a Name token, determine whether a space is
    /// needed between them. Returns true for special names like `before(`,
    /// `at(`, `filter(`, and `offset(` that are clause keywords rather than
    /// function calls.
    fn bracket_open_needs_space_after_name(
        prev_type: Option<TokenType>,
        prev: Option<&Node>,
        arena: &[Node],
    ) -> bool {
        if prev_type != Some(TokenType::Name) {
            return false;
        }
        let prev_node = match prev {
            Some(n) => n,
            None => return false,
        };
        // Snowflake DDL: before(, at( always need a space
        if prev_node.value.eq_ignore_ascii_case("before")
            || prev_node.value.eq_ignore_ascii_case("at")
        {
            return true;
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
                    return true;
                }
            }
        }
        false
    }

    /// Reset state (for new query).
    /// Only clears NodeManager-level state (filtered brackets, jinja blocks used
    /// by handle_* methods). Does NOT clear node_open_brackets/node_open_jinja
    /// because they track the running node-level state and the next
    /// compute_open_brackets will maintain them from the previous arena node.
    pub(crate) fn reset(&mut self) {
        self.open_brackets.clear();
        self.open_jinja_blocks.clear();
        self.formatting_disabled_depth = 0;
    }
}

#[cfg(test)]
#[path = "node_manager_test.rs"]
mod tests;
