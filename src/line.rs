use crate::comment::Comment;
use crate::node::{Node, NodeIndex};
use crate::token::{Token, TokenType};

/// A Line is a collection of Nodes intended to be printed on one line,
/// plus any attached comments.
#[derive(Debug, Clone)]
pub struct Line {
    pub previous_node: Option<NodeIndex>,
    pub nodes: Vec<NodeIndex>,
    pub comments: Vec<Comment>,
    pub formatting_disabled: Vec<Token>,
}

impl Line {
    pub fn new(previous_node: Option<NodeIndex>) -> Self {
        Self {
            previous_node,
            nodes: Vec::new(),
            comments: Vec::new(),
            formatting_disabled: Vec::new(),
        }
    }

    pub fn is_blank_line(&self, arena: &[Node]) -> bool {
        self.nodes.len() == 1
            && arena[self.nodes[0]].is_newline()
            && self.comments.is_empty()
    }

    /// Depth of the first non-newline node.
    pub fn depth(&self, arena: &[Node]) -> (usize, usize) {
        for &idx in &self.nodes {
            if !arena[idx].is_newline() {
                return arena[idx].depth();
            }
        }
        // Blank line: use previous_node depth or (0,0)
        self.previous_node
            .map(|i| arena[i].depth())
            .unwrap_or((0, 0))
    }

    /// Number of spaces for indentation: 4 per SQL depth + 4 per Jinja depth.
    pub fn indent_size(&self, arena: &[Node]) -> usize {
        let (sql, jinja) = self.depth(arena);
        4 * (sql + jinja)
    }

    /// Indentation prefix string.
    pub fn indentation(&self, arena: &[Node]) -> String {
        " ".repeat(self.indent_size(arena))
    }

    /// Render the line to a string (nodes only, no standalone comments).
    pub fn render(&self, arena: &[Node]) -> String {
        if self.is_blank_line(arena) {
            return "\n".to_string();
        }
        let mut result = String::new();
        let mut first_content = true;
        for &idx in &self.nodes {
            let node = &arena[idx];
            if node.is_newline() {
                continue;
            }
            if first_content {
                // First content node: use indentation instead of prefix
                result.push_str(&self.indentation(arena));
                result.push_str(&node.value);
                first_content = false;
            } else {
                result.push_str(&node.to_formatted_string());
            }
        }
        result.push('\n');
        result
    }

    /// Render with comments, respecting max_line_length.
    pub fn render_with_comments(&self, arena: &[Node], max_line_length: usize) -> String {
        if self.comments.is_empty() {
            return self.render(arena);
        }

        let mut result = String::new();
        let prefix = self.indentation(arena);

        // Standalone comments go before the line
        for comment in &self.comments {
            if comment.is_standalone {
                result.push_str(&comment.render_standalone(&prefix, max_line_length));
            }
        }

        // Render the main line content
        let base = self.render(arena);
        if self.is_blank_line(arena) && !result.is_empty() {
            // If the line is blank but we wrote comments, just return comments
            return result;
        }

        // Inline comments appended at end of line
        let inline_comments: Vec<&Comment> =
            self.comments.iter().filter(|c| c.is_inline()).collect();
        if inline_comments.is_empty() {
            result.push_str(&base);
        } else {
            // Strip trailing newline, add inline comment, re-add newline
            let trimmed = base.trim_end_matches('\n');
            result.push_str(trimmed);
            for c in inline_comments {
                result.push_str(&c.render_inline());
            }
            result.push('\n');
        }

        result
    }

    /// Length of the rendered line (longest sub-line if multiline Jinja).
    pub fn len(&self, arena: &[Node]) -> usize {
        self.render(arena)
            .lines()
            .map(|l| l.len())
            .max()
            .unwrap_or(0)
    }

    /// True if the line has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    // --- Content node helpers ---

    /// Get the first non-newline node.
    pub fn first_content_node<'a>(&self, arena: &'a [Node]) -> Option<&'a Node> {
        self.nodes
            .iter()
            .map(|&i| &arena[i])
            .find(|n| !n.is_newline())
    }

    /// Get the index of the first non-newline node.
    pub fn first_content_node_idx(&self, arena: &[Node]) -> Option<NodeIndex> {
        self.nodes.iter().copied().find(|&i| !arena[i].is_newline())
    }

    /// Get the last non-newline node.
    pub fn last_content_node<'a>(&self, arena: &'a [Node]) -> Option<&'a Node> {
        self.nodes
            .iter()
            .rev()
            .map(|&i| &arena[i])
            .find(|n| !n.is_newline())
    }

    // --- Classification properties ---

    pub fn starts_with_comma(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_comma())
            .unwrap_or(false)
    }

    pub fn starts_with_operator(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_operator(arena))
            .unwrap_or(false)
    }

    pub fn starts_with_unterm_keyword(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_unterm_keyword())
            .unwrap_or(false)
    }

    pub fn starts_with_boolean_operator(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_boolean_operator())
            .unwrap_or(false)
    }

    pub fn starts_with_set_operator(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_set_operator())
            .unwrap_or(false)
    }

    pub fn starts_with_semicolon(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_semicolon())
            .unwrap_or(false)
    }

    pub fn starts_with_opening_bracket(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_opening_bracket())
            .unwrap_or(false)
    }

    pub fn closes_bracket_from_previous_line(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_closing_bracket())
            .unwrap_or(false)
    }

    pub fn contains_operator(&self, arena: &[Node]) -> bool {
        self.nodes.iter().any(|&i| arena[i].is_operator(arena))
    }

    pub fn contains_jinja(&self, arena: &[Node]) -> bool {
        self.nodes.iter().any(|&i| arena[i].is_jinja())
    }

    pub fn contains_multiline_jinja(&self, arena: &[Node]) -> bool {
        self.nodes.iter().any(|&i| arena[i].is_multiline_jinja())
    }

    pub fn ends_with_comma(&self, arena: &[Node]) -> bool {
        self.last_content_node(arena)
            .map(|n| n.is_comma())
            .unwrap_or(false)
    }

    pub fn ends_with_opening_bracket(&self, arena: &[Node]) -> bool {
        self.last_content_node(arena)
            .map(|n| n.is_opening_bracket())
            .unwrap_or(false)
    }

    /// True if this line has only one non-newline content node.
    pub fn is_standalone_content(&self, arena: &[Node]) -> bool {
        let content_count = self
            .nodes
            .iter()
            .filter(|&&i| !arena[i].is_newline())
            .count();
        content_count == 1
    }

    /// True if this line is a standalone operator (single operator + optional newline).
    pub fn is_standalone_operator(&self, arena: &[Node]) -> bool {
        self.starts_with_operator(arena)
            && !self.starts_with_bracket_operator(arena)
            && self.is_standalone_content(arena)
    }

    /// True if this line is a standalone comma.
    pub fn is_standalone_comma(&self, arena: &[Node]) -> bool {
        self.starts_with_comma(arena) && self.is_standalone_content(arena)
    }

    /// True if first node is a bracket operator.
    pub fn starts_with_bracket_operator(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_bracket_operator(arena))
            .unwrap_or(false)
    }

    /// Check if the token preceding this line (via previous_node) is a comma.
    pub fn previous_token_is_comma(&self, arena: &[Node]) -> bool {
        if let Some(prev_idx) = self.previous_node {
            let mut idx = Some(prev_idx);
            while let Some(i) = idx {
                let node = &arena[i];
                if node.token.token_type.does_not_set_prev_sql_context() {
                    idx = node.previous_node;
                } else {
                    return node.token.token_type == TokenType::Comma;
                }
            }
        }
        false
    }

    /// True if this closes a simple jinja block from a previous line.
    pub fn closes_simple_jinja_block(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_closing_jinja_block())
            .unwrap_or(false)
    }

    /// True if this line marks the start of a new segment relative to prev_segment_depth.
    /// Mirrors Python's `starts_new_segment(prev_segment_depth)`.
    pub fn starts_new_segment_at_depth(
        &self,
        prev_segment_depth: (usize, usize),
        arena: &[Node],
    ) -> bool {
        let depth = self.depth(arena);
        if depth <= prev_segment_depth || depth.1 < prev_segment_depth.1 {
            if (self.closes_bracket_from_previous_line(arena)
                || self.closes_simple_jinja_block(arena)
                || self.is_blank_line(arena))
                && depth == prev_segment_depth
            {
                return false;
            }
            return true;
        }
        false
    }

    /// True if this line marks the start of a new segment (simple version).
    pub fn starts_new_segment(&self, arena: &[Node]) -> bool {
        if self.is_blank_line(arena) {
            return self.depth(arena) == (0, 0);
        }
        self.closes_bracket_from_previous_line(arena)
            || self.starts_with_unterm_keyword(arena)
            || self.starts_with_set_operator(arena)
            || self.starts_with_semicolon(arena)
    }

    /// True if formatting is disabled for this line.
    pub fn has_formatting_disabled(&self) -> bool {
        !self.formatting_disabled.is_empty()
    }

    /// Append a node index to this line.
    pub fn append_node(&mut self, node_idx: NodeIndex) {
        self.nodes.push(node_idx);
    }

    /// Append a comment to this line.
    pub fn append_comment(&mut self, comment: Comment) {
        self.comments.push(comment);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{Token, TokenType};

    fn make_node_in_arena(
        arena: &mut Vec<Node>,
        token_type: TokenType,
        value: &str,
        prefix: &str,
    ) -> NodeIndex {
        let idx = arena.len();
        let prev = if idx > 0 { Some(idx - 1) } else { None };
        arena.push(Node::new(
            Token::new(token_type, "", value, 0, value.len()),
            prev,
            prefix.to_string(),
            value.to_string(),
            Vec::new(),
            Vec::new(),
        ));
        idx
    }

    #[test]
    fn test_blank_line() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        assert!(line.is_blank_line(&arena));
    }

    #[test]
    fn test_line_depth() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
        arena[idx].open_brackets = vec![99]; // 1 open bracket
        let mut line = Line::new(None);
        line.append_node(idx);
        assert_eq!(line.depth(&arena), (1, 0));
        assert_eq!(line.indent_size(&arena), 4);
    }

    #[test]
    fn test_render_simple() {
        let mut arena = Vec::new();
        make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let select_idx = make_node_in_arena(&mut arena, TokenType::UntermKeyword, "select", "");
        let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", " ");

        let mut line = Line::new(Some(0));
        line.nodes.push(select_idx);
        line.nodes.push(name_idx);

        let rendered = line.render(&arena);
        assert_eq!(rendered, "select a\n");
    }

    #[test]
    fn test_starts_with_comma() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::Comma, ",", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        assert!(line.starts_with_comma(&arena));
    }

    #[test]
    fn test_is_standalone_operator() {
        let mut arena = Vec::new();
        let op_idx = make_node_in_arena(&mut arena, TokenType::Operator, "+", "");
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(op_idx);
        line.append_node(nl_idx);
        assert!(line.is_standalone_operator(&arena));
    }

    #[test]
    fn test_is_standalone_comma() {
        let mut arena = Vec::new();
        let comma_idx = make_node_in_arena(&mut arena, TokenType::Comma, ",", "");
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(comma_idx);
        line.append_node(nl_idx);
        assert!(line.is_standalone_comma(&arena));
    }

    #[test]
    fn test_line_is_too_long() {
        let mut arena = Vec::new();
        let long_value = "a".repeat(100);
        let idx = make_node_in_arena(&mut arena, TokenType::Name, &long_value, "");
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        line.append_node(nl_idx);
        assert!(line.len(&arena) > 88);
    }

    #[test]
    fn test_starts_with_operator() {
        let mut arena = Vec::new();
        let op_idx = make_node_in_arena(&mut arena, TokenType::Operator, "+", "");
        let mut line = Line::new(None);
        line.append_node(op_idx);
        assert!(line.starts_with_operator(&arena));
    }

    #[test]
    fn test_starts_with_boolean_operator() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::BooleanOperator, "and", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        assert!(line.starts_with_boolean_operator(&arena));
    }

    #[test]
    fn test_starts_with_set_operator() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::SetOperator, "union all", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        assert!(line.starts_with_set_operator(&arena));
    }

    #[test]
    fn test_starts_with_semicolon() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::Semicolon, ";", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        assert!(line.starts_with_semicolon(&arena));
    }

    #[test]
    fn test_starts_with_opening_bracket() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::BracketOpen, "(", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        assert!(line.starts_with_opening_bracket(&arena));
    }

    #[test]
    fn test_closes_bracket_from_previous_line() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::BracketClose, ")", "");
        let mut line = Line::new(None);
        line.append_node(idx);
        assert!(line.closes_bracket_from_previous_line(&arena));
    }

    #[test]
    fn test_ends_with_comma() {
        let mut arena = Vec::new();
        let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
        let comma_idx = make_node_in_arena(&mut arena, TokenType::Comma, ",", "");
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(name_idx);
        line.append_node(comma_idx);
        line.append_node(nl_idx);
        assert!(line.ends_with_comma(&arena));
    }

    #[test]
    fn test_ends_with_opening_bracket() {
        let mut arena = Vec::new();
        let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "count", "");
        let bracket_idx = make_node_in_arena(&mut arena, TokenType::BracketOpen, "(", "");
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(name_idx);
        line.append_node(bracket_idx);
        line.append_node(nl_idx);
        assert!(line.ends_with_opening_bracket(&arena));
    }

    #[test]
    fn test_contains_jinja() {
        let mut arena = Vec::new();
        let jinja_idx = make_node_in_arena(&mut arena, TokenType::JinjaExpression, "{{ x }}", "");
        let mut line = Line::new(None);
        line.append_node(jinja_idx);
        assert!(line.contains_jinja(&arena));

        // Line without jinja
        let mut arena2 = Vec::new();
        let name_idx = make_node_in_arena(&mut arena2, TokenType::Name, "x", "");
        let mut line2 = Line::new(None);
        line2.append_node(name_idx);
        assert!(!line2.contains_jinja(&arena2));
    }

    #[test]
    fn test_first_content_node_idx() {
        let mut arena = Vec::new();
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
        let mut line = Line::new(None);
        line.append_node(nl_idx);
        line.append_node(name_idx);
        assert_eq!(line.first_content_node_idx(&arena), Some(name_idx));
    }

    #[test]
    fn test_render_blank_line() {
        let mut arena = Vec::new();
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(nl_idx);
        assert_eq!(line.render(&arena), "\n");
    }

    #[test]
    fn test_indentation_with_depth() {
        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
        arena[idx].open_brackets = vec![99]; // depth 1
        let mut line = Line::new(None);
        line.append_node(idx);
        assert_eq!(line.indentation(&arena), "    "); // 4 spaces per depth level
    }

    #[test]
    fn test_has_formatting_disabled() {
        let mut line = Line::new(None);
        assert!(!line.has_formatting_disabled());
        line.formatting_disabled.push(Token::new(TokenType::FmtOff, "", "-- fmt: off", 0, 11));
        assert!(line.has_formatting_disabled());
    }

    #[test]
    fn test_is_empty_line() {
        let line = Line::new(None);
        assert!(line.is_empty());

        let mut arena = Vec::new();
        let idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
        let mut line2 = Line::new(None);
        line2.append_node(idx);
        assert!(!line2.is_empty());
    }

    #[test]
    fn test_render_with_comments_inline() {
        let mut arena = Vec::new();
        let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(name_idx);
        line.append_node(nl_idx);

        // Add an inline comment
        let comment = Comment::new(
            Token::new(TokenType::Comment, "", "-- inline comment", 0, 17),
            false,
            None,
        );
        line.append_comment(comment);

        let rendered = line.render_with_comments(&arena, 88);
        assert!(rendered.contains("a"));
        assert!(rendered.contains("inline comment"));
    }

    #[test]
    fn test_render_with_comments_standalone() {
        let mut arena = Vec::new();
        let name_idx = make_node_in_arena(&mut arena, TokenType::Name, "a", "");
        let nl_idx = make_node_in_arena(&mut arena, TokenType::Newline, "\n", "");
        let mut line = Line::new(None);
        line.append_node(name_idx);
        line.append_node(nl_idx);

        // Add a standalone comment
        let comment = Comment::new(
            Token::new(TokenType::Comment, "", "-- standalone", 0, 13),
            true,
            None,
        );
        line.append_comment(comment);

        let rendered = line.render_with_comments(&arena, 88);
        assert!(rendered.contains("standalone"));
        assert!(rendered.contains("a"));
    }
}
