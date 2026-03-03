use std::rc::Rc;
use std::sync::LazyLock;

use crate::comment::Comment;
use crate::node::{Node, NodeIndex};
use crate::token::TokenType;

/// Pre-computed indentation strings for common indent sizes (0..=200).
/// Avoids allocating a new String on every `indentation()` call.
/// Covers up to 50 nesting levels × 4 spaces each = 200 spaces.
static INDENT_CACHE: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    (0..=200)
        .map(|n| {
            let s = " ".repeat(n);
            // Leak the string to get a 'static reference.
            // This is a one-time cost at program startup.
            &*Box::leak(s.into_boxed_str())
        })
        .collect()
});

/// Return a cached `&'static str` of `n` spaces. Avoids allocation for n <= 200.
/// For n > 200, leaks a one-off string (should essentially never happen).
pub(crate) fn indent_str(n: usize) -> &'static str {
    if n <= 200 {
        INDENT_CACHE[n]
    } else {
        &*Box::leak(" ".repeat(n).into_boxed_str())
    }
}

/// A Line is a collection of Nodes intended to be printed on one line,
/// plus any attached comments.
#[derive(Debug, Clone)]
pub(crate) struct Line {
    pub(crate) previous_node: Option<NodeIndex>,
    pub(crate) nodes: Vec<NodeIndex>,
    pub(crate) comments: Rc<Vec<Comment>>,
    pub(crate) formatting_disabled: bool,
}

impl Line {
    pub(crate) fn new(previous_node: Option<NodeIndex>) -> Self {
        Self {
            previous_node,
            nodes: Vec::with_capacity(4),
            comments: Rc::new(Vec::new()),
            formatting_disabled: false,
        }
    }

    pub(crate) fn is_blank_line(&self, arena: &[Node]) -> bool {
        self.nodes.len() == 1 && arena[self.nodes[0]].is_newline() && self.comments.is_empty()
    }

    /// True if this line consists only of standalone comments (no SQL content).
    pub(crate) fn is_standalone_comment_line(&self, arena: &[Node]) -> bool {
        self.nodes.len() == 1
            && arena[self.nodes[0]].is_newline()
            && !self.comments.is_empty()
            && self.comments.iter().any(|c| c.is_standalone)
    }

    /// Depth of the first non-newline node.
    pub(crate) fn depth(&self, arena: &[Node]) -> (usize, usize) {
        for &idx in &self.nodes {
            if !arena[idx].is_newline() {
                return arena[idx].depth();
            }
        }
        self.previous_node
            .map(|i| arena[i].depth())
            .unwrap_or((0, 0))
    }

    /// Number of spaces for indentation: 4 per SQL depth + 4 per Jinja depth.
    pub(crate) fn indent_size(&self, arena: &[Node]) -> usize {
        let (sql, jinja) = self.depth(arena);
        4 * (sql + jinja)
    }

    /// Indentation prefix string. Returns a cached `&str` for common sizes.
    pub(crate) fn indentation<'a>(&self, arena: &[Node]) -> &'a str {
        indent_str(self.indent_size(arena))
    }

    /// Render the line to a string (nodes only, no standalone comments).
    pub(crate) fn render(&self, arena: &[Node]) -> String {
        if self.is_blank_line(arena) {
            return "\n".to_string();
        }
        if self.has_formatting_disabled() {
            return self.render_formatting_disabled(arena);
        }
        let mut result = String::with_capacity(self.indent_size(arena) + 80);
        let mut first_content = true;
        for &idx in &self.nodes {
            let node = &arena[idx];
            if node.is_newline() {
                continue;
            }
            if first_content {
                result.push_str(self.indentation(arena));
                result.push_str(&node.value);
                first_content = false;
            } else {
                node.push_formatted_to(&mut result);
            }
        }
        result.push('\n');
        result
    }

    /// Render a formatting-disabled line preserving original whitespace.
    /// Uses the original token prefix and token text from the source.
    fn render_formatting_disabled(&self, arena: &[Node]) -> String {
        let mut result = String::new();
        let mut trailing_newline_idx: Option<NodeIndex> = None;
        for &idx in &self.nodes {
            let node = &arena[idx];
            if node.is_newline() {
                trailing_newline_idx = Some(idx);
                continue;
            }
            result.push_str(&node.token.prefix);
            result.push_str(&node.token.text);
        }
        if let Some(nl_idx) = trailing_newline_idx {
            result.push_str(&arena[nl_idx].token.prefix);
        }
        result.push('\n');
        result
    }

    /// Render with comments, respecting max_line_length.
    /// If `indent_override` is provided, use it for standalone comment indentation
    /// instead of the line's own depth-based indentation.
    pub(crate) fn render_with_comments(
        &self,
        arena: &[Node],
        max_line_length: usize,
        indent_override: Option<&str>,
    ) -> String {
        if self.comments.is_empty() {
            return self.render(arena);
        }

        let mut result = String::new();
        let own_prefix = self.indentation(arena);
        let prefix = indent_override.unwrap_or(own_prefix);

        // A multiline comment that is not standalone (e.g., /* ... */ appearing mid-line)
        // must still be rendered as standalone to avoid being silently dropped.
        for comment in self.comments.iter() {
            if comment.is_standalone || comment.is_multiline() {
                result.push_str(&comment.render_standalone(prefix, max_line_length));
            }
        }

        let base = self.render(arena);
        let has_only_newline_node = self.nodes.len() == 1 && arena[self.nodes[0]].is_newline();
        if has_only_newline_node && !result.is_empty() {
            return result;
        }

        let inline_comments: Vec<&Comment> =
            self.comments.iter().filter(|c| c.is_inline()).collect();
        if inline_comments.is_empty() {
            result.push_str(&base);
        } else {
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
    /// Computed arithmetically for the common case to avoid allocating a String.
    pub(crate) fn len(&self, arena: &[Node]) -> usize {
        if self.is_blank_line(arena) {
            return 1;
        }
        if self.has_formatting_disabled() {
            return self
                .render_formatting_disabled(arena)
                .lines()
                .map(|l| l.len())
                .max()
                .unwrap_or(0);
        }
        // Check if any node contains a newline (multiline jinja).
        // If so, fall back to render-based measurement since we need
        // to find the longest sub-line.
        let has_multiline_node = self
            .nodes
            .iter()
            .any(|&idx| !arena[idx].is_newline() && arena[idx].value.contains('\n'));
        if has_multiline_node {
            return self
                .render(arena)
                .lines()
                .map(|l| l.len())
                .max()
                .unwrap_or(0);
        }
        // Fast path: compute length arithmetically
        let mut length = self.indent_size(arena);
        let mut first_content = true;
        for &idx in &self.nodes {
            let node = &arena[idx];
            if node.is_newline() {
                continue;
            }
            if first_content {
                length += node.value.len();
                first_content = false;
            } else {
                length += node.len();
            }
        }
        length
    }

    /// Get the first non-newline node.
    pub(crate) fn first_content_node<'a>(&self, arena: &'a [Node]) -> Option<&'a Node> {
        self.nodes
            .iter()
            .map(|&i| &arena[i])
            .find(|n| !n.is_newline())
    }

    /// Get the second non-newline node (useful for leading-comma lines like `, lateral`).
    pub(crate) fn second_content_node<'a>(&self, arena: &'a [Node]) -> Option<&'a Node> {
        self.nodes
            .iter()
            .map(|&i| &arena[i])
            .filter(|n| !n.is_newline())
            .nth(1)
    }

    /// Get the index of the first non-newline node.
    pub(crate) fn first_content_node_idx(&self, arena: &[Node]) -> Option<NodeIndex> {
        self.nodes.iter().copied().find(|&i| !arena[i].is_newline())
    }

    /// Get the last non-newline node.
    pub(crate) fn last_content_node<'a>(&self, arena: &'a [Node]) -> Option<&'a Node> {
        self.nodes
            .iter()
            .rev()
            .map(|&i| &arena[i])
            .find(|n| !n.is_newline())
    }

    pub(crate) fn starts_with_comma(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_comma())
            .unwrap_or(false)
    }

    pub(crate) fn starts_with_operator(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_operator(arena))
            .unwrap_or(false)
    }

    pub(crate) fn closes_bracket_from_previous_line(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_closing_bracket())
            .unwrap_or(false)
    }

    pub(crate) fn ends_with_comma(&self, arena: &[Node]) -> bool {
        self.last_content_node(arena)
            .map(|n| n.is_comma())
            .unwrap_or(false)
    }

    /// True if this line has only one non-newline content node.
    pub(crate) fn is_standalone_content(&self, arena: &[Node]) -> bool {
        let content_count = self
            .nodes
            .iter()
            .filter(|&&i| !arena[i].is_newline())
            .count();
        content_count == 1
    }

    /// True if this line is a standalone operator (single operator + optional newline).
    pub(crate) fn is_standalone_operator(&self, arena: &[Node]) -> bool {
        self.starts_with_operator(arena)
            && !self.starts_with_bracket_operator(arena)
            && self.is_standalone_content(arena)
    }

    /// True if this line is a standalone comma.
    pub(crate) fn is_standalone_comma(&self, arena: &[Node]) -> bool {
        self.starts_with_comma(arena) && self.is_standalone_content(arena)
    }

    /// True if first node is a bracket operator.
    pub(crate) fn starts_with_bracket_operator(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_bracket_operator(arena))
            .unwrap_or(false)
    }

    /// Check if the token preceding this line (via previous_node) is a comma.
    pub(crate) fn previous_token_is_comma(&self, arena: &[Node]) -> bool {
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
    pub(crate) fn closes_simple_jinja_block(&self, arena: &[Node]) -> bool {
        self.first_content_node(arena)
            .map(|n| n.is_closing_jinja_block())
            .unwrap_or(false)
    }

    /// True if this line marks the start of a new segment relative to prev_segment_depth.
    /// Mirrors Python's `starts_new_segment(prev_segment_depth)`.
    pub(crate) fn starts_new_segment_at_depth(
        &self,
        prev_segment_depth: (usize, usize),
        arena: &[Node],
    ) -> bool {
        // Formatting boundaries always start new segments:
        // FmtOff/FmtOn directives and formatting-disabled lines should not
        // be merged with adjacent formatting-enabled content.
        if self.has_formatting_disabled() {
            return true;
        }
        if let Some(n) = self.first_content_node(arena) {
            if matches!(
                n.token.token_type,
                crate::token::TokenType::FmtOff | crate::token::TokenType::FmtOn
            ) {
                return true;
            }
        }

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

    /// True if formatting is disabled for this line.
    pub(crate) fn has_formatting_disabled(&self) -> bool {
        self.formatting_disabled
    }

    /// Append a node index to this line.
    pub(crate) fn append_node(&mut self, node_idx: NodeIndex) {
        self.nodes.push(node_idx);
    }

    /// Append a comment to this line.
    pub(crate) fn append_comment(&mut self, comment: Comment) {
        Rc::make_mut(&mut self.comments).push(comment);
    }
}

#[cfg(test)]
#[path = "line_test.rs"]
mod tests;
