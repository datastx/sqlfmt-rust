use compact_str::CompactString;

use crate::comment::Comment;
use crate::line::Line;
use crate::node::{Node, NodeIndex};
use crate::token::{Token, TokenType};

/// LineSplitter breaks lines at keywords, operators, brackets, and commas.
/// This is Stage 1 of the formatting pipeline.
///
/// Mirrors the Python sqlfmt LineSplitter exactly:
/// - Iterates node-by-node with split_before/split_after flags
/// - Splits AFTER opening brackets, keywords, query dividers
/// - Splits BEFORE commas, operators, keywords, closing brackets, multiline jinja
/// - Uses iterative (not recursive) approach to handle very long lines
pub(crate) struct LineSplitter;

impl LineSplitter {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Split a single line into multiple lines based on SQL structure.
    /// This always splits — it does not check line length first.
    /// The Python splitter also always splits (length checking is done by the merger).
    /// Takes ownership of the line to avoid cloning in common paths.
    pub(crate) fn maybe_split(&self, mut line: Line, arena: &mut Vec<Node>) -> Vec<Line> {
        if line.has_formatting_disabled() {
            return vec![line];
        }

        let mut new_lines: Vec<Line> = Vec::new();
        let rc_comments = std::mem::take(&mut line.comments);
        let mut comments = std::rc::Rc::try_unwrap(rc_comments).unwrap_or_else(|rc| (*rc).clone());
        let mut head: usize = 0;
        let mut always_split_after = false;
        let mut never_split_after = false;

        for i in 0..line.nodes.len() {
            let node_idx = line.nodes[i];
            let node = &arena[node_idx];

            if node.is_newline() {
                if head == 0 {
                    line.comments = std::rc::Rc::new(comments);
                    new_lines.push(line);
                } else {
                    let (new_line, _remaining_comments) =
                        self.split_at_index(&line, head, i, comments, true, arena);
                    new_lines.push(new_line);
                }
                return new_lines;
            } else if i > head
                && !never_split_after
                && !Self::node_has_formatting_disabled(node_idx, arena)
                && (always_split_after || self.maybe_split_before(node_idx, arena))
            {
                let (new_line, remaining_comments) =
                    self.split_at_index(&line, head, i, comments, false, arena);
                comments = remaining_comments;
                new_lines.push(new_line);
                head = i;
            }

            let (split_after, no_split_after) = self.maybe_split_after(node_idx, arena);
            always_split_after = split_after;
            never_split_after = no_split_after;
        }

        let (new_line, _remaining_comments) =
            self.split_at_index(&line, head, line.nodes.len(), comments, true, arena);
        new_lines.push(new_line);
        new_lines
    }

    /// Return true if we should split before this node.
    fn maybe_split_before(&self, node_idx: NodeIndex, arena: &[Node]) -> bool {
        let node = &arena[node_idx];

        // Do NOT split before multiline jinja in Stage 1.
        // Multiline jinja nodes are created by the JinjaFormatter in Stage 2.
        // If they need splitting, Stage 2b (split_multiline_jinja) handles it
        // with length-based checks. Splitting unconditionally here would break
        // idempotency: in the second pass, already-multiline Jinja would be
        // split from preceding content (e.g., `= {{ ... }}` or `on {{ ... }}`).
        // Note: the operator/keyword split rules below will still break lines
        // at operators and keywords that precede multiline Jinja.
        if node.is_unterm_keyword() {
            return true;
        }
        if node.is_opening_jinja_block() {
            return true;
        }
        // Split before operators — BUT NOT the AND after BETWEEN,
        // and NOT before cast (::) or colon (:) operators
        if node.is_operator(arena) {
            if node.is_the_and_after_between(arena) {
                return false;
            }
            if matches!(node.token.token_type, TokenType::Colon) {
                return false;
            }
            return true;
        }
        // NOT the AND after BETWEEN, and NOT "not" after "or"/"and"
        if node.is_boolean_operator() {
            if node.is_the_and_after_between(arena) {
                return false;
            }
            // "not" after "or"/"and" stays on the same line (e.g., "or not x in ...")
            if node.value.eq_ignore_ascii_case("not") {
                if let Some(prev) = node.get_previous_sql_token(arena) {
                    if prev.token_type == TokenType::BooleanOperator {
                        return false;
                    }
                }
            }
            return true;
        }
        // Split before closing brackets — but NOT before > (angle bracket close)
        // since angle bracket content should stay on the same line
        if node.is_closing_bracket() {
            if node.value == ">" {
                return false;
            }
            return true;
        }
        if node.is_closing_jinja_block() {
            return true;
        }
        if node.divides_queries() {
            return true;
        }
        // e.g., split(my_field)[offset(1)]
        if self.maybe_split_between_brackets(node_idx, arena) {
            return true;
        }

        // Leading-comma style: split BEFORE commas so they lead the next line
        if node.is_comma() {
            return true;
        }

        false
    }

    /// Return true if this is an open bracket that follows a closing bracket.
    fn maybe_split_between_brackets(&self, node_idx: NodeIndex, arena: &[Node]) -> bool {
        let node = &arena[node_idx];
        if !node.is_opening_bracket() {
            return false;
        }
        if let Some(prev_idx) = node.previous_node {
            let prev = &arena[prev_idx];
            if prev.is_closing_bracket() {
                return true;
            }
            if let Some(prev_token) = node.get_previous_sql_token(arena) {
                if prev_token.token_type.is_closing_bracket() {
                    return true;
                }
            }
        }
        false
    }

    /// Return (always_split_after, never_split_after).
    fn maybe_split_after(&self, node_idx: NodeIndex, arena: &[Node]) -> (bool, bool) {
        let node = &arena[node_idx];

        // BUT NOT after angle brackets (< for type constructors like array<int64>).
        // Angle bracket content is typically short and should stay on the same line.
        if node.is_opening_bracket() {
            if node.value == "<" {
                return (false, false);
            }
            return (true, false);
        }
        // But for JinjaBlockKeyword ({% else %}, {% elif %}), don't force
        // split after — let the following content stay on the same line so
        // the merger can decide (e.g., {% else %} {{ config() }}).
        if node.is_opening_jinja_block() {
            if node.token.token_type == TokenType::JinjaBlockKeyword {
                return (false, false);
            }
            return (true, false);
        }
        // not after LATERAL when followed by ( (it should stay as "lateral(" like a function call)
        if node.is_unterm_keyword() {
            if node.value.eq_ignore_ascii_case("lateral") {
                let next_idx = node_idx + 1;
                if next_idx < arena.len() && arena[next_idx].is_opening_bracket() {
                    return (false, false);
                }
            }
            return (true, false);
        }
        if node.divides_queries() {
            return (true, false);
        }
        if node.formatting_disabled {
            return (false, true);
        }

        (false, false)
    }

    /// Split a line at the given index. Returns the new head line and remaining comments.
    fn split_at_index(
        &self,
        line: &Line,
        head: usize,
        index: usize,
        comments: Vec<Comment>,
        no_tail: bool,
        arena: &mut Vec<Node>,
    ) -> (Line, Vec<Comment>) {
        let new_nodes: Vec<NodeIndex> = if index >= line.nodes.len() {
            line.nodes[head..].to_vec()
        } else {
            line.nodes[head..index].to_vec()
        };

        if new_nodes.is_empty() {
            let empty_line = Line::new(line.previous_node);
            return (empty_line, comments);
        }

        // - Inline comments stay with the line containing their previous_node
        // - Standalone comments go to the NEXT line (they describe what follows)
        // - Orphaned comments (previous_node from an earlier split) attach to current head
        let (head_comments, tail_comments) = if no_tail {
            (comments, Vec::new())
        } else if comments.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            // Use slice contains() instead of HashSet for small node sets
            let remaining_nodes: &[NodeIndex] = if index < line.nodes.len() {
                &line.nodes[index..]
            } else {
                &[]
            };
            let mut head_c = Vec::new();
            let mut tail_c = Vec::new();
            for comment in comments {
                let prev_in_head = comment
                    .previous_node
                    .is_some_and(|prev_idx| new_nodes.contains(&prev_idx));
                let prev_in_remaining = comment
                    .previous_node
                    .is_some_and(|prev_idx| remaining_nodes.contains(&prev_idx));

                if prev_in_head {
                    if comment.is_inline() {
                        head_c.push(comment);
                    } else {
                        tail_c.push(comment);
                    }
                } else if prev_in_remaining {
                    tail_c.push(comment);
                } else {
                    head_c.push(comment);
                }
            }
            (head_c, tail_c)
        };

        let prev = if !new_nodes.is_empty() {
            arena[new_nodes[0]].previous_node
        } else {
            line.previous_node
        };
        let mut new_line = Line::new(prev);
        for &idx in &new_nodes {
            new_line.append_node(idx);
        }
        new_line.comments = std::rc::Rc::new(head_comments);

        if let Some(&last_node) = new_nodes.last() {
            if !arena[last_node].is_newline() {
                self.append_newline(&mut new_line, arena);
            }
        }

        (new_line, tail_comments)
    }

    /// Append a newline node to the end of a line.
    fn append_newline(&self, line: &mut Line, arena: &mut Vec<Node>) {
        let prev_idx = line.nodes.last().copied();
        let spos = prev_idx.map(|i| arena[i].token.epos).unwrap_or(0);
        let nl_token = Token::new(TokenType::Newline, "", "\n", spos, spos);
        let nl_node = Node::new(
            nl_token,
            prev_idx,
            CompactString::new(""),
            CompactString::from("\n"),
            prev_idx.map(|i| arena[i].bracket_depth).unwrap_or(0),
            prev_idx.map(|i| arena[i].jinja_depth).unwrap_or(0),
        );
        let idx = arena.len();
        arena.push(nl_node);
        line.append_node(idx);
    }

    /// Check if a node has formatting disabled.
    fn node_has_formatting_disabled(node_idx: NodeIndex, arena: &[Node]) -> bool {
        arena[node_idx].formatting_disabled
    }
}

#[cfg(test)]
#[path = "splitter_test.rs"]
mod tests;
