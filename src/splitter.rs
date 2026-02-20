use crate::comment::Comment;
use crate::line::Line;
use crate::node::{Node, NodeIndex};
use crate::token::{Token, TokenType};

/// LineSplitter breaks lines at keywords, operators, brackets, and commas.
/// This is Stage 1 of the formatting pipeline.
///
/// Mirrors the Python sqlfmt LineSplitter exactly:
/// - Iterates node-by-node with split_before/split_after flags
/// - Splits AFTER commas, opening brackets, keywords, query dividers
/// - Splits BEFORE operators, keywords, closing brackets, multiline jinja
/// - Uses iterative (not recursive) approach to handle very long lines
pub struct LineSplitter {
    _max_length: usize,
}

impl LineSplitter {
    pub fn new(max_length: usize) -> Self {
        Self {
            _max_length: max_length,
        }
    }

    /// Split a single line into multiple lines based on SQL structure.
    /// This always splits — it does not check line length first.
    /// The Python splitter also always splits (length checking is done by the merger).
    pub fn maybe_split(&self, line: &Line, arena: &mut Vec<Node>) -> Vec<Line> {
        if line.has_formatting_disabled() {
            return vec![line.clone()];
        }

        let mut new_lines: Vec<Line> = Vec::new();
        let mut comments = line.comments.clone();
        let mut head: usize = 0;
        let mut always_split_after = false;
        let mut never_split_after = false;

        for i in 0..line.nodes.len() {
            let node_idx = line.nodes[i];
            let node = &arena[node_idx];

            if node.is_newline() {
                if head == 0 {
                    new_lines.push(line.clone());
                } else {
                    let (new_line, _remaining_comments) =
                        self.split_at_index(line, head, i, &comments, true, arena);
                    new_lines.push(new_line);
                }
                return new_lines;
            } else if i > head
                && !never_split_after
                && !Self::node_has_formatting_disabled(node_idx, arena)
                && (always_split_after || self.maybe_split_before(node_idx, arena))
            {
                let (new_line, remaining_comments) =
                    self.split_at_index(line, head, i, &comments, false, arena);
                comments = remaining_comments;
                new_lines.push(new_line);
                head = i;
            }

            let (split_after, no_split_after) = self.maybe_split_after(node_idx, arena);
            always_split_after = split_after;
            never_split_after = no_split_after;
        }

        // Handle remaining nodes (no newline at end)
        let (new_line, _remaining_comments) =
            self.split_at_index(line, head, line.nodes.len(), &comments, true, arena);
        new_lines.push(new_line);
        new_lines
    }

    /// Return true if we should split before this node.
    fn maybe_split_before(&self, node_idx: NodeIndex, arena: &[Node]) -> bool {
        let node = &arena[node_idx];

        // Do NOT split before bracket operators (array indexing, etc.)
        if node.is_bracket_operator(arena) {
            return false;
        }
        // Split before multiline jinja
        if node.is_multiline_jinja() {
            return true;
        }
        // Split before any unterm keyword
        if node.is_unterm_keyword() {
            return true;
        }
        // Split before any opening jinja block
        if node.is_opening_jinja_block() {
            return true;
        }
        // Split before operators — BUT NOT the AND after BETWEEN,
        // and NOT before cast (::) or colon (:) operators
        if node.is_operator(arena) {
            if node.is_the_and_after_between(arena) {
                return false;
            }
            if matches!(
                node.token.token_type,
                TokenType::DoublColon | TokenType::Colon
            ) {
                return false;
            }
            return true;
        }
        // Split before boolean operators (and, or, not) — same as regular operators
        // but NOT the AND after BETWEEN
        if node.is_boolean_operator() {
            if node.is_the_and_after_between(arena) {
                return false;
            }
            return true;
        }
        // Split before closing brackets
        if node.is_closing_bracket() {
            return true;
        }
        // Split before closing jinja blocks
        if node.is_closing_jinja_block() {
            return true;
        }
        // Split before query dividers (semicolon, set operators)
        if node.divides_queries() {
            return true;
        }
        // Split if opening bracket follows closing bracket
        // (e.g., split(my_field)[offset(1)])
        if self.maybe_split_between_brackets(node_idx, arena) {
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
            // Walk back past newlines/jinja to find prev SQL token
            let prev = &arena[prev_idx];
            if prev.is_closing_bracket() {
                return true;
            }
            // Also check via get_previous_sql_token
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

        // Always split after commas
        if node.is_comma() {
            return (true, false);
        }
        // Always split after opening brackets
        if node.is_opening_bracket() {
            return (true, false);
        }
        // Always split after opening jinja blocks ({% if %}, {% for %}).
        // But for JinjaBlockKeyword ({% else %}, {% elif %}), don't force
        // split after — let the following content stay on the same line so
        // the merger can decide (e.g., {% else %} {{ config() }}).
        if node.is_opening_jinja_block() {
            if node.token.token_type == TokenType::JinjaBlockKeyword {
                return (false, false);
            }
            return (true, false);
        }
        // Always split after unterm keywords
        if node.is_unterm_keyword() {
            return (true, false);
        }
        // Always split after query dividers
        if node.divides_queries() {
            return (true, false);
        }
        // Never split after formatting-disabled nodes
        if !node.formatting_disabled.is_empty() {
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
        comments: &[Comment],
        no_tail: bool,
        arena: &mut Vec<Node>,
    ) -> (Line, Vec<Comment>) {
        let new_nodes: Vec<NodeIndex> = if index >= line.nodes.len() {
            line.nodes[head..].to_vec()
        } else {
            line.nodes[head..index].to_vec()
        };

        if new_nodes.is_empty() {
            // Shouldn't happen, but return empty line
            let empty_line = Line::new(line.previous_node);
            return (empty_line, comments.to_vec());
        }

        // Determine comment distribution
        // - Inline comments stay with the line containing their previous_node
        // - Standalone comments go to the NEXT line (they describe what follows)
        // - Orphaned comments (previous_node from an earlier split) attach to current head
        let (head_comments, tail_comments) = if no_tail {
            (comments.to_vec(), Vec::new())
        } else if comments.is_empty() {
            (Vec::new(), Vec::new())
        } else if new_nodes.len() == 1 && arena[new_nodes[0]].token.token_type == TokenType::Comma {
            // If head is just a comma, pass all comments to tail
            (Vec::new(), comments.to_vec())
        } else {
            let remaining_nodes: Vec<NodeIndex> = if index < line.nodes.len() {
                line.nodes[index..].to_vec()
            } else {
                Vec::new()
            };
            let mut head_c = Vec::new();
            let mut tail_c = Vec::new();
            for comment in comments {
                let prev_in_head = comment.previous_node.map_or(false, |prev_idx| {
                    new_nodes.contains(&prev_idx)
                });
                let prev_in_remaining = comment.previous_node.map_or(false, |prev_idx| {
                    remaining_nodes.contains(&prev_idx)
                });

                if prev_in_head {
                    if comment.is_inline() {
                        head_c.push(comment.clone());
                    } else {
                        // Standalone comment after a head node → goes to next line
                        tail_c.push(comment.clone());
                    }
                } else if prev_in_remaining {
                    tail_c.push(comment.clone());
                } else {
                    // Orphaned: previous_node was in an earlier split → attach to head
                    head_c.push(comment.clone());
                }
            }
            (head_c, tail_c)
        };

        // Build the new line
        let prev = if !new_nodes.is_empty() {
            arena[new_nodes[0]].previous_node
        } else {
            line.previous_node
        };
        let mut new_line = Line::new(prev);
        for &idx in &new_nodes {
            new_line.append_node(idx);
        }
        new_line.comments = head_comments;

        // Ensure line ends with a newline node
        if !new_nodes.is_empty() && !arena[*new_nodes.last().unwrap()].is_newline() {
            self.append_newline(&mut new_line, arena);
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
            String::new(),
            "\n".to_string(),
            prev_idx
                .map(|i| arena[i].open_brackets.clone())
                .unwrap_or_default(),
            prev_idx
                .map(|i| arena[i].open_jinja_blocks.clone())
                .unwrap_or_default(),
        );
        let idx = arena.len();
        arena.push(nl_node);
        line.append_node(idx);
    }

    /// Check if a node has formatting disabled.
    fn node_has_formatting_disabled(node_idx: NodeIndex, arena: &[Node]) -> bool {
        !arena[node_idx].formatting_disabled.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use crate::token::Token;

    fn make_node(arena: &mut Vec<Node>, tt: TokenType, val: &str, prefix: &str) -> NodeIndex {
        let idx = arena.len();
        let prev = if idx > 0 { Some(idx - 1) } else { None };
        arena.push(Node::new(
            Token::new(tt, "", val, 0, val.len()),
            prev,
            prefix.to_string(),
            val.to_string(),
            smallvec::SmallVec::new(),
            smallvec::SmallVec::new(),
        ));
        idx
    }

    #[test]
    fn test_no_split_single_node() {
        let mut arena = Vec::new();
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        let a = make_node(&mut arena, TokenType::Name, "a", "");

        let mut line = Line::new(None);
        line.append_node(a);
        line.append_node(nl);

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_split_at_keyword() {
        let mut arena = Vec::new();
        let select = make_node(&mut arena, TokenType::UntermKeyword, "select", "");
        let name = make_node(&mut arena, TokenType::Name, "a", " ");
        let from = make_node(&mut arena, TokenType::UntermKeyword, "from", " ");
        let table = make_node(&mut arena, TokenType::Name, "t", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![select, name, from, table, nl];

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        // Should split: select a \n, from \n, t \n
        assert!(
            result.len() >= 2,
            "Expected at least 2 lines, got {}",
            result.len()
        );
    }

    #[test]
    fn test_split_after_comma() {
        let mut arena = Vec::new();
        let a = make_node(&mut arena, TokenType::Name, "a", "");
        let comma = make_node(&mut arena, TokenType::Comma, ",", "");
        let b = make_node(&mut arena, TokenType::Name, "b", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![a, comma, b, nl];

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        // Should split after comma: "a," and "b"
        assert!(
            result.len() >= 2,
            "Expected at least 2 lines, got {}",
            result.len()
        );
    }

    #[test]
    fn test_split_before_operator() {
        let mut arena = Vec::new();
        let a = make_node(&mut arena, TokenType::Name, "a", "");
        let op = make_node(&mut arena, TokenType::Operator, "+", " ");
        let b = make_node(&mut arena, TokenType::Name, "b", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![a, op, b, nl];

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        // Should split before operator: "a" and "+ b"
        assert!(
            result.len() >= 2,
            "Expected split before operator, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_split_before_closing_bracket() {
        let mut arena = Vec::new();
        let open = make_node(&mut arena, TokenType::BracketOpen, "(", "");
        let name = make_node(&mut arena, TokenType::Name, "x", "");
        let close = make_node(&mut arena, TokenType::BracketClose, ")", "");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![open, name, close, nl];

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        // Should split: "(" then "x" then ")"
        assert!(
            result.len() >= 2,
            "Expected split at brackets, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_split_before_semicolon() {
        let mut arena = Vec::new();
        let select = make_node(&mut arena, TokenType::UntermKeyword, "select", "");
        let one = make_node(&mut arena, TokenType::Number, "1", " ");
        let semi = make_node(&mut arena, TokenType::Semicolon, ";", "");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![select, one, semi, nl];

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        assert!(
            result.len() >= 2,
            "Expected split before semicolon, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_split_after_opening_bracket() {
        let mut arena = Vec::new();
        let name = make_node(&mut arena, TokenType::Name, "count", "");
        let open = make_node(&mut arena, TokenType::BracketOpen, "(", "");
        let star = make_node(&mut arena, TokenType::Star, "*", "");
        let close = make_node(&mut arena, TokenType::BracketClose, ")", "");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![name, open, star, close, nl];

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        // Should split after "(": "count(" then "*" then ")"
        assert!(
            result.len() >= 2,
            "Expected split after open bracket, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_no_split_bracket_operator() {
        // Array indexing: arr[0] - should NOT split before [
        let mut arena = Vec::new();
        let arr = make_node(&mut arena, TokenType::Name, "arr", "");
        let bracket = make_node(&mut arena, TokenType::BracketOpen, "[", "");
        arena[bracket].value = "[".to_string();
        let zero = make_node(&mut arena, TokenType::Number, "0", "");
        let close = make_node(&mut arena, TokenType::BracketClose, "]", "");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![arr, bracket, zero, close, nl];

        let _splitter = LineSplitter::new(88);
        // is_bracket_operator checks previous_sql_token - in our test the [
        // follows Name "arr", so it should be a bracket operator
        assert!(arena[bracket].is_bracket_operator(&arena));
    }

    #[test]
    fn test_split_formatting_disabled_returns_unchanged() {
        let mut arena = Vec::new();
        let a = make_node(&mut arena, TokenType::Name, "a", "");
        let op = make_node(&mut arena, TokenType::Operator, "+", " ");
        let b = make_node(&mut arena, TokenType::Name, "b", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![a, op, b, nl];
        // Mark as formatting disabled
        line.formatting_disabled
            .push(Token::new(TokenType::FmtOff, "", "-- fmt: off", 0, 11));

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        // Should return line unchanged
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_split_set_operator() {
        let mut arena = Vec::new();
        let one = make_node(&mut arena, TokenType::Number, "1", "");
        let union = make_node(&mut arena, TokenType::SetOperator, "union all", " ");
        let two = make_node(&mut arena, TokenType::Number, "2", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![one, union, two, nl];

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        // Set operator divides queries, should split
        assert!(
            result.len() >= 2,
            "Expected split at set operator, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_split_blank_line_returns_unchanged() {
        let mut arena = Vec::new();
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.append_node(nl);

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &mut arena);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_blank_line(&arena));
    }
}
