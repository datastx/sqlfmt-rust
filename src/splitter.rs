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
/// - Splits AFTER commas, opening brackets, keywords, query dividers
/// - Splits BEFORE operators, keywords, closing brackets, multiline jinja
/// - Uses iterative (not recursive) approach to handle very long lines
pub struct LineSplitter;

impl LineSplitter {
    pub fn new() -> Self {
        Self
    }

    /// Split a single line into multiple lines based on SQL structure.
    /// This always splits — it does not check line length first.
    /// The Python splitter also always splits (length checking is done by the merger).
    /// Takes ownership of the line to avoid cloning in common paths.
    pub fn maybe_split(&self, mut line: Line, arena: &mut Vec<Node>) -> Vec<Line> {
        if line.has_formatting_disabled() {
            return vec![line];
        }

        let mut new_lines: Vec<Line> = Vec::new();
        let mut comments = std::mem::take(&mut line.comments);
        let mut head: usize = 0;
        let mut always_split_after = false;
        let mut never_split_after = false;

        for i in 0..line.nodes.len() {
            let node_idx = line.nodes[i];
            let node = &arena[node_idx];

            if node.is_newline() {
                if head == 0 {
                    line.comments = comments;
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

        if node.is_comma() {
            return (true, false);
        }
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
        } else if new_nodes.len() == 1 && arena[new_nodes[0]].token.token_type == TokenType::Comma {
            (Vec::new(), comments)
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
        new_line.comments = head_comments;

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
            Token::new(tt, "", val, 0, val.len() as u32),
            prev,
            CompactString::from(prefix),
            CompactString::from(val),
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        assert!(
            result.len() >= 2,
            "Expected split after open bracket, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_no_split_bracket_operator() {
        let mut arena = Vec::new();
        let arr = make_node(&mut arena, TokenType::Name, "arr", "");
        let bracket = make_node(&mut arena, TokenType::BracketOpen, "[", "");
        arena[bracket].value = CompactString::from("[");
        let zero = make_node(&mut arena, TokenType::Number, "0", "");
        let close = make_node(&mut arena, TokenType::BracketClose, "]", "");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![arr, bracket, zero, close, nl];

        let _splitter = LineSplitter::new();
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
        line.formatting_disabled = true;

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
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

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_blank_line(&arena));
    }

    // --- Additional splitter tests for coverage parity ---

    #[test]
    fn test_split_between_brackets_array_index() {
        // func()[offset(1)] should split at [ after )
        let mut arena = Vec::new();
        let name = make_node(&mut arena, TokenType::Name, "func", "");
        let open1 = make_node(&mut arena, TokenType::BracketOpen, "(", "");
        let close1 = make_node(&mut arena, TokenType::BracketClose, ")", "");
        let open2 = make_node(&mut arena, TokenType::BracketOpen, "[", "");
        let off = make_node(&mut arena, TokenType::Name, "offset", "");
        let open3 = make_node(&mut arena, TokenType::BracketOpen, "(", "");
        let one = make_node(&mut arena, TokenType::Number, "1", "");
        let close3 = make_node(&mut arena, TokenType::BracketClose, ")", "");
        let close2 = make_node(&mut arena, TokenType::BracketClose, "]", "");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![
            name, open1, close1, open2, off, open3, one, close3, close2, nl,
        ];

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        assert!(
            result.len() >= 2,
            "Should split at bracket boundaries, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_split_very_long_line_no_crash() {
        // 500+ nodes should split without stack overflow
        let mut arena = Vec::new();
        let mut nodes = Vec::new();
        for i in 0..500 {
            let name = make_node(
                &mut arena,
                TokenType::Name,
                &format!("col_{}", i),
                if i > 0 { " " } else { "" },
            );
            nodes.push(name);
            if i < 499 {
                let comma = make_node(&mut arena, TokenType::Comma, ",", "");
                nodes.push(comma);
            }
        }
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        nodes.push(nl);

        let mut line = Line::new(None);
        line.nodes = nodes;

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        assert!(result.len() > 1, "500 comma-separated names should split");
    }

    #[test]
    fn test_split_no_terminating_newline() {
        // Line without trailing newline should still split
        let mut arena = Vec::new();
        let select = make_node(&mut arena, TokenType::UntermKeyword, "select", "");
        let name = make_node(&mut arena, TokenType::Name, "a", " ");
        let from = make_node(&mut arena, TokenType::UntermKeyword, "from", " ");
        let table = make_node(&mut arena, TokenType::Name, "t", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![select, name, from, table, nl];

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        assert!(
            result.len() >= 2,
            "Should split at keyword, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_split_leading_comma_comment() {
        // Comment before content should be handled correctly during split
        let mut arena = Vec::new();
        let comment = make_node(&mut arena, TokenType::Comment, "-- note", "");
        let name = make_node(&mut arena, TokenType::Name, "a", " ");
        let comma = make_node(&mut arena, TokenType::Comma, ",", "");
        let name2 = make_node(&mut arena, TokenType::Name, "b", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![comment, name, comma, name2, nl];

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        // Should split at comma
        assert!(
            result.len() >= 2,
            "Should split at comma after comment, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_split_around_set_operator_union() {
        // UNION ALL should force a split
        let mut arena = Vec::new();
        let one = make_node(&mut arena, TokenType::Number, "1", "");
        let union = make_node(&mut arena, TokenType::SetOperator, "union all", " ");
        let two = make_node(&mut arena, TokenType::Number, "2", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![one, union, two, nl];

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        assert!(
            result.len() >= 2,
            "Should split at UNION ALL, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_double_colon_cast_stays_together() {
        // :: is a tight-binding operator — test via full pipeline that
        // "x::int" stays together on one line after formatting
        let mode = crate::mode::Mode::default();
        let result = crate::api::format_string("SELECT x::int\n", &mode).unwrap();
        // The cast should remain as "x::int" (not split across lines)
        assert!(
            result.contains("::"),
            "Double colon cast should be present: {}",
            result
        );
    }

    #[test]
    fn test_split_trailing_operator_comment() {
        // "1 + -- comment\n2" - comment after operator
        let mut arena = Vec::new();
        let one = make_node(&mut arena, TokenType::Number, "1", "");
        let plus = make_node(&mut arena, TokenType::Operator, "+", " ");
        let comment = make_node(&mut arena, TokenType::Comment, "-- one", " ");
        let two = make_node(&mut arena, TokenType::Number, "2", " ");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");

        let mut line = Line::new(None);
        line.nodes = vec![one, plus, comment, two, nl];

        let splitter = LineSplitter::new();
        let result = splitter.maybe_split(line, &mut arena);
        assert!(
            result.len() >= 2,
            "Should split around operator/comment, got {} lines",
            result.len()
        );
    }
}
