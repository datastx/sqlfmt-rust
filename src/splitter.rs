use crate::line::Line;
use crate::node::{Node, NodeIndex};
use crate::token::TokenType;

/// LineSplitter breaks long lines at keywords, operators, brackets, and commas.
/// This is Stage 1 of the formatting pipeline.
pub struct LineSplitter {
    max_length: usize,
}

impl LineSplitter {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    /// Split a single line if it exceeds the max length.
    /// Returns the original line if no split is needed.
    pub fn maybe_split(&self, line: &Line, arena: &[Node]) -> Vec<Line> {
        // Don't split blank lines or lines with formatting disabled
        if line.is_blank_line(arena) || line.has_formatting_disabled() {
            return vec![line.clone()];
        }

        // Don't split if line fits
        if line.len(arena) <= self.max_length {
            return vec![line.clone()];
        }

        // Try to find split points
        let split_result = self.find_and_apply_splits(line, arena);
        if split_result.len() > 1 {
            // Recursively split any still-too-long lines (up to a limit)
            let mut final_lines = Vec::new();
            for l in split_result {
                if l.len(arena) > self.max_length && l.nodes.len() > 2 {
                    final_lines.extend(self.maybe_split(&l, arena));
                } else {
                    final_lines.push(l);
                }
            }
            final_lines
        } else {
            split_result
        }
    }

    /// Find the best split point and create two lines.
    fn find_and_apply_splits(&self, line: &Line, arena: &[Node]) -> Vec<Line> {
        let content_nodes: Vec<(usize, NodeIndex)> = line
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, &idx)| !arena[idx].is_newline())
            .map(|(i, &idx)| (i, idx))
            .collect();

        if content_nodes.len() <= 1 {
            return vec![line.clone()];
        }

        // Find the best split point (highest priority)
        let mut best_split: Option<(usize, SplitPriority)> = None;

        for (pos, (line_pos, node_idx)) in content_nodes.iter().enumerate() {
            if pos == 0 {
                continue; // Don't split before the first node
            }

            let node = &arena[*node_idx];
            let priority = self.split_priority(node, arena);

            if let Some(priority) = priority {
                match &best_split {
                    None => {
                        best_split = Some((*line_pos, priority));
                    }
                    Some((_, best_prio)) => {
                        // Higher priority wins; for equal priority, prefer later split
                        if priority >= *best_prio {
                            best_split = Some((*line_pos, priority));
                        }
                    }
                }
            }
        }

        match best_split {
            Some((split_pos, _)) => self.split_at(line, split_pos, arena),
            None => vec![line.clone()],
        }
    }

    /// Determine the split priority for splitting *before* this node.
    fn split_priority(&self, node: &Node, arena: &[Node]) -> Option<SplitPriority> {
        let tt = node.token.token_type;
        match tt {
            // Highest priority: split before keywords
            TokenType::UntermKeyword => Some(SplitPriority::Keyword),
            // Split before boolean operators (AND, OR)
            TokenType::BooleanOperator => {
                if node.is_the_and_after_between(arena) {
                    None // Don't split BETWEEN x AND y
                } else {
                    Some(SplitPriority::BooleanOperator)
                }
            }
            // Split before ON
            TokenType::On => Some(SplitPriority::On),
            // Split before word operators
            TokenType::WordOperator => Some(SplitPriority::WordOperator),
            // Split before regular operators
            TokenType::Operator | TokenType::DoublColon => Some(SplitPriority::Operator),
            // Split before commas
            TokenType::Comma => Some(SplitPriority::Comma),
            // Split after opening bracket (split before next node)
            _ => None,
        }
    }

    /// Split a line at the given position (by line.nodes index).
    fn split_at(&self, line: &Line, split_pos: usize, _arena: &[Node]) -> Vec<Line> {
        let mut first_line = Line::new(line.previous_node);
        let mut second_line = Line::new(line.previous_node);

        for (i, &node_idx) in line.nodes.iter().enumerate() {
            if i < split_pos {
                first_line.append_node(node_idx);
            } else {
                second_line.append_node(node_idx);
            }
        }

        // Comments stay with the first line for now
        first_line.comments = line.comments.clone();

        if first_line.nodes.is_empty() || second_line.nodes.is_empty() {
            return vec![line.clone()];
        }

        vec![first_line, second_line]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SplitPriority {
    Operator,
    Comma,
    WordOperator,
    On,
    BooleanOperator,
    Keyword,
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
            Vec::new(),
            Vec::new(),
        ));
        idx
    }

    #[test]
    fn test_no_split_short_line() {
        let mut arena = Vec::new();
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        let a = make_node(&mut arena, TokenType::Name, "a", "");

        let mut line = Line::new(None);
        line.append_node(nl);
        line.append_node(a);

        let splitter = LineSplitter::new(88);
        let result = splitter.maybe_split(&line, &arena);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_split_at_keyword() {
        let mut arena = Vec::new();
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        let select = make_node(&mut arena, TokenType::UntermKeyword, "select", "");
        let name = make_node(&mut arena, TokenType::Name, "a_very_long_column_name_that_makes_line_too_long", " ");
        let from = make_node(&mut arena, TokenType::UntermKeyword, "from", " ");
        let table = make_node(&mut arena, TokenType::Name, "another_very_long_table_name_that_pushes_over", " ");

        let mut line = Line::new(None);
        line.nodes = vec![nl, select, name, from, table];

        let splitter = LineSplitter::new(40);
        let result = splitter.maybe_split(&line, &arena);
        assert!(result.len() >= 2);
    }
}
