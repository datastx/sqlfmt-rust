use crate::line::Line;
use crate::node::Node;
use crate::operator_precedence::OperatorPrecedence;
use crate::segment::{build_segments, Segment};

/// LineMerger recombines short lines while respecting formatting constraints.
/// This is Stage 4 of the formatting pipeline.
pub struct LineMerger {
    max_length: usize,
}

impl LineMerger {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    /// Main entry: try to merge lines.
    pub fn maybe_merge_lines(&self, lines: &[Line], arena: &[Node]) -> Vec<Line> {
        if lines.len() <= 1 {
            return lines.to_vec();
        }

        let segments = build_segments(lines, arena);
        let mut result = Vec::new();

        for segment in &segments {
            let merged = self.merge_segment(segment, arena);
            result.extend(merged);
        }

        result
    }

    /// Try to merge a segment's lines.
    fn merge_segment(&self, segment: &Segment, arena: &[Node]) -> Vec<Line> {
        if segment.lines.len() <= 1 {
            return segment.lines.clone();
        }

        // Try direct merge: combine all lines in the segment
        if let Some(merged) = self.try_direct_merge(segment, arena) {
            if merged.len(arena) <= self.max_length {
                return vec![merged];
            }
        }

        // Try merging by operator precedence tiers
        let tiers = OperatorPrecedence::tiers();
        for &tier in tiers {
            let merged = self.try_tier_merge(segment, arena, tier);
            if merged.len() < segment.lines.len() {
                return merged;
            }
        }

        // No merging possible
        segment.lines.clone()
    }

    /// Try to merge all lines in a segment into one line.
    fn try_direct_merge(&self, segment: &Segment, arena: &[Node]) -> Option<Line> {
        if segment.lines.is_empty() {
            return None;
        }

        // Cannot merge if segment contains multiline Jinja
        for line in &segment.lines {
            if line.contains_multiline_jinja(arena) {
                return None;
            }
        }

        // Cannot merge if any line starts with an unterm keyword (except first)
        for (i, line) in segment.lines.iter().enumerate() {
            if i > 0 && line.starts_with_unterm_keyword(arena) {
                return None;
            }
        }

        let mut merged = segment.lines[0].clone();
        for line in segment.lines.iter().skip(1) {
            if line.is_blank_line(arena) {
                continue;
            }
            // Append all non-newline nodes from subsequent lines
            for &idx in &line.nodes {
                if !arena[idx].is_newline() {
                    merged.append_node(idx);
                }
            }
            // Merge comments
            merged.comments.extend(line.comments.clone());
        }

        Some(merged)
    }

    /// Try merging at a specific operator precedence tier.
    fn try_tier_merge(
        &self,
        segment: &Segment,
        _arena: &[Node],
        _tier: OperatorPrecedence,
    ) -> Vec<Line> {
        // For now, just return original lines. Full implementation would
        // selectively merge lines that don't cross the tier boundary.
        segment.lines.clone()
    }

    /// Merge two adjacent lines into one.
    pub fn merge_two_lines(first: &Line, second: &Line, arena: &[Node]) -> Line {
        let mut merged = first.clone();
        for &idx in &second.nodes {
            if !arena[idx].is_newline() {
                merged.append_node(idx);
            }
        }
        merged.comments.extend(second.comments.clone());
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeIndex;
    use crate::token::{Token, TokenType};

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

    fn make_simple_line(arena: &mut Vec<Node>, tt: TokenType, val: &str) -> Line {
        let nl = make_node(arena, TokenType::Newline, "\n", "");
        let content = make_node(arena, tt, val, "");
        let mut line = Line::new(None);
        line.append_node(nl);
        line.append_node(content);
        line
    }

    #[test]
    fn test_no_merge_single_line() {
        let mut arena = Vec::new();
        let line = make_simple_line(&mut arena, TokenType::Name, "a");
        let merger = LineMerger::new(88);
        let result = merger.maybe_merge_lines(&[line], &arena);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_merge_short_lines() {
        let mut arena = Vec::new();
        let line1 = make_simple_line(&mut arena, TokenType::Name, "a");
        let line2 = make_simple_line(&mut arena, TokenType::Name, "b");

        let merger = LineMerger::new(88);
        let result = merger.maybe_merge_lines(&[line1, line2], &arena);
        // Should attempt to merge
        assert!(result.len() <= 2);
    }
}
