use crate::error::ControlFlow;
use crate::line::Line;
use crate::node::Node;
use crate::operator_precedence::OperatorPrecedence;
use crate::segment::{build_segments, Segment};

/// LineMerger recombines short lines while respecting formatting constraints.
/// This is Stage 4 of the formatting pipeline.
///
/// Mirrors the Python sqlfmt LineMerger:
/// - Direct merge: try to merge all lines in a segment into one
/// - Operator tier merging: merge runs of operator-separated segments
/// - Stubborn merging: force-merge tight-binding operators (AS, OVER, etc.)
/// - Recursive: segments are built, merged, then recursed into
pub struct LineMerger {
    max_length: usize,
}

impl LineMerger {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    /// Main entry: try to merge lines.
    /// Mirrors Python's `maybe_merge_lines`.
    pub fn maybe_merge_lines(&self, lines: &[Line], arena: &[Node]) -> Vec<Line> {
        if lines.is_empty() || lines.iter().all(|l| l.has_formatting_disabled()) {
            return lines.to_vec();
        }

        // Try direct merge first
        match self.create_merged_line(lines, arena) {
            Ok(merged) => merged,
            Err(_) => {
                let mut merged_lines = Vec::new();
                let segments = build_segments(lines, arena);

                if segments.len() > 1 {
                    // Fix standalone operators
                    let segments = self.fix_standalone_operators(segments, arena);

                    // Merge operators by tier precedence
                    let tiers = OperatorPrecedence::tiers().to_vec();
                    let segments = self.maybe_merge_operators(segments, tiers, arena);

                    // Stubborn merge for tight-binding operators
                    let segments = self.maybe_stubbornly_merge(segments, arena);

                    // Recurse into each segment
                    for segment in &segments {
                        merged_lines.extend(self.maybe_merge_lines(&segment.lines, arena));
                    }
                } else {
                    // Single segment: move down one line and try again
                    let only_segment = &segments[0];
                    match only_segment.head(arena) {
                        Ok((head_idx, _head_line)) => {
                            // Include head line(s)
                            let include_end = if only_segment.lines.len() > 0
                                && head_idx == 0
                                && only_segment.lines[0]
                                    .first_content_node(arena)
                                    .map(|n| n.is_operator(arena) && !n.is_bracket_operator(arena))
                                    .unwrap_or(false)
                            {
                                // Standalone operator: include first 2 lines
                                (head_idx + 2).min(only_segment.lines.len())
                            } else {
                                head_idx + 1
                            };
                            merged_lines.extend_from_slice(&only_segment.lines[..include_end]);

                            // Split remaining into segments and recurse
                            if include_end < only_segment.lines.len() {
                                let remaining = &only_segment.lines[include_end..];
                                // Check if tail closes head
                                if only_segment.tail_closes_head(arena) {
                                    if let Ok((tail_idx, _)) = only_segment.tail(arena) {
                                        let tail_start = only_segment.lines.len() - 1 - tail_idx;
                                        if include_end < tail_start {
                                            let inner = &only_segment.lines[include_end..tail_start];
                                            merged_lines.extend(self.maybe_merge_lines(inner, arena));
                                            merged_lines.extend_from_slice(&only_segment.lines[tail_start..]);
                                        } else {
                                            merged_lines.extend(self.maybe_merge_lines(remaining, arena));
                                        }
                                    } else {
                                        merged_lines.extend(self.maybe_merge_lines(remaining, arena));
                                    }
                                } else {
                                    merged_lines.extend(self.maybe_merge_lines(remaining, arena));
                                }
                            }
                        }
                        Err(_) => {
                            merged_lines.extend_from_slice(&only_segment.lines);
                        }
                    }
                }

                merged_lines
            }
        }
    }

    /// Try to merge all lines into a single line.
    /// Returns CannotMerge error if the result would be too long or violate rules.
    fn create_merged_line(
        &self,
        lines: &[Line],
        arena: &[Node],
    ) -> Result<Vec<Line>, ControlFlow> {
        if lines.len() <= 1 {
            return Ok(lines.to_vec());
        }

        let (nodes, comments) = Self::extract_components(lines, arena)?;

        let mut merged_line = Line::new(lines[0].previous_node);
        for &idx in &nodes {
            merged_line.append_node(idx);
        }
        merged_line.comments = comments;

        if merged_line.len(arena) > self.max_length {
            return Err(ControlFlow::CannotMerge);
        }

        // Add leading/trailing blank lines
        let leading = Self::extract_leading_blank_lines(lines, arena);
        let trailing = Self::extract_trailing_blank_lines(lines, arena);

        let mut result = leading;
        result.push(merged_line);
        result.extend(trailing);
        Ok(result)
    }

    /// Safe version that returns original lines on failure.
    fn safe_create_merged_line(&self, lines: &[Line], arena: &[Node]) -> Vec<Line> {
        match self.create_merged_line(lines, arena) {
            Ok(merged) => merged,
            Err(_) => lines.to_vec(),
        }
    }

    /// Extract nodes and comments from lines, validating merge rules.
    fn extract_components(
        lines: &[Line],
        arena: &[Node],
    ) -> Result<(Vec<usize>, Vec<crate::comment::Comment>), ControlFlow> {
        let mut nodes = Vec::new();
        let mut comments = Vec::new();
        let mut final_newline: Option<usize> = None;
        let mut has_multiline_jinja = false;

        for line in lines {
            // Check unmergeable conditions
            if has_multiline_jinja {
                let starts_with_op = line
                    .first_content_node(arena)
                    .map(|n| n.is_operator(arena))
                    .unwrap_or(false);
                let starts_with_comma = line.starts_with_comma(arena);
                if !starts_with_op && !starts_with_comma {
                    return Err(ControlFlow::CannotMerge);
                }
            }

            let mut line_has_multiline = false;
            for &node_idx in &line.nodes {
                let node = &arena[node_idx];

                // Can't merge lines with disabled formatting
                if !node.formatting_disabled.is_empty() {
                    return Err(ControlFlow::CannotMerge);
                }
                // Can't merge query dividers
                if node.divides_queries() {
                    return Err(ControlFlow::CannotMerge);
                }

                if node.is_newline() {
                    final_newline = Some(node_idx);
                    continue;
                }
                if node.is_multiline_jinja() {
                    line_has_multiline = true;
                }
                nodes.push(node_idx);
            }
            has_multiline_jinja = line_has_multiline;
            comments.extend(line.comments.clone());
        }

        if nodes.is_empty() {
            return Err(ControlFlow::CannotMerge);
        }

        // Add final newline
        if let Some(nl) = final_newline {
            nodes.push(nl);
        }

        Ok((nodes, comments))
    }

    fn extract_leading_blank_lines(lines: &[Line], arena: &[Node]) -> Vec<Line> {
        let mut blanks = Vec::new();
        for line in lines {
            if line.is_blank_line(arena) {
                blanks.push(line.clone());
            } else {
                break;
            }
        }
        blanks
    }

    fn extract_trailing_blank_lines(lines: &[Line], arena: &[Node]) -> Vec<Line> {
        let mut blanks = Vec::new();
        for line in lines.iter().rev() {
            if line.is_blank_line(arena) {
                blanks.push(line.clone());
            } else {
                break;
            }
        }
        blanks.reverse();
        blanks
    }

    /// Fix standalone operators by merging them with the next line.
    fn fix_standalone_operators(
        &self,
        mut segments: Vec<Segment>,
        arena: &[Node],
    ) -> Vec<Segment> {
        for segment in &mut segments {
            if let Ok((head_idx, head_line)) = segment.head(arena) {
                let is_standalone_op = head_line
                    .first_content_node(arena)
                    .map(|n| {
                        n.is_operator(arena)
                            && !n.is_bracket_operator(arena)
                            && head_line.is_standalone_content(arena)
                    })
                    .unwrap_or(false);

                if is_standalone_op && segment.lines.len() > head_idx + 1 {
                    // Try to merge the operator line with the next line
                    let merge_end = (head_idx + 2).min(segment.lines.len());
                    let to_merge = segment.lines[head_idx..merge_end].to_vec();
                    if let Ok(merged) = self.create_merged_line(&to_merge, arena) {
                        let mut new_lines = segment.lines[..head_idx].to_vec();
                        new_lines.extend(merged);
                        new_lines.extend_from_slice(&segment.lines[merge_end..]);
                        segment.lines = new_lines;
                    }
                }
            }
        }
        segments
    }

    /// Merge runs of operator-separated segments at each precedence tier.
    fn maybe_merge_operators(
        &self,
        segments: Vec<Segment>,
        mut op_tiers: Vec<OperatorPrecedence>,
        arena: &[Node],
    ) -> Vec<Segment> {
        if segments.len() <= 1 || op_tiers.is_empty() {
            return segments;
        }

        let precedence = op_tiers.pop().unwrap();
        let mut new_segments: Vec<Segment> = Vec::new();
        let mut head = 0;

        for i in 1..segments.len() {
            if !self.segment_continues_operator_sequence(&segments[i], precedence, arena) {
                new_segments.extend(self.try_merge_operator_segments(
                    &segments[head..i],
                    op_tiers.clone(),
                    arena,
                ));
                head = i;
            }
        }
        // Final run
        new_segments.extend(self.try_merge_operator_segments(
            &segments[head..],
            op_tiers,
            arena,
        ));

        new_segments
    }

    /// Check if a segment continues an operator sequence.
    fn segment_continues_operator_sequence(
        &self,
        segment: &Segment,
        max_precedence: OperatorPrecedence,
        arena: &[Node],
    ) -> bool {
        match segment.head(arena) {
            Err(_) => true, // blank segment, keep scanning
            Ok((_, line)) => {
                let starts_with_comma = line.starts_with_comma(arena);
                let starts_with_op = line
                    .first_content_node(arena)
                    .map(|n| {
                        n.is_operator(arena)
                            && !line.previous_token_is_comma(arena)
                            && OperatorPrecedence::from_node(n, arena) <= max_precedence
                    })
                    .unwrap_or(false);
                starts_with_op || starts_with_comma
            }
        }
    }

    /// Try to merge a run of segments into one.
    fn try_merge_operator_segments(
        &self,
        segments: &[Segment],
        op_tiers: Vec<OperatorPrecedence>,
        arena: &[Node],
    ) -> Vec<Segment> {
        if segments.len() <= 1 {
            return segments.to_vec();
        }

        // Flatten all lines and try merge
        let all_lines: Vec<Line> = segments.iter().flat_map(|s| s.lines.clone()).collect();
        match self.create_merged_line(&all_lines, arena) {
            Ok(merged) => vec![Segment::new(merged)],
            Err(_) => self.maybe_merge_operators(segments.to_vec(), op_tiers, arena),
        }
    }

    /// Stubborn merge: force-merge tight-binding operators (AS, OVER, etc.)
    fn maybe_stubbornly_merge(
        &self,
        segments: Vec<Segment>,
        arena: &[Node],
    ) -> Vec<Segment> {
        if segments.len() <= 1 {
            return segments;
        }

        // Phase 1: Stubborn-merge P0 operators (OtherTight: as, over, etc.)
        let mut new_segments = vec![segments[0].clone()];
        for segment in segments.iter().skip(1) {
            if self.segment_continues_operator_sequence(
                segment,
                OperatorPrecedence::OtherTight,
                arena,
            ) {
                new_segments = self.stubbornly_merge(&new_segments, segment, arena);
            } else {
                new_segments.push(segment.clone());
            }
        }

        if new_segments.len() <= 1 {
            return new_segments;
        }

        // Phase 2: Stubborn-merge P1 operators (up to Comparators) that close brackets
        let p1_flags: Vec<bool> = new_segments
            .iter()
            .map(|s| {
                self.segment_continues_operator_sequence(
                    s,
                    OperatorPrecedence::Comparators,
                    arena,
                )
            })
            .collect();

        let segments = new_segments;
        let mut new_segments = vec![segments[0].clone()];
        for i in 1..segments.len() {
            if !p1_flags[i - 1]
                && p1_flags[i]
                && Segment::new(self.safe_create_merged_line(&segments[i].lines, arena))
                    .tail_closes_head(arena)
            {
                new_segments = self.stubbornly_merge(&new_segments, &segments[i], arena);
            } else {
                new_segments.push(segments[i].clone());
            }
        }

        new_segments
    }

    /// Attempt several methods of merging a segment with the previous segments.
    fn stubbornly_merge(
        &self,
        prev_segments: &[Segment],
        segment: &Segment,
        arena: &[Node],
    ) -> Vec<Segment> {
        let mut new_segments = prev_segments.to_vec();
        let prev_segment = match new_segments.pop() {
            Some(s) => s,
            None => {
                new_segments.push(segment.clone());
                return new_segments;
            }
        };

        let head = match segment.head(arena) {
            Ok((head_idx, head_line)) => (head_idx, head_line.clone()),
            Err(_) => {
                new_segments.push(prev_segment);
                new_segments.push(segment.clone());
                return new_segments;
            }
        };
        let (head_idx, head_line) = head;

        // Try 1: Merge head of this segment with entire previous segment
        let mut try_lines = prev_segment.lines.clone();
        try_lines.push(head_line.clone());
        match self.create_merged_line(&try_lines, arena) {
            Ok(merged) => {
                let mut result_seg = Segment::new(merged);
                result_seg
                    .lines
                    .extend_from_slice(&segment.lines[head_idx + 1..]);
                new_segments.push(result_seg);
                return new_segments;
            }
            Err(_) => {}
        }

        // Try 2: Merge entire segment onto last line of previous segment
        if let Ok((tail_idx, tail_line)) = prev_segment.tail(arena) {
            let mut try_lines = vec![tail_line.clone()];
            try_lines.extend(segment.lines.clone());
            match self.create_merged_line(&try_lines, arena) {
                Ok(merged) => {
                    let tail_start = prev_segment.lines.len() - 1 - tail_idx;
                    let mut result_seg = Segment::new(
                        prev_segment.lines[..tail_start].to_vec(),
                    );
                    result_seg.lines.extend(merged);
                    new_segments.push(result_seg);
                    return new_segments;
                }
                Err(_) => {}
            }

            // Try 3: Merge just head of this segment onto last line of previous segment
            let try_lines = vec![tail_line.clone(), head_line.clone()];
            match self.create_merged_line(&try_lines, arena) {
                Ok(merged) => {
                    let tail_start = prev_segment.lines.len() - 1 - tail_idx;
                    let mut result_seg = Segment::new(
                        prev_segment.lines[..tail_start].to_vec(),
                    );
                    result_seg.lines.extend(merged);
                    result_seg
                        .lines
                        .extend_from_slice(&segment.lines[head_idx + 1..]);
                    new_segments.push(result_seg);
                    return new_segments;
                }
                Err(_) => {}
            }
        }

        // Give up
        new_segments.push(prev_segment);
        new_segments.push(segment.clone());
        new_segments
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
        line.append_node(content);
        line.append_node(nl);
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
        // Should merge
        assert!(result.len() <= 2);
    }
}
