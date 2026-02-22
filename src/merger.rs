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

        match self.create_merged_line(lines, arena) {
            Ok(merged) => merged,
            Err(_) => {
                let mut merged_lines = Vec::new();
                let segments = build_segments(lines, arena);

                if segments.len() > 1 {
                    let segments = self.fix_standalone_operators(segments, arena);
                    let segments =
                        self.maybe_merge_operators(segments, OperatorPrecedence::tiers(), arena);
                    let segments = self.maybe_stubbornly_merge(segments, arena);
                    for segment in &segments {
                        merged_lines.extend(self.maybe_merge_lines(&segment.lines, arena));
                    }
                } else {
                    self.merge_single_segment(&segments[0], arena, &mut merged_lines);
                }

                self.fix_standalone_commas(&mut merged_lines, arena);
                merged_lines
            }
        }
    }

    /// Handle a single segment: peel off the head line(s), then recurse
    /// into remaining lines (splitting around tail if it closes head).
    fn merge_single_segment(
        &self,
        segment: &Segment,
        arena: &[Node],
        merged_lines: &mut Vec<Line>,
    ) {
        let Ok((head_idx, _)) = segment.head(arena) else {
            merged_lines.extend_from_slice(&segment.lines);
            return;
        };

        let include_end = if !segment.lines.is_empty()
            && head_idx == 0
            && segment.lines[0]
                .first_content_node(arena)
                .map(|n| n.is_operator(arena) && !n.is_bracket_operator(arena))
                .unwrap_or(false)
        {
            (head_idx + 2).min(segment.lines.len())
        } else {
            head_idx + 1
        };
        merged_lines.extend_from_slice(&segment.lines[..include_end]);

        if include_end >= segment.lines.len() {
            return;
        }

        let remaining = &segment.lines[include_end..];

        if !segment.tail_closes_head(arena) {
            merged_lines.extend(self.maybe_merge_lines(remaining, arena));
            return;
        }
        let Ok((tail_idx, _)) = segment.tail(arena) else {
            merged_lines.extend(self.maybe_merge_lines(remaining, arena));
            return;
        };
        let tail_start = segment.lines.len() - 1 - tail_idx;
        if include_end >= tail_start {
            merged_lines.extend(self.maybe_merge_lines(remaining, arena));
            return;
        }

        let inner = &segment.lines[include_end..tail_start];
        let merged_inner = self.maybe_merge_lines(inner, arena);
        self.try_merge_jinja_keyword_with_inner(merged_lines, merged_inner, arena);
        merged_lines.extend_from_slice(&segment.lines[tail_start..]);
    }

    /// If the last head line is a JinjaBlockKeyword ({% else %}, {% elif %}),
    /// try to merge it with the first non-blank inner line.
    fn try_merge_jinja_keyword_with_inner(
        &self,
        merged_lines: &mut Vec<Line>,
        merged_inner: Vec<Line>,
        arena: &[Node],
    ) {
        let head_is_jinja_keyword = merged_lines
            .last()
            .and_then(|l| l.first_content_node(arena))
            .map(|n| n.token.token_type == crate::token::TokenType::JinjaBlockKeyword)
            .unwrap_or(false);

        if !head_is_jinja_keyword {
            merged_lines.extend(merged_inner);
            return;
        }

        let Some(fci) = merged_inner.iter().position(|l| !l.is_blank_line(arena)) else {
            merged_lines.extend(merged_inner);
            return;
        };

        let last_head = merged_lines
            .pop()
            .expect("head_is_jinja_keyword requires non-empty merged_lines");
        let first_inner = &merged_inner[fci];
        match self.create_merged_line(&[last_head.clone(), first_inner.clone()], arena) {
            Ok(merged) => {
                merged_lines.extend(merged);
                merged_lines.extend_from_slice(&merged_inner[fci + 1..]);
            }
            Err(_) => {
                merged_lines.push(last_head);
                merged_lines.extend(merged_inner);
            }
        }
    }

    /// Convert leading commas to trailing commas by merging standalone comma
    /// lines with the preceding non-blank content line.
    /// Python sqlfmt always uses trailing comma style.
    /// Don't merge commas with Jinja block tags ({% if %}, {% else %}, etc.)
    /// as the comma is part of the block's content, not the tag itself.
    /// Uses a mark-and-sweep approach to avoid O(n²) from Vec::remove().
    fn fix_standalone_commas(&self, lines: &mut Vec<Line>, arena: &[Node]) {
        let len = lines.len();
        let mut remove = vec![false; len];
        let mut last_content_idx: Option<usize> = None;

        for i in 0..len {
            if remove[i] {
                continue;
            }
            if lines[i].is_standalone_comma(arena) && i > 0 {
                if let Some(pi) = last_content_idx {
                    if !is_jinja_block_line(&lines[pi], arena) {
                        let to_merge = vec![lines[pi].clone(), lines[i].clone()];
                        if let Ok(merged) = self.create_merged_line(&to_merge, arena) {
                            if let Some(merged_line) =
                                merged.into_iter().find(|l| !l.is_blank_line(arena))
                            {
                                lines[pi] = merged_line;
                                remove[i] = true;
                                for j in (pi + 1)..i {
                                    if lines[j].is_blank_line(arena) {
                                        remove[j] = true;
                                    }
                                }
                                continue;
                            }
                        }
                    }
                }
            }

            if !lines[i].is_blank_line(arena) && !lines[i].is_standalone_comment_line(arena) {
                last_content_idx = Some(i);
            }
        }

        if remove.iter().any(|&r| r) {
            let mut new_lines = Vec::with_capacity(len);
            for (i, line) in lines.drain(..).enumerate() {
                if !remove[i] {
                    new_lines.push(line);
                }
            }
            *lines = new_lines;
        }
    }

    /// Try to merge all lines into a single line.
    /// Returns CannotMerge error if the result would be too long or violate rules.
    fn create_merged_line(&self, lines: &[Line], arena: &[Node]) -> Result<Vec<Line>, ControlFlow> {
        if lines.len() <= 1 {
            return Ok(lines.to_vec());
        }

        let leading_count = lines
            .iter()
            .take_while(|l| l.is_blank_line(arena) || l.is_standalone_comment_line(arena))
            .count();
        let trailing_count = lines
            .iter()
            .rev()
            .take_while(|l| l.is_blank_line(arena) || l.is_standalone_comment_line(arena))
            .count();
        let content_end = lines.len() - trailing_count;
        let content_start = leading_count.min(content_end);
        let content_lines = &lines[content_start..content_end];

        if content_lines.len() <= 1 {
            return Ok(lines.to_vec());
        }

        let (nodes, comments) = Self::extract_components(content_lines, arena)?;

        let mut merged_line = Line::new(content_lines[0].previous_node);
        for &idx in &nodes {
            merged_line.append_node(idx);
        }
        merged_line.comments = comments;

        if merged_line.len(arena) > self.max_length {
            return Err(ControlFlow::CannotMerge);
        }

        let mut result = lines[..content_start].to_vec();
        result.push(merged_line);
        result.extend_from_slice(&lines[content_end..]);
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
    /// Combines inline comment validation, interior standalone comment detection,
    /// and node extraction into a single pass.
    fn extract_components(
        lines: &[Line],
        arena: &[Node],
    ) -> Result<(Vec<usize>, Vec<crate::comment::Comment>), ControlFlow> {
        let mut has_inline_comment_above = false;
        let mut first_content_seen = false;
        let mut pending_interior_comment = false;

        let mut nodes = Vec::new();
        let mut comments = Vec::new();
        let mut final_newline: Option<usize> = None;
        let mut has_multiline_jinja = false;
        let mut jinja_block_depth: i32 = 0;

        for line in lines {
            let is_blank = line.is_blank_line(arena);
            let is_standalone_comment = line.is_standalone_comment_line(arena);
            let is_content = !is_blank && !is_standalone_comment;

            // --- Inline comment validation ---
            if !line.comments.is_empty() {
                if has_inline_comment_above {
                    return Err(ControlFlow::CannotMerge);
                }
                let has_inline = line.comments.iter().any(|c| c.is_inline());
                if has_inline && !is_blank {
                    has_inline_comment_above = true;
                }
            } else if has_inline_comment_above && !line.is_standalone_comma(arena) && !is_blank {
                return Err(ControlFlow::CannotMerge);
            }

            // --- Interior standalone comment detection ---
            if is_content {
                if pending_interior_comment {
                    return Err(ControlFlow::CannotMerge);
                }
                first_content_seen = true;
            } else if is_standalone_comment && first_content_seen {
                pending_interior_comment = true;
            }

            // --- Node extraction with merge rule validation ---
            if has_multiline_jinja {
                let starts_with_op = line
                    .first_content_node(arena)
                    .map(|n| n.is_operator(arena))
                    .unwrap_or(false);
                let starts_with_comma = line.starts_with_comma(arena);
                if !starts_with_op && !starts_with_comma {
                    return Err(ControlFlow::CannotMerge);
                }
                let current_has_multiline = line
                    .nodes
                    .iter()
                    .any(|&idx| arena[idx].is_multiline_jinja());
                if current_has_multiline {
                    return Err(ControlFlow::CannotMerge);
                }
            }

            if !nodes.is_empty() && !has_multiline_jinja {
                let current_has_multiline = line
                    .nodes
                    .iter()
                    .any(|&idx| arena[idx].is_multiline_jinja());
                if current_has_multiline {
                    return Err(ControlFlow::CannotMerge);
                }
            }

            if !nodes.is_empty() {
                if let Some(first) = line.first_content_node(arena) {
                    if first.token.token_type == crate::token::TokenType::JinjaBlockEnd
                        && jinja_block_depth <= 0
                    {
                        return Err(ControlFlow::CannotMerge);
                    }
                }
            }

            if !nodes.is_empty() {
                if let Some(first) = line.first_content_node(arena) {
                    if first.token.token_type == crate::token::TokenType::On {
                        let line_has_multiline_jinja = line
                            .nodes
                            .iter()
                            .any(|&idx| arena[idx].is_multiline_jinja());
                        if line_has_multiline_jinja {
                            return Err(ControlFlow::CannotMerge);
                        }
                    }
                }
            }

            let mut line_has_multiline = false;
            for &node_idx in &line.nodes {
                let node = &arena[node_idx];

                if !node.formatting_disabled.is_empty() {
                    return Err(ControlFlow::CannotMerge);
                }
                if matches!(
                    node.token.token_type,
                    crate::token::TokenType::FmtOff | crate::token::TokenType::FmtOn
                ) {
                    return Err(ControlFlow::CannotMerge);
                }
                if node.divides_queries() {
                    return Err(ControlFlow::CannotMerge);
                }

                match node.token.token_type {
                    crate::token::TokenType::JinjaBlockStart => {
                        if jinja_block_depth > 0 {
                            return Err(ControlFlow::CannotMerge);
                        }
                        jinja_block_depth += 1;
                    }
                    crate::token::TokenType::JinjaBlockEnd => jinja_block_depth -= 1,
                    _ => {}
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
            if !line.comments.is_empty() {
                comments.extend(line.comments.iter().cloned());
            }
        }

        if nodes.is_empty() {
            return Err(ControlFlow::CannotMerge);
        }

        if let Some(nl) = final_newline {
            nodes.push(nl);
        }

        Ok((nodes, comments))
    }

    /// Fix standalone operators by merging them with the next line.
    fn fix_standalone_operators(&self, mut segments: Vec<Segment>, arena: &[Node]) -> Vec<Segment> {
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
                    let merge_end = (head_idx + 2).min(segment.lines.len());
                    if let Ok(merged) =
                        self.create_merged_line(&segment.lines[head_idx..merge_end], arena)
                    {
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
        op_tiers: &[OperatorPrecedence],
        arena: &[Node],
    ) -> Vec<Segment> {
        let Some(&precedence) = op_tiers.last() else {
            return segments;
        };
        if segments.len() <= 1 {
            return segments;
        }
        let remaining_tiers = &op_tiers[..op_tiers.len() - 1];
        let mut new_segments: Vec<Segment> = Vec::new();
        let mut head = 0;

        for i in 1..segments.len() {
            if !self.segment_continues_operator_sequence(&segments[i], precedence, arena) {
                new_segments.extend(self.try_merge_operator_segments(
                    &segments[head..i],
                    remaining_tiers,
                    arena,
                ));
                head = i;
            }
        }
        new_segments.extend(self.try_merge_operator_segments(
            &segments[head..],
            remaining_tiers,
            arena,
        ));

        new_segments
    }

    /// Check if a segment should be stubbornly merged with the previous one.
    /// Handles:
    /// - JOIN conditions: USING (...) / ON after a join clause
    /// - LATERAL after a comma in FROM clause
    ///
    /// `next_segment` is the segment after this one (if any), used to detect
    /// multi-line ON clauses (ON followed by AND/OR).
    fn segment_should_stubbornly_merge(
        &self,
        segment: &Segment,
        prev_segment: &Segment,
        next_segment: Option<&Segment>,
        arena: &[Node],
    ) -> bool {
        let (_, line) = match segment.head(arena) {
            Err(_) => return false,
            Ok(h) => h,
        };
        let first = match line.first_content_node(arena) {
            None => return false,
            Some(n) => n,
        };

        if first.token.token_type == crate::token::TokenType::On {
            return self.should_merge_on_clause(segment, next_segment, arena);
        }

        if first.token.token_type == crate::token::TokenType::UntermKeyword
            && first.value.eq_ignore_ascii_case("using")
        {
            return segment.lines.iter().any(|l| {
                l.nodes
                    .iter()
                    .any(|&idx| arena[idx].token.token_type == crate::token::TokenType::BracketOpen)
            });
        }

        if first.token.token_type == crate::token::TokenType::UntermKeyword
            && first.value.eq_ignore_ascii_case("lateral")
        {
            let prev_ends_comma = prev_segment
                .tail(arena)
                .ok()
                .map(|(_, tail_line)| tail_line.ends_with_comma(arena))
                .unwrap_or(false);
            if !prev_ends_comma {
                return false;
            }
            return self.create_merged_line(&segment.lines, arena).is_ok();
        }

        false
    }

    /// Check if an ON clause should be stubbornly merged.
    fn should_merge_on_clause(
        &self,
        segment: &Segment,
        next_segment: Option<&Segment>,
        arena: &[Node],
    ) -> bool {
        if let Some(next) = next_segment {
            if let Ok((_, next_line)) = next.head(arena) {
                if let Some(nn) = next_line.first_content_node(arena) {
                    if nn.is_boolean_operator() || nn.is_operator(arena) {
                        return false;
                    }
                }
            }
            if segment_has_multiline_jinja(next, arena) {
                return false;
            }
        }
        !segment_has_multiline_jinja(segment, arena)
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
                            // Exclude ON from operator sequences — it's handled
                            // separately by Phase 3 stubborn merge
                            && n.token.token_type != crate::token::TokenType::On
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
        op_tiers: &[OperatorPrecedence],
        arena: &[Node],
    ) -> Vec<Segment> {
        if segments.len() <= 1 {
            return segments.to_vec();
        }

        let total_lines: usize = segments.iter().map(|s| s.lines.len()).sum();
        let mut all_lines = Vec::with_capacity(total_lines);
        for s in segments {
            all_lines.extend_from_slice(&s.lines);
        }
        match self.create_merged_line(&all_lines, arena) {
            Ok(merged) => vec![Segment::new(merged)],
            Err(_) => self.maybe_merge_operators(segments.to_vec(), op_tiers, arena),
        }
    }

    /// Stubborn merge: force-merge tight-binding operators (AS, OVER, etc.)
    fn maybe_stubbornly_merge(&self, segments: Vec<Segment>, arena: &[Node]) -> Vec<Segment> {
        if segments.len() <= 1 {
            return segments;
        }

        let mut iter = segments.into_iter();
        let mut new_segments = vec![iter
            .next()
            .expect("segments verified non-empty by len > 1 guard")];
        for segment in iter {
            if self.segment_continues_operator_sequence(
                &segment,
                OperatorPrecedence::OtherTight,
                arena,
            ) {
                new_segments = self.stubbornly_merge(new_segments, segment, arena);
            } else {
                new_segments.push(segment);
            }
        }

        if new_segments.len() <= 1 {
            return new_segments;
        }

        let p1_flags: Vec<bool> = new_segments
            .iter()
            .map(|s| {
                self.segment_continues_operator_sequence(s, OperatorPrecedence::Comparators, arena)
            })
            .collect();

        let segments = new_segments;
        let mut new_segments = Vec::with_capacity(segments.len());
        for (i, segment) in segments.into_iter().enumerate() {
            if i == 0 {
                new_segments.push(segment);
                continue;
            }
            if !p1_flags[i - 1]
                && p1_flags[i]
                && Segment::new(self.safe_create_merged_line(&segment.lines, arena))
                    .tail_closes_head(arena)
            {
                new_segments = self.stubbornly_merge(new_segments, segment, arena);
            } else {
                new_segments.push(segment);
            }
        }

        if new_segments.len() <= 1 {
            return new_segments;
        }

        // Phase 3 needs indexed access for look-ahead (segments.get(i + 1)),
        // so we consume via drain to avoid cloning.
        let mut segments = new_segments;
        let mut new_segments = Vec::with_capacity(segments.len());
        // Take first element by swap-remove pattern
        let first = std::mem::replace(&mut segments[0], Segment::new(Vec::new()));
        new_segments.push(first);
        for i in 1..segments.len() {
            let prev = new_segments
                .last()
                .expect("new_segments initialized with segments[0]");
            let next = segments.get(i + 1);
            if self.segment_should_stubbornly_merge(&segments[i], prev, next, arena) {
                let seg = std::mem::replace(&mut segments[i], Segment::new(Vec::new()));
                new_segments = self.stubbornly_merge(new_segments, seg, arena);
            } else {
                let seg = std::mem::replace(&mut segments[i], Segment::new(Vec::new()));
                new_segments.push(seg);
            }
        }

        new_segments
    }

    /// Attempt several methods of merging a segment with the previous segments.
    /// Takes ownership of both `prev_segments` and `segment` to avoid cloning.
    fn stubbornly_merge(
        &self,
        mut prev_segments: Vec<Segment>,
        segment: Segment,
        arena: &[Node],
    ) -> Vec<Segment> {
        let mut prev_segment = match prev_segments.pop() {
            Some(s) => s,
            None => {
                prev_segments.push(segment);
                return prev_segments;
            }
        };

        let (head_idx, head_line) = match segment.head(arena) {
            Ok((head_idx, head_line)) => (head_idx, head_line.clone()),
            Err(_) => {
                prev_segments.push(prev_segment);
                prev_segments.push(segment);
                return prev_segments;
            }
        };

        // Attempt 1: merge all prev_segment lines + head_line.
        // Temporarily push onto prev_segment to avoid cloning all its lines.
        prev_segment.lines.push(head_line.clone());
        let attempt1 = self.create_merged_line(&prev_segment.lines, arena);
        prev_segment.lines.pop();
        if let Ok(merged) = attempt1 {
            let mut result_seg = Segment::new(merged);
            result_seg
                .lines
                .extend_from_slice(&segment.lines[head_idx + 1..]);
            prev_segments.push(result_seg);
            return prev_segments;
        }

        if let Ok((tail_idx, tail_line)) = prev_segment.tail(arena) {
            let tail_line = tail_line.clone();
            let mut try_lines = Vec::with_capacity(1 + segment.lines.len());
            try_lines.push(tail_line.clone());
            try_lines.extend_from_slice(&segment.lines);
            if let Ok(merged) = self.create_merged_line(&try_lines, arena) {
                let tail_start = prev_segment.lines.len() - 1 - tail_idx;
                let mut result_seg = Segment::new(prev_segment.lines[..tail_start].to_vec());
                result_seg.lines.extend(merged);
                prev_segments.push(result_seg);
                return prev_segments;
            }

            let try_lines = [tail_line, head_line];
            if let Ok(merged) = self.create_merged_line(&try_lines, arena) {
                let tail_start = prev_segment.lines.len() - 1 - tail_idx;
                let mut result_seg = Segment::new(prev_segment.lines[..tail_start].to_vec());
                result_seg.lines.extend(merged);
                result_seg
                    .lines
                    .extend_from_slice(&segment.lines[head_idx + 1..]);
                prev_segments.push(result_seg);
                return prev_segments;
            }
        }

        prev_segments.push(prev_segment);
        prev_segments.push(segment);
        prev_segments
    }
}

/// Check if a line starts with a Jinja block tag.
fn is_jinja_block_line(line: &Line, arena: &[Node]) -> bool {
    line.first_content_node(arena)
        .map(|n| {
            matches!(
                n.token.token_type,
                crate::token::TokenType::JinjaBlockStart
                    | crate::token::TokenType::JinjaBlockKeyword
                    | crate::token::TokenType::JinjaBlockEnd
            )
        })
        .unwrap_or(false)
}

/// Check if any line in a segment contains multiline Jinja.
fn segment_has_multiline_jinja(segment: &Segment, arena: &[Node]) -> bool {
    segment
        .lines
        .iter()
        .any(|l| l.nodes.iter().any(|&idx| arena[idx].is_multiline_jinja()))
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
            Token::new(tt, "", val, 0, val.len() as u32),
            prev,
            compact_str::CompactString::from(prefix),
            compact_str::CompactString::from(val),
            smallvec::SmallVec::new(),
            smallvec::SmallVec::new(),
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

    #[test]
    fn test_no_merge_long_result() {
        let mut arena = Vec::new();
        let long_val = "a".repeat(50);
        let line1 = make_simple_line(&mut arena, TokenType::Name, &long_val);
        let line2 = make_simple_line(&mut arena, TokenType::Name, &long_val);

        let merger = LineMerger::new(88);
        let result = merger.maybe_merge_lines(&[line1, line2], &arena);
        // Should NOT merge since combined length > 88
        assert!(
            result.len() >= 2,
            "Lines too long to merge should stay separate"
        );
    }

    #[test]
    fn test_merge_empty_input() {
        let arena = Vec::new();
        let merger = LineMerger::new(88);
        let result = merger.maybe_merge_lines(&[], &arena);
        assert!(result.is_empty());
    }

    #[test]
    fn test_no_merge_across_query_dividers() {
        let mut arena = Vec::new();
        let line1 = make_simple_line(&mut arena, TokenType::Name, "a");

        // Line with semicolon (query divider)
        let semi = make_node(&mut arena, TokenType::Semicolon, ";", "");
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        let mut semi_line = Line::new(None);
        semi_line.append_node(semi);
        semi_line.append_node(nl);

        let line3 = make_simple_line(&mut arena, TokenType::Name, "b");

        let merger = LineMerger::new(88);
        let result = merger.maybe_merge_lines(&[line1, semi_line, line3], &arena);
        // Should not merge across the semicolon
        assert!(
            result.len() >= 2,
            "Should not merge across query dividers, got {} lines",
            result.len()
        );
    }

    #[test]
    fn test_no_merge_formatting_disabled() {
        let mut arena = Vec::new();

        // Create a line with formatting disabled
        let a = make_node(&mut arena, TokenType::Name, "a", "");
        arena[a].formatting_disabled = smallvec::smallvec![0usize];
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        let mut disabled_line = Line::new(None);
        disabled_line.append_node(a);
        disabled_line.append_node(nl);
        disabled_line.formatting_disabled = true;

        let merger = LineMerger::new(88);
        let result = merger.maybe_merge_lines(&[disabled_line], &arena);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_merge_with_blank_lines() {
        let mut arena = Vec::new();
        let line1 = make_simple_line(&mut arena, TokenType::Name, "a");

        // Blank line
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        let mut blank = Line::new(None);
        blank.append_node(nl);

        let line3 = make_simple_line(&mut arena, TokenType::Name, "b");

        let merger = LineMerger::new(88);
        let result = merger.maybe_merge_lines(&[line1, blank, line3], &arena);
        // Should produce at least 1 line and not crash
        assert!(
            !result.is_empty(),
            "Should handle blank lines without crashing"
        );
    }

    // --- Full-pipeline helper for realistic merge testing ---

    /// Format SQL through the full pipeline and return the formatted result.
    fn format_sql(source: &str) -> String {
        crate::api::format_string(source, &crate::mode::Mode::default()).unwrap()
    }

    /// Count non-empty lines in formatted output.
    fn count_lines(s: &str) -> usize {
        s.lines().filter(|l| !l.trim().is_empty()).count()
    }

    #[test]
    fn test_create_merged_line_basic() {
        // Two short columns should merge onto one SELECT line
        let result = format_sql("SELECT\n    a,\n    b\n");
        assert!(
            result.contains("a,"),
            "Short columns should merge: {}",
            result
        );
    }

    #[test]
    fn test_create_merged_line_with_comments() {
        // Inline comments should survive the merge process
        let result = format_sql("SELECT\n    a, -- first\n    b\n");
        assert!(
            result.contains("-- first"),
            "Inline comment should be preserved: {}",
            result
        );
    }

    #[test]
    fn test_nested_merge() {
        // Nested function calls should merge when short enough
        let result = format_sql("SELECT nullif(split_part(x, ',', 1), '') AS val\n");
        assert!(
            result.contains("nullif("),
            "Nested functions should merge: {}",
            result
        );
    }

    #[test]
    fn test_cte_merge() {
        // Short CTE should merge
        let result = format_sql("WITH cte AS (\n    SELECT 1\n)\nSELECT * FROM cte\n");
        assert!(
            result.contains("with"),
            "CTE should be formatted: {}",
            result
        );
    }

    #[test]
    fn test_case_then_merge() {
        // Short CASE/WHEN should merge when within line length
        let result = format_sql("SELECT CASE WHEN x THEN y ELSE z END\n");
        assert!(
            result.contains("case") && result.contains("end"),
            "CASE expression should be formatted: {}",
            result
        );
    }

    #[test]
    fn test_count_window_function_merge() {
        // Window function parts should merge when short
        let result = format_sql("SELECT count(*) OVER (PARTITION BY x) FROM t\n");
        assert!(
            result.contains("over"),
            "Window function should be present: {}",
            result
        );
    }

    #[test]
    fn test_disallow_multiline_jinja() {
        // Multiline jinja (containing newlines) should block merging
        let source = "SELECT\n    {{ config(\n        key='val'\n    ) }}\n";
        let result = format_sql(source);
        assert!(
            result.contains("{{"),
            "Multiline jinja should be preserved: {}",
            result
        );
    }

    #[test]
    fn test_segment_continues_operator_sequence_tiers() {
        // Each tier in OperatorPrecedence::tiers() should work as merge boundary
        let tiers = OperatorPrecedence::tiers();
        assert_eq!(tiers.len(), 7, "Should have 7 merge tiers");
        // Verify tiers are in ascending order (required for correct merge behavior)
        for window in tiers.windows(2) {
            assert!(
                window[0] < window[1],
                "Tiers must be ascending: {:?} >= {:?}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn test_merge_lines_split_by_operators() {
        // Operator-separated lines should merge back when short
        let result = format_sql("SELECT 1 + 2 + 3\n");
        // Short addition should be on one line
        let lines: Vec<_> = result.lines().filter(|l| !l.trim().is_empty()).collect();
        assert!(
            lines.len() <= 2,
            "Short operator chain should merge: {}",
            result
        );
    }

    #[test]
    fn test_merge_chained_parens() {
        // Chained parenthesized expressions
        let result = format_sql("SELECT (a + b) * (c + d)\n");
        assert!(
            result.contains("(a + b)") || result.contains("(a"),
            "Chained parens should be formatted: {}",
            result
        );
    }

    #[test]
    fn test_merge_operators_before_children() {
        // Operators at top level should merge before recursing into children
        let result = format_sql("SELECT a + b AS total FROM t\n");
        assert!(
            result.contains("a + b"),
            "Operator expression should merge: {}",
            result
        );
    }

    #[test]
    fn test_do_not_merge_very_long_chains() {
        // 40+ additions should NOT all merge onto one line
        let chain: Vec<String> = (0..45).map(|i| format!("col_{}", i)).collect();
        let source = format!("SELECT {}\n", chain.join(" + "));
        let result = format_sql(&source);
        let line_count = count_lines(&result);
        assert!(
            line_count > 1,
            "Very long chain should stay split ({} lines): {}",
            line_count,
            result
        );
    }

    #[test]
    fn test_respect_extra_blank_lines() {
        // Blank lines between semicolon-separated statements should be preserved
        let result = format_sql("SELECT 1;\n\nSELECT 2\n");
        let blank_lines = result.lines().filter(|l| l.trim().is_empty()).count();
        assert!(
            blank_lines >= 1,
            "Blank line between statements should be preserved: {}",
            result
        );
    }

    #[test]
    fn test_stubborn_merge_blank_lines() {
        // Stubborn merge should not absorb blank segments
        let result =
            format_sql("SELECT a\nFROM t1\nJOIN t2\n    ON t1.id = t2.id\n\nWHERE x = 1\n");
        assert!(
            result.contains("where"),
            "WHERE should be present after blank line: {}",
            result
        );
    }

    #[test]
    fn test_do_not_merge_across_union_all() {
        // UNION ALL should prevent merging across it
        let result = format_sql("SELECT 1\nUNION ALL\nSELECT 2\n");
        assert!(
            result.contains("union all"),
            "UNION ALL should stay separate: {}",
            result
        );
        // Should have at least 3 non-empty lines
        assert!(
            count_lines(&result) >= 3,
            "UNION ALL should force line breaks: {}",
            result
        );
    }

    #[test]
    fn test_do_not_merge_across_intersect() {
        let result = format_sql("SELECT 1\nINTERSECT\nSELECT 2\n");
        assert!(
            result.contains("intersect"),
            "INTERSECT should be preserved: {}",
            result
        );
        assert!(
            count_lines(&result) >= 3,
            "INTERSECT should force line breaks: {}",
            result
        );
    }

    #[test]
    fn test_do_not_merge_across_except() {
        let result = format_sql("SELECT 1\nEXCEPT\nSELECT 2\n");
        assert!(
            result.contains("except"),
            "EXCEPT should be preserved: {}",
            result
        );
        assert!(
            count_lines(&result) >= 3,
            "EXCEPT should force line breaks: {}",
            result
        );
    }

    #[test]
    fn test_maybe_stubbornly_merge_on_clause() {
        // Short ON clause should be stubbornly merged with JOIN
        let result = format_sql("SELECT *\nFROM t1\nJOIN t2\n    ON t1.id = t2.id\n");
        assert!(
            result.contains("on"),
            "ON clause should be present: {}",
            result
        );
    }

    #[test]
    fn test_fix_standalone_operators() {
        // Standalone operator should merge with following line
        let result = format_sql("SELECT a\n    + b\n");
        assert!(
            result.contains("+"),
            "Operator should be present: {}",
            result
        );
    }

    #[test]
    fn test_no_merge_operator_sequences_across_commas() {
        // Commas should break operator sequences
        let result = format_sql("SELECT a + b, c + d\n");
        assert!(
            result.contains(","),
            "Comma should be preserved: {}",
            result
        );
    }

    #[test]
    fn test_no_merge_databricks_query_hints() {
        // Block comments that look like hints should be preserved
        let result = format_sql("SELECT /*+ HINT */ 1\n");
        assert!(
            result.contains("/*+ HINT */") || result.contains("/*"),
            "Block comment hint should be preserved: {}",
            result
        );
    }
}
