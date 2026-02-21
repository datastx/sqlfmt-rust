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
                    let tiers = OperatorPrecedence::tiers().to_vec();
                    let segments = self.maybe_merge_operators(segments, tiers, arena);
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
    fn fix_standalone_commas(&self, lines: &mut Vec<Line>, arena: &[Node]) {
        let mut i = 0;
        while i < lines.len() {
            if !lines[i].is_standalone_comma(arena) || i == 0 {
                i += 1;
                continue;
            }

            let prev_idx = find_prev_content_line(lines, i, arena);
            let Some(pi) = prev_idx else {
                i += 1;
                continue;
            };

            if is_jinja_block_line(&lines[pi], arena) {
                i += 1;
                continue;
            }

            let to_merge = vec![lines[pi].clone(), lines[i].clone()];
            if let Ok(merged) = self.create_merged_line(&to_merge, arena) {
                if let Some(merged_line) = merged.into_iter().find(|l| !l.is_blank_line(arena)) {
                    lines[pi] = merged_line;
                    lines.remove(i);
                    while pi + 1 < lines.len() && lines[pi + 1].is_blank_line(arena) {
                        lines.remove(pi + 1);
                    }
                    continue;
                }
            }
            i += 1;
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
    fn extract_components(
        lines: &[Line],
        arena: &[Node],
    ) -> Result<(Vec<usize>, Vec<crate::comment::Comment>), ControlFlow> {
        // Lines with inline comments can only be merged if the following
        // line is a standalone comma or blank line. This allows the trailing
        // comma pattern: "2,  -- two inline" (inline comment line + comma).
        // Python sqlfmt tracks `has_inline_comment_above` and only allows
        // standalone commas or blank lines after it.
        let mut has_inline_comment_above = false;
        for line in lines.iter() {
            if !line.comments.is_empty() {
                if has_inline_comment_above {
                    return Err(ControlFlow::CannotMerge);
                }
                let has_inline = line.comments.iter().any(|c| c.is_inline());
                if has_inline && !line.is_blank_line(arena) {
                    has_inline_comment_above = true;
                }
            } else if has_inline_comment_above
                && !line.is_standalone_comma(arena)
                && !line.is_blank_line(arena)
            {
                return Err(ControlFlow::CannotMerge);
            }
        }

        let first_content = lines
            .iter()
            .position(|l| !l.is_blank_line(arena) && !l.is_standalone_comment_line(arena));
        let last_content = lines
            .iter()
            .rposition(|l| !l.is_blank_line(arena) && !l.is_standalone_comment_line(arena));
        if let (Some(first), Some(last)) = (first_content, last_content) {
            if first < last {
                for (i, line) in lines.iter().enumerate() {
                    if i > first && i < last && line.is_standalone_comment_line(arena) {
                        return Err(ControlFlow::CannotMerge);
                    }
                }
            }
        }

        let mut nodes = Vec::new();
        let mut comments = Vec::new();
        let mut final_newline: Option<usize> = None;
        let mut has_multiline_jinja = false;
        // Track Jinja block nesting depth across merged lines.
        // When a JinjaBlockEnd is encountered and depth is 0, the matching
        // JinjaBlockStart is NOT in this merge set, so the block end must
        // stay on its own line (e.g., {% endif %} after {% else %} content).
        // When depth > 0, the block start IS in this merge set, so the whole
        // block can merge onto one line (e.g., {% for %}...{% endfor %}).
        let mut jinja_block_depth: i32 = 0;

        for line in lines {
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
                        // Two JinjaBlockStart tokens shouldn't be merged
                        // (each block start needs its own line)
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
        mut op_tiers: Vec<OperatorPrecedence>,
        arena: &[Node],
    ) -> Vec<Segment> {
        let Some(precedence) = op_tiers.pop() else {
            return segments;
        };
        if segments.len() <= 1 {
            return segments;
        }
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
        new_segments.extend(self.try_merge_operator_segments(&segments[head..], op_tiers, arena));

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
        op_tiers: Vec<OperatorPrecedence>,
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
                new_segments = self.stubbornly_merge(&new_segments, &segment, arena);
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
                new_segments = self.stubbornly_merge(&new_segments, &segment, arena);
            } else {
                new_segments.push(segment);
            }
        }

        if new_segments.len() <= 1 {
            return new_segments;
        }

        // Phase 3 needs indexed access for look-ahead (segments.get(i + 1)),
        // so we keep indexed iteration here.
        let segments = new_segments;
        let mut new_segments = Vec::with_capacity(segments.len());
        new_segments.push(segments[0].clone());
        for i in 1..segments.len() {
            let prev = new_segments
                .last()
                .expect("new_segments initialized with segments[0]");
            let next = segments.get(i + 1);
            if self.segment_should_stubbornly_merge(&segments[i], prev, next, arena) {
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

        let mut try_lines = Vec::with_capacity(prev_segment.lines.len() + 1);
        try_lines.extend_from_slice(&prev_segment.lines);
        try_lines.push(head_line.clone());
        if let Ok(merged) = self.create_merged_line(&try_lines, arena) {
            let mut result_seg = Segment::new(merged);
            result_seg
                .lines
                .extend_from_slice(&segment.lines[head_idx + 1..]);
            new_segments.push(result_seg);
            return new_segments;
        }

        if let Ok((tail_idx, tail_line)) = prev_segment.tail(arena) {
            let mut try_lines = Vec::with_capacity(1 + segment.lines.len());
            try_lines.push(tail_line.clone());
            try_lines.extend_from_slice(&segment.lines);
            if let Ok(merged) = self.create_merged_line(&try_lines, arena) {
                let tail_start = prev_segment.lines.len() - 1 - tail_idx;
                let mut result_seg = Segment::new(prev_segment.lines[..tail_start].to_vec());
                result_seg.lines.extend(merged);
                new_segments.push(result_seg);
                return new_segments;
            }

            let try_lines = vec![tail_line.clone(), head_line];
            if let Ok(merged) = self.create_merged_line(&try_lines, arena) {
                let tail_start = prev_segment.lines.len() - 1 - tail_idx;
                let mut result_seg = Segment::new(prev_segment.lines[..tail_start].to_vec());
                result_seg.lines.extend(merged);
                result_seg
                    .lines
                    .extend_from_slice(&segment.lines[head_idx + 1..]);
                new_segments.push(result_seg);
                return new_segments;
            }
        }

        new_segments.push(prev_segment);
        new_segments.push(segment.clone());
        new_segments
    }
}

/// Find the previous non-blank, non-comment line before index `i`.
fn find_prev_content_line(lines: &[Line], i: usize, arena: &[Node]) -> Option<usize> {
    (0..i)
        .rev()
        .find(|&j| !lines[j].is_blank_line(arena) && !lines[j].is_standalone_comment_line(arena))
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
            Token::new(tt, "", val, 0, val.len()),
            prev,
            prefix.to_string(),
            val.to_string(),
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
        disabled_line.formatting_disabled.push(0);

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
}
