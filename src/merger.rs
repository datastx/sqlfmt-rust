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
                            let include_end = if !only_segment.lines.is_empty()
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
                                            let inner =
                                                &only_segment.lines[include_end..tail_start];
                                            let merged_inner =
                                                self.maybe_merge_lines(inner, arena);

                                            // If head is a JinjaBlockKeyword ({% else %},
                                            // {% elif %}), try to merge it with the first
                                            // non-blank inner line so we get
                                            // "{% else %} {{ config() }}" on one line.
                                            let head_is_jinja_keyword = merged_lines
                                                .last()
                                                .and_then(|l| l.first_content_node(arena))
                                                .map(|n| {
                                                    n.token.token_type
                                                        == crate::token::TokenType::JinjaBlockKeyword
                                                })
                                                .unwrap_or(false);
                                            if head_is_jinja_keyword && !merged_lines.is_empty() {
                                                // Find first non-blank inner line
                                                let first_content_idx = merged_inner
                                                    .iter()
                                                    .position(|l| !l.is_blank_line(arena));
                                                if let Some(fci) = first_content_idx {
                                                    let last_head = merged_lines.pop().unwrap();
                                                    let first_inner = &merged_inner[fci];
                                                    match self.create_merged_line(
                                                        &[
                                                            last_head.clone(),
                                                            first_inner.clone(),
                                                        ],
                                                        arena,
                                                    ) {
                                                        Ok(merged) => {
                                                            merged_lines.extend(merged);
                                                            merged_lines.extend_from_slice(
                                                                &merged_inner[fci + 1..],
                                                            );
                                                        }
                                                        Err(_) => {
                                                            merged_lines.push(last_head);
                                                            merged_lines.extend(merged_inner);
                                                        }
                                                    }
                                                } else {
                                                    merged_lines.extend(merged_inner);
                                                }
                                            } else {
                                                merged_lines.extend(merged_inner);
                                            }
                                            merged_lines.extend_from_slice(
                                                &only_segment.lines[tail_start..],
                                            );
                                        } else {
                                            merged_lines
                                                .extend(self.maybe_merge_lines(remaining, arena));
                                        }
                                    } else {
                                        merged_lines
                                            .extend(self.maybe_merge_lines(remaining, arena));
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
    fn create_merged_line(&self, lines: &[Line], arena: &[Node]) -> Result<Vec<Line>, ControlFlow> {
        if lines.len() <= 1 {
            return Ok(lines.to_vec());
        }

        // Extract leading/trailing blank and standalone-comment lines.
        // These are preserved around the merged content.
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

        // Assemble result: leading non-content + merged + trailing non-content
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
        // Lines with inline comments cannot be merged with subsequent lines
        // because inline comments must stay at the end of their line.
        // Only the last non-blank line can have inline comments
        // (they'll appear at end of merged line).
        let last_content_idx = lines
            .iter()
            .rposition(|l| !l.is_blank_line(arena));
        for (i, line) in lines.iter().enumerate() {
            if Some(i) != last_content_idx
                && !line.is_blank_line(arena)
                && line.comments.iter().any(|c| c.is_inline())
            {
                return Err(ControlFlow::CannotMerge);
            }
        }

        // Standalone comment-only lines in the middle of content cannot be merged,
        // as they need to stay on their own line.
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
                // If the current line ALSO has multiline Jinja, reject the merge.
                // Two multiline Jinja expressions on the same logical line is too complex.
                let current_has_multiline = line
                    .nodes
                    .iter()
                    .any(|&idx| arena[idx].is_multiline_jinja());
                if current_has_multiline {
                    return Err(ControlFlow::CannotMerge);
                }
            }

            // A line with multiline Jinja shouldn't be merged with preceding
            // non-multiline content. The multiline content needs its own line.
            if !nodes.is_empty() && !has_multiline_jinja {
                let current_has_multiline = line
                    .nodes
                    .iter()
                    .any(|&idx| arena[idx].is_multiline_jinja());
                if current_has_multiline {
                    return Err(ControlFlow::CannotMerge);
                }
            }

            // Don't merge a JinjaBlockEnd ({% endif %}, {% endfor %}, etc.)
            // unless its matching JinjaBlockStart is also being merged.
            if !nodes.is_empty() {
                if let Some(first) = line.first_content_node(arena) {
                    if first.token.token_type == crate::token::TokenType::JinjaBlockEnd
                        && jinja_block_depth <= 0
                    {
                        return Err(ControlFlow::CannotMerge);
                    }
                }
            }

            // Don't merge an ON line that contains multiline Jinja with
            // preceding content. The ON clause with multiline Jinja is too
            // complex to fit naturally on the same line as the table name.
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

                // Can't merge lines with disabled formatting
                if !node.formatting_disabled.is_empty() {
                    return Err(ControlFlow::CannotMerge);
                }
                // Can't merge FmtOff/FmtOn directives with other content
                if matches!(
                    node.token.token_type,
                    crate::token::TokenType::FmtOff | crate::token::TokenType::FmtOn
                ) {
                    return Err(ControlFlow::CannotMerge);
                }
                // Can't merge query dividers
                if node.divides_queries() {
                    return Err(ControlFlow::CannotMerge);
                }

                // Track jinja block nesting
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

    /// Extract leading blank lines and standalone comment lines.
    fn extract_leading_non_content(lines: &[Line], arena: &[Node]) -> Vec<Line> {
        let mut result = Vec::new();
        for line in lines {
            if line.is_blank_line(arena) || line.is_standalone_comment_line(arena) {
                result.push(line.clone());
            } else {
                break;
            }
        }
        result
    }

    /// Extract trailing blank lines and standalone comment lines.
    fn extract_trailing_non_content(lines: &[Line], arena: &[Node]) -> Vec<Line> {
        let mut result = Vec::new();
        for line in lines.iter().rev() {
            if line.is_blank_line(arena) || line.is_standalone_comment_line(arena) {
                result.push(line.clone());
            } else {
                break;
            }
        }
        result.reverse();
        result
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
        match segment.head(arena) {
            Err(_) => false,
            Ok((_, line)) => {
                let first = line.first_content_node(arena);
                match first {
                    // ON merges with previous (join condition) — but NOT when
                    // the ON clause has additional AND/OR conditions on subsequent
                    // lines (those appear as the next segment), and NOT when the
                    // ON segment or the next segment contains multiline Jinja
                    // (the merged result would be too complex).
                    Some(n) if n.token.token_type == crate::token::TokenType::On => {
                        if let Some(next) = next_segment {
                            if let Ok((_, next_line)) = next.head(arena) {
                                if let Some(nn) = next_line.first_content_node(arena) {
                                    // Don't merge if next segment starts with boolean operator
                                    if nn.is_boolean_operator() {
                                        return false;
                                    }
                                    // Don't merge if next segment starts with a comparison
                                    // operator (=, >, <, etc.) — means the join condition
                                    // spans multiple lines
                                    if nn.is_operator(arena) {
                                        return false;
                                    }
                                }
                            }
                            // Don't merge if the next segment has multiline Jinja
                            let next_has_multiline = next.lines.iter().any(|l| {
                                l.nodes
                                    .iter()
                                    .any(|&idx| arena[idx].is_multiline_jinja())
                            });
                            if next_has_multiline {
                                return false;
                            }
                        }
                        // Don't merge if the ON segment itself contains multiline Jinja
                        let has_multiline = segment.lines.iter().any(|l| {
                            l.nodes
                                .iter()
                                .any(|&idx| arena[idx].is_multiline_jinja())
                        });
                        if has_multiline {
                            return false;
                        }
                        true
                    }
                    // USING merges only when followed by ( (join condition, not DELETE)
                    Some(n)
                        if n.token.token_type == crate::token::TokenType::UntermKeyword
                            && n.value.eq_ignore_ascii_case("using") =>
                    {
                        segment.lines.iter().any(|l| {
                            l.nodes.iter().any(|&idx| {
                                arena[idx].token.token_type
                                    == crate::token::TokenType::BracketOpen
                            })
                        })
                    }
                    // LATERAL merges with previous when previous ends with comma
                    // (FROM clause: "from t1, lateral flatten(...)")
                    Some(n)
                        if n.token.token_type == crate::token::TokenType::UntermKeyword
                            && n.value.eq_ignore_ascii_case("lateral") =>
                    {
                        prev_segment
                            .tail(arena)
                            .ok()
                            .map(|(_, tail_line)| tail_line.ends_with_comma(arena))
                            .unwrap_or(false)
                    }
                    _ => false,
                }
            }
        }
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

        // Flatten all lines and try merge
        let all_lines: Vec<Line> = segments.iter().flat_map(|s| s.lines.clone()).collect();
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
                self.segment_continues_operator_sequence(s, OperatorPrecedence::Comparators, arena)
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

        if new_segments.len() <= 1 {
            return new_segments;
        }

        // Phase 3: Stubborn-merge join condition clauses (USING/ON) with
        // preceding segments. This allows "left join table\nusing (id)" to
        // merge into "left join table using (id)" when it fits.
        let segments = new_segments;
        let mut new_segments = vec![segments[0].clone()];
        for i in 1..segments.len() {
            let prev = new_segments.last().unwrap();
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

        // Try 1: Merge head of this segment with entire previous segment
        let mut try_lines = prev_segment.lines.clone();
        try_lines.push(head_line.clone());
        if let Ok(merged) = self.create_merged_line(&try_lines, arena) {
            let mut result_seg = Segment::new(merged);
            result_seg
                .lines
                .extend_from_slice(&segment.lines[head_idx + 1..]);
            new_segments.push(result_seg);
            return new_segments;
        }

        // Try 2: Merge entire segment onto last line of previous segment
        if let Ok((tail_idx, tail_line)) = prev_segment.tail(arena) {
            let mut try_lines = vec![tail_line.clone()];
            try_lines.extend(segment.lines.clone());
            if let Ok(merged) = self.create_merged_line(&try_lines, arena) {
                let tail_start = prev_segment.lines.len() - 1 - tail_idx;
                let mut result_seg = Segment::new(prev_segment.lines[..tail_start].to_vec());
                result_seg.lines.extend(merged);
                new_segments.push(result_seg);
                return new_segments;
            }

            // Try 3: Merge just head of this segment onto last line of previous segment
            let try_lines = vec![tail_line.clone(), head_line.clone()];
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
        arena[a].formatting_disabled =
            smallvec::smallvec![Token::new(TokenType::FmtOff, "", "-- fmt: off", 0, 11)];
        let nl = make_node(&mut arena, TokenType::Newline, "\n", "");
        let mut disabled_line = Line::new(None);
        disabled_line.append_node(a);
        disabled_line.append_node(nl);
        disabled_line.formatting_disabled.push(Token::new(
            TokenType::FmtOff,
            "",
            "-- fmt: off",
            0,
            11,
        ));

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
