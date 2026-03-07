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
pub(crate) struct LineMerger {
    max_length: usize,
}

impl LineMerger {
    pub(crate) fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    /// Main entry: try to merge lines.
    /// Mirrors Python's `maybe_merge_lines`.
    /// Takes ownership to avoid cloning on early-exit and recursive paths.
    pub(crate) fn maybe_merge_lines(&self, lines: Vec<Line>, arena: &[Node]) -> Vec<Line> {
        if lines.is_empty() || lines.iter().all(|l| l.has_formatting_disabled()) {
            return lines;
        }

        match self.create_merged_line(&lines, arena) {
            Ok(merged) => merged,
            Err(_) => {
                let input_line_count = lines.len();
                let mut merged_lines = Vec::new();
                let segments = build_segments(lines, arena);

                if segments.len() > 1 {
                    let segments = self.fix_standalone_operators(segments, arena);
                    let segments =
                        self.maybe_merge_operators(segments, OperatorPrecedence::tiers(), arena);
                    let segments = self.maybe_stubbornly_merge(segments, arena);

                    // Guard against infinite recursion: if segment processing
                    // collapsed multiple segments back into a single segment
                    // with the same number of lines as the input, fall through
                    // to single-segment processing instead of recursing with
                    // identical input.
                    if segments.len() == 1 && segments[0].lines.len() == input_line_count {
                        let segment = segments
                            .into_iter()
                            .next()
                            .expect("segments verified len == 1");
                        self.merge_single_segment(segment, arena, &mut merged_lines);
                    } else {
                        for segment in segments {
                            merged_lines.extend(self.maybe_merge_lines(segment.lines, arena));
                        }
                    }
                } else {
                    let segment = segments
                        .into_iter()
                        .next()
                        .expect("build_segments returns at least one segment");
                    self.merge_single_segment(segment, arena, &mut merged_lines);
                }

                self.fix_standalone_commas(&mut merged_lines, arena);
                merged_lines
            }
        }
    }

    /// Handle a single segment: peel off the head line(s), then recurse
    /// into remaining lines (splitting around tail if it closes head).
    /// Takes ownership to avoid cloning line slices for recursive calls.
    fn merge_single_segment(
        &self,
        mut segment: Segment,
        arena: &[Node],
        merged_lines: &mut Vec<Line>,
    ) {
        let Ok((head_idx, _)) = segment.head(arena) else {
            merged_lines.extend(segment.lines);
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

        if include_end >= segment.lines.len() {
            merged_lines.extend(segment.lines);
            return;
        }

        if !segment.tail_closes_head(arena) {
            let remaining = segment.lines.split_off(include_end);
            merged_lines.extend(segment.lines);
            merged_lines.extend(self.maybe_merge_lines(remaining, arena));
            return;
        }
        let Ok((tail_idx, _)) = segment.tail(arena) else {
            let remaining = segment.lines.split_off(include_end);
            merged_lines.extend(segment.lines);
            merged_lines.extend(self.maybe_merge_lines(remaining, arena));
            return;
        };
        let tail_start = segment.lines.len() - 1 - tail_idx;
        if include_end >= tail_start {
            let remaining = segment.lines.split_off(include_end);
            merged_lines.extend(segment.lines);
            merged_lines.extend(self.maybe_merge_lines(remaining, arena));
            return;
        }

        // Split into three parts: head, inner, tail
        let tail_lines = segment.lines.split_off(tail_start);
        let inner_lines = segment.lines.split_off(include_end);
        merged_lines.extend(segment.lines);
        let merged_inner = self.maybe_merge_lines(inner_lines, arena);
        self.try_merge_jinja_keyword_with_inner(merged_lines, merged_inner, arena);
        merged_lines.extend(tail_lines);
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
        let try_lines = [last_head.clone(), first_inner.clone()];
        match self.try_create_merged_line(&try_lines, arena) {
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

    /// Merge standalone comma lines with the FOLLOWING content line.
    /// Leading-comma style: `, next_item` on one line.
    /// Don't merge commas with Jinja block tags ({% if %}, {% else %}, etc.)
    /// as the comma is part of the block's content, not the tag itself.
    /// Uses a mark-and-sweep approach to avoid O(n²) from Vec::remove().
    fn fix_standalone_commas(&self, lines: &mut Vec<Line>, arena: &[Node]) {
        let len = lines.len();
        let mut remove = vec![false; len];

        for i in 0..len {
            if remove[i] {
                continue;
            }
            if lines[i].is_standalone_comma(arena)
                && self.try_merge_standalone_comma(i, lines, &mut remove, arena)
            {
                continue;
            }
        }

        if remove.iter().any(|&r| r) {
            let new_lines: Vec<_> = lines
                .drain(..)
                .enumerate()
                .filter(|(i, _)| !remove[*i])
                .map(|(_, line)| line)
                .collect();
            *lines = new_lines;
        }
    }

    /// Try to merge a standalone comma at index `i` with the next content line.
    /// Returns `true` if the merge succeeded and the caller should `continue`.
    /// Marks the comma line (and intervening blanks) for removal in `remove`.
    fn try_merge_standalone_comma(
        &self,
        i: usize,
        lines: &mut [Line],
        remove: &mut [bool],
        arena: &[Node],
    ) -> bool {
        let len = lines.len();
        // Find next non-blank content line
        let ni = match (i + 1..len).find(|&j| {
            !remove[j]
                && !lines[j].is_blank_line(arena)
                && !lines[j].is_standalone_comment_line(arena)
        }) {
            Some(idx) => idx,
            None => return false,
        };

        if is_jinja_block_line(&lines[ni], arena)
            || line_has_interior_split_points(&lines[ni], arena)
        {
            return false;
        }

        let to_merge = vec![lines[i].clone(), lines[ni].clone()];
        let merged = match self.try_create_merged_line(&to_merge, arena) {
            Ok(m) => m,
            Err(_) => return false,
        };

        let merged_line = match merged.into_iter().find(|l| !l.is_blank_line(arena)) {
            Some(l) => l,
            None => return false,
        };

        lines[ni] = merged_line;
        remove[i] = true;
        // Remove blank lines between comma and next content
        for j in (i + 1)..ni {
            if lines[j].is_blank_line(arena) {
                remove[j] = true;
            }
        }
        true
    }

    /// Cheap pre-check: can these lines merge without exceeding max_length?
    /// Uses fast arithmetic length check, then validates with extract_components.
    fn can_merge_lines(&self, lines: &[Line], arena: &[Node]) -> bool {
        if lines.len() <= 1 {
            return true;
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
            return true;
        }

        // extract_components with length tracking handles both validation and length check
        Self::extract_components(content_lines, arena).is_ok()
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
        merged_line.comments = std::rc::Rc::new(comments);

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

    /// Combined can_merge + create_merged_line in a single extract_components call.
    /// Does arithmetic length check first, then constructs the merged line.
    /// This avoids the redundant second extract_components call.
    fn try_create_merged_line(
        &self,
        lines: &[Line],
        arena: &[Node],
    ) -> Result<Vec<Line>, ControlFlow> {
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

        // Arithmetic length check before constructing merged line
        let indent_size = content_lines[0].indent_size(arena);
        let has_multiline_node = nodes.iter().any(|&idx| {
            let node = &arena[idx];
            !node.is_newline() && node.value.contains('\n')
        });

        if !has_multiline_node {
            let mut length = indent_size;
            let mut first_content = true;
            for &idx in &nodes {
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
            if length > self.max_length {
                return Err(ControlFlow::CannotMerge);
            }
        }

        // Construct the merged line (reusing already-extracted components)
        let mut merged_line = Line::new(content_lines[0].previous_node);
        for &idx in &nodes {
            merged_line.append_node(idx);
        }
        merged_line.comments = std::rc::Rc::new(comments);

        // Final check with actual Line::len for multiline cases
        if has_multiline_node && merged_line.len(arena) > self.max_length {
            return Err(ControlFlow::CannotMerge);
        }

        let mut result = lines[..content_start].to_vec();
        result.push(merged_line);
        result.extend_from_slice(&lines[content_end..]);
        Ok(result)
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

        let estimated_nodes: usize = lines.iter().map(|l| l.nodes.len()).sum();
        let estimated_comments: usize = lines.iter().map(|l| l.comments.len()).sum();
        let mut nodes = Vec::with_capacity(estimated_nodes);
        let mut comments = Vec::with_capacity(estimated_comments);
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

            // Leading-comma style: never merge across comma-starting lines.
            // Must check BEFORE setting first_content_seen so that the first
            // content line of a segment (e.g., `, sum(`) is not blocked.
            if is_content && first_content_seen && line.starts_with_comma(arena) {
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

                if node.formatting_disabled {
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

        // Compute split points (indices where a new run starts)
        let mut split_points = Vec::new();
        for (i, seg) in segments.iter().enumerate().skip(1) {
            if !self.segment_continues_operator_sequence(seg, precedence, arena) {
                split_points.push(i);
            }
        }

        // If no splits, the entire vec is one run
        if split_points.is_empty() {
            return self.try_merge_operator_segments(segments, remaining_tiers, arena);
        }

        // Split segments into runs and merge each
        let mut new_segments: Vec<Segment> = Vec::new();
        let mut remaining = segments;
        let mut offset = 0;
        for &split in &split_points {
            let idx = split - offset;
            let tail = remaining.split_off(idx);
            new_segments.extend(self.try_merge_operator_segments(
                remaining,
                remaining_tiers,
                arena,
            ));
            remaining = tail;
            offset = split;
        }
        new_segments.extend(self.try_merge_operator_segments(remaining, remaining_tiers, arena));

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

        // LATERAL: with leading commas, the comma is on the LATERAL line itself
        // (e.g., `, lateral flatten(...)`) so check if the head line starts with comma
        // and the second content node is LATERAL, or if the first content is LATERAL
        // and the previous segment's tail ends with comma.
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
            return self.can_merge_lines(&segment.lines, arena);
        }
        // Leading-comma style: `, lateral` — first content is comma, second is lateral
        if first.is_comma() {
            if let Some(second) = line.second_content_node(arena) {
                if second.token.token_type == crate::token::TokenType::UntermKeyword
                    && second.value.eq_ignore_ascii_case("lateral")
                {
                    return self.can_merge_lines(&segment.lines, arena);
                }
            }
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
            // blank segment, keep scanning
            Err(_) => true,
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
        segments: Vec<Segment>,
        op_tiers: &[OperatorPrecedence],
        arena: &[Node],
    ) -> Vec<Segment> {
        if segments.len() <= 1 {
            return segments;
        }

        let total_lines: usize = segments.iter().map(|s| s.lines.len()).sum();
        let mut all_lines = Vec::with_capacity(total_lines);
        for s in &segments {
            all_lines.extend_from_slice(&s.lines);
        }
        match self.try_create_merged_line(&all_lines, arena) {
            Ok(merged) => vec![Segment::new(merged)],
            Err(_) => self.maybe_merge_operators(segments, op_tiers, arena),
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

        let head_idx = match segment.head(arena) {
            Ok((head_idx, _)) => head_idx,
            Err(_) => {
                prev_segments.push(prev_segment);
                prev_segments.push(segment);
                return prev_segments;
            }
        };

        // Attempt 1: merge all prev_segment lines + head line.
        // Temporarily push onto prev_segment to avoid cloning all its lines.
        prev_segment.lines.push(segment.lines[head_idx].clone());
        let attempt1 = self.try_create_merged_line(&prev_segment.lines, arena);
        prev_segment.lines.pop();
        if let Ok(merged) = attempt1 {
            let mut result_seg = Segment::new(merged);
            result_seg
                .lines
                .extend_from_slice(&segment.lines[head_idx + 1..]);
            prev_segments.push(result_seg);
            return prev_segments;
        }

        if let Some(result_seg) =
            self.try_merge_via_prev_tail(&prev_segment, &segment, head_idx, arena)
        {
            prev_segments.push(result_seg);
            return prev_segments;
        }

        prev_segments.push(prev_segment);
        prev_segments.push(segment);
        prev_segments
    }

    /// Try merging via the previous segment's tail line.
    /// Attempt 2: merge tail + all segment lines.
    /// Attempt 3: merge tail + head line only.
    /// Returns the merged segment on success, or `None` if both attempts fail.
    fn try_merge_via_prev_tail(
        &self,
        prev_segment: &Segment,
        segment: &Segment,
        head_idx: usize,
        arena: &[Node],
    ) -> Option<Segment> {
        let (tail_idx, tail_line) = prev_segment.tail(arena).ok()?;

        // Attempt 2: merge tail + all segment lines
        let mut try_lines = Vec::with_capacity(1 + segment.lines.len());
        try_lines.push(tail_line.clone());
        try_lines.extend_from_slice(&segment.lines);
        if let Ok(merged) = self.try_create_merged_line(&try_lines, arena) {
            let tail_start = prev_segment.lines.len() - 1 - tail_idx;
            let mut result_seg = Segment::new(prev_segment.lines[..tail_start].to_vec());
            result_seg.lines.extend(merged);
            return Some(result_seg);
        }

        // Attempt 3: merge tail + head line only
        let try_lines = [tail_line.clone(), segment.lines[head_idx].clone()];
        if let Ok(merged) = self.try_create_merged_line(&try_lines, arena) {
            let tail_start = prev_segment.lines.len() - 1 - tail_idx;
            let mut result_seg = Segment::new(prev_segment.lines[..tail_start].to_vec());
            result_seg.lines.extend(merged);
            result_seg
                .lines
                .extend_from_slice(&segment.lines[head_idx + 1..]);
            return Some(result_seg);
        }

        None
    }
}

/// Check if a line has interior nodes that would cause the splitter to re-split
/// if the line were prepended with a comma. This prevents fix_standalone_commas
/// from creating non-idempotent output.
fn line_has_interior_split_points(line: &Line, arena: &[Node]) -> bool {
    let mut first_content = true;
    for &idx in &line.nodes {
        let node = &arena[idx];
        if node.is_newline() {
            continue;
        }
        if first_content {
            first_content = false;
            continue;
        }
        // These would cause the splitter to split before them
        if node.is_operator(arena) && !node.is_bracket_operator(arena) {
            return true;
        }
        if node.is_boolean_operator() {
            return true;
        }
        if node.is_unterm_keyword() {
            return true;
        }
    }
    false
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
#[path = "merger_test.rs"]
mod tests;
