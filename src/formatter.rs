use compact_str::CompactString;

use crate::jinja_formatter::JinjaFormatter;
use crate::line::Line;
use crate::merger::LineMerger;
use crate::node::Node;
use crate::query::Query;
use crate::splitter::LineSplitter;
use crate::token::TokenType;

/// QueryFormatter runs the 5-stage formatting pipeline:
///   1. Split long lines
///   2. Format Jinja tags
///   3. Dedent Jinja blocks
///   4. Merge short lines
///   5. Remove extra blank lines
pub(crate) struct QueryFormatter {
    line_length: usize,
    no_jinjafmt: bool,
}

impl QueryFormatter {
    pub(crate) fn new(line_length: usize, no_jinjafmt: bool) -> Self {
        Self {
            line_length,
            no_jinjafmt,
        }
    }

    /// Run the full formatting pipeline on a query.
    pub(crate) fn format(&self, query: &mut Query, arena: &mut Vec<Node>) {
        self.split_lines(query, arena);

        if !self.no_jinjafmt {
            self.format_jinja(query, arena);
        }

        // Stage 2b: Re-split lines that now contain multiline Jinja.
        // The JinjaFormatter may have made single-line expressions multiline,
        // so split before those multiline nodes.
        self.split_multiline_jinja(query, arena);

        self.dedent_jinja_blocks(query, arena);

        self.merge_lines(query, arena);

        self.remove_extra_blank_lines(query, arena);
    }

    /// Stage 1: Split lines based on SQL structure.
    fn split_lines(&self, query: &mut Query, arena: &mut Vec<Node>) {
        let splitter = LineSplitter::new();
        let old_lines = std::mem::take(&mut query.lines);
        let mut new_lines = Vec::with_capacity(old_lines.len() * 2);
        for line in old_lines {
            new_lines.extend(splitter.maybe_split(line, arena));
        }
        query.lines = new_lines;
    }

    /// Stage 2: Format Jinja templates.
    fn format_jinja(&self, query: &mut Query, arena: &mut [Node]) {
        let formatter = JinjaFormatter::new(self.line_length);
        for line in &mut query.lines {
            if !line.has_formatting_disabled() {
                formatter.format_line(line, arena);
            }
        }
    }

    /// Stage 2b: Re-split lines where the JinjaFormatter created multiline nodes.
    /// After jinja formatting, some nodes may have become multiline. We only split
    /// when the resulting line exceeds the line length limit. This handles cases like
    /// `= {{ short_multiline }}` (stays together if it fits) vs
    /// `= {{ very_long_multiline }}` (gets split because it exceeds the limit).
    /// Lines starting with ON + multiline Jinja are never split (join conditions).
    fn split_multiline_jinja(&self, query: &mut Query, arena: &mut Vec<Node>) {
        let old_lines = std::mem::take(&mut query.lines);
        let mut new_lines = Vec::with_capacity(old_lines.len());
        for line in old_lines {
            if line.has_formatting_disabled() {
                new_lines.push(line);
                continue;
            }

            let analysis = analyze_multiline_jinja(&line, arena);

            if !analysis.needs_split(self.line_length, &line, arena) {
                new_lines.push(line);
                continue;
            }

            if let Some(split_pos) = analysis.first_multiline_pos {
                let (line1, line2) = split_line_at_jinja(line, split_pos, arena);
                new_lines.push(line1);
                new_lines.push(line2);
            } else {
                new_lines.push(line);
            }
        }
        query.lines = new_lines;
    }

    /// Stage 3: Adjust indentation of Jinja block start/end to match
    /// the least-indented content inside the block.
    fn dedent_jinja_blocks(&self, query: &mut Query, arena: &mut [Node]) {
        let lines = &mut query.lines;
        if lines.is_empty() {
            return;
        }

        let mut i = 0;
        while i < lines.len() {
            if !is_jinja_block_start_line(&lines[i], arena) || lines[i].has_formatting_disabled() {
                i += 1;
                continue;
            }

            let start_depth = lines[i].depth(arena);
            let (end_j, min_sql_depth, _min_jinja_depth) =
                find_jinja_block_end(lines, i, start_depth, arena);

            if min_sql_depth < usize::MAX && min_sql_depth < start_depth.0 {
                adjust_bracket_depth(&lines[i], min_sql_depth, arena);
                if let Some(ej) = end_j {
                    adjust_bracket_depth(&lines[ej], min_sql_depth, arena);
                }
            }
            i += 1;
        }
    }

    /// Stage 4: Merge short lines back together.
    fn merge_lines(&self, query: &mut Query, arena: &[Node]) {
        let merger = LineMerger::new(self.line_length);
        let lines = std::mem::take(&mut query.lines);
        query.lines = merger.maybe_merge_lines(lines, arena);
    }

    /// Stage 5: Remove extra blank lines.
    /// At depth (0,0): max 2 consecutive blank lines.
    /// At any other depth: max 1 consecutive blank line.
    /// Also removes blank lines immediately after standalone comment lines
    /// (Python sqlfmt hoists comments to attach directly to the next statement).
    fn remove_extra_blank_lines(&self, query: &mut Query, arena: &[Node]) {
        let old_lines = std::mem::take(&mut query.lines);
        let mut new_lines: Vec<Line> = Vec::with_capacity(old_lines.len());
        let mut consecutive_blanks = 0;
        let mut after_standalone_comment = false;

        for line in old_lines {
            if line.is_blank_line(arena) {
                if after_standalone_comment && !line.has_formatting_disabled() {
                    continue;
                }
                // Preserve blank lines in formatting-disabled regions
                if line.has_formatting_disabled() {
                    consecutive_blanks = 0;
                    new_lines.push(line);
                } else {
                    consecutive_blanks += 1;
                    let depth = line.depth(arena);
                    let max_blanks = if depth == (0, 0) { 2 } else { 1 };
                    if consecutive_blanks <= max_blanks {
                        new_lines.push(line);
                    }
                }
            } else {
                consecutive_blanks = 0;
                after_standalone_comment = line.is_standalone_comment_line(arena);
                new_lines.push(line);
            }
        }

        while new_lines
            .last()
            .map(|l| l.is_blank_line(arena))
            .unwrap_or(false)
        {
            new_lines.pop();
        }

        query.lines = new_lines;
    }
}

/// Result of analyzing a line for multiline Jinja content.
struct MultilineJinjaAnalysis {
    content_count: usize,
    multiline_count: usize,
    first_content_is_on: bool,
    has_multiline: bool,
    first_multiline_pos: Option<usize>,
}

impl MultilineJinjaAnalysis {
    fn needs_split(&self, max_length: usize, line: &Line, arena: &[Node]) -> bool {
        if !self.has_multiline || self.content_count < 2 {
            return false;
        }
        if self.multiline_count > 1 {
            return true;
        }
        if self.first_content_is_on {
            return false;
        }
        line.len(arena) > max_length
    }
}

/// Analyze a line for multiline Jinja nodes, computing counts and split position.
fn analyze_multiline_jinja(line: &Line, arena: &[Node]) -> MultilineJinjaAnalysis {
    let mut content_count = 0;
    let mut multiline_count = 0;
    let mut first_content_is_on = false;
    let mut has_multiline = false;
    let mut first_multiline_pos: Option<usize> = None;

    for (pos, &idx) in line.nodes.iter().enumerate() {
        let node = &arena[idx];
        if node.is_newline() {
            continue;
        }
        content_count += 1;
        if content_count == 1 {
            first_content_is_on = node.token.token_type == TokenType::On;
        }
        if node.is_multiline_jinja() {
            multiline_count += 1;
            has_multiline = true;
            if first_multiline_pos.is_none() && content_count >= 2 {
                first_multiline_pos = Some(pos);
            }
        }
    }

    MultilineJinjaAnalysis {
        content_count,
        multiline_count,
        first_content_is_on,
        has_multiline,
        first_multiline_pos,
    }
}

/// Split a line at the given position, creating two lines with a newline node between them.
fn split_line_at_jinja(line: Line, split_pos: usize, arena: &mut Vec<Node>) -> (Line, Line) {
    let prev_idx = if split_pos > 0 {
        Some(line.nodes[split_pos - 1])
    } else {
        line.previous_node
    };

    let mut line1 = Line::new(line.previous_node);
    for &idx in &line.nodes[..split_pos] {
        line1.append_node(idx);
    }

    let spos = prev_idx.map(|i| arena[i].token.epos).unwrap_or(0);
    let nl_token = crate::token::Token::new(TokenType::Newline, "", "\n", spos, spos);
    let nl_node = Node::new(
        nl_token,
        prev_idx,
        CompactString::new(""),
        CompactString::from("\n"),
        prev_idx.map(|i| arena[i].bracket_depth).unwrap_or(0),
        prev_idx.map(|i| arena[i].jinja_depth).unwrap_or(0),
    );
    let nl_idx = arena.len();
    arena.push(nl_node);
    line1.append_node(nl_idx);
    line1.formatting_disabled = line.formatting_disabled;

    let mut line2 = Line::new(prev_idx);
    for &idx in &line.nodes[split_pos..] {
        line2.append_node(idx);
    }
    line2.formatting_disabled = line.formatting_disabled;

    let comments = std::rc::Rc::try_unwrap(line.comments).unwrap_or_else(|rc| (*rc).clone());
    for comment in comments {
        if comment.is_standalone {
            line2.append_comment(comment);
        } else {
            line1.append_comment(comment);
        }
    }

    (line1, line2)
}

/// Check if a line starts with a Jinja block start tag.
fn is_jinja_block_start_line(line: &Line, arena: &[Node]) -> bool {
    line.first_content_node(arena)
        .map(|n| n.is_opening_jinja_block() && n.token.token_type == TokenType::JinjaBlockStart)
        .unwrap_or(false)
}

/// Scan forward from a jinja block start to find its end, tracking minimum depths.
/// Returns (end_line_index, min_sql_depth, min_jinja_depth).
fn find_jinja_block_end(
    lines: &[Line],
    start: usize,
    start_depth: (usize, usize),
    arena: &[Node],
) -> (Option<usize>, usize, usize) {
    let mut min_sql_depth = usize::MAX;
    let mut min_jinja_depth = usize::MAX;

    for (j, line) in lines.iter().enumerate().skip(start + 1) {
        if line.is_blank_line(arena) {
            continue;
        }
        let d = line.depth(arena);
        let is_end = line
            .first_content_node(arena)
            .map(|fc| fc.is_closing_jinja_block() && d.1 <= start_depth.1)
            .unwrap_or(false);
        if is_end {
            return (Some(j), min_sql_depth, min_jinja_depth);
        }
        min_sql_depth = min_sql_depth.min(d.0);
        min_jinja_depth = min_jinja_depth.min(d.1);
    }

    (None, min_sql_depth, min_jinja_depth)
}

/// Adjust bracket depth of a line's first content node to a target depth.
fn adjust_bracket_depth(line: &Line, target_depth: usize, arena: &mut [Node]) {
    if let Some(node_idx) = line.first_content_node_idx(arena) {
        if (arena[node_idx].bracket_depth as usize) > target_depth {
            arena[node_idx].bracket_depth = target_depth as u16;
        }
    }
}

#[cfg(test)]
#[path = "formatter_test.rs"]
mod tests;
