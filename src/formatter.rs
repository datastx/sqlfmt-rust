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
pub struct QueryFormatter {
    line_length: usize,
    no_jinjafmt: bool,
}

impl QueryFormatter {
    pub fn new(line_length: usize, no_jinjafmt: bool) -> Self {
        Self {
            line_length,
            no_jinjafmt,
        }
    }

    /// Run the full formatting pipeline on a query.
    pub fn format(&self, query: &mut Query, arena: &mut Vec<Node>) {
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
        query.lines = merger.maybe_merge_lines(&query.lines, arena);
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
        String::new(),
        "\n".to_string(),
        prev_idx
            .map(|i| arena[i].open_brackets.clone())
            .unwrap_or_default(),
        prev_idx
            .map(|i| arena[i].open_jinja_blocks.clone())
            .unwrap_or_default(),
    );
    let nl_idx = arena.len();
    arena.push(nl_node);
    line1.append_node(nl_idx);
    line1.formatting_disabled = line.formatting_disabled.clone();

    let mut line2 = Line::new(prev_idx);
    for &idx in &line.nodes[split_pos..] {
        line2.append_node(idx);
    }
    line2.formatting_disabled = line.formatting_disabled;

    for comment in line.comments {
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
        while arena[node_idx].open_brackets.len() > target_depth {
            arena[node_idx].open_brackets.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::Analyzer;
    use crate::node_manager::NodeManager;
    use crate::rules;

    fn format_sql(source: &str) -> (Query, Vec<Node>) {
        let rules = rules::main_rules();
        let nm = NodeManager::new(false);
        let mut analyzer = Analyzer::new(rules, nm, 88);
        let mut query = analyzer.parse_query(source).unwrap();
        let mut arena = std::mem::take(&mut analyzer.arena);

        let formatter = QueryFormatter::new(88, false);
        formatter.format(&mut query, &mut arena);

        (query, arena)
    }

    #[test]
    fn test_format_simple_select() {
        let (query, arena) = format_sql("SELECT 1\n");
        let rendered = query.render(&arena);
        assert!(rendered.contains("select"));
        assert!(rendered.contains("1"));
    }

    #[test]
    fn test_format_preserves_content() {
        let (query, arena) = format_sql("SELECT a, b, c FROM my_table\n");
        let rendered = query.render(&arena);
        assert!(rendered.contains("select"));
        assert!(rendered.contains("a"));
        assert!(rendered.contains("b"));
        assert!(rendered.contains("c"));
        assert!(rendered.contains("from"));
        assert!(rendered.contains("my_table"));
    }

    #[test]
    fn test_format_removes_extra_blank_lines() {
        let (query, arena) = format_sql("SELECT 1\n\n\n\n\nSELECT 2\n");
        let rendered = query.render(&arena);
        // Should not have more than 2 consecutive blank lines at root
        assert!(!rendered.contains("\n\n\n\n"));
    }

    #[test]
    fn test_format_splits_long_line() {
        let (query, arena) = format_sql(
            "SELECT a_very_long_field_name, another_very_long_field_name, yet_another_long_field_name, and_one_more_field FROM my_table\n",
        );
        let rendered = query.render(&arena);
        // Should be split into multiple lines since it's too long
        let line_count = rendered.lines().count();
        assert!(
            line_count > 1,
            "Long line should be split: got {} lines from: {}",
            line_count,
            rendered
        );
    }

    #[test]
    fn test_format_merges_short_lines() {
        // When lines are short enough, the merger should combine them
        let (query, arena) = format_sql("SELECT 1\n");
        let rendered = query.render(&arena);
        // "select 1" fits on one line at 88 chars
        assert!(rendered.trim().lines().count() <= 2);
    }

    #[test]
    fn test_format_case_expression() {
        let (query, arena) = format_sql(
            "SELECT CASE WHEN x = 1 THEN 'a' WHEN x = 2 THEN 'b' ELSE 'c' END AS result FROM t\n",
        );
        let rendered = query.render(&arena);
        assert!(rendered.contains("case"));
        assert!(rendered.contains("when"));
        assert!(rendered.contains("then"));
        assert!(rendered.contains("else"));
        assert!(rendered.contains("end"));
    }

    #[test]
    fn test_format_join_query() {
        let (query, arena) =
            format_sql("SELECT a.id, b.name FROM table_a a LEFT JOIN table_b b ON a.id = b.a_id\n");
        let rendered = query.render(&arena);
        assert!(rendered.contains("left join"));
        assert!(rendered.contains("on"));
    }

    #[test]
    fn test_format_with_comments() {
        let (query, arena) = format_sql("-- comment\nSELECT 1\n");
        let rendered = query.render(&arena);
        assert!(rendered.contains("select") || rendered.contains("1"));
    }

    #[test]
    fn test_format_idempotent() {
        let source = "SELECT a, b, c FROM my_table WHERE x = 1 ORDER BY a\n";
        let (query1, arena1) = format_sql(source);
        let rendered1 = query1.render(&arena1);

        let (query2, arena2) = format_sql(&rendered1);
        let rendered2 = query2.render(&arena2);

        assert_eq!(rendered1, rendered2, "Formatting should be idempotent");
    }

    #[test]
    fn test_format_removes_trailing_blank_lines() {
        let (query, arena) = format_sql("SELECT 1\n\n\n");
        let rendered = query.render(&arena);
        // Should not end with multiple blank lines
        assert!(
            !rendered.ends_with("\n\n\n"),
            "Should remove trailing blanks: {:?}",
            rendered
        );
    }

    #[test]
    fn test_format_cte_query() {
        let (query, arena) =
            format_sql("WITH cte AS (SELECT 1 AS id, 'hello' AS name) SELECT * FROM cte\n");
        let rendered = query.render(&arena);
        assert!(rendered.contains("with"));
        assert!(rendered.contains("as"));
        assert!(rendered.contains("from"));
    }

    #[test]
    fn test_format_subquery() {
        let (query, arena) =
            format_sql("SELECT * FROM (SELECT id FROM users WHERE active = true) sub\n");
        let rendered = query.render(&arena);
        assert!(rendered.contains("select"));
        assert!(rendered.contains("from"));
    }

    #[test]
    fn test_format_jinja_block() {
        let (query, arena) = format_sql("{% if flag %}\nSELECT 1\n{% endif %}\n");
        let rendered = query.render(&arena);
        assert!(rendered.contains("{% if flag %}"));
        assert!(rendered.contains("{% endif %}"));
    }
}
