use crate::jinja_formatter::JinjaFormatter;
use crate::line::Line;
use crate::merger::LineMerger;
use crate::node::Node;
use crate::query::Query;
use crate::splitter::LineSplitter;

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
        // Stage 1: Split long lines
        self.split_lines(query, arena);

        // Stage 2: Format Jinja tags
        if !self.no_jinjafmt {
            self.format_jinja(query, arena);
        }

        // Stage 2b: Re-split lines that now contain multiline Jinja.
        // The JinjaFormatter may have made single-line expressions multiline,
        // so split before those multiline nodes.
        self.split_multiline_jinja(query, arena);

        // Stage 3: Dedent Jinja blocks
        self.dedent_jinja_blocks(query, arena);

        // Stage 4: Merge short lines
        self.merge_lines(query, arena);

        // Stage 5: Remove extra blank lines
        self.remove_extra_blank_lines(query, arena);
    }

    /// Stage 1: Split lines based on SQL structure.
    fn split_lines(&self, query: &mut Query, arena: &mut Vec<Node>) {
        let splitter = LineSplitter::new(self.line_length);
        let mut new_lines = Vec::new();
        for line in &query.lines {
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
        let mut new_lines = Vec::new();
        for line in &query.lines {
            if line.has_formatting_disabled() {
                new_lines.push(line.clone());
                continue;
            }
            // Check if a multiline Jinja node needs to be split from preceding content.
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
                    first_content_is_on =
                        node.token.token_type == crate::token::TokenType::On;
                }
                if node.is_multiline_jinja() {
                    multiline_count += 1;
                    has_multiline = true;
                    if first_multiline_pos.is_none() && content_count >= 2 {
                        first_multiline_pos = Some(pos);
                    }
                }
            }

            let mut needs_split = false;
            if has_multiline && content_count >= 2 {
                if multiline_count > 1 {
                    // Multiple multiline Jinja nodes on one line: always split
                    needs_split = true;
                } else if first_content_is_on {
                    // ON + multiline Jinja: never split (join conditions stay together)
                    needs_split = false;
                } else {
                    // Single multiline Jinja with other content: split only if too long
                    let line_len = line.len(arena);
                    if line_len > self.line_length {
                        needs_split = true;
                    }
                }
            }

            if needs_split {
                if let Some(split_pos) = first_multiline_pos {
                    // Split the line before the first multiline Jinja node.
                    let prev_idx = if split_pos > 0 {
                        Some(line.nodes[split_pos - 1])
                    } else {
                        line.previous_node
                    };

                    // Line 1: nodes before split_pos + newline
                    let mut line1 = Line::new(line.previous_node);
                    for &idx in &line.nodes[..split_pos] {
                        line1.append_node(idx);
                    }
                    // Add a newline node for line1 (inherit open_brackets/open_jinja_blocks)
                    let spos = prev_idx.map(|i| arena[i].token.epos).unwrap_or(0);
                    let nl_token = crate::token::Token::new(
                        crate::token::TokenType::Newline, "", "\n", spos, spos,
                    );
                    let nl_node = Node::new(
                        nl_token,
                        prev_idx,
                        String::new(),
                        "\n".to_string(),
                        prev_idx.map(|i| arena[i].open_brackets.clone()).unwrap_or_default(),
                        prev_idx.map(|i| arena[i].open_jinja_blocks.clone()).unwrap_or_default(),
                    );
                    let nl_idx = arena.len();
                    arena.push(nl_node);
                    line1.append_node(nl_idx);
                    line1.formatting_disabled = line.formatting_disabled.clone();

                    // Line 2: nodes from split_pos onwards (including original newline)
                    let mut line2 = Line::new(prev_idx);
                    for &idx in &line.nodes[split_pos..] {
                        line2.append_node(idx);
                    }
                    line2.formatting_disabled = line.formatting_disabled.clone();

                    // Distribute comments
                    for comment in &line.comments {
                        if comment.is_standalone {
                            line2.append_comment(comment.clone());
                        } else {
                            line1.append_comment(comment.clone());
                        }
                    }

                    new_lines.push(line1);
                    new_lines.push(line2);
                } else {
                    new_lines.push(line.clone());
                }
            } else {
                new_lines.push(line.clone());
            }
        }
        query.lines = new_lines;
    }

    /// Stage 3: Adjust indentation of Jinja block start/end to match
    /// the least-indented content inside the block.
    fn dedent_jinja_blocks(&self, query: &mut Query, arena: &mut [Node]) {
        // Jinja block dedenting: scan for jinja blocks and adjust the depth
        // of the block start/end lines to match the minimum depth inside.
        let lines = &mut query.lines;
        if lines.is_empty() {
            return;
        }

        let mut i = 0;
        while i < lines.len() {
            // Check if this line starts with a jinja block start
            let is_block_start = lines[i]
                .first_content_node(arena)
                .map(|n| {
                    n.is_opening_jinja_block()
                        && n.token.token_type == crate::token::TokenType::JinjaBlockStart
                })
                .unwrap_or(false);

            if is_block_start && !lines[i].has_formatting_disabled() {
                let start_depth = lines[i].depth(arena);
                let mut j = i + 1;
                let mut min_sql_depth = usize::MAX;
                let mut min_jinja_depth = usize::MAX;
                let mut end_j = None;

                while j < lines.len() {
                    if !lines[j].is_blank_line(arena) {
                        let d = lines[j].depth(arena);
                        let is_end = lines[j]
                            .first_content_node(arena)
                            .map(|fc| fc.is_closing_jinja_block() && d.1 <= start_depth.1)
                            .unwrap_or(false);
                        if is_end {
                            end_j = Some(j);
                            break;
                        }
                        if d.0 < min_sql_depth {
                            min_sql_depth = d.0;
                        }
                        if d.1 < min_jinja_depth {
                            min_jinja_depth = d.1;
                        }
                    }
                    j += 1;
                }

                // Apply dedent: adjust block start/end nodes to min_depth
                if min_sql_depth < usize::MAX && min_sql_depth < start_depth.0 {
                    // Adjust block start line
                    if let Some(node_idx) = lines[i].first_content_node_idx(arena) {
                        while arena[node_idx].open_brackets.len() > min_sql_depth {
                            arena[node_idx].open_brackets.pop();
                        }
                    }
                    // Adjust block end line
                    if let Some(ej) = end_j {
                        if let Some(node_idx) = lines[ej].first_content_node_idx(arena) {
                            while arena[node_idx].open_brackets.len() > min_sql_depth {
                                arena[node_idx].open_brackets.pop();
                            }
                        }
                    }
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
        let mut new_lines: Vec<Line> = Vec::new();
        let mut consecutive_blanks = 0;
        let mut after_standalone_comment = false;

        for line in &query.lines {
            if line.is_blank_line(arena) {
                if after_standalone_comment && !line.has_formatting_disabled() {
                    // Skip blank lines immediately after standalone comment lines
                    continue;
                }
                // Preserve blank lines in formatting-disabled regions
                if line.has_formatting_disabled() {
                    consecutive_blanks = 0;
                    new_lines.push(line.clone());
                } else {
                    consecutive_blanks += 1;
                    let depth = line.depth(arena);
                    let max_blanks = if depth == (0, 0) { 2 } else { 1 };
                    if consecutive_blanks <= max_blanks {
                        new_lines.push(line.clone());
                    }
                }
            } else {
                consecutive_blanks = 0;
                after_standalone_comment = line.is_standalone_comment_line(arena);
                new_lines.push(line.clone());
            }
        }

        // Remove trailing blank lines
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
