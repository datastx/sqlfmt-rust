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
    fn format_jinja(&self, query: &mut Query, arena: &mut Vec<Node>) {
        let formatter = JinjaFormatter::new(self.line_length);
        for line in &mut query.lines {
            formatter.format_line(line, arena);
        }
    }

    /// Stage 3: Adjust indentation of Jinja block start/end to match
    /// the least-indented content inside the block.
    fn dedent_jinja_blocks(&self, query: &mut Query, arena: &[Node]) {
        // Jinja block dedenting: scan for jinja blocks and adjust the depth
        // of the block start/end lines to match the minimum depth inside.
        // This is important for proper indentation of {% if %}/{% endif %} blocks.
        let lines = &mut query.lines;
        let len = lines.len();
        if len == 0 {
            return;
        }

        // Find jinja block start/end pairs and adjust depth
        let mut i = 0;
        while i < lines.len() {
            let line = &lines[i];
            // Check if this line starts with a jinja block start
            if let Some(first) = line.first_content_node(arena) {
                if first.is_opening_jinja_block() && first.token.token_type == crate::token::TokenType::JinjaBlockStart {
                    // Find the matching end
                    let start_depth = line.depth(arena);
                    let mut j = i + 1;
                    let mut min_depth = (usize::MAX, usize::MAX);
                    while j < lines.len() {
                        let inner = &lines[j];
                        if !inner.is_blank_line(arena) {
                            let d = inner.depth(arena);
                            // Check if this is the closing block
                            if let Some(fc) = inner.first_content_node(arena) {
                                if fc.is_closing_jinja_block() && d.1 <= start_depth.1 {
                                    break;
                                }
                            }
                            if d.0 < min_depth.0 {
                                min_depth.0 = d.0;
                            }
                            if d.1 < min_depth.1 {
                                min_depth.1 = d.1;
                            }
                        }
                        j += 1;
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
    fn remove_extra_blank_lines(&self, query: &mut Query, arena: &[Node]) {
        let mut new_lines: Vec<Line> = Vec::new();
        let mut consecutive_blanks = 0;

        for line in &query.lines {
            if line.is_blank_line(arena) {
                consecutive_blanks += 1;
                let depth = line.depth(arena);
                let max_blanks = if depth == (0, 0) { 2 } else { 1 };
                if consecutive_blanks <= max_blanks {
                    new_lines.push(line.clone());
                }
            } else {
                consecutive_blanks = 0;
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
}
