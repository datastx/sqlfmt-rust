use crate::line::Line;
use crate::node::Node;

/// A Query holds the source string, formatting configuration,
/// and the lines produced by the lexer/formatter.
#[derive(Debug, Clone)]
pub struct Query {
    pub source_string: String,
    pub line_length: usize,
    pub lines: Vec<Line>,
}

impl Query {
    pub fn new(source_string: String, line_length: usize, lines: Vec<Line>) -> Self {
        Self {
            source_string,
            line_length,
            lines,
        }
    }

    /// Render the full formatted output.
    pub fn render(&self, arena: &[Node]) -> String {
        let mut result = String::new();
        for (i, line) in self.lines.iter().enumerate() {
            // For standalone comment-only lines, use the depth of the next
            // content line for indentation, since comments should be indented
            // to match what follows them, not what precedes them.
            let indent_override = if line.is_standalone_comment_line(arena) {
                self.next_content_indent(i, arena)
            } else {
                None
            };
            result.push_str(&line.render_with_comments(
                arena,
                self.line_length,
                indent_override.as_deref(),
            ));
        }
        result
    }

    /// Find the indentation of the next non-blank, non-comment-only line.
    fn next_content_indent(&self, from: usize, arena: &[Node]) -> Option<String> {
        for j in (from + 1)..self.lines.len() {
            let next = &self.lines[j];
            if next.is_blank_line(arena) || next.is_standalone_comment_line(arena) {
                continue;
            }
            // For formatting-disabled lines, extract indentation from original
            // token prefix (since depth is always 0 for these lines)
            if next.has_formatting_disabled() {
                if let Some(&first_idx) = next.nodes.iter().find(|&&idx| !arena[idx].is_newline())
                {
                    let prefix = &arena[first_idx].token.prefix;
                    return Some(prefix.clone());
                }
            }
            return Some(next.indentation(arena));
        }
        None
    }

    /// Collect all tokens from all lines (for equivalence checking).
    pub fn tokens<'a>(&'a self, arena: &'a [Node]) -> Vec<&'a Node> {
        let mut nodes = Vec::new();
        for line in &self.lines {
            for &idx in &line.nodes {
                nodes.push(&arena[idx]);
            }
        }
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_query() {
        let q = Query::new("".to_string(), 88, Vec::new());
        let arena: Vec<Node> = Vec::new();
        assert_eq!(q.render(&arena), "");
    }
}
