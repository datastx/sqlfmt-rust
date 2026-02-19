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
        for line in &self.lines {
            result.push_str(&line.render_with_comments(arena, self.line_length));
        }
        result
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
