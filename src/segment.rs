use crate::error::SqlfmtError;
use crate::line::Line;
use crate::node::Node;

/// A Segment is a group of consecutive Lines used by the merger.
/// Lines in the same segment share the same base indentation level.
#[derive(Debug, Clone)]
pub struct Segment {
    pub lines: Vec<Line>,
}

impl Segment {
    pub fn new(lines: Vec<Line>) -> Self {
        Self { lines }
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// First non-blank line and its index.
    pub fn head(&self, arena: &[Node]) -> Result<(usize, &Line), SqlfmtError> {
        for (i, line) in self.lines.iter().enumerate() {
            if !line.is_blank_line(arena) {
                return Ok((i, line));
            }
        }
        Err(SqlfmtError::Segment(
            "All lines in segment are empty".into(),
        ))
    }

    /// Last non-blank line and its index.
    pub fn tail(&self, arena: &[Node]) -> Result<(usize, &Line), SqlfmtError> {
        for (i, line) in self.lines.iter().enumerate().rev() {
            if !line.is_blank_line(arena) {
                return Ok((i, line));
            }
        }
        Err(SqlfmtError::Segment(
            "All lines in segment are empty".into(),
        ))
    }

    /// True if the tail line closes a bracket opened by the head line.
    pub fn tail_closes_head(&self, arena: &[Node]) -> bool {
        let head = match self.head(arena) {
            Ok((_, h)) => h,
            Err(_) => return false,
        };
        let tail = match self.tail(arena) {
            Ok((_, t)) => t,
            Err(_) => return false,
        };

        head.ends_with_opening_bracket(arena) && tail.closes_bracket_from_previous_line(arena)
    }

    /// Split the segment at the given line index: lines[..=idx] and lines[idx+1..].
    pub fn split_after(&self, idx: usize) -> (Segment, Segment) {
        let left = Segment::new(self.lines[..=idx].to_vec());
        let right = Segment::new(self.lines[idx + 1..].to_vec());
        (left, right)
    }
}

/// Build segments from a flat list of lines.
/// A new segment starts when `line.starts_new_segment()` is true.
pub fn build_segments(lines: &[Line], arena: &[Node]) -> Vec<Segment> {
    if lines.is_empty() {
        return Vec::new();
    }

    let mut segments: Vec<Segment> = Vec::new();
    let mut current_lines: Vec<Line> = Vec::new();

    for line in lines {
        if !current_lines.is_empty() && line.starts_new_segment(arena) {
            segments.push(Segment::new(std::mem::take(&mut current_lines)));
        }
        current_lines.push(line.clone());
    }

    if !current_lines.is_empty() {
        segments.push(Segment::new(current_lines));
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use crate::token::{Token, TokenType};

    fn make_node(arena: &mut Vec<Node>, tt: TokenType, val: &str) -> usize {
        let idx = arena.len();
        arena.push(Node::new(
            Token::new(tt, "", val, 0, val.len()),
            if idx > 0 { Some(idx - 1) } else { None },
            String::new(),
            val.to_string(),
            Vec::new(),
            Vec::new(),
        ));
        idx
    }

    fn make_line(arena: &mut Vec<Node>, tt: TokenType, val: &str) -> Line {
        let idx = make_node(arena, tt, val);
        let nl = make_node(arena, TokenType::Newline, "\n");
        let mut line = Line::new(None);
        line.nodes.push(idx);
        line.nodes.push(nl);
        line
    }

    #[test]
    fn test_segment_head_tail() {
        let mut arena = Vec::new();
        let line1 = make_line(&mut arena, TokenType::Name, "a");
        let line2 = make_line(&mut arena, TokenType::Name, "b");

        let seg = Segment::new(vec![line1, line2]);
        let (head_idx, _) = seg.head(&arena).unwrap();
        let (tail_idx, _) = seg.tail(&arena).unwrap();
        assert_eq!(head_idx, 0);
        assert_eq!(tail_idx, 1);
    }

    #[test]
    fn test_segment_split_after() {
        let mut arena = Vec::new();
        let line1 = make_line(&mut arena, TokenType::Name, "a");
        let line2 = make_line(&mut arena, TokenType::Name, "b");
        let line3 = make_line(&mut arena, TokenType::Name, "c");

        let seg = Segment::new(vec![line1, line2, line3]);
        let (left, right) = seg.split_after(0);
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 2);
    }
}
