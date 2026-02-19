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

    /// Last non-blank line and its index (from the bottom).
    pub fn tail(&self, arena: &[Node]) -> Result<(usize, &Line), SqlfmtError> {
        for (i, line) in self.lines.iter().enumerate().rev() {
            if !line.is_blank_line(arena) {
                let from_bottom = self.lines.len() - 1 - i;
                return Ok((from_bottom, line));
            }
        }
        Err(SqlfmtError::Segment(
            "All lines in segment are empty".into(),
        ))
    }

    /// True if the tail line closes a bracket or simple jinja block
    /// opened by the head line.
    pub fn tail_closes_head(&self, arena: &[Node]) -> bool {
        if self.lines.len() <= 1 {
            return false;
        }

        let (head_idx, head) = match self.head(arena) {
            Ok(h) => h,
            Err(_) => return false,
        };
        let (tail_from_bottom, tail) = match self.tail(arena) {
            Ok(t) => t,
            Err(_) => return false,
        };

        let tail_idx = self.lines.len() - 1 - tail_from_bottom;
        if head_idx == tail_idx {
            return false;
        }

        let head_depth = head.depth(arena);
        let tail_depth = tail.depth(arena);

        if tail_depth != head_depth {
            return false;
        }

        let between = &self.lines[head_idx + 1..tail_idx];

        // Bracket closing
        if tail.closes_bracket_from_previous_line(arena)
            && between.iter().all(|l| l.depth(arena).0 > head_depth.0)
        {
            return true;
        }

        // Jinja block closing
        if tail.closes_simple_jinja_block(arena)
            && between.iter().all(|l| l.depth(arena).1 > head_depth.1)
        {
            return true;
        }

        false
    }

    /// Split the segment after the given line index.
    pub fn split_after(&self, idx: usize, arena: &[Node]) -> Vec<Segment> {
        if self.tail_closes_head(arena) {
            let (tail_from_bottom, _) = match self.tail(arena) {
                Ok(t) => t,
                Err(_) => return vec![Segment::new(self.lines[idx + 1..].to_vec())],
            };
            let tail_start = self.lines.len() - 1 - tail_from_bottom;
            if idx + 1 < tail_start {
                vec![
                    Segment::new(self.lines[idx + 1..tail_start].to_vec()),
                    Segment::new(self.lines[tail_start..].to_vec()),
                ]
            } else {
                vec![Segment::new(self.lines[idx + 1..].to_vec())]
            }
        } else {
            vec![Segment::new(self.lines[idx + 1..].to_vec())]
        }
    }
}

/// Build segments from a flat list of lines.
/// Mirrors Python's `create_segments_from_lines`:
/// A segment is a list of consecutive lines that are indented from the first line.
pub fn build_segments(lines: &[Line], arena: &[Node]) -> Vec<Segment> {
    if lines.is_empty() {
        return Vec::new();
    }

    let mut segments: Vec<Segment> = Vec::new();
    let mut j = 0;

    while j < lines.len() {
        let target_depth = lines[j].depth(arena);

        // Determine start index for scanning
        let start_idx = if lines[j].is_standalone_operator(arena) {
            j + 2
        } else {
            j + 1
        };

        let mut found = false;
        for i in start_idx..lines.len() {
            if lines[i].starts_new_segment_at_depth(target_depth, arena) {
                segments.push(Segment::new(lines[j..i].to_vec()));
                j = i;
                found = true;
                break;
            }
        }

        if !found {
            segments.push(Segment::new(lines[j..].to_vec()));
            break;
        }
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
        let (tail_from_bottom, _) = seg.tail(&arena).unwrap();
        assert_eq!(head_idx, 0);
        assert_eq!(tail_from_bottom, 0);
    }

    #[test]
    fn test_build_segments() {
        let mut arena = Vec::new();
        let line1 = make_line(&mut arena, TokenType::Name, "a");
        let line2 = make_line(&mut arena, TokenType::Name, "b");

        let segments = build_segments(&[line1, line2], &arena);
        assert!(!segments.is_empty());
    }
}
