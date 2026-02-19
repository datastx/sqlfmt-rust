use crate::node::NodeIndex;
use crate::token::Token;

/// Comment markers we recognize.
const COMMENT_MARKERS: &[&str] = &["--", "#", "//", "/*", "{#"];

/// A SQL comment, extracted during lexing.
#[derive(Debug, Clone)]
pub struct Comment {
    pub token: Token,
    pub is_standalone: bool,
    pub previous_node: Option<NodeIndex>,
}

impl Comment {
    pub fn new(token: Token, is_standalone: bool, previous_node: Option<NodeIndex>) -> Self {
        Self {
            token,
            is_standalone,
            previous_node,
        }
    }

    pub fn is_multiline(&self) -> bool {
        self.token.token.contains('\n')
    }

    pub fn is_c_style(&self) -> bool {
        self.token.token.starts_with("/*")
    }

    pub fn is_jinja_comment(&self) -> bool {
        self.token.token.starts_with("{#")
    }

    pub fn is_inline(&self) -> bool {
        !self.is_standalone && !self.is_multiline() && !self.is_c_style()
    }

    /// Return the comment marker (e.g., "--", "/*", "{#-").
    pub fn marker(&self) -> &str {
        let text = &self.token.token;
        // Try markers from longest to shortest
        for marker in COMMENT_MARKERS {
            if text.starts_with(marker) {
                // Check for Jinja comment with whitespace control
                if *marker == "{#" && text.len() > 2 && text.as_bytes()[2] == b'-' {
                    return &text[..3];
                }
                return marker;
            }
        }
        "--"
    }

    /// Return the comment body (text after the marker, trimmed).
    pub fn body(&self) -> &str {
        let text = &self.token.token;
        let marker = self.marker();
        let after_marker = &text[marker.len()..];
        after_marker.trim()
    }

    /// Render as inline comment: `  -- comment text`
    pub fn render_inline(&self) -> String {
        format!("  {} {}", self.marker(), self.body())
    }

    /// Render as standalone comment on its own line(s).
    pub fn render_standalone(&self, prefix: &str, max_line_length: usize) -> String {
        if self.is_multiline() || self.is_c_style() || self.is_jinja_comment() {
            // Preserve multiline / C-style / Jinja comments as-is with proper prefix
            return format!("{}{}\n", prefix, self.token.token.trim());
        }

        let marker = self.marker();
        let body = self.body();

        if body.is_empty() {
            return format!("{}{}\n", prefix, marker);
        }

        // Compute available width for comment text
        let overhead = prefix.len() + marker.len() + 1; // +1 for space after marker
        let max_text_width = if max_line_length > overhead {
            max_line_length - overhead
        } else {
            40 // fallback
        };

        if body.len() <= max_text_width {
            return format!("{}{} {}\n", prefix, marker, body);
        }

        // Wrap long comment text at word boundaries
        let mut result = String::new();
        let mut current_line = String::new();
        for word in body.split_whitespace() {
            if current_line.is_empty() {
                current_line.push_str(word);
            } else if current_line.len() + 1 + word.len() <= max_text_width {
                current_line.push(' ');
                current_line.push_str(word);
            } else {
                result.push_str(&format!("{}{} {}\n", prefix, marker, current_line));
                current_line = word.to_string();
            }
        }
        if !current_line.is_empty() {
            result.push_str(&format!("{}{} {}\n", prefix, marker, current_line));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::TokenType;

    fn make_comment(text: &str, standalone: bool) -> Comment {
        Comment::new(
            Token::new(TokenType::Comment, "", text, 0, text.len()),
            standalone,
            None,
        )
    }

    #[test]
    fn test_marker_detection() {
        assert_eq!(make_comment("-- hello", false).marker(), "--");
        assert_eq!(make_comment("# hello", false).marker(), "#");
        assert_eq!(make_comment("// hello", false).marker(), "//");
        assert_eq!(make_comment("/* hello */", false).marker(), "/*");
        assert_eq!(make_comment("{# hello #}", false).marker(), "{#");
        assert_eq!(make_comment("{#- hello #}", false).marker(), "{#-");
    }

    #[test]
    fn test_body_extraction() {
        assert_eq!(make_comment("-- hello world", false).body(), "hello world");
        assert_eq!(make_comment("--   spaces  ", false).body(), "spaces");
        assert_eq!(make_comment("--", false).body(), "");
    }

    #[test]
    fn test_inline_rendering() {
        let c = make_comment("-- inline", false);
        assert_eq!(c.render_inline(), "  -- inline");
    }

    #[test]
    fn test_standalone_rendering() {
        let c = make_comment("-- standalone", true);
        assert_eq!(c.render_standalone("    ", 88), "    -- standalone\n");
    }

    #[test]
    fn test_multiline_detection() {
        assert!(make_comment("-- line1\n-- line2", true).is_multiline());
        assert!(!make_comment("-- single", true).is_multiline());
    }

    #[test]
    fn test_c_style_detection() {
        assert!(make_comment("/* block */", false).is_c_style());
        assert!(!make_comment("-- line", false).is_c_style());
    }
}
