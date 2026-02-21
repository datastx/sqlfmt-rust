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
        self.token.text.contains('\n')
    }

    pub fn is_c_style(&self) -> bool {
        self.token.text.starts_with("/*")
    }

    pub fn is_jinja_comment(&self) -> bool {
        self.token.text.starts_with("{#")
    }

    pub fn is_inline(&self) -> bool {
        !self.is_standalone && !self.is_multiline()
    }

    /// Return the comment marker (e.g., "--", "/*", "{#-").
    pub fn marker(&self) -> &str {
        let text = &self.token.text;
        for marker in COMMENT_MARKERS {
            if text.starts_with(marker) {
                if *marker == "{#" && text.len() > 2 && text.as_bytes()[2] == b'-' {
                    return &text[..3];
                }
                return marker;
            }
        }
        "--"
    }

    /// Return the output marker.
    /// Python sqlfmt normalizes `//` to `--` but preserves `#` as-is.
    pub fn output_marker(&self) -> &str {
        let m = self.marker();
        if m == "//" {
            "--"
        } else {
            m
        }
    }

    /// Return the comment body (text after the marker, leading whitespace trimmed).
    /// Trailing whitespace is preserved to match Python sqlfmt behavior.
    pub fn body(&self) -> &str {
        let text = &self.token.text;
        let marker = self.marker();
        let after_marker = &text[marker.len()..];
        after_marker.trim_start()
    }

    /// Render as inline comment: `  -- comment text`
    pub fn render_inline(&self) -> String {
        if self.is_c_style() {
            // Preserve C-style comments exactly (especially hints like /*+ ... */)
            format!("  {}", self.token.text.trim())
        } else {
            format!("  {} {}", self.output_marker(), self.body())
        }
    }

    /// Render as standalone comment on its own line(s).
    pub fn render_standalone(&self, prefix: &str, max_line_length: usize) -> String {
        if self.is_multiline() || self.is_c_style() || self.is_jinja_comment() {
            return format!("{}{}\n", prefix, self.token.text.trim());
        }

        let marker = self.output_marker();
        let body = self.body();

        if body.is_empty() {
            return format!("{}{}\n", prefix, marker);
        }

        let overhead = prefix.len() + marker.len() + 1; // +1 for space after marker
        let max_text_width = if max_line_length > overhead {
            max_line_length - overhead
        } else {
            40 // fallback
        };

        if body.len() <= max_text_width {
            return format!("{}{} {}\n", prefix, marker, body);
        }

        // Python sqlfmt does NOT wrap single-line comments containing Jinja
        // expressions ({{ ... }}) or other structured content.
        // Only wrap plain text comments at word boundaries.
        if body.contains("{{") || body.contains("{%") || body.contains("{#") {
            return format!("{}{} {}\n", prefix, marker, body);
        }

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
        assert_eq!(make_comment("--   spaces  ", false).body(), "spaces  ");
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

    #[test]
    fn test_is_inline() {
        // Non-standalone, non-multiline => inline
        assert!(make_comment("-- inline", false).is_inline());

        // Standalone => NOT inline
        assert!(!make_comment("-- standalone", true).is_inline());

        // Multiline => NOT inline
        assert!(!make_comment("-- line1\n-- line2", false).is_inline());

        // Single-line C-style, non-standalone => inline (for hints like /*+ ... */)
        assert!(make_comment("/* block */", false).is_inline());

        // Multiline C-style => NOT inline
        assert!(!make_comment("/* line1\n   line2 */", false).is_inline());
    }

    #[test]
    fn test_empty_comment() {
        let c = make_comment("--", true);
        assert_eq!(c.marker(), "--");
        assert_eq!(c.body(), "");
        assert_eq!(c.render_standalone("", 88), "--\n");
    }

    #[test]
    fn test_jinja_comment() {
        let c = make_comment("{# this is a jinja comment #}", true);
        assert!(c.is_jinja_comment());
        assert_eq!(c.marker(), "{#");
    }

    #[test]
    fn test_render_multiline_comment() {
        let c = make_comment("/* line1\n   line2 */", true);
        assert!(c.is_multiline());
        assert!(c.is_c_style());
        let rendered = c.render_standalone("    ", 88);
        assert!(rendered.starts_with("    "));
        assert!(rendered.contains("line1"));
    }

    #[test]
    fn test_wrap_long_comment() {
        let long_text = "-- this is a very long comment that should be wrapped because it exceeds the maximum line length limit of the formatter tool";
        let c = make_comment(long_text, true);
        let rendered = c.render_standalone("", 40);
        // Should produce multiple lines
        let line_count = rendered.lines().count();
        assert!(
            line_count > 1,
            "Long comment should wrap: got {} lines from: {}",
            line_count,
            rendered
        );
    }

    #[test]
    fn test_jinja_comment_not_wrapped() {
        // Comments with Jinja expressions should NOT be wrapped
        let text = "-- depends_on: {{ ref('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') }}";
        let c = make_comment(text, true);
        let rendered = c.render_standalone("", 88);
        let line_count = rendered.lines().count();
        assert_eq!(
            line_count, 1,
            "Jinja comment should NOT wrap: got {} lines from: {}",
            line_count, rendered
        );
    }

    #[test]
    fn test_hash_comment_marker() {
        let c = make_comment("# python style comment", false);
        assert_eq!(c.marker(), "#");
        assert_eq!(c.body(), "python style comment");
    }

    #[test]
    fn test_double_slash_comment_marker() {
        let c = make_comment("// C++ style comment", false);
        assert_eq!(c.marker(), "//");
        assert_eq!(c.body(), "C++ style comment");
    }
}
