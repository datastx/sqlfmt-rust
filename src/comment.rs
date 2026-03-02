use crate::node::NodeIndex;
use crate::token::Token;

/// Comment markers we recognize.
const COMMENT_MARKERS: &[&str] = &["--", "#", "//", "/*", "{#"];

/// A SQL comment, extracted during lexing.
#[derive(Debug, Clone)]
pub(crate) struct Comment {
    pub(crate) token: Token,
    pub(crate) is_standalone: bool,
    pub(crate) previous_node: Option<NodeIndex>,
}

impl Comment {
    pub(crate) fn new(token: Token, is_standalone: bool, previous_node: Option<NodeIndex>) -> Self {
        Self {
            token,
            is_standalone,
            previous_node,
        }
    }

    pub(crate) fn is_multiline(&self) -> bool {
        self.token.text.contains('\n')
    }

    pub(crate) fn is_c_style(&self) -> bool {
        self.token.text.starts_with("/*")
    }

    pub(crate) fn is_jinja_comment(&self) -> bool {
        self.token.text.starts_with("{#")
    }

    pub(crate) fn is_inline(&self) -> bool {
        !self.is_standalone && !self.is_multiline()
    }

    /// Return the comment marker (e.g., "--", "/*", "{#-").
    pub(crate) fn marker(&self) -> &str {
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
    pub(crate) fn output_marker(&self) -> &str {
        let m = self.marker();
        if m == "//" {
            "--"
        } else {
            m
        }
    }

    /// Return the comment body (text after the marker, leading whitespace trimmed).
    /// Trailing whitespace is preserved to match Python sqlfmt behavior.
    pub(crate) fn body(&self) -> &str {
        let text = &self.token.text;
        let marker = self.marker();
        let after_marker = &text[marker.len()..];
        after_marker.trim_start()
    }

    /// Render as inline comment: `  -- comment text`
    pub(crate) fn render_inline(&self) -> String {
        if self.is_c_style() {
            // Preserve C-style comments exactly (especially hints like /*+ ... */)
            format!("  {}", self.token.text.trim())
        } else {
            format!("  {} {}", self.output_marker(), self.body())
        }
    }

    /// Render as standalone comment on its own line(s).
    pub(crate) fn render_standalone(&self, prefix: &str, max_line_length: usize) -> String {
        if self.is_multiline() || self.is_c_style() || self.is_jinja_comment() {
            return format!("{}{}\n", prefix, self.token.text.trim());
        }

        let marker = self.output_marker();
        let body = self.body();

        if body.is_empty() {
            return format!("{}{}\n", prefix, marker);
        }

        // +1 for space after marker
        let overhead = prefix.len() + marker.len() + 1;
        let max_text_width = if max_line_length > overhead {
            max_line_length - overhead
        } else {
            // fallback
            40
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
#[path = "comment_test.rs"]
mod tests;
