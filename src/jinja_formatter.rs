use crate::line::Line;
use crate::node::Node;

/// JinjaFormatter normalizes whitespace in Jinja tags.
/// In the Python version, this integrates with Black to format Python code
/// inside Jinja expressions. In the Rust port, we start with simple
/// whitespace normalization and preserve the tag content.
pub struct JinjaFormatter {
    pub max_length: usize,
}

impl JinjaFormatter {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    /// Format Jinja tags in a line.
    /// Currently normalizes whitespace inside Jinja delimiters.
    pub fn format_line(&self, line: &mut Line, arena: &mut Vec<Node>) {
        for &idx in &line.nodes {
            if arena[idx].is_jinja() {
                self.normalize_jinja_whitespace(&mut arena[idx]);
            }
        }
    }

    /// Normalize whitespace inside a Jinja tag.
    fn normalize_jinja_whitespace(&self, node: &mut Node) {
        let value = node.value.clone();
        if let Some(normalized) = self.normalize_delimiters(&value) {
            node.value = normalized;
        }
    }

    /// Normalize whitespace around Jinja delimiters.
    fn normalize_delimiters(&self, value: &str) -> Option<String> {
        let trimmed = value.trim();

        // {{ expression }}
        if trimmed.starts_with("{{") && trimmed.ends_with("}}") {
            let inner = trimmed[2..trimmed.len() - 2].trim();
            let (open, inner) = if let Some(rest) = inner.strip_prefix('-') {
                ("{{-", rest.trim_start())
            } else {
                ("{{", inner)
            };
            let (close, inner) = if let Some(rest) = inner.strip_suffix('-') {
                ("-}}", rest.trim_end())
            } else {
                ("}}", inner)
            };
            return Some(format!("{} {} {}", open, inner.trim(), close));
        }

        // {% statement %}
        if trimmed.starts_with("{%") && trimmed.ends_with("%}") {
            let inner = trimmed[2..trimmed.len() - 2].trim();
            let (open, inner) = if let Some(rest) = inner.strip_prefix('-') {
                ("{%-", rest.trim_start())
            } else {
                ("{%", inner)
            };
            let (close, inner) = if let Some(rest) = inner.strip_suffix('-') {
                ("-%}", rest.trim_end())
            } else {
                ("%}", inner)
            };
            let normalized: String = inner.split_whitespace().collect::<Vec<_>>().join(" ");
            return Some(format!("{} {} {}", open, normalized, close));
        }

        // {# comment #}
        if trimmed.starts_with("{#") && trimmed.ends_with("#}") {
            let inner = trimmed[2..trimmed.len() - 2].trim();
            let (open, inner) = if let Some(rest) = inner.strip_prefix('-') {
                ("{#-", rest.trim_start())
            } else {
                ("{#", inner)
            };
            let (close, inner) = if let Some(rest) = inner.strip_suffix('-') {
                ("-#}", rest.trim_end())
            } else {
                ("#}", inner)
            };
            return Some(format!("{} {} {}", open, inner.trim(), close));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_expression() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_delimiters("{{  my_var  }}");
        assert_eq!(result, Some("{{ my_var }}".to_string()));
    }

    #[test]
    fn test_normalize_statement() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_delimiters("{%  if  condition  %}");
        assert_eq!(result, Some("{% if condition %}".to_string()));
    }

    #[test]
    fn test_normalize_with_whitespace_control() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_delimiters("{%- if condition -%}");
        assert_eq!(result, Some("{%- if condition -%}".to_string()));
    }

    #[test]
    fn test_normalize_expression_with_dash() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_delimiters("{{- my_var -}}");
        assert_eq!(result, Some("{{- my_var -}}".to_string()));
    }

    #[test]
    fn test_normalize_comment() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_delimiters("{#  comment text  #}");
        assert_eq!(result, Some("{# comment text #}".to_string()));
    }
}
