use crate::line::Line;
use crate::node::Node;
use crate::token::TokenType;

/// JinjaFormatter normalizes whitespace in Jinja tags and formats
/// long Jinja expressions across multiple lines (like Python's black formatter).
pub struct JinjaFormatter {
    pub max_length: usize,
}

impl JinjaFormatter {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    /// Format Jinja tags in a line.
    /// First normalizes whitespace, then applies multiline formatting
    /// if the line would exceed max_length.
    pub fn format_line(&self, line: &mut Line, arena: &mut [Node]) {
        let base_indent = line.indent_size(arena);

        for &idx in &line.nodes {
            let node = &arena[idx];
            match node.token.token_type {
                TokenType::JinjaExpression => {
                    let value = node.value.clone();
                    if let Some(normalized) = self.normalize_expression(&value) {
                        arena[idx].value = normalized;
                    }
                }
                TokenType::JinjaStatement
                | TokenType::JinjaBlockStart
                | TokenType::JinjaBlockEnd
                | TokenType::JinjaBlockKeyword => {
                    let value = node.value.clone();
                    if let Some(normalized) = self.normalize_statement(&value) {
                        arena[idx].value = normalized;
                    }
                }
                _ => {}
            }
        }

        // After normalization, check if any Jinja expression needs multiline formatting
        for &idx in &line.nodes {
            let node = &arena[idx];
            if node.token.token_type == TokenType::JinjaExpression {
                let line_len = base_indent + node.value.len();
                if line_len > self.max_length && !node.value.contains('\n') {
                    if let Some(multiline) = self.format_expression_multiline(&node.value, base_indent) {
                        arena[idx].value = multiline;
                    }
                }
            }
        }
    }

    /// Normalize a {{ expression }} tag.
    fn normalize_expression(&self, value: &str) -> Option<String> {
        let trimmed = value.trim();
        if !trimmed.starts_with("{{") || !trimmed.ends_with("}}") {
            return None;
        }

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
        let inner = inner.trim();

        // If already multiline, preserve the structure
        if inner.contains('\n') {
            return None;
        }

        // Normalize whitespace (collapse internal whitespace)
        let inner = Self::normalize_inner_whitespace(inner);
        // Normalize quotes (single → double) matching black's behavior
        let inner = Self::normalize_quotes(&inner);
        // Add spaces around operators
        let inner = Self::add_operator_spaces(&inner);

        Some(format!("{} {} {}", open, inner, close))
    }

    /// Normalize a {% statement %} tag.
    fn normalize_statement(&self, value: &str) -> Option<String> {
        let trimmed = value.trim();
        if !trimmed.starts_with("{%") || !trimmed.ends_with("%}") {
            return None;
        }

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
        Some(format!("{} {} {}", open, normalized, close))
    }

    /// Normalize a {# comment #} tag.
    fn normalize_comment(&self, value: &str) -> Option<String> {
        let trimmed = value.trim();
        if !trimmed.starts_with("{#") || !trimmed.ends_with("#}") {
            return None;
        }

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
        Some(format!("{} {} {}", open, inner.trim(), close))
    }

    /// Normalize internal whitespace in Jinja content, respecting strings.
    /// Collapses runs of whitespace (including newlines) to single spaces,
    /// but preserves whitespace inside string literals.
    fn normalize_inner_whitespace(content: &str) -> String {
        let bytes = content.as_bytes();
        let mut result = String::with_capacity(content.len());
        let mut i = 0;
        let mut in_whitespace = false;

        while i < bytes.len() {
            // Handle string literals
            if bytes[i] == b'\'' || bytes[i] == b'"' {
                if in_whitespace {
                    result.push(' ');
                    in_whitespace = false;
                }
                let quote = bytes[i];
                result.push(quote as char);
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        result.push(bytes[i] as char);
                        result.push(bytes[i + 1] as char);
                        i += 2;
                        continue;
                    }
                    result.push(bytes[i] as char);
                    i += 1;
                }
                if i < bytes.len() {
                    result.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }

            // Handle whitespace
            if bytes[i].is_ascii_whitespace() {
                in_whitespace = true;
                i += 1;
                continue;
            }

            if in_whitespace {
                result.push(' ');
                in_whitespace = false;
            }
            result.push(bytes[i] as char);
            i += 1;
        }
        result
    }

    /// Add spaces around operators in Jinja content (like black).
    /// Handles: +, |, ~
    /// Does NOT modify operators inside strings.
    fn add_operator_spaces(content: &str) -> String {
        let bytes = content.as_bytes();
        let mut result = String::with_capacity(content.len() + 16);
        let mut i = 0;

        while i < bytes.len() {
            // Skip strings
            if bytes[i] == b'\'' || bytes[i] == b'"' {
                let quote = bytes[i];
                result.push(quote as char);
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        result.push(bytes[i] as char);
                        result.push(bytes[i + 1] as char);
                        i += 2;
                        continue;
                    }
                    result.push(bytes[i] as char);
                    i += 1;
                }
                if i < bytes.len() {
                    result.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }

            // Check for operators: +, |, ~
            let is_operator = match bytes[i] {
                b'+' => i + 1 >= bytes.len() || bytes[i + 1] != b'=', // + but not +=
                b'|' => i + 1 >= bytes.len() || bytes[i + 1] != b'|', // | but not ||
                b'~' => true,
                _ => false,
            };

            if is_operator {
                // Ensure space before: trim trailing whitespace, then add one space
                let trimmed_len = result.trim_end().len();
                // Don't add space after opening paren/bracket
                if trimmed_len > 0 {
                    let last_non_ws = result.as_bytes()[trimmed_len - 1];
                    if last_non_ws != b'(' && last_non_ws != b'[' {
                        result.truncate(trimmed_len);
                        result.push(' ');
                    }
                }
                result.push(bytes[i] as char);
                i += 1;
                // Skip any whitespace after operator
                while i < bytes.len() && bytes[i] == b' ' {
                    i += 1;
                }
                // Add exactly one space after (unless at end or before closing paren/bracket)
                if i < bytes.len() && bytes[i] != b')' && bytes[i] != b']' {
                    result.push(' ');
                }
                continue;
            }

            result.push(bytes[i] as char);
            i += 1;
        }
        result
    }

    /// Normalize Python string quotes inside Jinja content.
    /// Matches black's behavior: single quotes → double quotes,
    /// unless the string contains unescaped double quotes.
    fn normalize_quotes(content: &str) -> String {
        let bytes = content.as_bytes();
        let mut result = String::with_capacity(content.len());
        let mut i = 0;
        while i < bytes.len() {
            // Skip double-quoted strings entirely (preserve as-is)
            if bytes[i] == b'"' {
                // Check for triple-double-quote (""")
                if i + 2 < bytes.len() && bytes[i + 1] == b'"' && bytes[i + 2] == b'"' {
                    result.push_str("\"\"\"");
                    i += 3;
                    while i < bytes.len() {
                        if i + 2 < bytes.len()
                            && bytes[i] == b'"'
                            && bytes[i + 1] == b'"'
                            && bytes[i + 2] == b'"'
                        {
                            result.push_str("\"\"\"");
                            i += 3;
                            break;
                        }
                        result.push(bytes[i] as char);
                        i += 1;
                    }
                    continue;
                }
                result.push('"');
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        result.push(bytes[i] as char);
                        result.push(bytes[i + 1] as char);
                        i += 2;
                        continue;
                    }
                    result.push(bytes[i] as char);
                    i += 1;
                }
                if i < bytes.len() {
                    result.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }
            if bytes[i] == b'\'' {
                // Check for triple-single-quote (''')
                if i + 2 < bytes.len() && bytes[i + 1] == b'\'' && bytes[i + 2] == b'\'' {
                    let start = i;
                    i += 3;
                    let mut has_double_quote = false;
                    let mut end = None;
                    while i < bytes.len() {
                        if i + 2 < bytes.len()
                            && bytes[i] == b'\''
                            && bytes[i + 1] == b'\''
                            && bytes[i + 2] == b'\''
                        {
                            end = Some(i + 2);
                            break;
                        }
                        if bytes[i] == b'"' {
                            has_double_quote = true;
                        }
                        i += 1;
                    }
                    if let Some(end_pos) = end {
                        if has_double_quote {
                            result.push_str(&content[start..=end_pos]);
                        } else {
                            result.push_str("\"\"\"");
                            result.push_str(&content[start + 3..end_pos - 2]);
                            result.push_str("\"\"\"");
                        }
                        i = end_pos + 1;
                    } else {
                        result.push_str(&content[start..]);
                        break;
                    }
                    continue;
                }
                // Find the matching closing single quote
                let start = i;
                i += 1;
                let mut has_double_quote = false;
                let mut end = None;
                while i < bytes.len() {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        i += 2; // skip escaped char
                        continue;
                    }
                    if bytes[i] == b'"' {
                        has_double_quote = true;
                    }
                    if bytes[i] == b'\'' {
                        end = Some(i);
                        break;
                    }
                    i += 1;
                }
                if let Some(end_pos) = end {
                    if has_double_quote {
                        // Keep single quotes if string contains double quotes
                        result.push_str(&content[start..=end_pos]);
                    } else {
                        // Convert to double quotes
                        result.push('"');
                        result.push_str(&content[start + 1..end_pos]);
                        result.push('"');
                    }
                    i = end_pos + 1;
                } else {
                    // No matching close quote found, keep as-is
                    result.push_str(&content[start..]);
                    break;
                }
            } else {
                result.push(bytes[i] as char);
                i += 1;
            }
        }
        result
    }

    /// Format a Jinja expression as multiline when it would exceed max_length.
    /// Produces output like:
    /// ```
    /// {{
    ///     config(
    ///         arg1="val1",
    ///         arg2="val2",
    ///     )
    /// }}
    /// ```
    fn format_expression_multiline(&self, value: &str, base_indent: usize) -> Option<String> {
        let trimmed = value.trim();
        if !trimmed.starts_with("{{") || !trimmed.ends_with("}}") {
            return None;
        }

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

        // Find the top-level function call pattern: name(...)
        let inner = inner.trim();
        if let Some(paren_pos) = find_top_level_paren(inner) {
            // Check if the content ends with )
            if inner.ends_with(')') {
                let func_name = inner[..paren_pos].trim();
                let args_content = &inner[paren_pos + 1..inner.len() - 1];
                let args = split_by_commas(args_content);

                if args.len() <= 1 && args.iter().all(|a| a.len() < 40) {
                    // Single argument or no arguments — keep on one line
                    return None;
                }

                let indent1 = " ".repeat(base_indent + 4);
                let indent2 = " ".repeat(base_indent + 8);

                let mut lines = Vec::new();
                lines.push(open.to_string());
                lines.push(format!("{}{}(", indent1, func_name));
                for arg in &args {
                    let trimmed_arg = arg.trim();
                    if !trimmed_arg.is_empty() {
                        lines.push(format!("{}{},", indent2, trimmed_arg));
                    }
                }
                lines.push(format!("{})", indent1));
                let close_indent = " ".repeat(base_indent);
                lines.push(format!("{}{}", close_indent, close));

                return Some(lines.join("\n"));
            }
        }

        // No function call pattern — try simple multiline with content on its own line
        let indent1 = " ".repeat(base_indent + 4);
        let close_indent = " ".repeat(base_indent);
        Some(format!("{}\n{}{}\n{}{}", open, indent1, inner, close_indent, close))
    }
}

/// Find the position of the first top-level opening parenthesis.
fn find_top_level_paren(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            let quote = bytes[i];
            i += 1;
            while i < bytes.len() && bytes[i] != quote {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 1;
                }
                i += 1;
            }
            if i < bytes.len() {
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'(' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Split content by top-level commas (respecting parentheses, brackets, strings).
fn split_by_commas(s: &str) -> Vec<String> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            let quote = bytes[i];
            i += 1;
            while i < bytes.len() && bytes[i] != quote {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 1;
                }
                i += 1;
            }
            if i < bytes.len() {
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'(' || bytes[i] == b'[' || bytes[i] == b'{' {
            depth += 1;
        } else if bytes[i] == b')' || bytes[i] == b']' || bytes[i] == b'}' {
            depth -= 1;
        } else if bytes[i] == b',' && depth == 0 {
            parts.push(s[start..i].to_string());
            start = i + 1;
        }
        i += 1;
    }
    if start < s.len() {
        let remaining = s[start..].trim();
        if !remaining.is_empty() {
            parts.push(s[start..].to_string());
        }
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_expression() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_expression("{{  my_var  }}");
        assert_eq!(result, Some("{{ my_var }}".to_string()));
    }

    #[test]
    fn test_normalize_statement() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_statement("{%  if  condition  %}");
        assert_eq!(result, Some("{% if condition %}".to_string()));
    }

    #[test]
    fn test_normalize_with_whitespace_control() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_statement("{%- if condition -%}");
        assert_eq!(result, Some("{%- if condition -%}".to_string()));
    }

    #[test]
    fn test_normalize_expression_with_dash() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_expression("{{- my_var -}}");
        assert_eq!(result, Some("{{- my_var -}}".to_string()));
    }

    #[test]
    fn test_normalize_comment() {
        let formatter = JinjaFormatter::new(88);
        let result = formatter.normalize_comment("{#  comment text  #}");
        assert_eq!(result, Some("{# comment text #}".to_string()));
    }

    #[test]
    fn test_operator_spaces() {
        let result = JinjaFormatter::add_operator_spaces("a+b");
        assert_eq!(result, "a + b");

        let result = JinjaFormatter::add_operator_spaces("x|filter");
        assert_eq!(result, "x | filter");

        // Inside strings should not be modified
        let result = JinjaFormatter::add_operator_spaces("'a+b'");
        assert_eq!(result, "'a+b'");
    }

    #[test]
    fn test_multiline_expression() {
        let formatter = JinjaFormatter::new(88);
        let value = r#"{{ config(target_database="analytics", target_schema=target.schema + "_snapshots", unique_key="id", strategy="timestamp", updated_at="updated_at") }}"#;
        let result = formatter.format_expression_multiline(value, 4);
        assert!(result.is_some());
        let multiline = result.unwrap();
        assert!(multiline.contains('\n'));
        assert!(multiline.starts_with("{{"));
        assert!(multiline.ends_with("}}"));
    }

    #[test]
    fn test_split_by_commas() {
        let result = split_by_commas(r#"a="1", b="2", c="3""#);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_split_by_commas_nested() {
        let result = split_by_commas(r#"a=func(1, 2), b="3""#);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_normalize_inner_whitespace() {
        let result = JinjaFormatter::normalize_inner_whitespace("a  +  b");
        assert_eq!(result, "a + b");

        let result = JinjaFormatter::normalize_inner_whitespace("a\n  +\n  b");
        assert_eq!(result, "a + b");

        // Whitespace inside strings preserved
        let result = JinjaFormatter::normalize_inner_whitespace("'hello  world'");
        assert_eq!(result, "'hello  world'");
    }
}
