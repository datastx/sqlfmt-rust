use compact_str::CompactString;

use crate::line::{indent_str, Line};
use crate::node::Node;
use crate::string_utils::{skip_string_literal, skip_string_literal_into};
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
                        arena[idx].value = CompactString::from(normalized);
                    }
                }
                TokenType::JinjaStatement
                | TokenType::JinjaBlockStart
                | TokenType::JinjaBlockEnd
                | TokenType::JinjaBlockKeyword => {
                    let value = node.value.clone();
                    if let Some(normalized) = self.normalize_statement(&value) {
                        arena[idx].value = CompactString::from(normalized);
                    }
                }
                _ => {}
            }
        }

        // "Magic trailing comma" — if a list has a trailing comma,
        // always format as multiline (matching black's behavior).
        for &idx in &line.nodes {
            let node = &arena[idx];
            let line_len = base_indent + node.value.len();
            let has_magic_trailing_comma = has_trailing_comma_in_brackets(&node.value);
            if (line_len <= self.max_length && !has_magic_trailing_comma)
                || node.value.contains('\n')
            {
                continue;
            }
            match node.token.token_type {
                TokenType::JinjaExpression => {
                    if let Some(multiline) =
                        self.format_expression_multiline(&node.value, base_indent)
                    {
                        arena[idx].value = CompactString::from(multiline);
                    }
                }
                TokenType::JinjaStatement
                | TokenType::JinjaBlockStart
                | TokenType::JinjaBlockEnd
                | TokenType::JinjaBlockKeyword => {
                    if let Some(multiline) =
                        self.format_statement_multiline(&node.value, base_indent)
                    {
                        arena[idx].value = CompactString::from(multiline);
                    }
                }
                _ => {}
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

        // If already multiline with complex content (triple quotes, dicts),
        // preserve the structure since we can't safely re-format these.
        // Only check for { outside string literals ({{ this }} inside a string is fine).
        if inner.contains('\n') && has_complex_structure_outside_strings(inner) {
            return None;
        }

        let inner = Self::normalize_chain(inner);

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
        let inner = inner.trim();

        // If already multiline with complex content (triple quotes, dicts),
        // preserve the structure. Only check for { outside string literals.
        if inner.contains('\n') && has_complex_structure_outside_strings(inner) {
            return None;
        }

        // If originally multiline and no function call or list pattern, preserve
        // structure. This handles cases like {% extends ... if ... else ... %}
        // that are intentionally wrapped by the user.
        if inner.contains('\n')
            && find_top_level_paren(inner).is_none()
            && find_top_level_bracket(inner).is_none()
        {
            // Preserve multiline with per-line whitespace cleanup but no quote change
            let cleaned_lines: Vec<&str> = inner
                .lines()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty())
                .collect();
            let indent = "    ";
            let mut result = format!("{} \n", open);
            for line in &cleaned_lines {
                result.push_str(indent);
                result.push_str(line);
                result.push_str(" \n");
            }
            result.push_str(close);
            return Some(result);
        }

        let normalized = Self::normalize_chain(inner);

        Some(format!("{} {} {}", open, normalized, close))
    }

    /// Run the full normalization chain using two reusable buffers (ping-pong).
    /// Reduces allocations from 5 (one per step) to 2 reused buffers.
    fn normalize_chain(inner: &str) -> String {
        // Step 1: normalize_inner_whitespace → buf_a
        let mut buf_a = Self::normalize_inner_whitespace(inner);
        // Step 2: normalize_quotes: buf_a → buf_b
        let mut buf_b = Self::normalize_quotes(&buf_a);
        // Step 3: add_operator_spaces: buf_b → reuse buf_a
        buf_a.clear();
        Self::add_operator_spaces_into(&buf_b, &mut buf_a);
        // Step 4: add_comma_spaces: buf_a → reuse buf_b
        buf_b.clear();
        Self::add_comma_spaces_into(&buf_a, &mut buf_b);
        // Step 5: strip_paren_spaces: buf_b → reuse buf_a
        buf_a.clear();
        Self::strip_paren_spaces_into(&buf_b, &mut buf_a);
        buf_a
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
            if bytes[i] == b'\'' || bytes[i] == b'"' {
                if in_whitespace {
                    result.push(' ');
                    in_whitespace = false;
                }
                i = skip_string_literal_into(bytes, i, &mut result);
                continue;
            }

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
    /// Handles: +, |, ~, = (at depth 0), ==, !=, >=, <=
    /// Does NOT modify operators inside strings.
    #[cfg(test)]
    fn add_operator_spaces(content: &str) -> String {
        let mut result = String::with_capacity(content.len() + 16);
        Self::add_operator_spaces_into(content, &mut result);
        result
    }

    /// Add operator spaces, writing into the provided buffer.
    fn add_operator_spaces_into(content: &str, result: &mut String) {
        result.reserve(content.len() + 16);
        let bytes = content.as_bytes();
        let mut i = 0;
        let mut paren_depth: i32 = 0;

        while i < bytes.len() {
            if bytes[i] == b'\'' || bytes[i] == b'"' {
                i = skip_string_literal_into(bytes, i, result);
                continue;
            }

            if bytes[i] == b'(' || bytes[i] == b'[' {
                paren_depth += 1;
                result.push(bytes[i] as char);
                i += 1;
                continue;
            }
            if bytes[i] == b')' || bytes[i] == b']' {
                paren_depth -= 1;
                result.push(bytes[i] as char);
                i += 1;
                continue;
            }

            if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                let is_comparison = matches!(bytes[i], b'=' | b'!' | b'>' | b'<');
                if is_comparison {
                    let trimmed_len = result.trim_end().len();
                    if trimmed_len > 0 {
                        result.truncate(trimmed_len);
                        result.push(' ');
                    }
                    result.push(bytes[i] as char);
                    result.push(b'=' as char);
                    i += 2;
                    while i < bytes.len() && bytes[i] == b' ' {
                        i += 1;
                    }
                    if i < bytes.len() && bytes[i] != b')' && bytes[i] != b']' {
                        result.push(' ');
                    }
                    continue;
                }
            }

            let is_operator = match bytes[i] {
                b'+' => i + 1 >= bytes.len() || bytes[i + 1] != b'=', // + but not +=
                b'|' => i + 1 >= bytes.len() || bytes[i + 1] != b'|', // | but not ||
                b'~' => true,
                // = at depth 0 is assignment (gets spaces), at depth > 0 is kwarg (no spaces)
                b'=' => paren_depth == 0 && (i + 1 >= bytes.len() || bytes[i + 1] != b'='),
                _ => false,
            };

            if is_operator {
                let trimmed_len = result.trim_end().len();
                if trimmed_len > 0 {
                    let last_non_ws = result.as_bytes()[trimmed_len - 1];
                    if last_non_ws != b'(' && last_non_ws != b'[' {
                        result.truncate(trimmed_len);
                        result.push(' ');
                    }
                }
                result.push(bytes[i] as char);
                i += 1;
                while i < bytes.len() && bytes[i] == b' ' {
                    i += 1;
                }
                if i < bytes.len() && bytes[i] != b')' && bytes[i] != b']' {
                    result.push(' ');
                }
                continue;
            }

            result.push(bytes[i] as char);
            i += 1;
        }
    }

    /// Normalize Python string quotes inside Jinja content.
    /// Matches black's behavior: single quotes → double quotes,
    /// unless the string contains unescaped double quotes.
    fn normalize_quotes(content: &str) -> String {
        let bytes = content.as_bytes();
        let mut result = String::with_capacity(content.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'"' {
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
                        // Keep single quotes if string contains unescaped double quotes
                        result.push_str(&content[start..=end_pos]);
                    } else {
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

    /// Strip paren spaces, writing into the provided buffer.
    fn strip_paren_spaces_into(content: &str, result: &mut String) {
        result.reserve(content.len());
        let bytes = content.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            if bytes[i] == b'\'' || bytes[i] == b'"' {
                i = skip_string_literal_into(bytes, i, result);
                continue;
            }

            if bytes[i] == b'(' {
                let trimmed_len = result.trim_end().len();
                if trimmed_len > 0 {
                    let last_byte = result.as_bytes()[trimmed_len - 1];
                    if last_byte.is_ascii_alphanumeric() || last_byte == b'_' || last_byte == b'.' {
                        result.truncate(trimmed_len);
                    }
                }
                result.push('(');
                i += 1;
                while i < bytes.len() && bytes[i] == b' ' {
                    i += 1;
                }
                continue;
            }

            if bytes[i] == b'[' {
                result.push(bytes[i] as char);
                i += 1;
                while i < bytes.len() && bytes[i] == b' ' {
                    i += 1;
                }
                continue;
            }

            if bytes[i] == b')' || bytes[i] == b']' {
                let trimmed = result.trim_end().len();
                result.truncate(trimmed);
                result.push(bytes[i] as char);
                i += 1;
                continue;
            }

            result.push(bytes[i] as char);
            i += 1;
        }
    }

    /// Add comma spaces, writing into the provided buffer.
    fn add_comma_spaces_into(content: &str, result: &mut String) {
        result.reserve(content.len() + 16);
        let bytes = content.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            if bytes[i] == b'\'' || bytes[i] == b'"' {
                i = skip_string_literal_into(bytes, i, result);
                continue;
            }

            if bytes[i] == b',' {
                result.push(',');
                i += 1;
                while i < bytes.len() && bytes[i] == b' ' {
                    i += 1;
                }
                if i < bytes.len() && bytes[i] != b')' && bytes[i] != b']' && bytes[i] != b'}' {
                    result.push(' ');
                }
                continue;
            }

            result.push(bytes[i] as char);
            i += 1;
        }
    }

    /// Format a Jinja expression as multiline when it would exceed max_length.
    /// Produces output like:
    /// ```text
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

        let inner = inner.trim();
        if let Some(paren_pos) = find_top_level_paren(inner) {
            if inner.ends_with(')') {
                let func_name = inner[..paren_pos].trim();
                let args_content = &inner[paren_pos + 1..inner.len() - 1];
                let args = split_by_commas(args_content);

                if args.len() <= 1 {
                    let single_arg = args.first().map(|a| a.trim()).unwrap_or("");
                    if single_arg.starts_with('[') && single_arg.ends_with(']') {
                        let list_content = &single_arg[1..single_arg.len() - 1];
                        let list_items = split_by_commas(list_content);
                        if list_items.len() > 1 {
                            let indent1 = indent_str(base_indent + 4);
                            let indent2 = indent_str(base_indent + 8);
                            let indent3 = indent_str(base_indent + 12);
                            let close_indent = indent_str(base_indent);

                            let mut result = String::with_capacity(256);
                            result.push_str(open);
                            result.push('\n');
                            result.push_str(indent1);
                            result.push_str(func_name);
                            result.push_str("(\n");
                            result.push_str(indent2);
                            result.push_str("[\n");
                            for item in &list_items {
                                let trimmed_item = item.trim();
                                if !trimmed_item.is_empty() {
                                    result.push_str(indent3);
                                    result.push_str(trimmed_item);
                                    result.push_str(",\n");
                                }
                            }
                            result.push_str(indent2);
                            result.push_str("]\n");
                            result.push_str(indent1);
                            result.push_str(")\n");
                            result.push_str(close_indent);
                            result.push_str(close);
                            return Some(result);
                        }
                    }
                    if single_arg.len() < 40 {
                        return None;
                    }
                }

                let indent1 = indent_str(base_indent + 4);
                let indent2 = indent_str(base_indent + 8);
                let close_indent = indent_str(base_indent);

                let mut result = String::with_capacity(256);
                result.push_str(open);
                result.push('\n');
                result.push_str(indent1);
                result.push_str(func_name);
                result.push_str("(\n");
                for arg in &args {
                    let trimmed_arg = arg.trim();
                    if !trimmed_arg.is_empty() {
                        result.push_str(indent2);
                        result.push_str(trimmed_arg);
                        result.push_str(",\n");
                    }
                }
                // Remove trailing comma for single-arg functions
                if args.len() == 1 && result.ends_with(",\n") {
                    result.truncate(result.len() - 2);
                    result.push('\n');
                }
                result.push_str(indent1);
                result.push_str(")\n");
                result.push_str(close_indent);
                result.push_str(close);

                return Some(result);
            }
        }

        let indent1 = indent_str(base_indent + 4);
        let close_indent = indent_str(base_indent);
        Some(format!(
            "{}\n{}{}\n{}{}",
            open, indent1, inner, close_indent, close
        ))
    }

    /// Format a Jinja statement as multiline when it would exceed max_length.
    /// Handles tags like `{% macro name(arg1, arg2, ...) %}` and
    /// `{% call name(arg1, arg2, ...) %}`.
    /// Produces output like:
    /// ```text
    /// {% macro name(
    ///     arg1,
    ///     arg2,
    /// ) %}
    /// ```
    fn format_statement_multiline(&self, value: &str, base_indent: usize) -> Option<String> {
        let trimmed = value.trim();

        let (open_delim, inner, close_delim) = if trimmed.starts_with("{%-") {
            if trimmed.ends_with("-%}") {
                ("{%-", &trimmed[3..trimmed.len() - 3], "-%}")
            } else if trimmed.ends_with("%}") {
                ("{%-", &trimmed[3..trimmed.len() - 2], "%}")
            } else {
                return None;
            }
        } else if trimmed.starts_with("{%") {
            if trimmed.ends_with("-%}") {
                ("{%", &trimmed[2..trimmed.len() - 3], "-%}")
            } else if trimmed.ends_with("%}") {
                ("{%", &trimmed[2..trimmed.len() - 2], "%}")
            } else {
                return None;
            }
        } else {
            return None;
        };

        let inner = inner.trim();

        if let Some(paren_pos) = find_top_level_paren(inner) {
            if let Some(close_pos) = find_matching_close(inner, paren_pos) {
                let before_paren = &inner[..paren_pos];
                let args_content = &inner[paren_pos + 1..close_pos];
                let after_close = inner[close_pos + 1..].trim();
                let args = split_by_commas(args_content);

                let indent1 = indent_str(base_indent + 4);
                let close_indent = indent_str(base_indent);

                let mut result = String::with_capacity(256);
                result.push_str(open_delim);
                result.push(' ');
                result.push_str(before_paren);
                result.push('(');
                let strip_trailing_comma = args.len() == 1 || !args_content.trim().ends_with(',');
                let arg_count = args.len();
                for (ai, arg) in args.iter().enumerate() {
                    let trimmed_arg = arg.trim();
                    if !trimmed_arg.is_empty() {
                        result.push('\n');
                        result.push_str(indent1);
                        result.push_str(trimmed_arg);
                        if ai == arg_count - 1 && strip_trailing_comma {
                            // Don't add trailing comma
                        } else {
                            result.push(',');
                        }
                    }
                }
                result.push('\n');
                result.push_str(close_indent);
                result.push_str(") ");
                if !after_close.is_empty() {
                    result.push_str(after_close);
                    result.push(' ');
                }
                result.push_str(close_delim);

                return Some(result);
            }
        }

        if let Some(bracket_pos) = find_top_level_bracket(inner) {
            if inner.ends_with(']') {
                let before_bracket = &inner[..bracket_pos];
                let list_content = &inner[bracket_pos + 1..inner.len() - 1];
                let items = split_by_commas(list_content);

                if items.len() <= 1 {
                    let tilde_parts = split_by_tilde(list_content);
                    if tilde_parts.len() > 1 {
                        let indent1 = indent_str(base_indent + 4);
                        let close_indent = indent_str(base_indent);
                        let mut result = String::with_capacity(256);
                        result.push_str(open_delim);
                        result.push(' ');
                        result.push_str(before_bracket);
                        result.push('[');
                        for (i, part) in tilde_parts.iter().enumerate() {
                            let trimmed_part = part.trim();
                            result.push('\n');
                            result.push_str(indent1);
                            if i > 0 {
                                result.push_str("~ ");
                            }
                            result.push_str(trimmed_part);
                        }
                        result.push('\n');
                        result.push_str(close_indent);
                        result.push_str("] ");
                        result.push_str(close_delim);
                        return Some(result);
                    }
                    return None;
                }

                let indent1 = indent_str(base_indent + 4);
                let close_indent = indent_str(base_indent);

                let mut result = String::with_capacity(256);
                result.push_str(open_delim);
                result.push(' ');
                result.push_str(before_bracket);
                result.push('[');
                for item in &items {
                    let trimmed_item = item.trim();
                    if !trimmed_item.is_empty() {
                        result.push('\n');
                        result.push_str(indent1);
                        result.push_str(trimmed_item);
                        result.push(',');
                    }
                }
                result.push('\n');
                result.push_str(close_indent);
                result.push_str("] ");
                result.push_str(close_delim);

                return Some(result);
            }
        }

        if inner.len() + open_delim.len() + close_delim.len() + 4 > self.max_length {
            let indent1 = indent_str(base_indent + 4);
            let close_indent = indent_str(base_indent);
            return Some(format!(
                "{} \n{}{}\n{}{}",
                open_delim, indent1, inner, close_indent, close_delim
            ));
        }

        None
    }
}

/// Check if a Jinja tag value has a trailing comma inside brackets (parens or square brackets).
/// This implements "magic trailing comma" behavior from Python's black formatter:
/// if a list or function call has a trailing comma, it should always be formatted as multiline.
fn has_trailing_comma_in_brackets(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            i = skip_string_literal(bytes, i);
            continue;
        }
        if bytes[i] == b',' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && (bytes[j] == b')' || bytes[j] == b']') {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Check if content has complex structure outside of string literals.
/// Returns true if there are `{`, triple-quoted strings (`"""` or `'''`)
/// at the top level (not inside string literals). This is used to detect
/// dict literals, nested templates, etc. that we can't safely reformat.
fn has_complex_structure_outside_strings(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            let quote = bytes[i];
            if i + 2 < bytes.len() && bytes[i + 1] == quote && bytes[i + 2] == quote {
                // Triple-quoted strings ARE complex structure
                return true;
            }
            i = skip_string_literal(bytes, i);
            continue;
        }
        if bytes[i] == b'{' {
            return true;
        }
        i += 1;
    }
    false
}

/// Find the position of the first top-level opening bracket `[`.
fn find_top_level_bracket(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut paren_depth = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            i = skip_string_literal(bytes, i);
            continue;
        }
        if bytes[i] == b'(' {
            paren_depth += 1;
        } else if bytes[i] == b')' {
            paren_depth -= 1;
        }
        if bytes[i] == b'[' && paren_depth == 0 {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find the position of the first top-level opening parenthesis.
fn find_top_level_paren(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            i = skip_string_literal(bytes, i);
            continue;
        }
        if bytes[i] == b'(' {
            // Skip empty or trivially short function calls like `lower()`.
            // Find the matching `)` and check if the content is substantial.
            let paren_start = i;
            let mut depth = 1;
            let mut j = i + 1;
            while j < bytes.len() && depth > 0 {
                if bytes[j] == b'\'' || bytes[j] == b'"' {
                    j = skip_string_literal(bytes, j);
                    continue;
                } else if bytes[j] == b'(' {
                    depth += 1;
                } else if bytes[j] == b')' {
                    depth -= 1;
                }
                j += 1;
            }
            let content = &s[paren_start + 1..j.saturating_sub(1)];
            if !content.trim().is_empty() {
                return Some(paren_start);
            }
            i = j;
            continue;
        }
        i += 1;
    }
    None
}

/// Find the position of the closing `)` that matches the `(` at `open_pos`.
fn find_matching_close(s: &str, open_pos: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 1;
    let mut i = open_pos + 1;
    while i < bytes.len() && depth > 0 {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            i = skip_string_literal(bytes, i);
            continue;
        } else if bytes[i] == b'(' {
            depth += 1;
        } else if bytes[i] == b')' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Split content by top-level commas (respecting parentheses, brackets, strings).
/// Returns borrowed slices to avoid per-part String allocations.
fn split_by_commas(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            i = skip_string_literal(bytes, i);
            continue;
        }
        if bytes[i] == b'(' || bytes[i] == b'[' || bytes[i] == b'{' {
            depth += 1;
        } else if bytes[i] == b')' || bytes[i] == b']' || bytes[i] == b'}' {
            depth -= 1;
        } else if bytes[i] == b',' && depth == 0 {
            parts.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    if start < s.len() {
        let remaining = s[start..].trim();
        if !remaining.is_empty() {
            parts.push(&s[start..]);
        }
    }
    parts
}

/// Split content by top-level `~` (tilde) operators (respecting strings and brackets).
/// Returns borrowed slices to avoid per-part String allocations.
fn split_by_tilde(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            i = skip_string_literal(bytes, i);
            continue;
        }
        if bytes[i] == b'(' || bytes[i] == b'[' || bytes[i] == b'{' {
            depth += 1;
        } else if bytes[i] == b')' || bytes[i] == b']' || bytes[i] == b'}' {
            depth -= 1;
        } else if bytes[i] == b'~' && depth == 0 {
            parts.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    if start < s.len() {
        let remaining = s[start..].trim();
        if !remaining.is_empty() {
            parts.push(&s[start..]);
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
