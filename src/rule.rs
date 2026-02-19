use regex::Regex;

use crate::action::Action;

/// A lexing rule: name, priority, compiled regex, and action.
/// Rules are tried in ascending priority order; first match wins.
#[derive(Clone)]
pub struct Rule {
    pub name: String,
    pub priority: u32,
    pub pattern: Regex,
    pub action: Action,
}

impl Rule {
    /// Create a new rule. The pattern is compiled with case-insensitive and
    /// DOTALL flags, and anchored to match at the current position.
    /// The pattern should capture:
    ///   Group 1: leading whitespace (non-newline)
    ///   Group 2: the actual token
    pub fn new(name: &str, priority: u32, pattern: &str, action: Action) -> Self {
        // (?si) = case-insensitive + dot-matches-newline
        // \A = anchor at start of input (we'll be slicing the source)
        let full_pattern = format!(r"(?si)\A([^\S\n]*)({})", pattern);
        let compiled = Regex::new(&full_pattern)
            .unwrap_or_else(|e| panic!("Invalid regex for rule '{}': {}", name, e));
        Self {
            name: name.to_string(),
            priority,
            pattern: compiled,
            action,
        }
    }
}

impl std::fmt::Debug for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Rule")
            .field("name", &self.name)
            .field("priority", &self.priority)
            .field("action", &self.action)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::TokenType;

    #[test]
    fn test_rule_creation_and_matching() {
        let rule = Rule::new(
            "test_select",
            100,
            r"(select)\b",
            Action::AddNode {
                token_type: TokenType::UntermKeyword,
            },
        );
        assert_eq!(rule.name, "test_select");
        assert_eq!(rule.priority, 100);

        // Should match "select" at start
        let caps = rule.pattern.captures("select foo");
        assert!(caps.is_some());
        let caps = caps.unwrap();
        assert_eq!(&caps[2], "select");

        // Should match with leading whitespace
        let caps = rule.pattern.captures("  select foo");
        assert!(caps.is_some());
        let caps = caps.unwrap();
        assert_eq!(&caps[1], "  ");
        assert_eq!(&caps[2], "select");
    }

    #[test]
    fn test_rule_case_insensitive() {
        let rule = Rule::new(
            "test_select",
            100,
            r"(select)\b",
            Action::AddNode {
                token_type: TokenType::UntermKeyword,
            },
        );
        let caps = rule.pattern.captures("SELECT foo");
        assert!(caps.is_some());
        assert_eq!(&caps.unwrap()[2], "SELECT");
    }

    #[test]
    fn test_rule_no_match() {
        let rule = Rule::new(
            "test_select",
            100,
            r"(select)\b",
            Action::AddNode {
                token_type: TokenType::UntermKeyword,
            },
        );
        // Should not match if not at start
        let caps = rule.pattern.captures("foo select");
        assert!(caps.is_none());
    }
}
