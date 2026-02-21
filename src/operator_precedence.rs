use crate::node::Node;
use crate::token::TokenType;

/// Operator precedence levels (lower = tighter binding).
/// Directly mirrors the Python IntEnum with 15 levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OperatorPrecedence {
    DoubleColon = 0,
    As = 1,
    SquareBrackets = 2,
    OtherTight = 3,
    Exponent = 4,
    Multiplication = 5,
    Addition = 6,
    Other = 7,
    Membership = 8,
    Comparators = 9,
    Presence = 10,
    BoolNot = 11,
    BoolAnd = 12,
    BoolOr = 13,
    On = 14,
}

impl OperatorPrecedence {
    /// The 7 tier boundaries used for merge decisions.
    pub fn tiers() -> &'static [OperatorPrecedence] {
        &[
            Self::OtherTight,
            Self::Multiplication,
            Self::Other,
            Self::Comparators,
            Self::BoolNot,
            Self::BoolAnd,
            Self::On,
        ]
    }

    /// Determine precedence from a Node.
    pub fn from_node(node: &Node, arena: &[Node]) -> Self {
        match node.token.token_type {
            TokenType::DoubleColon => Self::DoubleColon,
            TokenType::On => Self::On,
            TokenType::BooleanOperator => {
                let lower = node.value.to_ascii_lowercase();
                match lower.as_str() {
                    "and" => Self::BoolAnd,
                    "or" => Self::BoolOr,
                    "not" => Self::BoolNot,
                    _ => Self::Other,
                }
            }
            TokenType::WordOperator => Self::from_word_operator(&node.value),
            TokenType::Operator => Self::from_symbol_operator(&node.value),
            _ if node.is_bracket_operator(arena) => Self::SquareBrackets,
            _ if node.is_multiplication_star(arena) => Self::Multiplication,
            _ => Self::Other,
        }
    }

    fn from_word_operator(value: &str) -> Self {
        let lower = value.to_ascii_lowercase();
        // Normalize internal whitespace for multi-word operators
        let normalized: String = lower.split_whitespace().collect::<Vec<_>>().join(" ");
        match normalized.as_str() {
            "as" => Self::As,
            "over" | "within group" | "filter" => Self::OtherTight,
            "in" | "not in" | "global not in" | "global in" | "like" | "not like" | "like any"
            | "like all" | "not like any" | "not like all" | "ilike" | "not ilike"
            | "ilike any" | "ilike all" | "not ilike any" | "not ilike all" | "similar to"
            | "not similar to" | "regexp" | "not regexp" | "rlike" | "not rlike" => {
                Self::Membership
            }
            "between" | "not between" => Self::Membership,
            "is"
            | "is not"
            | "isnull"
            | "notnull"
            | "is distinct from"
            | "is not distinct from"
            | "exists"
            | "not exists" => Self::Presence,
            "interval" | "some" => Self::Other,
            _ => Self::Other,
        }
    }

    fn from_symbol_operator(value: &str) -> Self {
        match value {
            "**" => Self::Exponent,
            "*" | "/" | "%" | "||" => Self::Multiplication,
            "+" | "-" => Self::Addition,
            "=" | "==" | "!=" | "<>" | "<" | ">" | "<=" | ">=" | "<=>" | "~" | "!~" | "~*"
            | "!~*" | "@>" | "<@" | "@@" | "<->" | "!!" | "&&" | "?|" | "?&" | "-|-" => {
                Self::Comparators
            }
            _ => Self::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use crate::token::Token;

    fn make_node(tt: TokenType, value: &str) -> Node {
        Node::new(
            Token::new(tt, "", value, 0, value.len()),
            None,
            String::new(),
            value.to_string(),
            smallvec::SmallVec::new(),
            smallvec::SmallVec::new(),
        )
    }

    #[test]
    fn test_double_colon_precedence() {
        let node = make_node(TokenType::DoubleColon, "::");
        let arena = vec![];
        assert_eq!(
            OperatorPrecedence::from_node(&node, &arena),
            OperatorPrecedence::DoubleColon
        );
    }

    #[test]
    fn test_boolean_operators() {
        let and_node = make_node(TokenType::BooleanOperator, "and");
        let or_node = make_node(TokenType::BooleanOperator, "or");
        let not_node = make_node(TokenType::BooleanOperator, "not");
        let arena = vec![];

        assert_eq!(
            OperatorPrecedence::from_node(&and_node, &arena),
            OperatorPrecedence::BoolAnd
        );
        assert_eq!(
            OperatorPrecedence::from_node(&or_node, &arena),
            OperatorPrecedence::BoolOr
        );
        assert_eq!(
            OperatorPrecedence::from_node(&not_node, &arena),
            OperatorPrecedence::BoolNot
        );
    }

    #[test]
    fn test_word_operators() {
        let as_node = make_node(TokenType::WordOperator, "as");
        let in_node = make_node(TokenType::WordOperator, "in");
        let over_node = make_node(TokenType::WordOperator, "over");
        let arena = vec![];

        assert_eq!(
            OperatorPrecedence::from_node(&as_node, &arena),
            OperatorPrecedence::As
        );
        assert_eq!(
            OperatorPrecedence::from_node(&in_node, &arena),
            OperatorPrecedence::Membership
        );
        assert_eq!(
            OperatorPrecedence::from_node(&over_node, &arena),
            OperatorPrecedence::OtherTight
        );
    }

    #[test]
    fn test_symbol_operators() {
        let plus = make_node(TokenType::Operator, "+");
        let mul = make_node(TokenType::Operator, "*");
        let eq = make_node(TokenType::Operator, "=");
        let exp = make_node(TokenType::Operator, "**");
        let arena = vec![];

        assert_eq!(
            OperatorPrecedence::from_node(&plus, &arena),
            OperatorPrecedence::Addition
        );
        assert_eq!(
            OperatorPrecedence::from_node(&mul, &arena),
            OperatorPrecedence::Multiplication
        );
        assert_eq!(
            OperatorPrecedence::from_node(&eq, &arena),
            OperatorPrecedence::Comparators
        );
        assert_eq!(
            OperatorPrecedence::from_node(&exp, &arena),
            OperatorPrecedence::Exponent
        );
    }

    #[test]
    fn test_tier_ordering() {
        let tiers = OperatorPrecedence::tiers();
        assert_eq!(tiers.len(), 7);
        // Tiers should be in ascending order
        for window in tiers.windows(2) {
            assert!(window[0] < window[1]);
        }
    }

    #[test]
    fn test_between_and_precedence() {
        // "between" is a Membership-level operator
        let between_node = make_node(TokenType::WordOperator, "between");
        let arena = vec![];
        assert_eq!(
            OperatorPrecedence::from_node(&between_node, &arena),
            OperatorPrecedence::Membership
        );

        // "not between" also Membership
        let not_between = make_node(TokenType::WordOperator, "not between");
        assert_eq!(
            OperatorPrecedence::from_node(&not_between, &arena),
            OperatorPrecedence::Membership
        );
    }

    #[test]
    fn test_presence_operators() {
        let arena = vec![];

        let is_node = make_node(TokenType::WordOperator, "is");
        assert_eq!(
            OperatorPrecedence::from_node(&is_node, &arena),
            OperatorPrecedence::Presence
        );

        let is_not_node = make_node(TokenType::WordOperator, "is not");
        assert_eq!(
            OperatorPrecedence::from_node(&is_not_node, &arena),
            OperatorPrecedence::Presence
        );

        let exists_node = make_node(TokenType::WordOperator, "exists");
        assert_eq!(
            OperatorPrecedence::from_node(&exists_node, &arena),
            OperatorPrecedence::Presence
        );
    }

    #[test]
    fn test_membership_operators() {
        let arena = vec![];

        for op in &[
            "in",
            "not in",
            "like",
            "not like",
            "ilike",
            "not ilike",
            "similar to",
        ] {
            let node = make_node(TokenType::WordOperator, op);
            assert_eq!(
                OperatorPrecedence::from_node(&node, &arena),
                OperatorPrecedence::Membership,
                "Expected Membership for '{}'",
                op
            );
        }
    }

    #[test]
    fn test_pg_comparison_operators() {
        let arena = vec![];

        for op in &["@>", "<@", "@@", "<->", "&&", "?|", "?&", "-|-"] {
            let node = make_node(TokenType::Operator, op);
            assert_eq!(
                OperatorPrecedence::from_node(&node, &arena),
                OperatorPrecedence::Comparators,
                "Expected Comparators for '{}'",
                op
            );
        }
    }

    #[test]
    fn test_on_precedence() {
        let arena = vec![];
        let on_node = make_node(TokenType::On, "on");
        assert_eq!(
            OperatorPrecedence::from_node(&on_node, &arena),
            OperatorPrecedence::On
        );
    }

    #[test]
    fn test_as_precedence() {
        let arena = vec![];
        let as_node = make_node(TokenType::WordOperator, "as");
        assert_eq!(
            OperatorPrecedence::from_node(&as_node, &arena),
            OperatorPrecedence::As
        );
    }
}
