use crate::node::Node;
use crate::token::TokenType;

/// Operator precedence levels (lower = tighter binding).
/// Directly mirrors the Python IntEnum with 15 levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub(crate) enum OperatorPrecedence {
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

/// Compare two strings case-insensitively with whitespace normalization,
/// without allocating. Returns true if the words in `input` match `target`
/// when compared case-insensitively and with whitespace collapsed.
fn eq_ignore_case_ws(input: &str, target: &str) -> bool {
    let mut input_words = input.split_ascii_whitespace();
    let mut target_words = target.split_ascii_whitespace();
    loop {
        match (input_words.next(), target_words.next()) {
            (Some(a), Some(b)) => {
                if !a.eq_ignore_ascii_case(b) {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false,
        }
    }
}

impl OperatorPrecedence {
    /// The 7 tier boundaries used for merge decisions.
    pub(crate) fn tiers() -> &'static [OperatorPrecedence] {
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
    pub(crate) fn from_node(node: &Node) -> Self {
        match node.token.token_type {
            TokenType::DoubleColon => Self::DoubleColon,
            TokenType::On => Self::On,
            TokenType::BooleanOperator => {
                if node.value.eq_ignore_ascii_case("and") {
                    Self::BoolAnd
                } else if node.value.eq_ignore_ascii_case("or") {
                    Self::BoolOr
                } else if node.value.eq_ignore_ascii_case("not") {
                    Self::BoolNot
                } else {
                    Self::Other
                }
            }
            TokenType::WordOperator => Self::from_word_operator(&node.value),
            TokenType::Operator => Self::from_symbol_operator(&node.value),
            _ if node.is_bracket_operator => Self::SquareBrackets,
            _ if node.is_multiplication_star => Self::Multiplication,
            _ => Self::Other,
        }
    }

    /// Classify a word operator without allocating. Uses case-insensitive
    /// comparison with whitespace normalization for multi-word operators.
    fn from_word_operator(value: &str) -> Self {
        // Single-word operators (fast path — simple eq_ignore_ascii_case)
        if value.eq_ignore_ascii_case("as") {
            return Self::As;
        }
        if value.eq_ignore_ascii_case("over")
            || value.eq_ignore_ascii_case("filter")
            || eq_ignore_case_ws(value, "within group")
        {
            return Self::OtherTight;
        }
        if value.eq_ignore_ascii_case("interval") || value.eq_ignore_ascii_case("some") {
            return Self::Other;
        }

        // Presence operators
        if value.eq_ignore_ascii_case("is")
            || eq_ignore_case_ws(value, "is not")
            || value.eq_ignore_ascii_case("isnull")
            || value.eq_ignore_ascii_case("notnull")
            || eq_ignore_case_ws(value, "is distinct from")
            || eq_ignore_case_ws(value, "is not distinct from")
            || value.eq_ignore_ascii_case("exists")
            || eq_ignore_case_ws(value, "not exists")
        {
            return Self::Presence;
        }

        // Membership operators
        if value.eq_ignore_ascii_case("in")
            || eq_ignore_case_ws(value, "not in")
            || eq_ignore_case_ws(value, "global not in")
            || eq_ignore_case_ws(value, "global in")
            || value.eq_ignore_ascii_case("like")
            || eq_ignore_case_ws(value, "not like")
            || eq_ignore_case_ws(value, "like any")
            || eq_ignore_case_ws(value, "like all")
            || eq_ignore_case_ws(value, "not like any")
            || eq_ignore_case_ws(value, "not like all")
            || value.eq_ignore_ascii_case("ilike")
            || eq_ignore_case_ws(value, "not ilike")
            || eq_ignore_case_ws(value, "ilike any")
            || eq_ignore_case_ws(value, "ilike all")
            || eq_ignore_case_ws(value, "not ilike any")
            || eq_ignore_case_ws(value, "not ilike all")
            || eq_ignore_case_ws(value, "similar to")
            || eq_ignore_case_ws(value, "not similar to")
            || value.eq_ignore_ascii_case("regexp")
            || eq_ignore_case_ws(value, "not regexp")
            || value.eq_ignore_ascii_case("rlike")
            || eq_ignore_case_ws(value, "not rlike")
            || value.eq_ignore_ascii_case("between")
            || eq_ignore_case_ws(value, "not between")
        {
            return Self::Membership;
        }

        Self::Other
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
#[path = "operator_precedence_test.rs"]
mod tests;
