//! Shared test helper functions for unit tests within the crate.
//!
//! Consolidates duplicated `make_node` variants and mode builders
//! from across 6+ test modules into one canonical location.

#![allow(dead_code)]

use compact_str::CompactString;

use crate::line::Line;
use crate::mode::Mode;
use crate::node::{Node, NodeIndex};
use crate::token::{Token, TokenType};

/// Create a standalone Node (not inserted into an arena).
pub(crate) fn make_node(tt: TokenType, value: &str) -> Node {
    Node::new(
        Token::new(tt, "", value, 0, value.len() as u32),
        None,
        CompactString::new(""),
        CompactString::from(value),
        0,
        0,
    )
}

/// Create a standalone Node with an explicit previous_node link.
pub(crate) fn make_node_with_prev(tt: TokenType, value: &str, prev: Option<NodeIndex>) -> Node {
    Node::new(
        Token::new(tt, "", value, 0, value.len() as u32),
        prev,
        CompactString::new(""),
        CompactString::from(value),
        0,
        0,
    )
}

/// Push a new Node into an arena and return its index.
/// Automatically links `previous_node` to the last element in the arena.
pub(crate) fn push_node(
    arena: &mut Vec<Node>,
    tt: TokenType,
    val: &str,
    prefix: &str,
) -> NodeIndex {
    let idx = arena.len();
    let prev = if idx > 0 { Some(idx - 1) } else { None };
    arena.push(Node::new(
        Token::new(tt, "", val, 0, val.len() as u32),
        prev,
        CompactString::from(prefix),
        CompactString::from(val),
        0,
        0,
    ));
    idx
}

/// Create a simple Line containing one content node + a trailing newline node.
pub(crate) fn make_simple_line(arena: &mut Vec<Node>, tt: TokenType, val: &str) -> Line {
    let nl = push_node(arena, TokenType::Newline, "\n", "");
    let content = push_node(arena, tt, val, "");
    let mut line = Line::new(None);
    line.append_node(content);
    line.append_node(nl);
    line
}

/// Default formatting mode (polyglot dialect, line_length=88).
pub(crate) fn default_mode() -> Mode {
    Mode::default()
}

/// ClickHouse dialect mode.
pub(crate) fn clickhouse_mode() -> Mode {
    Mode {
        dialect_name: "clickhouse".to_string(),
        ..Mode::default()
    }
}

/// DuckDB dialect mode.
pub(crate) fn duckdb_mode() -> Mode {
    Mode {
        dialect_name: "duckdb".to_string(),
        ..Mode::default()
    }
}
