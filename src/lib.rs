pub mod action;
pub mod analyzer;
pub mod api;
pub mod comment;
pub mod config;
pub mod dialect;
pub mod error;
pub mod formatter;
pub mod jinja_formatter;
pub mod line;
pub mod merger;
pub mod mode;
pub mod node;
pub mod node_manager;
pub mod operator_precedence;
pub mod query;
pub mod report;
pub mod rule;
pub mod rules;
pub mod segment;
pub mod splitter;
pub mod token;

// Re-export the main public API
pub use api::{format_string, get_matching_paths, run};
pub use mode::Mode;
