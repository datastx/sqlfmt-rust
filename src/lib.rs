pub(crate) mod action;
pub(crate) mod analyzer;
pub mod api;
pub(crate) mod comment;
pub(crate) mod config;
pub(crate) mod dialect;
pub mod error;
pub(crate) mod formatter;
pub(crate) mod jinja_formatter;
pub(crate) mod line;
pub(crate) mod merger;
pub mod mode;
pub(crate) mod node;
pub(crate) mod node_manager;
pub(crate) mod operator_precedence;
pub(crate) mod query;
pub mod report;
pub(crate) mod rule;
pub(crate) mod rules;
pub(crate) mod segment;
pub(crate) mod splitter;
pub(crate) mod string_utils;
pub(crate) mod token;

// Re-export the main public API
pub use api::{format_string, get_matching_paths, run};
pub use config::load_config;
pub use mode::Mode;
