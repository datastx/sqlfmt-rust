//! # sqlfmt
//!
//! An opinionated SQL formatter optimized for Snowflake and DuckDB.
//! Ported from [Python sqlfmt](https://github.com/tconbeer/sqlfmt).
//!
//! ## Quick Start
//!
//! ```
//! use sqlfmt::{format_string, Mode};
//!
//! let mode = Mode::default();
//! let result = format_string("SELECT   a,b FROM t WHERE x=1\n", &mode).unwrap();
//! assert!(result.contains("select"));
//! ```
//!
//! ## Library API
//!
//! The primary entry point for library users is [`format_string`], which formats
//! a SQL string in memory. For batch file processing, use [`run`].
//!
//! Configuration is controlled by the [`Mode`] struct, which supports all
//! formatting options (line length, dialect, safety checks, etc.).

pub(crate) mod action;
pub(crate) mod analyzer;
pub mod api;
pub(crate) mod comment;
pub(crate) mod config;
pub(crate) mod dialect;
pub mod error;
pub(crate) mod formatter;
pub(crate) mod jinja_formatter;
pub(crate) mod lexer;
pub(crate) mod line;
pub(crate) mod merger;
pub mod mode;
pub(crate) mod node;
pub(crate) mod node_manager;
pub(crate) mod operator_precedence;
pub(crate) mod query;
pub mod report;
pub(crate) mod segment;
pub(crate) mod splitter;
pub(crate) mod string_utils;
pub(crate) mod token;

// Re-export the main public API
pub use api::{format_string, get_matching_paths, run};
pub use config::load_config;
pub use mode::Mode;
