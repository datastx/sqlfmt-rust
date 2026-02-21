use thiserror::Error;

/// User-facing errors.
#[derive(Error, Debug)]
pub enum SqlfmtError {
    #[error("sqlfmt config error: {0}")]
    Config(String),

    #[error("sqlfmt unicode error: {0}")]
    Unicode(String),

    #[error("sqlfmt parsing error at position {position}: {message}")]
    Parsing { position: usize, message: String },

    #[error("sqlfmt bracket error: {0}")]
    Bracket(String),

    #[error("sqlfmt segment error: {0}")]
    Segment(String),

    #[error("sqlfmt equivalence error: {0}")]
    Equivalence(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
}

/// Internal control flow signals (never exposed to users).
#[derive(Debug)]
pub enum ControlFlow {
    CannotMerge,
}

pub type Result<T> = std::result::Result<T, SqlfmtError>;
