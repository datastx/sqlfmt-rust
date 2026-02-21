use std::io::{self, Read};
use std::path::PathBuf;

use clap::Parser;

use sqlfmt::mode::Mode;

/// sqlfmt - An opinionated SQL formatter.
/// Optimized for Snowflake and DuckDB.
#[derive(Parser, Debug)]
#[command(name = "sqlfmt", version, about)]
struct Cli {
    /// Files or directories to format. Use "-" to read from stdin.
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Maximum line length.
    #[arg(short = 'l', long, default_value_t = 88)]
    line_length: usize,

    /// SQL dialect: polyglot, duckdb, clickhouse.
    #[arg(short = 'd', long, default_value = "polyglot")]
    dialect: String,

    /// Check formatting without writing changes.
    #[arg(long)]
    check: bool,

    /// Show formatting diff.
    #[arg(long)]
    diff: bool,

    /// Skip safety equivalence check (faster).
    #[arg(long)]
    fast: bool,

    /// Disable Jinja template formatting.
    #[arg(long)]
    no_jinjafmt: bool,

    /// Glob patterns to exclude.
    #[arg(long)]
    exclude: Vec<String>,

    /// File encoding.
    #[arg(long, default_value = "utf-8")]
    encoding: String,

    /// Verbose output.
    #[arg(short, long)]
    verbose: bool,

    /// Quiet output (errors only).
    #[arg(short, long)]
    quiet: bool,

    /// Disable progress bar.
    #[arg(long)]
    no_progressbar: bool,

    /// Force color output.
    #[arg(long)]
    force_color: bool,

    /// Disable color output.
    #[arg(long)]
    no_color: bool,

    /// Number of threads for parallel processing (0 = all cores).
    #[arg(short = 't', long, default_value_t = 0)]
    threads: usize,

    /// Disable multi-threaded processing.
    #[arg(long)]
    single_process: bool,

    /// Reset formatting cache.
    #[arg(short = 'k', long)]
    reset_cache: bool,

    /// Path to config file (pyproject.toml or sqlfmt.toml).
    #[arg(long)]
    config: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();

    let is_stdin = cli.files.len() == 1 && cli.files[0].to_string_lossy() == "-";

    let base_mode = match sqlfmt::load_config(&cli.files, cli.config.as_deref()) {
        Ok(mode) => mode,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            std::process::exit(2);
        }
    };

    let mode = Mode {
        line_length: cli.line_length,
        dialect_name: cli.dialect,
        check: cli.check,
        diff: cli.diff,
        fast: cli.fast,
        no_jinjafmt: cli.no_jinjafmt,
        exclude: if cli.exclude.is_empty() {
            base_mode.exclude
        } else {
            cli.exclude
        },
        encoding: cli.encoding,
        verbose: cli.verbose,
        quiet: cli.quiet,
        no_progressbar: cli.no_progressbar,
        no_color: cli.no_color,
        force_color: cli.force_color,
        threads: cli.threads,
        single_process: cli.single_process,
        reset_cache: cli.reset_cache,
    };

    if is_stdin {
        let mut source = String::new();
        if let Err(e) = io::stdin().read_to_string(&mut source) {
            eprintln!("Error reading stdin: {}", e);
            std::process::exit(2);
        }

        match sqlfmt::format_string(&source, &mode) {
            Ok(formatted) => {
                print!("{}", formatted);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(2);
            }
        }
    } else {
        let report = sqlfmt::run(&cli.files, &mode);

        if !mode.quiet {
            print_verbose_results(&report, &mode);
            eprintln!("{}", report.summary());
        }

        report.print_errors();

        if report.has_errors() {
            std::process::exit(2);
        } else if mode.check && report.has_changes() {
            std::process::exit(1);
        }
    }
}

fn print_verbose_results(report: &sqlfmt::report::Report, mode: &Mode) {
    if !mode.verbose {
        return;
    }
    for result in &report.results {
        match result.status {
            sqlfmt::report::FileStatus::Changed => {
                eprintln!("reformatted {}", result.path.display());
            }
            sqlfmt::report::FileStatus::Error => {
                eprintln!(
                    "error: {}: {}",
                    result.path.display(),
                    result.error.as_deref().unwrap_or("unknown error")
                );
            }
            sqlfmt::report::FileStatus::Unchanged => {}
        }
    }
}
