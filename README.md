# sqlfmt

An opinionated SQL formatter written in Rust. Ported from [Python sqlfmt](https://github.com/tconbeer/sqlfmt), optimized for Snowflake and DuckDB.

## Installation

### Download a prebuilt binary

Prebuilt binaries are available for Linux and macOS from
[GitHub Releases](https://github.com/datastx/sqlfmt-rust/releases/latest).

> **Note:** Replace `VERSION` below with the release version (e.g. `v0.3.1`).
> Check the [releases page](https://github.com/datastx/sqlfmt-rust/releases/latest)
> for the latest version.

**Linux (x86_64):**

```bash
VERSION=v0.3.1
curl -fsSL "https://github.com/datastx/sqlfmt-rust/releases/download/${VERSION}/sqlfmt-${VERSION}-x86_64-unknown-linux-musl.tar.gz" \
  | tar xz
sudo mv "sqlfmt-${VERSION}-x86_64-unknown-linux-musl/sqlfmt" /usr/local/bin/
```

**Linux (aarch64 / ARM64):**

```bash
VERSION=v0.3.1
curl -fsSL "https://github.com/datastx/sqlfmt-rust/releases/download/${VERSION}/sqlfmt-${VERSION}-aarch64-unknown-linux-musl.tar.gz" \
  | tar xz
sudo mv "sqlfmt-${VERSION}-aarch64-unknown-linux-musl/sqlfmt" /usr/local/bin/
```

**macOS (Apple Silicon):**

```bash
VERSION=v0.3.1
curl -fsSL "https://github.com/datastx/sqlfmt-rust/releases/download/${VERSION}/sqlfmt-${VERSION}-aarch64-apple-darwin.tar.gz" \
  | tar xz
sudo mv "sqlfmt-${VERSION}-aarch64-apple-darwin/sqlfmt" /usr/local/bin/
```

**macOS (Intel):**

```bash
VERSION=v0.3.1
curl -fsSL "https://github.com/datastx/sqlfmt-rust/releases/download/${VERSION}/sqlfmt-${VERSION}-x86_64-apple-darwin.tar.gz" \
  | tar xz
sudo mv "sqlfmt-${VERSION}-x86_64-apple-darwin/sqlfmt" /usr/local/bin/
```

### Verify the download

Each release includes a `checksums-sha256.txt` file. After downloading, verify:

```bash
sha256sum -c checksums-sha256.txt
```

### Build from source

Requires [Rust](https://rustup.rs/) 1.70+.

```bash
git clone https://github.com/datastx/sqlfmt-rust.git
cd sqlfmt-rust
cargo install --path .
```

## Usage

Format files or directories in place:

```bash
sqlfmt .
sqlfmt queries/
sqlfmt path/to/query.sql
```

Read from stdin, write to stdout:

```bash
echo "SELECT   a,b FROM t WHERE x=1" | sqlfmt -
```

Check formatting without modifying files (exit code 1 if changes needed):

```bash
sqlfmt --check .
```

Show a diff of what would change:

```bash
sqlfmt --diff .
```

### Options

```
Usage: sqlfmt [OPTIONS] <FILES>...

Arguments:
  <FILES>...  Files or directories to format. Use "-" to read from stdin

Options:
  -l, --line-length <LINE_LENGTH>  Maximum line length [default: 88]
  -d, --dialect <DIALECT>          SQL dialect: polyglot, duckdb, clickhouse [default: polyglot]
      --check                      Check formatting without writing changes
      --diff                       Show formatting diff
      --fast                       Skip safety equivalence check (faster)
      --no-jinjafmt                Disable Jinja template formatting
      --exclude <EXCLUDE>          Glob patterns to exclude
      --encoding <ENCODING>        File encoding [default: utf-8]
  -v, --verbose                    Verbose output
  -q, --quiet                      Quiet output (errors only)
      --no-progressbar             Disable progress bar
      --force-color                Force color output
      --no-color                   Disable color output
  -t, --threads <THREADS>          Number of threads for parallel processing (0 = all cores) [default: 0]
      --single-process             Disable multi-threaded processing
  -k, --reset-cache                Reset formatting cache
      --config <CONFIG>            Path to config file (pyproject.toml or sqlfmt.toml)
  -h, --help                       Print help
  -V, --version                    Print version
```

### Environment variables

You can set environment variables to enable performance options without passing flags on every invocation:

| Variable | Equivalent flag | Description |
|---|---|---|
| `SQLFMT_FAST=1` | `--fast` | Skip the safety equivalence check for faster formatting |
| `SQLFMT_THREADS=N` | `--threads N` | Number of parallel threads (`0` = all cores) |

Accepted values for `SQLFMT_FAST`: `1`, `true`, `yes` (case-insensitive). CLI flags always take precedence over environment variables.

```bash
# Format a large directory as fast as possible
export SQLFMT_FAST=1
export SQLFMT_THREADS=8
sqlfmt .
```

### Configuration file

sqlfmt reads settings from `sqlfmt.toml` or the `[tool.sqlfmt]` section of `pyproject.toml`:

```toml
# sqlfmt.toml
line_length = 100
dialect = "duckdb"
exclude = ["migrations/**"]
```

## Supported platforms

| Platform             | Architecture | Binary target                    |
| -------------------- | ------------ | -------------------------------- |
| Linux                | x86_64       | `x86_64-unknown-linux-musl`      |
| Linux                | aarch64      | `aarch64-unknown-linux-musl`     |
| macOS (Apple Silicon)| aarch64      | `aarch64-apple-darwin`           |
| macOS (Intel)        | x86_64       | `x86_64-apple-darwin`            |

Linux binaries are statically linked (musl) and have no runtime dependencies.

## Acknowledgments

This project is a Rust port of [sqlfmt](https://github.com/tconbeer/sqlfmt) by
[Ted Conbeer](https://github.com/tconbeer), originally written in Python and
licensed under the Apache License 2.0. The SQL test fixture files in `tests/data/`
are derived from the original project. See the [NOTICE](NOTICE) file for details.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for details.
