use sqlfmt::{format_string, Mode};
use std::hint::black_box;

fn main() {
    let source = std::fs::read_to_string(
        "tests/data/unformatted/216_gitlab_zuora_revenue_revenue_contract_line_source.sql",
    )
    .expect("failed to read test file");

    let mode = Mode::default();

    for _ in 0..5000 {
        let result = format_string(black_box(&source), black_box(&mode));
        black_box(result).ok();
    }
}
