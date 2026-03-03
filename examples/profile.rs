use sqlfmt::{format_string, Mode};

fn main() {
    let path = "tests/data/unformatted/216_gitlab_zuora_revenue_revenue_contract_line_source.sql";
    let content = std::fs::read_to_string(path).expect("Failed to read test file");

    // Extract input portion (before sentinel)
    let sql = if let Some(pos) = content.find(")))))__SQLFMT_OUTPUT__(((((") {
        &content[..pos]
    } else {
        &content
    };

    let mode = Mode::default();
    let iterations = 5000;

    eprintln!(
        "Formatting {} ({} bytes) x {} iterations...",
        path,
        sql.len(),
        iterations
    );

    for _ in 0..iterations {
        let _ = format_string(sql, &mode).unwrap();
    }

    eprintln!("Done.");
}
