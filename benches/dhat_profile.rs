use sqlfmt::{format_string, Mode};
use std::fs;
use std::path::Path;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const SENTINEL: &str = ")))))__SQLFMT_OUTPUT__(((((";

fn collect_fixtures(dir: &str) -> Vec<(String, String)> {
    let path = Path::new("tests/data").join(dir);
    let mut fixtures: Vec<(String, String)> = Vec::new();

    if let Ok(entries) = fs::read_dir(&path) {
        for entry in entries.flatten() {
            let file_path = entry.path();
            if file_path.extension().map_or(false, |e| e == "sql") {
                let name = file_path.file_stem().unwrap().to_string_lossy().to_string();
                let content = fs::read_to_string(&file_path).unwrap();
                let input = if let Some(pos) = content.find(SENTINEL) {
                    content[..pos].to_string()
                } else {
                    content
                };
                fixtures.push((name, input));
            }
        }
    }

    fixtures.sort_by(|a, b| a.0.cmp(&b.0));
    fixtures
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let mode = Mode::default();
    let mut all = collect_fixtures("unformatted");
    all.extend(collect_fixtures("preformatted"));
    all.extend(collect_fixtures("errors"));

    eprintln!("Profiling {} fixtures...", all.len());

    for (name, sql) in &all {
        eprintln!("  {} ({} bytes)", name, sql.len());
        let _ = format_string(sql, &mode);
    }

    eprintln!("Done. dhat results written to dhat-heap.json");
}
