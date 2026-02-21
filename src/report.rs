use std::path::PathBuf;

/// Status of formatting a single file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStatus {
    /// File was already formatted correctly.
    Unchanged,
    /// File was reformatted (or would be, in check mode).
    Changed,
    /// An error occurred while processing the file.
    Error,
}

/// Result of formatting a single file.
#[derive(Debug, Clone)]
pub struct FileResult {
    pub path: PathBuf,
    pub status: FileStatus,
    pub error: Option<String>,
}

/// Aggregated report of formatting results.
#[derive(Debug, Default)]
pub struct Report {
    pub results: Vec<FileResult>,
}

impl Report {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    pub fn add(&mut self, result: FileResult) {
        self.results.push(result);
    }

    pub fn total(&self) -> usize {
        self.results.len()
    }

    pub fn unchanged(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.status == FileStatus::Unchanged)
            .count()
    }

    pub fn changed(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.status == FileStatus::Changed)
            .count()
    }

    pub fn errors(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.status == FileStatus::Error)
            .count()
    }

    pub fn has_errors(&self) -> bool {
        self.errors() > 0
    }

    pub fn has_changes(&self) -> bool {
        self.changed() > 0
    }

    /// Generate a summary string.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        parts.push(format!("{} file(s) processed", self.total()));
        if self.changed() > 0 {
            parts.push(format!("{} reformatted", self.changed()));
        }
        if self.unchanged() > 0 {
            parts.push(format!("{} unchanged", self.unchanged()));
        }
        if self.errors() > 0 {
            parts.push(format!("{} error(s)", self.errors()));
        }
        parts.join(", ")
    }

    /// Print error details.
    pub fn print_errors(&self) {
        for result in &self.results {
            if let Some(ref error) = result.error {
                eprintln!("error: {}: {}", result.path.display(), error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_summary() {
        let mut report = Report::new();
        report.add(FileResult {
            path: PathBuf::from("a.sql"),
            status: FileStatus::Changed,
            error: None,
        });
        report.add(FileResult {
            path: PathBuf::from("b.sql"),
            status: FileStatus::Unchanged,
            error: None,
        });
        report.add(FileResult {
            path: PathBuf::from("c.sql"),
            status: FileStatus::Error,
            error: Some("parse error".to_string()),
        });

        assert_eq!(report.total(), 3);
        assert_eq!(report.changed(), 1);
        assert_eq!(report.unchanged(), 1);
        assert_eq!(report.errors(), 1);
        assert!(report.has_errors());
        assert!(report.has_changes());
    }

    #[test]
    fn test_empty_report() {
        let report = Report::new();
        assert_eq!(report.total(), 0);
        assert_eq!(report.changed(), 0);
        assert_eq!(report.unchanged(), 0);
        assert_eq!(report.errors(), 0);
        assert!(!report.has_errors());
        assert!(!report.has_changes());
        assert!(report.summary().contains("0 file(s) processed"));
    }

    #[test]
    fn test_all_unchanged_summary() {
        let mut report = Report::new();
        for name in &["a.sql", "b.sql", "c.sql"] {
            report.add(FileResult {
                path: PathBuf::from(name),
                status: FileStatus::Unchanged,
                error: None,
            });
        }
        assert_eq!(report.total(), 3);
        assert_eq!(report.unchanged(), 3);
        assert_eq!(report.changed(), 0);
        let summary = report.summary();
        assert!(summary.contains("3 file(s) processed"));
        assert!(summary.contains("3 unchanged"));
        assert!(!summary.contains("reformatted"));
    }

    #[test]
    fn test_all_changed_summary() {
        let mut report = Report::new();
        for name in &["a.sql", "b.sql"] {
            report.add(FileResult {
                path: PathBuf::from(name),
                status: FileStatus::Changed,
                error: None,
            });
        }
        assert_eq!(report.total(), 2);
        assert_eq!(report.changed(), 2);
        let summary = report.summary();
        assert!(summary.contains("reformatted"));
    }

    #[test]
    fn test_all_errors_summary() {
        let mut report = Report::new();
        for name in &["a.sql", "b.sql"] {
            report.add(FileResult {
                path: PathBuf::from(name),
                status: FileStatus::Error,
                error: Some("parse error".to_string()),
            });
        }
        assert_eq!(report.total(), 2);
        assert_eq!(report.errors(), 2);
        let summary = report.summary();
        assert!(summary.contains("error(s)"));
    }

    #[test]
    fn test_mixed_summary() {
        let mut report = Report::new();
        report.add(FileResult {
            path: PathBuf::from("a.sql"),
            status: FileStatus::Changed,
            error: None,
        });
        report.add(FileResult {
            path: PathBuf::from("b.sql"),
            status: FileStatus::Unchanged,
            error: None,
        });
        report.add(FileResult {
            path: PathBuf::from("c.sql"),
            status: FileStatus::Error,
            error: Some("err".to_string()),
        });
        let summary = report.summary();
        assert!(summary.contains("3 file(s) processed"));
        assert!(summary.contains("reformatted"));
        assert!(summary.contains("unchanged"));
        assert!(summary.contains("error(s)"));
    }

    #[test]
    fn test_has_errors_false() {
        let mut report = Report::new();
        report.add(FileResult {
            path: PathBuf::from("a.sql"),
            status: FileStatus::Changed,
            error: None,
        });
        report.add(FileResult {
            path: PathBuf::from("b.sql"),
            status: FileStatus::Unchanged,
            error: None,
        });
        assert!(!report.has_errors());
    }

    #[test]
    fn test_has_changes_false() {
        let mut report = Report::new();
        report.add(FileResult {
            path: PathBuf::from("a.sql"),
            status: FileStatus::Unchanged,
            error: None,
        });
        assert!(!report.has_changes());
    }

    #[test]
    fn test_has_errors_true() {
        let mut report = Report::new();
        report.add(FileResult {
            path: PathBuf::from("a.sql"),
            status: FileStatus::Error,
            error: Some("fail".to_string()),
        });
        assert!(report.has_errors());
    }

    #[test]
    fn test_has_changes_true() {
        let mut report = Report::new();
        report.add(FileResult {
            path: PathBuf::from("a.sql"),
            status: FileStatus::Changed,
            error: None,
        });
        assert!(report.has_changes());
    }
}
