use super::*;

#[test]
fn test_empty_query() {
    let q = Query::new(88, Vec::new());
    let arena: Vec<Node> = Vec::new();
    assert_eq!(q.render(&arena), "");
}

#[test]
fn test_comment_only_query() {
    // Use the full pipeline to verify comment-only input
    let mode = crate::mode::Mode::default();
    let result = crate::api::format_string("-- comment\n", &mode).unwrap();
    assert!(
        result.contains("-- comment"),
        "Comment should be preserved: {}",
        result
    );
}

#[test]
fn test_whitespace_only_query() {
    let mode = crate::mode::Mode::default();
    let result = crate::api::format_string("\n\n\n", &mode).unwrap();
    // Whitespace-only should produce empty or minimal output
    assert!(
        result.trim().is_empty(),
        "Whitespace-only should produce empty output: {:?}",
        result
    );
}
