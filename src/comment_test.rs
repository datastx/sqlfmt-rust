use super::*;
use crate::token::TokenType;

fn make_comment(text: &str, standalone: bool) -> Comment {
    Comment::new(
        Token::new(TokenType::Comment, "", text, 0, text.len() as u32),
        standalone,
        None,
    )
}

#[test]
fn test_marker_detection() {
    assert_eq!(make_comment("-- hello", false).marker(), "--");
    assert_eq!(make_comment("# hello", false).marker(), "#");
    assert_eq!(make_comment("// hello", false).marker(), "//");
    assert_eq!(make_comment("/* hello */", false).marker(), "/*");
    assert_eq!(make_comment("{# hello #}", false).marker(), "{#");
    assert_eq!(make_comment("{#- hello #}", false).marker(), "{#-");
}

#[test]
fn test_body_extraction() {
    assert_eq!(make_comment("-- hello world", false).body(), "hello world");
    assert_eq!(make_comment("--   spaces  ", false).body(), "spaces  ");
    assert_eq!(make_comment("--", false).body(), "");
}

#[test]
fn test_inline_rendering() {
    let c = make_comment("-- inline", false);
    assert_eq!(c.render_inline(), "  -- inline");
}

#[test]
fn test_standalone_rendering() {
    let c = make_comment("-- standalone", true);
    assert_eq!(c.render_standalone("    ", 88), "    -- standalone\n");
}

#[test]
fn test_multiline_detection() {
    assert!(make_comment("-- line1\n-- line2", true).is_multiline());
    assert!(!make_comment("-- single", true).is_multiline());
}

#[test]
fn test_c_style_detection() {
    assert!(make_comment("/* block */", false).is_c_style());
    assert!(!make_comment("-- line", false).is_c_style());
}

#[test]
fn test_is_inline() {
    // Non-standalone, non-multiline => inline
    assert!(make_comment("-- inline", false).is_inline());

    // Standalone => NOT inline
    assert!(!make_comment("-- standalone", true).is_inline());

    // Multiline => NOT inline
    assert!(!make_comment("-- line1\n-- line2", false).is_inline());

    // Single-line C-style, non-standalone => inline (for hints like /*+ ... */)
    assert!(make_comment("/* block */", false).is_inline());

    // Multiline C-style => NOT inline
    assert!(!make_comment("/* line1\n   line2 */", false).is_inline());
}

#[test]
fn test_empty_comment() {
    let c = make_comment("--", true);
    assert_eq!(c.marker(), "--");
    assert_eq!(c.body(), "");
    assert_eq!(c.render_standalone("", 88), "--\n");
}

#[test]
fn test_jinja_comment() {
    let c = make_comment("{# this is a jinja comment #}", true);
    assert!(c.is_jinja_comment());
    assert_eq!(c.marker(), "{#");
}

#[test]
fn test_render_multiline_comment() {
    let c = make_comment("/* line1\n   line2 */", true);
    assert!(c.is_multiline());
    assert!(c.is_c_style());
    let rendered = c.render_standalone("    ", 88);
    assert!(rendered.starts_with("    "));
    assert!(rendered.contains("line1"));
}

#[test]
fn test_wrap_long_comment() {
    let long_text = "-- this is a very long comment that should be wrapped because it exceeds the maximum line length limit of the formatter tool";
    let c = make_comment(long_text, true);
    let rendered = c.render_standalone("", 40);
    // Should produce multiple lines
    let line_count = rendered.lines().count();
    assert!(
        line_count > 1,
        "Long comment should wrap: got {} lines from: {}",
        line_count,
        rendered
    );
}

#[test]
fn test_jinja_comment_not_wrapped() {
    // Comments with Jinja expressions should NOT be wrapped
    let text = "-- depends_on: {{ ref('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') }}";
    let c = make_comment(text, true);
    let rendered = c.render_standalone("", 88);
    let line_count = rendered.lines().count();
    assert_eq!(
        line_count, 1,
        "Jinja comment should NOT wrap: got {} lines from: {}",
        line_count, rendered
    );
}

#[test]
fn test_hash_comment_marker() {
    let c = make_comment("# python style comment", false);
    assert_eq!(c.marker(), "#");
    assert_eq!(c.body(), "python style comment");
}

#[test]
fn test_double_slash_comment_marker() {
    let c = make_comment("// C++ style comment", false);
    assert_eq!(c.marker(), "//");
    assert_eq!(c.body(), "C++ style comment");
}
