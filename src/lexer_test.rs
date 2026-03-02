use super::*;
use crate::action::Action;
use crate::token::TokenType;

#[test]
fn test_skip_prefix_whitespace() {
    assert_eq!(skip_prefix_whitespace(b"  hello"), 2);
    assert_eq!(skip_prefix_whitespace(b"\thello"), 1);
    assert_eq!(skip_prefix_whitespace(b"\nhello"), 0); // newline is NOT whitespace prefix
    assert_eq!(skip_prefix_whitespace(b"hello"), 0);
}

#[test]
fn test_scan_word() {
    assert_eq!(scan_word(b"select"), 6);
    assert_eq!(scan_word(b"my_table"), 8);
    assert_eq!(scan_word(b"foo123 bar"), 6);
    assert_eq!(scan_word(b"123abc"), 6); // numbers are word chars
}

#[test]
fn test_scan_number() {
    assert_eq!(scan_number(b"42"), 2);
    assert_eq!(scan_number(b"3.14"), 4);
    assert_eq!(scan_number(b"1e10"), 4);
    assert_eq!(scan_number(b"0xFF"), 4);
    assert_eq!(scan_number(b"0b1010"), 6);
    assert_eq!(scan_number(b"0o777"), 5);
    assert_eq!(scan_number(b"1_000"), 5);
    assert_eq!(scan_number(b"10L"), 3);
}

#[test]
fn test_scan_string() {
    assert_eq!(scan_string(b"'hello'"), 7);
    assert_eq!(scan_string(b"'it\\'s'"), 7);
    assert_eq!(scan_string(b"\"world\""), 7);
}

#[test]
fn test_lex_simple_tokens() {
    let r = lex_one("  SELECT", LexState::Main).unwrap();
    assert_eq!(r.prefix, "  ");
    assert_eq!(r.token_text.to_ascii_lowercase(), "select");

    let r = lex_one(",", LexState::Main).unwrap();
    assert_eq!(r.token_text, ",");

    let r = lex_one(";", LexState::Main).unwrap();
    assert_eq!(r.token_text, ";");

    let r = lex_one("\n", LexState::Main).unwrap();
    assert_eq!(r.token_text, "\n");
}

#[test]
fn test_lex_keyword_classification() {
    let r = lex_one("select", LexState::Main).unwrap();
    // Should be classified as HandleReservedKeyword wrapping UntermKeyword
    assert!(matches!(r.action, Action::HandleReservedKeyword { .. }));

    let r = lex_one("from", LexState::Main).unwrap();
    assert!(matches!(r.action, Action::HandleReservedKeyword { .. }));
}

#[test]
fn test_lex_multi_word() {
    let r = lex_one("left outer join", LexState::Main).unwrap();
    assert_eq!(r.token_text.to_ascii_lowercase(), "left outer join");

    let r = lex_one("order by", LexState::Main).unwrap();
    assert_eq!(r.token_text.to_ascii_lowercase(), "order by");

    let r = lex_one("union all", LexState::Main).unwrap();
    assert_eq!(r.token_text.to_ascii_lowercase(), "union all");
}

#[test]
fn test_lex_string_literals() {
    let r = lex_one("'hello'", LexState::Main).unwrap();
    assert_eq!(r.token_text, "'hello'");

    let r = lex_one("\"my_table\"", LexState::Main).unwrap();
    assert_eq!(r.token_text, "\"my_table\"");
}

#[test]
fn test_lex_comments() {
    let r = lex_one("-- comment\n", LexState::Main).unwrap();
    assert_eq!(r.token_text, "-- comment");
    assert!(matches!(r.action, Action::AddComment));

    let r = lex_one("/* block */", LexState::Main).unwrap();
    assert_eq!(r.token_text, "/* block */");
}

#[test]
fn test_lex_fmt_markers() {
    let r = lex_one("-- fmt: off", LexState::Main).unwrap();
    assert!(matches!(
        r.action,
        Action::AddNode {
            token_type: TokenType::FmtOff
        }
    ));

    let r = lex_one("-- fmt: on", LexState::Main).unwrap();
    assert!(matches!(
        r.action,
        Action::AddNode {
            token_type: TokenType::FmtOn
        }
    ));
}

#[test]
fn test_lex_jinja() {
    let r = lex_one("{{ x }}", LexState::Main).unwrap();
    assert_eq!(r.token_text, "{{ x }}");

    let r = lex_one("{% if x %}", LexState::Main).unwrap();
    assert_eq!(r.token_text, "{% if x %}");
    assert!(matches!(r.action, Action::HandleJinjaBlockStart));
}

#[test]
fn test_lex_operators() {
    let r = lex_one(">=", LexState::Main).unwrap();
    assert_eq!(r.token_text, ">=");

    let r = lex_one("::text", LexState::Main).unwrap();
    assert_eq!(r.token_text, "::");
}

#[test]
fn test_lex_fmt_off_mode() {
    let r = lex_one("anything here", LexState::FmtOff).unwrap();
    assert_eq!(r.token_text, "anything here");
    assert!(matches!(
        r.action,
        Action::AddNode {
            token_type: TokenType::Data
        }
    ));

    let r = lex_one("-- fmt: on", LexState::FmtOff).unwrap();
    assert!(matches!(
        r.action,
        Action::AddNode {
            token_type: TokenType::FmtOn
        }
    ));
}

#[test]
fn test_check_fmt_marker() {
    assert!(check_fmt_marker(b"-- fmt: off").is_some());
    assert!(check_fmt_marker(b"--fmt:off").is_some());
    assert!(check_fmt_marker(b"# fmt: on").is_some());
    assert!(check_fmt_marker(b"-- regular comment").is_none());
}

#[test]
fn test_compound_operators() {
    assert_eq!(scan_compound_operator(b">="), 2);
    assert_eq!(scan_compound_operator(b"->"), 2);
    assert_eq!(scan_compound_operator(b"->>"), 3);
    assert_eq!(scan_compound_operator(b"->->"), 4);
    assert_eq!(scan_compound_operator(b"||"), 2);
    assert_eq!(scan_compound_operator(b"<=>"), 3);
    assert_eq!(scan_compound_operator(b"!="), 2);
}

#[test]
fn test_dollar_string() {
    assert_eq!(scan_dollar_string(b"$$hello$$"), 9);
    assert_eq!(scan_dollar_string(b"$tag$hello$tag$"), 15);
}

#[test]
fn test_frame_clause() {
    let r = lex_one(
        "rows between unbounded preceding and current row",
        LexState::Main,
    );
    assert!(r.is_some());
    let r = r.unwrap();
    assert_eq!(
        r.token_text.to_ascii_lowercase(),
        "rows between unbounded preceding and current row"
    );
}

#[test]
fn test_select_top_multiline() {
    let r = lex_one("select\ntop\n25\n*\n", LexState::Main).unwrap();
    eprintln!("token_text: {:?}, match_len: {}", r.token_text, r.match_len);
    assert_eq!(r.token_text, "select\ntop\n25");
}

#[test]
fn test_union_all_multiline() {
    let r = lex_one("union\nall\n", LexState::Main).unwrap();
    eprintln!("token_text: {:?}, match_len: {}", r.token_text, r.match_len);
    assert_eq!(r.token_text, "union\nall");
}
