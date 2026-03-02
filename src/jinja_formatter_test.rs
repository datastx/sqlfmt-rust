use super::*;

#[test]
fn test_normalize_expression() {
    let formatter = JinjaFormatter::new(88);
    let result = formatter.normalize_expression("{{  my_var  }}");
    assert_eq!(result, Some("{{ my_var }}".to_string()));
}

#[test]
fn test_normalize_statement() {
    let formatter = JinjaFormatter::new(88);
    let result = formatter.normalize_statement("{%  if  condition  %}");
    assert_eq!(result, Some("{% if condition %}".to_string()));
}

#[test]
fn test_normalize_with_whitespace_control() {
    let formatter = JinjaFormatter::new(88);
    let result = formatter.normalize_statement("{%- if condition -%}");
    assert_eq!(result, Some("{%- if condition -%}".to_string()));
}

#[test]
fn test_normalize_expression_with_dash() {
    let formatter = JinjaFormatter::new(88);
    let result = formatter.normalize_expression("{{- my_var -}}");
    assert_eq!(result, Some("{{- my_var -}}".to_string()));
}

#[test]
fn test_operator_spaces() {
    let result = JinjaFormatter::add_operator_spaces("a+b");
    assert_eq!(result, "a + b");

    let result = JinjaFormatter::add_operator_spaces("x|filter");
    assert_eq!(result, "x | filter");

    // Inside strings should not be modified
    let result = JinjaFormatter::add_operator_spaces("'a+b'");
    assert_eq!(result, "'a+b'");
}

#[test]
fn test_multiline_expression() {
    let formatter = JinjaFormatter::new(88);
    let value = r#"{{ config(target_database="analytics", target_schema=target.schema + "_snapshots", unique_key="id", strategy="timestamp", updated_at="updated_at") }}"#;
    let result = formatter.format_expression_multiline(value, 4);
    assert!(result.is_some());
    let multiline = result.unwrap();
    assert!(multiline.contains('\n'));
    assert!(multiline.starts_with("{{"));
    assert!(multiline.ends_with("}}"));
}

#[test]
fn test_split_by_commas() {
    let result = split_by_commas(r#"a="1", b="2", c="3""#);
    assert_eq!(result.len(), 3);
}

#[test]
fn test_split_by_commas_nested() {
    let result = split_by_commas(r#"a=func(1, 2), b="3""#);
    assert_eq!(result.len(), 2);
}

#[test]
fn test_normalize_inner_whitespace() {
    let result = JinjaFormatter::normalize_inner_whitespace("a  +  b");
    assert_eq!(result, "a + b");

    let result = JinjaFormatter::normalize_inner_whitespace("a\n  +\n  b");
    assert_eq!(result, "a + b");

    // Whitespace inside strings preserved
    let result = JinjaFormatter::normalize_inner_whitespace("'hello  world'");
    assert_eq!(result, "'hello  world'");
}
