pub mod common;
pub mod core;

use crate::action::Action;
use crate::rule::Rule;
use crate::token::TokenType;

/// Build the MAIN rule set used by the Polyglot (default) dialect.
/// This adds SQL keywords, operators, and statement handling on top of CORE rules.
pub fn main_rules() -> Vec<Rule> {
    let mut rules = core::core_rules();

    // ---- Statement start/end (CASE/END) ----

    rules.push(Rule::new(
        "statement_start",
        1000,
        r"(case)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::StatementStart,
            }),
        },
    ));

    rules.push(Rule::new(
        "statement_end",
        1010,
        r"(end)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::StatementEnd,
            }),
        },
    ));

    // ---- Unterminated keywords ----
    // These are top-level SQL keywords that start new clauses.

    // CASE block keywords: WHEN, THEN, ELSE
    let case_keywords = vec![
        ("when", 1020),
        ("then", 1021),
        ("else", 1022),
    ];
    for (kw, priority) in &case_keywords {
        rules.push(Rule::new(
            &format!("case_{}", kw),
            *priority,
            &format!(r"({})\b", kw),
            Action::HandleReservedKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            },
        ));
    }

    // Basic query clauses
    let unterm_keywords = vec![
        ("with", 1050),
        ("select", 1051),
        ("distinct", 1052),
        ("all", 1053),
        ("top", 1054),
    ];
    for (kw, priority) in &unterm_keywords {
        rules.push(Rule::new(
            &format!("unterm_{}", kw),
            *priority,
            &format!(r"({})\b", kw),
            Action::HandleReservedKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            },
        ));
    }

    // FROM and related clauses
    rules.push(Rule::new(
        "unterm_from",
        1060,
        r"(from)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            }),
        },
    ));

    rules.push(Rule::new(
        "unterm_delete_from",
        1059,
        r"(delete\s+from)\b",
        Action::AddNode {
            token_type: TokenType::UntermKeyword,
        },
    ));

    // JOIN types
    let join_keywords = [
        "join",
        r"inner\s+join",
        r"cross\s+join",
        r"full\s+outer\s+join",
        r"full\s+join",
        r"left\s+outer\s+join",
        r"left\s+join",
        r"right\s+outer\s+join",
        r"right\s+join",
        r"natural\s+join",
        r"lateral\s+join",
        r"cross\s+lateral\s+join",
        r"left\s+semi\s+join",
        r"right\s+semi\s+join",
        r"left\s+anti\s+join",
        r"right\s+anti\s+join",
        r"asof\s+join",
        r"left\s+asof\s+join",
        r"global\s+join",
        r"global\s+inner\s+join",
        r"global\s+left\s+join",
        r"global\s+right\s+join",
        r"global\s+full\s+join",
    ];
    let join_pattern = join_keywords
        .iter()
        .map(|k| format!("{}", k))
        .collect::<Vec<_>>()
        .join("|");
    rules.push(Rule::new(
        "unterm_join",
        1070,
        &format!(r"({})\b", join_pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));

    // USING (in JOIN context)
    rules.push(Rule::new(
        "unterm_using",
        1071,
        r"(using)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            }),
        },
    ));

    // WHERE, GROUP BY, HAVING, etc.
    let where_etc = vec![
        ("where", 1080),
        (r"group\s+by", 1090),
        ("having", 1100),
        ("window", 1101),
        (r"qualify", 1102), // Snowflake QUALIFY
        (r"order\s+by", 1110),
        ("limit", 1120),
        ("offset", 1121),
        ("fetch", 1122),
        ("for", 1123),
        ("values", 1130),
        ("returning", 1140),
        ("into", 1150),
        ("lateral", 1160),
        ("pivot", 1161),
        ("unpivot", 1162),
        (r"match_recognize", 1163),
        ("connect", 1164),
        (r"start\s+with", 1165),
    ];
    for (kw, priority) in &where_etc {
        rules.push(Rule::new(
            &format!("unterm_{}", kw.replace(r"\s+", "_")),
            *priority,
            &format!(r"({})\b", kw),
            Action::HandleReservedKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            },
        ));
    }

    // ---- ON keyword ----
    rules.push(Rule::new(
        "on",
        1180,
        r"(on)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::On,
                }),
            }),
        },
    ));

    // ---- Word operators ----
    let word_ops = vec![
        "as",
        "between",
        "cube",
        "exists",
        "filter",
        "grouping sets",
        "ilike",
        "in",
        r"is\s+not\s+distinct\s+from",
        r"is\s+distinct\s+from",
        r"is\s+not",
        "is",
        "isnull",
        "like",
        r"not\s+between",
        r"not\s+ilike",
        r"not\s+in",
        r"not\s+like",
        r"not\s+similar\s+to",
        "notnull",
        "over",
        "pivot",
        "respect nulls",
        "ignore nulls",
        "rollup",
        r"similar\s+to",
        "unpivot",
        r"within\s+group",
        "tablesample",
        "asc",
        "desc",
        r"nulls\s+first",
        r"nulls\s+last",
    ];
    let word_op_pattern = word_ops
        .iter()
        .map(|op| format!("{}", op))
        .collect::<Vec<_>>()
        .join("|");
    rules.push(Rule::new(
        "word_operator",
        1200,
        &format!(r"({})\b", word_op_pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::WordOperator,
            }),
        },
    ));

    // ---- Boolean operators ----
    rules.push(Rule::new(
        "boolean_operator",
        1300,
        r"(and|or|not)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::BooleanOperator,
            }),
        },
    ));

    // ---- Set operators ----
    rules.push(Rule::new(
        "set_operator",
        1320,
        r"(union\s+all|union\s+distinct|union\s+by\s+name|union|intersect\s+all|intersect\s+distinct|intersect|except\s+all|except\s+distinct|except|minus)\b",
        Action::HandleSetOperator,
    ));

    // ---- DDL keywords (passthrough) ----
    rules.push(Rule::new(
        "explain",
        2000,
        r"(explain)\b",
        Action::AddNode {
            token_type: TokenType::UntermKeyword,
        },
    ));

    // Unsupported DDL: these are passed through with minimal formatting
    let ddl_keywords = vec![
        r"create\s+or\s+replace",
        "create",
        "alter",
        "drop",
        "delete",
        r"insert\s+overwrite",
        r"insert\s+into",
        "insert",
        r"update",
        r"merge\s+into",
        "merge",
        "truncate",
        "rename",
        "set",
        "unset",
        "use",
        "execute",
        "call",
        "begin",
        "commit",
        "rollback",
        "copy",
        "clone",
        r"list\b",
        "remove",
        r"get\b",
        "put",
        "describe",
        "desc",
        "show",
        "comment",
        r"add\b",
        "undrop",
    ];
    let ddl_pattern = ddl_keywords
        .iter()
        .map(|k| format!("{}", k))
        .collect::<Vec<_>>()
        .join("|");
    rules.push(Rule::new(
        "unsupported_ddl",
        2999,
        &format!(r"({})\b", ddl_pattern),
        Action::AddNode {
            token_type: TokenType::UntermKeyword,
        },
    ));

    // Sort by priority
    rules.sort_by_key(|r| r.priority);
    rules
}

/// Build rules for the Jinja context.
pub fn jinja_rules() -> Vec<Rule> {
    // Jinja block start patterns
    let mut rules = vec![
        // {% if ... %}
        Rule::new(
            "jinja_if_block_start",
            200,
            r"(\{%-?\s*if\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% elif ... %}
        Rule::new(
            "jinja_elif",
            201,
            r"(\{%-?\s*elif\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockKeyword,
        ),
        // {% else %}
        Rule::new(
            "jinja_else",
            202,
            r"(\{%-?\s*else\s*-?%\})",
            Action::HandleJinjaBlockKeyword,
        ),
        // {% endif %}
        Rule::new(
            "jinja_endif",
            203,
            r"(\{%-?\s*endif\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% for ... %}
        Rule::new(
            "jinja_for_block_start",
            210,
            r"(\{%-?\s*for\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endfor %}
        Rule::new(
            "jinja_endfor",
            211,
            r"(\{%-?\s*endfor\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% macro ... %}
        Rule::new(
            "jinja_macro_start",
            220,
            r"(\{%-?\s*macro\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endmacro %}
        Rule::new(
            "jinja_endmacro",
            221,
            r"(\{%-?\s*endmacro\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% set ... %}
        Rule::new(
            "jinja_set_block_start",
            100,
            r"(\{%-?\s*set\s+[^=]+?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endset %}
        Rule::new(
            "jinja_endset",
            101,
            r"(\{%-?\s*endset\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% call ... %}
        Rule::new(
            "jinja_call_start",
            260,
            r"(\{%-?\s*call(?:\(.*?\))?\s+\w+[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endcall %}
        Rule::new(
            "jinja_endcall",
            265,
            r"(\{%-?\s*endcall\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% test ... %}
        Rule::new(
            "jinja_test_start",
            230,
            r"(\{%-?\s*test\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endtest %}
        Rule::new(
            "jinja_endtest",
            231,
            r"(\{%-?\s*endtest\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% snapshot ... %}
        Rule::new(
            "jinja_snapshot_start",
            240,
            r"(\{%-?\s*snapshot\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endsnapshot %}
        Rule::new(
            "jinja_endsnapshot",
            241,
            r"(\{%-?\s*endsnapshot\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
        // {% materialization ... %}
        Rule::new(
            "jinja_materialization_start",
            250,
            r"(\{%-?\s*materialization\b[\s\S]*?-?%\})",
            Action::HandleJinjaBlockStart,
        ),
        // {% endmaterialization %}
        Rule::new(
            "jinja_endmaterialization",
            251,
            r"(\{%-?\s*endmaterialization\s*-?%\})",
            Action::HandleJinjaBlockEnd,
        ),
    ];

    rules.sort_by_key(|r| r.priority);
    rules
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main_rules_created() {
        let rules = main_rules();
        assert!(rules.len() > 30);

        // Verify some key rules exist
        let names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"statement_start"));
        assert!(names.contains(&"statement_end"));
        assert!(names.contains(&"word_operator"));
        assert!(names.contains(&"boolean_operator"));
        assert!(names.contains(&"set_operator"));
    }

    #[test]
    fn test_main_rules_sorted_by_priority() {
        let rules = main_rules();
        for window in rules.windows(2) {
            assert!(
                window[0].priority <= window[1].priority,
                "Rule '{}' (priority {}) should come before '{}' (priority {})",
                window[0].name,
                window[0].priority,
                window[1].name,
                window[1].priority
            );
        }
    }

    #[test]
    fn test_case_keyword_matches() {
        let rules = main_rules();
        let case_rule = rules.iter().find(|r| r.name == "statement_start").unwrap();
        assert!(case_rule.pattern.is_match("case"));
        assert!(case_rule.pattern.is_match("CASE"));
        assert!(case_rule.pattern.is_match("  CASE"));
    }

    #[test]
    fn test_join_keyword_matches() {
        let rules = main_rules();
        let join_rule = rules.iter().find(|r| r.name == "unterm_join").unwrap();
        assert!(join_rule.pattern.is_match("join"));
        assert!(join_rule.pattern.is_match("left join"));
        assert!(join_rule.pattern.is_match("LEFT OUTER JOIN"));
        assert!(join_rule.pattern.is_match("full outer join"));
        assert!(join_rule.pattern.is_match("cross join"));
    }

    #[test]
    fn test_set_operator_matches() {
        let rules = main_rules();
        let set_rule = rules.iter().find(|r| r.name == "set_operator").unwrap();
        assert!(set_rule.pattern.is_match("union"));
        assert!(set_rule.pattern.is_match("UNION ALL"));
        assert!(set_rule.pattern.is_match("intersect"));
        assert!(set_rule.pattern.is_match("except"));
        assert!(set_rule.pattern.is_match("minus"));
    }

    #[test]
    fn test_jinja_rules_created() {
        let rules = jinja_rules();
        assert!(rules.len() > 10);

        let names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"jinja_if_block_start"));
        assert!(names.contains(&"jinja_endif"));
        assert!(names.contains(&"jinja_for_block_start"));
        assert!(names.contains(&"jinja_endfor"));
    }
}
