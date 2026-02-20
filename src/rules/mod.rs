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
            inner: Box::new(Action::SafeAddNode {
                token_type: TokenType::StatementEnd,
                alt_token_type: TokenType::Name,
            }),
        },
    ));

    // ---- Unterminated keywords ----
    // These are top-level SQL keywords that start new clauses.

    // SELECT INTO (must match before generic SELECT at 1050)
    // This is the workaround for Python's negative lookahead (?!\s+into)
    rules.push(Rule::new(
        "select_into",
        1040,
        r"(select\s+into)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            }),
        },
    ));

    // DELETE FROM (must match before generic FROM at 1045)
    rules.push(Rule::new(
        "unterm_delete_from",
        1044,
        r"(delete\s+from)\b",
        Action::AddNode {
            token_type: TokenType::UntermKeyword,
        },
    ));

    // FROM — uses HandleNonreservedTopLevelKeyword (inside brackets, FROM → Name)
    rules.push(Rule::new(
        "unterm_from",
        1045,
        r"(from)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            }),
        },
    ));

    // USING — uses HandleNonreservedTopLevelKeyword (inside brackets, USING → Name)
    rules.push(Rule::new(
        "unterm_using",
        1048,
        r"(using)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            }),
        },
    ));

    // ---- Consolidated unterm_keyword (priority 1050) ----
    // One comprehensive rule matching Python's single unterm_keyword rule.
    // Patterns ordered from most specific (longest) to least specific.
    let unterm_patterns = vec![
        // WITH (optional RECURSIVE)
        r"with\s+recursive",
        r"with",
        // SELECT with modifiers (SELECT INTO handled separately above)
        r"select\s+as\s+struct",
        r"select\s+as\s+value",
        r"select\s+all",
        r"select\s+distinct",
        r"select\s+top\s+\d+",
        r"select",
        // Comprehensive JOIN pattern (longest first)
        r"global\s+inner\s+join",
        r"global\s+left\s+outer\s+join",
        r"global\s+left\s+join",
        r"global\s+right\s+outer\s+join",
        r"global\s+right\s+join",
        r"global\s+full\s+outer\s+join",
        r"global\s+full\s+join",
        r"global\s+join",
        r"natural\s+full\s+outer\s+join",
        r"natural\s+full\s+join",
        r"natural\s+left\s+outer\s+join",
        r"natural\s+left\s+join",
        r"natural\s+right\s+outer\s+join",
        r"natural\s+right\s+join",
        r"natural\s+inner\s+join",
        r"natural\s+join",
        r"cross\s+lateral\s+join",
        r"cross\s+join",
        r"left\s+outer\s+join",
        r"left\s+semi\s+join",
        r"left\s+anti\s+join",
        r"left\s+asof\s+join",
        r"left\s+join",
        r"right\s+outer\s+join",
        r"right\s+semi\s+join",
        r"right\s+anti\s+join",
        r"right\s+join",
        r"full\s+outer\s+join",
        r"full\s+join",
        r"inner\s+join",
        r"asof\s+join",
        r"positional\s+join",
        r"any\s+join",
        r"lateral\s+join",
        r"join",
        // LATERAL VIEW
        r"lateral\s+view\s+outer",
        r"lateral\s+view",
        r"lateral",
        // Standard clauses
        r"prewhere",
        r"where",
        r"group\s+by",
        r"cluster\s+by",
        r"distribute\s+by",
        r"sort\s+by",
        r"having",
        r"qualify",
        r"window",
        r"order\s+by",
        r"limit",
        r"fetch\s+first",
        r"fetch\s+next",
        r"for\s+no\s+key\s+update",
        r"for\s+key\s+share",
        r"for\s+update",
        r"for\s+share",
        // CASE block keywords
        r"when",
        r"then",
        r"else",
        // PARTITION BY (important for window functions)
        r"partition\s+by",
        // Other clauses
        r"values",
        r"returning",
        r"into",
        r"pivot",
        r"unpivot",
        r"match_recognize",
        r"connect",
        r"start\s+with",
    ];
    let unterm_pattern = unterm_patterns.join("|");
    rules.push(Rule::new(
        "unterm_keyword",
        1050,
        &format!(r"({})\b", unterm_pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));

    // ---- ON keyword ----
    // Note: Python does NOT use HandleNonreservedTopLevelKeyword for ON.
    // ON should remain a keyword even inside brackets (JOIN ... ON in subqueries).
    rules.push(Rule::new(
        "on",
        1120,
        r"(on)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::On,
            }),
        },
    ));

    // ---- Functions that overlap with word operators ----
    // filter(), isnull(), like(), rlike(), ilike(), offset() before `(` → Name
    rules.push(Rule::new(
        "functions_that_overlap_with_word_operators",
        1099,
        r"((?:filter|isnull|(?:r|i)?like|offset)\s*\()",
        Action::HandleKeywordBeforeParen {
            token_type: TokenType::Name,
        },
    ));

    // ---- Word operators ----
    // Order matters: longer patterns must come before shorter ones
    let word_ops = vec![
        // Compound word operators (longest first)
        r"is\s+not\s+distinct\s+from",
        r"is\s+distinct\s+from",
        r"not\s+similar\s+to",
        r"similar\s+to",
        r"not\s+ilike\s+all",
        r"not\s+ilike\s+any",
        r"not\s+like\s+all",
        r"not\s+like\s+any",
        r"ilike\s+all",
        r"ilike\s+any",
        r"like\s+all",
        r"like\s+any",
        r"not\s+between",
        r"not\s+ilike",
        r"not\s+like",
        r"not\s+rlike",
        r"not\s+regexp",
        r"not\s+exists",
        r"global\s+not\s+in",
        r"not\s+in",
        r"is\s+not",
        r"grouping\s+sets",
        r"within\s+group",
        r"respect\s+nulls",
        r"ignore\s+nulls",
        r"nulls\s+first",
        r"nulls\s+last",
        // Single/simple word operators
        "as",
        "between",
        "cube",
        "exists",
        "filter",
        "ilike",
        "isnull",
        "in",
        "interval",
        "is",
        "like",
        "notnull",
        "over",
        "pivot",
        "regexp",
        "rlike",
        "rollup",
        "some",
        "tablesample",
        "unpivot",
        // Sort/order
        "asc",
        "desc",
    ];
    let word_op_pattern = word_ops
        .iter()
        .map(|op| format!("{}", op))
        .collect::<Vec<_>>()
        .join("|");
    rules.push(Rule::new(
        "word_operator",
        1100,
        &format!(r"({})\b", word_op_pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::WordOperator,
            }),
        },
    ));

    // ---- Star REPLACE/EXCLUDE (BigQuery) ----
    // exclude(...) and replace(...) after * → WordOperator
    rules.push(Rule::new(
        "star_replace_exclude",
        1101,
        r"((?:exclude|replace)\s*\()",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleKeywordBeforeParen {
                token_type: TokenType::WordOperator,
            }),
        },
    ));

    // ---- Boolean operators ----
    rules.push(Rule::new(
        "boolean_operator",
        1200,
        r"(and|or|not)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::BooleanOperator,
            }),
        },
    ));

    // ---- Frame clause (window frame specifications) ----
    let frame_pattern = r"(?:range|rows|groups)\s+(?:between\s+)?(?:(?:unbounded|\d+)\s+(?:preceding|following)|current\s+row)(?:\s+and\s+(?:(?:unbounded|\d+)\s+(?:preceding|following)|current\s+row))?";
    rules.push(Rule::new(
        "frame_clause",
        1305,
        &format!(r"({})", frame_pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));

    // ---- Offset keyword (not before parenthesis — that's a function) ----
    // offset followed by space (not `(`) at priority 1310
    // Note: offset() is caught by functions_that_overlap at 1099
    rules.push(Rule::new(
        "offset_keyword",
        1310,
        r"(offset)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));

    // ---- Set operators (comprehensive) ----
    let set_op_parts = vec![
        // Most specific patterns first
        r"union\s+all\s+by\s+name",
        r"union\s+by\s+name",
        r"union\s+all",
        r"union\s+distinct",
        r"intersect\s+all",
        r"intersect\s+distinct",
        r"except\s+all",
        r"except\s+distinct",
        r"union\s+all\s+corresponding\s+by",
        r"union\s+corresponding\s+by",
        r"union\s+strict\s+corresponding",
        r"union\s+corresponding",
        r"intersect\s+all\s+corresponding",
        r"intersect\s+corresponding",
        r"except\s+all\s+corresponding",
        r"except\s+corresponding",
        "union",
        "intersect",
        "except",
        "minus",
    ];
    let set_op_pattern = set_op_parts.join("|");
    rules.push(Rule::new(
        "set_operator",
        1320,
        &format!(r"({})\b", set_op_pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleSetOperator),
        },
    ));

    // ---- DDL keywords ----

    // EXPLAIN with HandleNonreservedTopLevelKeyword
    rules.push(Rule::new(
        "explain",
        2000,
        r"(explain(?:\s+(?:analyze|verbose|using))?)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            }),
        },
    ));

    // Unsupported DDL: comprehensive list with HandleNonreservedTopLevelKeyword
    let ddl_keywords = vec![
        r"create\s+or\s+replace",
        "create",
        "alter",
        "drop",
        "delete",
        r"insert\s+overwrite\s+into",
        r"insert\s+overwrite",
        r"insert\s+into",
        "insert",
        r"update",
        r"merge\s+into",
        "merge",
        "truncate",
        r"rename\s+table",
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
        r"cache\s+table",
        r"clear\s+cache",
        "cluster",
        "deallocate",
        "declare",
        "discard",
        "do",
        "export",
        "grant",
        "revoke",
        "handler",
        r"import\s+foreign\s+schema",
        r"import\s+table",
        "lock",
        "move",
        "prepare",
        r"reassign\s+owned",
        "repair",
        r"security\s+label",
        "unload",
        "validate",
        "vacuum",
        "analyze",
        "refresh",
        r"list",
        "remove",
        r"get",
        "put",
        "describe",
        "show",
        "comment",
        r"add",
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
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::AddNode {
                    token_type: TokenType::UntermKeyword,
                }),
            }),
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
        let unterm_rule = rules.iter().find(|r| r.name == "unterm_keyword").unwrap();
        assert!(unterm_rule.pattern.is_match("join"));
        assert!(unterm_rule.pattern.is_match("left join"));
        assert!(unterm_rule.pattern.is_match("LEFT OUTER JOIN"));
        assert!(unterm_rule.pattern.is_match("full outer join"));
        assert!(unterm_rule.pattern.is_match("cross join"));
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
