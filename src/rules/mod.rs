pub mod common;
pub mod core;

use std::sync::LazyLock;

use crate::action::Action;
use crate::rule::Rule;
use crate::token::TokenType;

/// Cached compiled main rules. Regex compilation is expensive (~50 patterns),
/// so we compile once and clone on each access. Regex::clone() is O(1) since
/// it uses Arc internally.
static MAIN_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_main_rules);

/// Cached compiled jinja rules.
static JINJA_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_jinja_rules);

/// Cached compiled fmt:off rules.
static FMT_OFF_RULES: LazyLock<Vec<Rule>> = LazyLock::new(core::fmt_off_rules);

/// Cached compiled jinja set block rules.
static JINJA_SET_BLOCK_RULES: LazyLock<Vec<Rule>> = LazyLock::new(core::jinja_set_block_rules);

/// Cached compiled unsupported DDL rules.
static UNSUPPORTED_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_unsupported_rules);

/// Cached compiled GRANT rules.
static GRANT_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_grant_rules);

/// Cached compiled FUNCTION rules.
static FUNCTION_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_function_rules);

/// Cached compiled WAREHOUSE rules.
static WAREHOUSE_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_warehouse_rules);

/// Cached compiled CLONE rules.
static CLONE_RULES: LazyLock<Vec<Rule>> = LazyLock::new(build_clone_rules);

/// Get the MAIN rule set, cloned from a cached compiled version.
pub fn main_rules() -> Vec<Rule> {
    MAIN_RULES.clone()
}

/// Get the FMT_OFF rule set, cloned from a cached compiled version.
pub fn fmt_off_rules() -> Vec<Rule> {
    FMT_OFF_RULES.clone()
}

/// Get the Jinja set block rule set, cloned from a cached compiled version.
pub fn jinja_set_block_rules() -> Vec<Rule> {
    JINJA_SET_BLOCK_RULES.clone()
}

/// Get the UNSUPPORTED DDL rule set.
pub fn unsupported_rules() -> Vec<Rule> {
    UNSUPPORTED_RULES.clone()
}

/// Get the GRANT DDL rule set.
pub fn grant_rules() -> Vec<Rule> {
    GRANT_RULES.clone()
}

/// Get the FUNCTION DDL rule set.
pub fn function_rules() -> Vec<Rule> {
    FUNCTION_RULES.clone()
}

/// Get the WAREHOUSE DDL rule set.
pub fn warehouse_rules() -> Vec<Rule> {
    WAREHOUSE_RULES.clone()
}

/// Get the CLONE DDL rule set.
pub fn clone_rules() -> Vec<Rule> {
    CLONE_RULES.clone()
}

/// Build rules for unsupported DDL passthrough.
/// Uses ALWAYS rules + Data tokens. Preserves original text verbatim
/// but handles comments, jinja, strings, and semicolons normally.
fn build_unsupported_rules() -> Vec<Rule> {
    let mut rules = core::always_rules();
    // Quoted names need to be lexed as DATA so they are not formatted
    // (higher priority than the quoted_name rule in ALWAYS)
    rules.push(Rule::new(
        "quoted_name_in_unsupported",
        199,
        r#"("(?:[^"\\]|\\.)*"|`(?:[^`\\]|\\.)*`)"#,
        Action::AddNode {
            token_type: TokenType::Data,
        },
    ));
    // Data: everything else up to semicolon or newline
    rules.push(Rule::new(
        "unsupported_line",
        1000,
        r"([^;\n]+)",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::Data,
            }),
        },
    ));
    rules.sort_by_key(|r| r.priority);
    rules
}

/// Build GRANT/REVOKE rules: CORE + grant-specific keywords.
fn build_grant_rules() -> Vec<Rule> {
    let mut rules = core::core_rules();
    let grant_keywords = vec![
        "grant",
        r"revoke(\s+grant\s+option\s+for)?",
        "on",
        "to",
        "from",
        r"with\s+grant\s+option",
        r"granted\s+by",
        "cascade",
        "restrict",
    ];
    let pattern = grant_keywords.join("|");
    rules.push(Rule::new(
        "unterm_keyword",
        1300,
        &format!(r"({})\b", pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));
    rules.sort_by_key(|r| r.priority);
    rules
}

/// Build CREATE/ALTER FUNCTION rules: CORE + function-specific keywords.
fn build_function_rules() -> Vec<Rule> {
    let mut rules = core::core_rules();
    // AS keyword → HandleDdlAs
    rules.push(Rule::new(
        "function_as",
        1100,
        r"(as)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleDdlAs),
        },
    ));
    // Word operators
    rules.push(Rule::new(
        "word_operator",
        1200,
        r"(to|from|runtime_version)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::WordOperator,
            }),
        },
    ));
    // Function-specific unterminated keywords
    let fn_keywords = vec![
        // CREATE/ALTER FUNCTION patterns
        r"create(\s+or\s+replace)?(\s+temp(orary)?)?(\s+secure)?(\s+external)?(\s+table)?\s+function(\s+if\s+not\s+exists)?",
        r"(alter|drop)\s+function(\s+if\s+exists)?",
        "language",
        "transform",
        "immutable",
        "stable",
        "volatile",
        r"(not\s+)?leakproof",
        r"called\s+on\s+null\s+input",
        r"returns\s+null\s+on\s+null\s+input",
        r"return(s)?",
        "strict",
        r"(external\s+)?security\s+(invoker|definer)",
        r"parallel\s+(unsafe|restricted|safe)",
        "cost",
        "rows",
        "support",
        // Snowflake
        r"((un)?set\s+)?comment",
        "imports",
        "packages",
        "handler",
        "target_path",
        r"(not\s+)?null",
        // Snowflake external function params
        r"((un)?set\s+)?api_integration",
        r"((un)?set\s+)?headers",
        r"((un)?set\s+)?context_headers",
        r"((un)?set\s+)?max_batch_rows",
        r"((un)?set\s+)?compression",
        r"((un)?set\s+)?request_translator",
        r"((un)?set\s+)?response_translator",
        // BigQuery
        "options",
        r"remote\s+with\s+connection",
        // ALTER
        r"rename\s+to",
        r"owner\s+to",
        r"set\s+schema",
        r"(no\s+)?depends\s+on\s+extension",
        "cascade",
        "restrict",
        // Alter snowflake
        r"(un)?set\s+secure",
        // PG catchall for set
        r"(re)?set(\s+all)?",
    ];
    let pattern = fn_keywords.join("|");
    rules.push(Rule::new(
        "unterm_keyword",
        1300,
        &format!(r"({})\b", pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));
    rules.sort_by_key(|r| r.priority);
    rules
}

/// Build CREATE/ALTER WAREHOUSE rules: CORE + warehouse-specific keywords.
fn build_warehouse_rules() -> Vec<Rule> {
    let mut rules = core::core_rules();
    let wh_keywords = vec![
        r"create(\s+or\s+replace)?\s+warehouse(\s+if\s+not\s+exists)?",
        r"alter\s+warehouse(\s+if\s+exists)?",
        // Object properties
        r"(with\s+|(un)?set\s+)?warehouse_type",
        r"(with\s+|(un)?set\s+)?warehouse_size",
        r"(with\s+|(un)?set\s+)?max_cluster_count",
        r"(with\s+|(un)?set\s+)?min_cluster_count",
        r"(with\s+|(un)?set\s+)?scaling_policy",
        r"(with\s+|(un)?set\s+)?auto_suspend",
        r"(with\s+|(un)?set\s+)?auto_resume",
        r"(with\s+|(un)?set\s+)?initially_suspended",
        r"(with\s+|(un)?set\s+)?resource_monitor",
        r"(with\s+|(un)?set\s+)?comment",
        r"(with\s+|(un)?set\s+)?enable_query_acceleration",
        r"(with\s+|(un)?set\s+)?query_acceleration_max_scale_factor",
        r"(with\s+|(un)?set\s+)?tag",
        // Object params
        r"(set\s+)?max_concurrency_level",
        r"(set\s+)?statement_queued_timeout_in_seconds",
        r"(set\s+)?statement_timeout_in_seconds",
        // Alter
        "suspend",
        r"resume(\s+if\s+suspended)?",
        r"abort\s+all\s+queries",
        r"rename\s+to",
    ];
    let pattern = wh_keywords.join("|");
    rules.push(Rule::new(
        "unterm_keyword",
        1300,
        &format!(r"({})\b", pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));
    rules.sort_by_key(|r| r.priority);
    rules
}

/// Build CREATE ... CLONE rules: CORE + clone-specific keywords.
fn build_clone_rules() -> Vec<Rule> {
    let mut rules = core::core_rules();
    let clone_keywords = vec![
        r"create(\s+or\s+replace)?\s+(database|schema|table|stage|file\s+format|sequence|stream|task)(\s+if\s+not\s+exists)?",
        "clone",
    ];
    let pattern = clone_keywords.join("|");
    rules.push(Rule::new(
        "unterm_keyword",
        1300,
        &format!(r"({})\b", pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));
    // Word operators: at, before
    rules.push(Rule::new(
        "word_operator",
        1500,
        r"(at|before)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::WordOperator,
            }),
        },
    ));
    rules.sort_by_key(|r| r.priority);
    rules
}

/// Build the MAIN rule set used by the Polyglot (default) dialect.
/// This adds SQL keywords, operators, and statement handling on top of CORE rules.
fn build_main_rules() -> Vec<Rule> {
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

    // FROM — always UntermKeyword (even inside brackets like CTEs)
    // Inside function brackets like EXTRACT(day FROM ts), FROM tries to pop the last
    // unterm keyword but finds `(` (a bracket, not unterm), so depth is preserved.
    // Inside CTEs like `with cte as (select a from t)`, FROM correctly pops `select`.
    rules.push(Rule::new(
        "unterm_from",
        1045,
        r"(from)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
            }),
        },
    ));

    // USING — UntermKeyword for proper depth tracking
    rules.push(Rule::new(
        "unterm_using",
        1048,
        r"(using)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::AddNode {
                token_type: TokenType::UntermKeyword,
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
        r"global\s+any\s+join",
        r"global\s+join",
        r"any\s+left\s+outer\s+join",
        r"any\s+left\s+join",
        r"any\s+right\s+outer\s+join",
        r"any\s+right\s+join",
        r"any\s+inner\s+join",
        r"any\s+full\s+outer\s+join",
        r"any\s+full\s+join",
        r"paste\s+join",
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
        r"semi\s+join",
        r"anti\s+join",
        r"asof\s+left\s+join",
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
    // filter(), isnull(), offset() before `(` → Name (function calls)
    // Note: like/rlike/ilike are NOT included — they are word operators
    // that should keep a space before parenthesized arguments.
    rules.push(Rule::new(
        "functions_that_overlap_with_word_operators",
        1099,
        r"((?:filter|isnull|offset)\s*\()",
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
        r"global\s+in",
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
        .map(|op| op.to_string())
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

    // ---- Star EXCEPT/REPLACE/EXCLUDE (BigQuery/Snowflake) ----
    // except(...), exclude(...) and replace(...) after * → WordOperator
    rules.push(Rule::new(
        "star_replace_exclude",
        1101,
        r"((?:except|exclude|replace)\s*\()",
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

    // GRANT/REVOKE → GRANT ruleset (must come before unsupported_ddl)
    rules.push(Rule::new(
        "grant",
        2010,
        r"(grant|revoke)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::LexRuleset {
                    ruleset_name: "grant".to_string(),
                }),
            }),
        },
    ));

    // CREATE ... CLONE → CLONE ruleset (must come before create_function and unsupported_ddl)
    rules.push(Rule::new(
        "create_clone",
        2015,
        r"(create(?:\s+or\s+replace)?\s+(?:database|schema|table|stage|file\s+format|sequence|stream|task)(?:\s+if\s+not\s+exists)?\s+\S+\s+clone)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::LexRuleset {
                    ruleset_name: "clone".to_string(),
                }),
            }),
        },
    ));

    // CREATE/ALTER FUNCTION → FUNCTION ruleset
    rules.push(Rule::new(
        "create_function",
        2020,
        r"(create(?:\s+or\s+replace)?(?:\s+temp(?:orary)?)?(?:\s+secure)?(?:\s+external)?(?:\s+table)?\s+function(?:\s+if\s+not\s+exists)?|(?:alter|drop)\s+function(?:\s+if\s+exists)?)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::LexRuleset {
                    ruleset_name: "function".to_string(),
                }),
            }),
        },
    ));

    // CREATE/ALTER WAREHOUSE → WAREHOUSE ruleset
    rules.push(Rule::new(
        "create_warehouse",
        2030,
        r"(create(?:\s+or\s+replace)?\s+warehouse(?:\s+if\s+not\s+exists)?|alter\s+warehouse(?:\s+if\s+exists)?)\b",
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::LexRuleset {
                    ruleset_name: "warehouse".to_string(),
                }),
            }),
        },
    ));

    // Generic unsupported DDL: comprehensive list (fallback)
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
        "unset",
        "use",
        "execute",
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
        .map(|k| k.to_string())
        .collect::<Vec<_>>()
        .join("|");
    rules.push(Rule::new(
        "unsupported_ddl",
        2999,
        &format!(r"({})\b", ddl_pattern),
        Action::HandleReservedKeyword {
            inner: Box::new(Action::HandleNonreservedTopLevelKeyword {
                inner: Box::new(Action::LexRuleset {
                    ruleset_name: "unsupported".to_string(),
                }),
            }),
        },
    ));

    // Sort by priority
    rules.sort_by_key(|r| r.priority);
    rules
}

/// Get Jinja rules, cloned from a cached compiled version.
pub fn jinja_rules() -> Vec<Rule> {
    JINJA_RULES.clone()
}

/// Build rules for the Jinja context.
fn build_jinja_rules() -> Vec<Rule> {
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
