# Snowflake Semantic Views + dbt Jinja Support

## Overview

Snowflake [semantic views](https://docs.snowflake.com/en/user-guide/views-semantic/overview) are schema-level objects that encode business semantics (dimensions, metrics, facts, relationships) directly in the database. The [`dbt_semantic_view`](https://hub.getdbt.com/Snowflake-Labs/dbt_semantic_view/latest/) package (by Snowflake Labs, v1.0.3) provides a custom `semantic_view` materialization that lets users author these in dbt with Jinja templating.

This document outlines how sqlfmt-rust should support formatting these constructs.

---

## What the SQL Looks Like

### Raw Snowflake Syntax

```sql
CREATE [ OR REPLACE ] SEMANTIC VIEW [ IF NOT EXISTS ] <name>
  TABLES ( logicalTable [, ...] )
  [ RELATIONSHIPS ( relationshipDef [, ...] ) ]
  [ FACTS ( factExpression [, ...] ) ]
  [ DIMENSIONS ( dimensionExpression [, ...] ) ]
  [ METRICS ( metricExpression [, ...] ) ]
  [ COMMENT = '<comment>' ]
  [ AI_SQL_GENERATION '<instructions>' ]
  [ AI_QUESTION_CATEGORIZATION '<instructions>' ]
  [ COPY GRANTS ]
```

Key sub-clauses:

```sql
-- Logical table
[<alias> AS] <table_name>
  [PRIMARY KEY (<col>, ...)]
  [UNIQUE (<col>, ...) [...]]
  [WITH SYNONYMS [=] ('<syn>', ...)]
  [COMMENT = '<comment>']

-- Relationship
[<id> AS] <alias>(<col>, ...) REFERENCES <ref_alias>
  [(<ref_col>, ... | BETWEEN <start> AND <end> EXCLUSIVE)]

-- Fact
[{PRIVATE | PUBLIC}] <alias>.<fact> AS <expr>
  [WITH SYNONYMS [=] ('<syn>', ...)]
  [COMMENT = '<comment>']

-- Dimension
[PUBLIC] <alias>.<dimension> AS <expr>
  [WITH SYNONYMS [=] ('<syn>', ...)]
  [COMMENT = '<comment>']

-- Metric
[{PRIVATE | PUBLIC}] <alias>.<metric>
  [NON ADDITIVE BY (<dim> [{ASC|DESC}] [NULLS {FIRST|LAST}], ...)]
  AS <expr>
  [WITH SYNONYMS [=] ('<syn>', ...)]
  [COMMENT = '<comment>']

-- Window function metric
[{PRIVATE | PUBLIC}] <alias>.<metric> AS
  <window_func>(<metric>) OVER (
    [PARTITION BY {<exprs> | EXCLUDING <dims>}]
    [ORDER BY <exprs> [{ASC|DESC}] [NULLS {FIRST|LAST}], ...]
    [<windowFrameClause>]
  )
```

### dbt + Jinja Pattern (via dbt_semantic_view package)

Users write dbt models like this:

```sql
{{ config(materialized='semantic_view') }}
TABLES(
    t1 AS {{ ref('base_table') }},
    t2 AS {{ source('seed_sources', 'base_table2') }}
)
DIMENSIONS(
    t1.count AS value,
    t2.volume AS value
)
METRICS(
    t1.total_rows AS SUM(t1.count),
    t2.max_volume AS MAX(t2.volume)
)
COMMENT = 'test semantic view'
COPY GRANTS
```

Note: there is **no** `CREATE SEMANTIC VIEW` or `SELECT` in the dbt model. The package's materialization macro wraps the body in the full DDL. The user only writes from `TABLES(...)` onward.

### Downstream References

```sql
{{ config(materialized='table') }}

select *
from semantic_view(
    {{ ref('semantic_view_basic') }}
    metrics total_rows
)
```

---

## Current State in sqlfmt-rust

| Area | Status |
|------|--------|
| `CREATE VIEW` | No-op (enters `LexState::Unsupported`, passes through unformatted) |
| `CREATE SEMANTIC VIEW` | Not recognized at all (would hit `Unsupported` DDL path) |
| Jinja tokenization | Robust: `{{ }}`, `{% %}`, `{# #}` all handled |
| Jinja formatting | Full support: normalization, multiline, operator spacing, dbt macros |
| Snowflake dialect | No dedicated dialect; uses polyglot. Only special-case: `before()`/`at()` spacing |
| Dialect system | Minimal trait: `case_sensitive_names()` + `initialize_analyzer()` |

### Key files

- `src/lexer.rs` — DDL detection (lines ~2378-2407), `LexState::Unsupported`
- `src/dialect.rs` — Dialect trait, `Polyglot`, `DuckDb`
- `src/formatter.rs` — 5-stage pipeline
- `src/jinja_formatter.rs` — Jinja normalization engine
- `src/mode.rs` — `Mode` struct, file extensions include `.sql.jinja`

---

## Design: How to Support This

### Approach: Semantic View as a New Lex State

Rather than trying to parse semantic view DDL into the full AST (overkill for formatting), treat it similarly to how `CREATE FUNCTION` and `CREATE WAREHOUSE` have dedicated lex states. The formatter can then apply targeted formatting rules.

### Phase 1: Recognize and Pass Through (Minimum Viable)

**Goal:** Stop mangling semantic view SQL. Ensure it passes through cleanly.

1. **Lexer: Recognize `CREATE SEMANTIC VIEW`**
   - In the DDL detection logic (`lexer.rs` ~line 2378), add pattern matching for `CREATE [OR REPLACE] SEMANTIC VIEW [IF NOT EXISTS]`.
   - Route to `LexState::Unsupported` (same as `CREATE VIEW` today).
   - This ensures the content is preserved as-is.

2. **Lexer: Recognize bare `TABLES(...)` as semantic view body (dbt pattern)**
   - When a file starts with `{{ config(materialized='semantic_view') }}` followed by `TABLES(`, the lexer sees no DDL keyword — it would try to parse `TABLES(` as a regular query.
   - **Solution:** After Jinja statement/expression tokens at the start of a file, if the next non-whitespace token is `TABLES`, enter a semantic view body lex state.
   - This is the critical path for dbt users.

3. **Tests:**
   - Add golden test files:
     - `910_create_semantic_view.sql` — raw Snowflake DDL (no-op, like `900_create_view.sql`)
     - `311_jinja_semantic_view.sql` — dbt model with Jinja `ref()`/`source()` calls

**Estimated complexity:** Low. Mostly lexer routing + test files.

### Phase 2: Format the Semantic View Body

**Goal:** Actively format semantic view content with consistent indentation and style.

#### 2a. New Lex State: `SemanticView`

Add `LexState::SemanticView` that tokenizes the body into structured tokens:

| Token | Examples |
|-------|----------|
| `SemanticKeyword` | `TABLES`, `DIMENSIONS`, `METRICS`, `FACTS`, `RELATIONSHIPS`, `COMMENT`, `COPY GRANTS`, `PRIMARY KEY`, `UNIQUE`, `WITH SYNONYMS`, `REFERENCES`, `NON ADDITIVE BY`, `PRIVATE`, `PUBLIC` |
| `Name` / `DottedName` | `t1`, `t1.total_rows` |
| `Bracket` | `(`, `)` |
| `Operator` | `AS`, `=` |
| `FunctionName` | `SUM`, `MAX`, `AVG`, `COUNT` |
| `JinjaExpression` | `{{ ref('base_table') }}` (already tokenized) |
| `JinjaStatement` | `{{ config(...) }}` (already tokenized) |

#### 2b. Formatting Rules

Target output style (consistent with sqlfmt conventions):

```sql
{{ config(materialized="semantic_view") }}
tables (
    t1 as {{ ref("base_table") }},
    t2 as {{ source("seed_sources", "base_table2") }}
)
dimensions (
    t1.count as value,
    t2.volume as value
)
metrics (
    t1.total_rows as sum(t1.count),
    t2.max_volume as max(t2.volume)
)
comment = 'test semantic view'
copy grants
```

Rules:
- **Top-level keywords** (`TABLES`, `DIMENSIONS`, `METRICS`, `FACTS`, `RELATIONSHIPS`) lowercased, at indent 0
- **Parenthesized lists** after top-level keywords: each item on its own line, indented 4 spaces
- **Sub-keywords** (`AS`, `REFERENCES`, `PRIMARY KEY`, etc.) lowercased
- **Aggregate functions** (`SUM`, `MAX`, etc.) lowercased (consistent with sqlfmt's approach to SQL keywords)
- **COMMENT**, **COPY GRANTS** at indent 0
- **Jinja tokens** formatted by existing `JinjaFormatter` (quotes normalized, spacing)
- **Nested sub-clauses** (e.g., `WITH SYNONYMS`, `NON ADDITIVE BY`) indented under their parent

For full `CREATE SEMANTIC VIEW` DDL:

```sql
create or replace semantic view my_semantic_view
    tables (
        t1 as my_database.my_schema.orders
            primary key (order_id),
        t2 as my_database.my_schema.customers
            primary key (customer_id)
            with synonyms = ('buyers', 'clients')
    )
    relationships (
        orders_to_customers as t1(customer_id) references t2(customer_id)
    )
    facts (
        t1.order_amount as t1.amount
    )
    dimensions (
        t1.order_date as t1.created_at
            comment = 'When the order was placed',
        t2.customer_name as t2.name
            with synonyms = ('buyer_name')
    )
    metrics (
        t1.total_revenue as sum(t1.order_amount),
        t1.avg_order_value as avg(t1.order_amount)
            comment = 'Average order value'
    )
    comment = 'Revenue analytics semantic view'
```

#### 2c. Downstream `semantic_view()` Function

The `semantic_view()` table function used in queries:

```sql
select *
from semantic_view(
    {{ ref("semantic_view_basic") }}
    metrics total_rows
    dimensions order_date
)
```

This should be handled by the existing query formatter since it's a regular SELECT. The `semantic_view()` call is just a function — ensure the formatter doesn't break on the non-standard argument syntax (no commas, keyword args like `metrics`, `dimensions`, `where`).

**Action:** Add this as a test case. If the current formatter handles it (likely as Unsupported or passthrough), that may be acceptable for Phase 2.

### Phase 3: Snowflake Dialect (Optional, Future)

If broader Snowflake-specific formatting is needed:

1. Add `Snowflake` struct implementing `Dialect` in `dialect.rs`
2. Register `"snowflake"` in `dialect_from_name()`
3. Snowflake dialect could enable:
   - Semantic view formatting by default
   - Snowflake-specific function recognition
   - `QUALIFY`, `FLATTEN`, `LATERAL` formatting tweaks
4. For dbt users, auto-detect based on `dbt_project.yml` profile or file extensions

This is **not required** for semantic view support — the polyglot dialect can handle it.

---

## Implementation Plan

### Milestone 1: Safe Pass-Through (1-2 days)

- [ ] Add `SEMANTIC VIEW` to DDL keyword detection in lexer
- [ ] Add `SEMANTIC` to the multi-word DDL pattern matching
- [ ] Route `CREATE [OR REPLACE] SEMANTIC VIEW` to `LexState::Unsupported`
- [ ] Handle bare `TABLES(` after Jinja config block (dbt pattern) — detect and pass through
- [ ] Add golden test: `910_create_semantic_view.sql` (raw DDL, no-op)
- [ ] Add golden test: `311_jinja_semantic_view.sql` (dbt model, no-op)
- [ ] Add golden test: downstream `semantic_view()` function call
- [ ] Verify all existing tests still pass

### Milestone 2: Active Formatting (3-5 days)

- [ ] Define `LexState::SemanticView` with keyword tokenization
- [ ] Implement semantic view body splitter rules
- [ ] Implement semantic view body merger rules
- [ ] Add formatting for `TABLES`, `DIMENSIONS`, `METRICS`, `FACTS`, `RELATIONSHIPS` blocks
- [ ] Handle `COMMENT =`, `COPY GRANTS`, `WITH SYNONYMS`, `PRIMARY KEY`, etc.
- [ ] Ensure Jinja tokens inside semantic view bodies are formatted
- [ ] Convert golden tests from no-op to formatted expected output
- [ ] Add complex golden tests: multi-table, relationships, window metrics, AI clauses

### Milestone 3: Edge Cases and Polish (2-3 days)

- [ ] Handle `ALTER SEMANTIC VIEW` (RENAME TO, SET/UNSET COMMENT)
- [ ] Handle `DROP SEMANTIC VIEW`, `DESCRIBE SEMANTIC VIEW`, `SHOW SEMANTIC VIEWS`
- [ ] Test with `WITH EXTENSION` clause (Cortex Analyst verified queries)
- [ ] Test multiline Jinja inside semantic view bodies (e.g., conditional dimensions)
- [ ] Test `{% for %}` loops generating dynamic dimensions/metrics
- [ ] Ensure `#!fmt:off` / `#!fmt:on` directives work within semantic views
- [ ] Performance: benchmark with large semantic view definitions

---

## Test Cases to Create

### Raw Snowflake DDL (910_create_semantic_view.sql)

```sql
CREATE OR REPLACE SEMANTIC VIEW revenue_analytics
  TABLES (
    orders AS my_db.my_schema.orders
      PRIMARY KEY (order_id),
    customers AS my_db.my_schema.customers
      PRIMARY KEY (customer_id)
      WITH SYNONYMS = ('buyers', 'clients')
  )
  RELATIONSHIPS (
    order_customer AS orders(customer_id) REFERENCES customers(customer_id)
  )
  FACTS (
    orders.order_amount AS orders.amount
  )
  DIMENSIONS (
    orders.order_date AS orders.created_at
      COMMENT = 'When the order was placed',
    customers.region AS customers.region
      WITH SYNONYMS = ('area', 'territory')
  )
  METRICS (
    orders.total_revenue AS SUM(orders.order_amount),
    orders.order_count AS COUNT(orders.order_id),
    orders.avg_order AS AVG(orders.order_amount)
      NON ADDITIVE BY (orders.order_date DESC NULLS LAST)
      COMMENT = 'Average order value'
  )
  COMMENT = 'Revenue analytics semantic model'
```

### dbt + Jinja (311_jinja_semantic_view.sql)

```sql
{{ config(materialized="semantic_view") }}
TABLES(
    t1 AS {{ ref("stg_orders") }},
    t2 AS {{ source("raw", "customers") }}
)
RELATIONSHIPS(
    orders_customers AS t1(customer_id) REFERENCES t2(customer_id)
)
DIMENSIONS(
    t1.order_date AS t1.created_at,
    t2.customer_name AS t2.name
)
METRICS(
    t1.total_revenue AS SUM(t1.amount),
    t1.order_count AS COUNT(t1.order_id)
)
COMMENT = '{{ var("semantic_view_comment", "order analytics") }}'
```

### dbt + Jinja with Conditionals

```sql
{{ config(materialized="semantic_view") }}
TABLES(
    t1 AS {{ ref("stg_orders") }}
)
DIMENSIONS(
    t1.order_date AS t1.created_at
    {% if var("include_region", false) %}
    , t1.region AS t1.ship_region
    {% endif %}
)
METRICS(
    t1.total_revenue AS SUM(t1.amount)
)
```

### Downstream Query

```sql
{{ config(materialized="table") }}

select *
from semantic_view(
    {{ ref("my_semantic_view") }}
    metrics total_revenue, order_count
    dimensions order_date
    where order_date >= '2024-01-01'
)
```

---

## Risks and Open Questions

1. **Bare `TABLES(` detection:** The dbt pattern has no `CREATE` keyword. We need to detect that a file starting with a Jinja config block followed by `TABLES(` is a semantic view body, not a regular query. This could false-positive on other uses of `TABLES(` though that's unlikely in practice.

2. **`semantic_view()` function syntax:** This function uses a non-standard argument style (no commas between keyword sections like `metrics`, `dimensions`, `where`). The current formatter may struggle with this. Needs investigation — it may need to be treated as a special function or passed through.

3. **Jinja conditionals inside clause bodies:** A `{% if %}` block that conditionally adds dimensions/metrics creates syntactically incomplete SQL in each branch. The formatter must handle this gracefully (the existing Jinja block tracking should help).

4. **Ordering constraint:** Snowflake requires clauses in a specific order (TABLES, RELATIONSHIPS, FACTS, DIMENSIONS, METRICS). The formatter should preserve user order, not reorder.

5. **`WITH EXTENSION` clause:** Used for Cortex Analyst verified queries, contains embedded JSON strings. Must be preserved as-is.

6. **ALTER SEMANTIC VIEW is limited:** Only supports RENAME and COMMENT operations — minimal formatting needed.
