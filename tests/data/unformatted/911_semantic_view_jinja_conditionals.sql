{{ config(materialized='semantic_view') }}
TABLES(
    T1 AS {{ ref('stg_orders') }}
)
DIMENSIONS(
    T1.order_date AS T1.created_at,
    {% if var('include_pii', false) %}
    T1.customer_email AS T1.email,
    {% endif %}
    T1.region AS T1.region
)
METRICS(
    T1.total_revenue AS SUM(T1.amount)
)
COMMENT = {{ var('semantic_view_comment', "'default analytics'") }}
)))))__SQLFMT_OUTPUT__(((((
{{ config(materialized="semantic_view") }}
tables(t1 as {{ ref("stg_orders") }})
dimensions(
    t1.order_date as t1.created_at
    ,
    {% if var("include_pii", false) %}
        t1.customer_email as t1.email
        ,
    {% endif %}
    t1.region as t1.region
)
metrics(t1.total_revenue as sum(t1.amount))
COMMENT = {{ var('semantic_view_comment', "'default analytics'") }}
