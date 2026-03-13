{{ config(materialized='semantic_view') }}
TABLES(
    T1 AS {{ ref('stg_orders') }}
)
DIMENSIONS(
    T1.order_date AS T1.created_at
)
METRICS(
    T1.total_revenue AS SUM(T1.amount)
)
WITH EXTENSION(
    CORTEX_ANALYST_VERIFIED_QUERIES = '{
      "verified_queries": [
        {
          "question": "What is the total revenue?",
          "sql": "SELECT SUM(amount) FROM orders"
        }
      ]
    }'
)
)))))__SQLFMT_OUTPUT__(((((
{{ config(materialized="semantic_view") }}
tables(t1 as {{ ref("stg_orders") }})
dimensions(t1.order_date as t1.created_at)
metrics(t1.total_revenue as sum(t1.amount))
with extension(cortex_analyst_verified_queries = '{
      "verified_queries": [
        {
          "question": "What is the total revenue?",
          "sql": "SELECT SUM(amount) FROM orders"
        }
      ]
    }')
