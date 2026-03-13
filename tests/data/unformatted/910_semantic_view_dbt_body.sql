{{ config(materialized='semantic_view') }}
TABLES(
    T1 AS {{ ref('stg_orders') }},
    T2 AS {{ source('raw', 'customers') }}
)
RELATIONSHIPS(
    orders_customers AS T1(customer_id) REFERENCES T2(customer_id)
)
DIMENSIONS(
    T1.order_date AS T1.created_at,
    T2.customer_name AS T2.name
)
METRICS(
    T1.total_revenue AS SUM(T1.amount),
    T1.order_count AS COUNT(T1.order_id)
)
COMMENT = 'order analytics'
COPY GRANTS
)))))__SQLFMT_OUTPUT__(((((
{{ config(materialized="semantic_view") }}
tables(
    t1 as {{ ref("stg_orders") }}
    ,
    t2 as {{ source("raw", "customers") }}
)
relationships(orders_customers as t1(customer_id) references t2(customer_id))
dimensions(
    t1.order_date as t1.created_at
    ,
    t2.customer_name as t2.name
)
metrics(
    t1.total_revenue as sum(t1.amount)
    ,
    t1.order_count as count(t1.order_id)
)
COMMENT = 'order analytics'
COPY GRANTS
