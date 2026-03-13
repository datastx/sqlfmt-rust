CREATE OR REPLACE SEMANTIC VIEW my_db.my_schema.order_analytics
TABLES(
    t1 AS orders,
    t2 AS customers
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
COMMENT = 'order analytics'
COPY GRANTS;
