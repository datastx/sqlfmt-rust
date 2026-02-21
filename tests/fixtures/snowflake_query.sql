SELECT
    customer_id,
    first_name,
    last_name,
    email,
    COUNT(*) AS order_count,
    SUM(order_total) AS total_spent,
    AVG(order_total) AS avg_order_value
FROM customers
LEFT JOIN orders ON customers.customer_id = orders.customer_id
WHERE customers.created_at >= '2024-01-01'
    AND customers.status = 'active'
GROUP BY customer_id, first_name, last_name, email
HAVING COUNT(*) > 5
ORDER BY total_spent DESC
LIMIT 100
