WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', sale_date) AS month,
        product_category,
        SUM(amount) AS total_sales,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM sales
    WHERE sale_date >= '2024-01-01'
    GROUP BY DATE_TRUNC('month', sale_date), product_category
)
SELECT
    month,
    product_category,
    total_sales,
    unique_customers,
    total_sales / unique_customers AS avg_per_customer,
    LAG(total_sales) OVER (PARTITION BY product_category ORDER BY month) AS prev_month_sales,
    total_sales - LAG(total_sales) OVER (PARTITION BY product_category ORDER BY month) AS month_over_month
FROM monthly_sales
ORDER BY month, product_category
