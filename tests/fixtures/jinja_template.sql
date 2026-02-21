{% set payment_methods = ['credit_card', 'bank_transfer', 'gift_card'] %}

SELECT
    order_id,
    customer_id,
    order_date,
    {% for method in payment_methods %}
    SUM(CASE WHEN payment_method = '{{ method }}' THEN amount ELSE 0 END) AS {{ method }}_amount
    {% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('orders') }}
WHERE order_date >= '{{ var("start_date") }}'
GROUP BY order_id, customer_id, order_date
