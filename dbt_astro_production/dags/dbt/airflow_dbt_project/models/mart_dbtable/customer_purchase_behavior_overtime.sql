SELECT
    c.customer_name,
    p.product_name,
    EXTRACT(YEAR FROM d.date::DATE) AS Year,
    EXTRACT(MONTH FROM d.date::DATE) AS Month,
    SUM(f.quantity) AS TotalQuantityPurchased,
    SUM(f.total_amount) AS TotalSpent
FROM
    {{ref('factsales')}} f
JOIN
    {{ref('dim_customer')}} c ON f.customer_id = c.customer_id
JOIN
    {{ref('dim_product')}} p ON f.product_id = p.product_id
JOIN
    {{ref('dim_date')}} d ON f.date = d.date
GROUP BY
    c.customer_name,
    p.product_name,
    EXTRACT(YEAR FROM d.date::DATE),
    EXTRACT(MONTH FROM d.date::DATE)
ORDER BY
    c.customer_name,
    Year,
    Month,
    p.product_name