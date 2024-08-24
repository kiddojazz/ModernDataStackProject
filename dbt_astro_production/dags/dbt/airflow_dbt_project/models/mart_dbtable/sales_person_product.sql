/*
1. Total Sales by Salesperson and Product
This query helps analyze sales performance by showing how much of each product has been sold by 
each salesperson, including details about the customer and the date of the sale.
*/
SELECT
    s.salesperson_name,
    p.product_name,
    SUM(f.quantity) AS TotalQuantitySold,
    SUM(f.total_amount) AS TotalSalesAmount,
    c.customer_name,
    d.date AS SalesDate
FROM
    {{ref('factsales')}} f 
JOIN
    {{ref('dim_salesperson')}} s ON f.salesperson_id = s.salesperson_id
JOIN
    {{ref('dim_product')}} p ON f.product_id = p.product_id
JOIN
    {{ref('dim_customer')}} c ON f.customer_id = c.customer_id
JOIN
    {{ref('dim_date')}} d ON f.date = d.date
GROUP BY
    s.salesperson_name,
    p.product_name,
    c.customer_name,
    d.date
ORDER BY
    s.salesperson_name,
    p.product_name,
    d.date