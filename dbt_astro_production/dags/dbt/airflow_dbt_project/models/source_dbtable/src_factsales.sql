WITH salesfact AS (
    SELECT
        *
    FROM
        airflowpostgredb.marketing.salesfact
)
SELECT
    _fivetran_synced as fivetran_synced,
    sales_id,
    customer_id,
    product_id,
    salesperson_id,
    date,
    quantity,
    total_amount
FROM salesfact
order by fivetran_synced asc
