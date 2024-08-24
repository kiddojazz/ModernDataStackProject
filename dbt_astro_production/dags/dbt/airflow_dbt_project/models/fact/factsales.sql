{{ 
    config(
        materialized = 'incremental',
        unique_key = 'sales_id',
        on_schema_change = 'fail'
    ) 
}}

WITH src_factsales AS (
    SELECT 
        fivetran_synced,
        sales_id,
        customer_id,
        product_id,
        salesperson_id,
        date,
        quantity,
        total_amount
    FROM {{ ref('src_factsales') }}
)

SELECT *
FROM src_factsales
{% if is_incremental() %}
    WHERE fivetran_synced > (SELECT max(fivetran_synced) FROM {{ this }})
{% endif %}

/*
This means if the fivetran_synced in the src is greater than the fact table
then it should perform incremental process
*/