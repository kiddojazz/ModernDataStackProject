{% snapshot scd_dimproduct %}

{{
   config(
       target_schema='dev',
       unique_key='product_id',
       strategy='timestamp',
       updated_at='fivetran_synced',
       invalidate_hard_deletes=True
   )
}}

select 
_fivetran_synced as fivetran_synced,
product_id,
product_name,
category,
price
FROM airflowpostgredb.marketing.dimproduct

{% endsnapshot %}