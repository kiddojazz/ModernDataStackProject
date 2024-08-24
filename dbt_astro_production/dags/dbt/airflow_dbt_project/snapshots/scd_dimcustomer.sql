{% snapshot scd_dimcustomer %}

{{
   config(
       target_schema='dev',
       unique_key='customer_id',
       strategy='timestamp',
       updated_at='fivetran_synced',
       invalidate_hard_deletes=True
   )
}}

select 
_fivetran_synced as fivetran_synced,
customer_id,
customer_name,
country
FROM airflowpostgredb.marketing.dimcustomer

{% endsnapshot %}