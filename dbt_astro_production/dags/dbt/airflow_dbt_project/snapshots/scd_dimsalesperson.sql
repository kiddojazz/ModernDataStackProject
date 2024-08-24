{% snapshot scd_dimsalesperson %}

{{
   config(
       target_schema='dev',
       unique_key='salesperson_id',
       strategy='timestamp',
       updated_at='fivetran_synced',
       invalidate_hard_deletes=True
   )
}}

select 
_fivetran_synced as fivetran_synced,
salesperson_id,
salesperson_name,
region
FROM airflowpostgredb.marketing.dimsalesperson

{% endsnapshot %}