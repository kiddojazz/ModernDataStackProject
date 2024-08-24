with dimsalesperson as(
    select * from airflowpostgredb.marketing.dimsalesperson 
)
select
_fivetran_synced as fivetran_synced,
salesperson_id,
salesperson_name,
region
from dimsalesperson
order by fivetran_synced asc
