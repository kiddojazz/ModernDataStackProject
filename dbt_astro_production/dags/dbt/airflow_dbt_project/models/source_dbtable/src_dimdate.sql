with dimdate as(
    select * from airflowpostgredb.marketing.dimdate
)
select 
_fivetran_synced as fivetran_synced,
date,
year,
month,
day
from dimdate
order by fivetran_synced asc