-- Deduplicate per Town, Street, Block, and Storey range
-- Get the latest Month
with 
addr as (
	select 
		*,
		town || '_' || street_name || '_' || block || '_' || storey_range as full_addr 
	from hdb_listings_raw
),
ordered as (
	select 
		*, 
		row_number() over (partition by full_addr order by month desc, id desc) as rankz
	from addr
)
select * 
from ordered 
where 
	rankz = 1 
	and resale_price > 0	-- sanity check
	and ingestion_id = %s
	and batch_id = %s