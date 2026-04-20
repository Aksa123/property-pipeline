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
		rank() over (partition by full_addr order by month desc) as rank_month,
		rank() over (partition by full_addr order by id desc) as rank_addr
	from addr
)
select * 
from ordered 
where 
	rank_month = 1 
	and rank_addr = 1
	and ingestion_id = %s
	and batch_id = %s