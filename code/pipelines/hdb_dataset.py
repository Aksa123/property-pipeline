import psycopg
import requests
import re
import polars as pl
from utils import get_full_hdb_listings_as_polar
from prefect import flow, task  
from prefect.cache_policies import NO_CACHE, INPUTS
from models import HDBListingsRaw
import uuid


@task(retries=3, retry_delay_seconds=5)
def extract_full_hdb_listings() -> pl.DataFrame:
    df = get_full_hdb_listings_as_polar()
    return df

@task(retries=3, retry_delay_seconds=5)
def load_to_staging(df: pl.DataFrame):
    ingestion_id = uuid.uuid4()
    limit = 1000
    offset = 0
    while df.height > offset:
        df_slice = df.slice(offset, limit)
        batch = []
        for i in df_slice.rows():
            storey_from, storey_to = re.findall(r'\d+', i[5])
            storey_from = int(storey_from)
            storey_to = int(storey_to)
            remaining = i[9]
            remaining_year = re.findall(r'(\d+) years', remaining)
            remaining_month = re.findall(r'(\d+) months', remaining)
            remaining_lease_year = remaining_year[0] if remaining_year else 0
            remaining_lease_month = remaining_month[0] if remaining_month else 0

            row = HDBListingsRaw(
                    ingestion_id=ingestion_id,
                    month=i[0],
                    town=i[1],
                    flat_type=i[2],
                    block=i[3],
                    street_name=i[4],
                    storey_range_from=storey_from,
                    storey_range_to=storey_to,
                    floor_area_sqm=i[6],
                    flat_model=i[7],
                    lease_commence_date=i[8],
                    remaining_lease_year=remaining_lease_year,
                    remaining_lease_month=remaining_lease_month,
                    resale_price=i[10]
                    )
            batch.append(row)
        
        # Bulk insert is more efficient
        HDBListingsRaw.bulk_create(batch)
        offset += limit
    return {'success': True, 'ingestion_id': ingestion_id, 'row_count': df.height}



@flow(name='hdb_dataset')
def hdb_dataset_flow():
    data = extract_full_hdb_listings()
    res = load_to_staging(data)
    print(res)

