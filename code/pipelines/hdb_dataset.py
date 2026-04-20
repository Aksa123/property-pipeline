from psycopg.rows import dict_row
import requests
import re
import polars as pl
from utils import get_full_hdb_listings_as_polar, get_hdb_listings
from prefect import flow, task  
from prefect.cache_policies import NO_CACHE, INPUTS
from models import HDBListingsRaw, HDBListingsNormalized, Towns, FlatTypes, Streets, FlatModels
import uuid
from decimal import Decimal
from urllib.parse import urlparse, parse_qs
from settings import QUERIES_PATH
from connections import conn_staging


@task
def start():
    ingestion_id = uuid.uuid4()

    offset = None
    while True:
        response = extract_hdb_listings(offset)
        data = response['result']['records']

        raw_dump = load_raw_to_staging(ingestion_id, data)
        batch_id = raw_dump['batch_id']
        deduped = transform_deduplicate(ingestion_id, batch_id)
        normalized = transform_normalize(deduped)
        normalized_dump = load_normalized_to_staging(ingestion_id, normalized)

        if 'next' in response['result']['_links']:
            next_url = response['result']['_links']['next']
            offset = int(parse_qs(urlparse(next_url).query)['offset'][0])
        else:
            break


@task(retries=3, retry_delay_seconds=5)
def extract_hdb_listings(offset: int | None = None) -> dict:    
    response = get_hdb_listings(offset)
    return response


@task(retries=3, retry_delay_seconds=5)
def load_raw_to_staging(ingestion_id: uuid.UUID, data: dict):
    batch_id = uuid.uuid4()
    batch = []
    for i in data:
        row = HDBListingsRaw(
                ingestion_id=ingestion_id,
                batch_id=batch_id,
                _id=i['_id'],
                month=i['month'],
                town=i['town'],
                flat_type=i['flat_type'],
                block=i['block'],
                street_name=i['street_name'],
                storey_range=i['storey_range'],
                floor_area_sqm=Decimal(i['floor_area_sqm']),
                flat_model=i['flat_model'],
                lease_commence_date=i['lease_commence_date'],
                remaining_lease=i['remaining_lease'],
                resale_price=Decimal(i['resale_price'])
                )
        batch.append(row)
    
    # Bulk insert is more efficient
    HDBListingsRaw.bulk_create(batch)
    return {'success': True, 'ingestion_id': ingestion_id, 'batch_id': batch_id, 'row_count': len(batch)}


@task
def transform_deduplicate(ingestion_id: uuid.UUID, batch_id: uuid.UUID) -> list[dict]:
    # Performed in staging side against raw data
    sql_name = 'hdb_listings_deduplicate.sql'
    with open(QUERIES_PATH / sql_name, 'r') as f:
        query = f.read()
    data = conn_staging.execute(query, [ingestion_id, batch_id], row_factory=dict_row).fetchall()
    return data


@task(retries=3, retry_delay_seconds=5)
def transform_normalize(data: list[dict]):
    output = []
    towns = Towns.get_id_dict()
    flat_types = FlatTypes.get_id_dict()
    streets = Streets.get_id_dict()
    flat_models = FlatModels.get_id_dict()

    for i in data:
        storey_from, storey_to = re.findall(r'\d+', i['storey_range'])
        storey_from = int(storey_from)
        storey_to = int(storey_to)
        remaining = i['remaining_lease']
        remaining_year = re.findall(r'(\d+) years', remaining)
        remaining_month = re.findall(r'(\d+) months', remaining)
        remaining_lease_year = remaining_year[0] if remaining_year else 0
        remaining_lease_month = remaining_month[0] if remaining_month else 0

        # Map the IDs
        town_name = i['town']
        if town_name in towns:
            town_id = towns[town_name]
        else:
            # New entry
            town_id = Towns(name=town_name).save()
            towns[town_name] = town_id
        
        flat_type_name = i['flat_type']
        if flat_type_name in flat_types:
            flat_type_id = flat_types[flat_type_name]
        else:
            flat_type_id = FlatTypes(name=flat_type_name).save()
            flat_types[flat_type_name] = flat_type_id

        street_name = i['street_name']
        if street_name in streets:
            street_id = streets[street_name]
        else:
            street_id = Streets(name=street_name).save()
            streets[street_name] = street_id

        flat_model_name = i['flat_model']
        if flat_model_name in flat_models:
            flat_model_id = flat_models[flat_model_name]
        else:
            flat_model_id = FlatModels(name=flat_model_name).save()
            flat_models[flat_model_name] = flat_model_id

        i['storey_range_from'] = storey_from
        i['storey_range_to'] = storey_to
        i['remaining_lease_year'] = remaining_lease_year
        i['remaining_lease_month'] = remaining_lease_month
        i['town_id'] = town_id
        i['flat_type_id'] = flat_type_id
        i['street_id'] = street_id
        i['flat_model_id'] = flat_model_id

        output.append(i)
    
    return output


@task(retries=3, retry_delay_seconds=5)
def load_normalized_to_staging(ingestion_id: uuid.UUID, data: list[dict]):
    batch = []
    for i in data:
        row = HDBListingsNormalized(
                ingestion_id=ingestion_id,
                _id=i['_id'],
                month=i['month'],
                town_id=i['town_id'],
                flat_type_id=i['flat_type_id'],
                block=i['block'],
                street_id=i['street_id'],
                storey_range_from=i['storey_range_from'],
                storey_range_to=i['storey_range_to'],
                floor_area_sqm=Decimal(i['floor_area_sqm']),
                flat_model_id=i['flat_model_id'],
                lease_commence_date=i['lease_commence_date'],
                remaining_lease_year=i['remaining_lease_year'],
                remaining_lease_month=i['remaining_lease_month'],
                resale_price=Decimal(i['resale_price']),
                full_address=i['full_addr']
                )
        batch.append(row)
    
    # Bulk insert is more efficient
    HDBListingsNormalized.upsert_many(batch)

    return {'success': True, 'ingestion_id': ingestion_id, 'row_count': len(data)}



@flow(name='hdb_dataset')
def hdb_dataset_flow():
    data = start()
    print('finish')

